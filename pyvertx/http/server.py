"""Application-facing HTTP server."""

from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Callable

from pyvertx.core.event_loop import EventLoop
from pyvertx.http.protocol import HTTPProtocol
from pyvertx.http.request import Request
from pyvertx.http.response import Response, coerce_response, plain
from pyvertx.http.router import Handler, Router
from pyvertx.net.tcp_server import TCPServer

logger = logging.getLogger(__name__)

# Best-effort reply when the connection limit is already reached. The socket
# is nonblocking, so a short write is acceptable: the peer still sees a close.
_BUSY = (
    b"HTTP/1.1 503 Service Unavailable\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"Content-Length: 19\r\n"
    b"Connection: close\r\n"
    b"Server: pyvertx\r\n"
    b"\r\n"
    b"Service Unavailable"
)


class HTTPServer:
    """HTTP/1.1 server.

    Register routes, then call :meth:`listen`. Handlers run on the event-loop
    thread and must not block. They may return ``str``, ``bytes``, ``dict``,
    or :class:`Response`. A dictionary is serialized as JSON.

    ``listen`` blocks the caller until the server stops. Pass ``blocking=False``
    to run the loop on a background thread (used by the tests). ``stop`` is
    safe to call from another thread.
    """

    def __init__(
        self,
        *,
        max_header_size: int = 8 * 1024,
        max_body_size: int = 1_048_576,
        max_connections: int = 10_000,
        backlog: int = 128,
        read_size: int = 65_536,
        write_chunk_size: int | None = None,
        high_water: int = 1_048_576,
        low_water: int = 262_144,
    ) -> None:
        _require_positive("max_header_size", max_header_size)
        _require_non_negative("max_body_size", max_body_size)
        _require_positive("max_connections", max_connections)
        _require_positive("backlog", backlog)
        _require_positive("read_size", read_size)
        _require_positive("high_water", high_water)
        _require_positive("low_water", low_water)
        if low_water >= high_water:
            raise ValueError("low_water must be lower than high_water")
        if write_chunk_size is not None:
            _require_positive("write_chunk_size", write_chunk_size)
        self._max_header_size = max_header_size
        self._max_body_size = max_body_size
        self._max_connections = max_connections
        self._backlog = backlog
        self._read_size = read_size
        self._write_chunk_size = write_chunk_size
        self._high_water = high_water
        self._low_water = low_water
        self._router = Router()
        self._loop: EventLoop | None = None
        self._tcp: TCPServer | None = None
        self._thread: threading.Thread | None = None
        self._connections: set[HTTPProtocol] = set()
        self._connection_count = 0
        self._closing = False
        self._stopped = threading.Event()
        self._stopped.set()
        self._host = ""
        self._port = 0

    def route(self, method: str, path: str) -> Callable[[Handler], Handler]:
        """Register ``method`` and ``path``. The decorated function is unchanged."""

        def decorator(handler: Handler) -> Handler:
            self._router.add(method, path, handler)
            return handler

        return decorator

    def get(self, path: str) -> Callable[[Handler], Handler]:
        return self.route("GET", path)

    def post(self, path: str) -> Callable[[Handler], Handler]:
        return self.route("POST", path)

    def listen(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        *,
        blocking: bool = True,
    ) -> None:
        """Bind and serve.

        The first ``getaddrinfo`` result is used, so pass an explicit address
        such as ``127.0.0.1`` when IPv4 is required. ``port`` may be ``0``.
        """
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        _require_port(port)
        if self.running:
            raise RuntimeError("server is already listening")
        self._closing = False
        self._stopped.clear()
        self._loop = EventLoop()
        try:
            self._tcp = TCPServer(
                self._loop,
                host,
                port,
                self._on_accept,
                backlog=self._backlog,
            )
        except Exception:
            self._loop.close()
            self._loop = None
            self._stopped.set()
            raise
        self._host = self._tcp.host
        self._port = self._tcp.port
        logger.info("listening on %s:%s", self.host, self.port)
        if not blocking:
            self._thread = threading.Thread(
                target=self._run,
                name="pyvertx",
                daemon=False,
            )
            self._thread.start()
            if not self._loop.wait_started(5):
                self.stop()
                raise RuntimeError("event loop failed to start")
            return
        self._run()

    def stop(self, timeout: float = 5.0) -> None:
        """Stop accepting, close connections, and wait for the loop to exit."""
        loop = self._loop
        if loop is not None and not loop.closed:
            loop.stop()
        if loop is not None and threading.get_ident() == loop.thread_id:
            return
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                raise TimeoutError("pyvertx server did not stop")
            return
        if not self._stopped.wait(timeout):
            raise TimeoutError("pyvertx server did not stop")

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def running(self) -> bool:
        loop = self._loop
        return loop is not None and loop.running

    @property
    def connection_count(self) -> int:
        return self._connection_count

    def handle_request(self, request: Request) -> tuple[Response, bool]:
        """Route one request. Returns the response and whether to keep the socket."""
        handler = self._router.handler(request.method, request.path)
        if handler is None:
            allowed = self._router.allowed(request.path)
            if allowed:
                response = Response(
                    b"Method Not Allowed",
                    status=405,
                    content_type="text/plain; charset=utf-8",
                    headers={"Allow": ", ".join(allowed)},
                )
                return response, request.keep_alive
            return plain(404, b"Not Found"), request.keep_alive
        try:
            return coerce_response(handler(request)), request.keep_alive
        except Exception:
            logger.exception("handler error on %s %s", request.method, request.path)
            return plain(500, b"Internal Server Error"), False

    def connection_closed(self, protocol: HTTPProtocol) -> None:
        if protocol in self._connections:
            self._connections.discard(protocol)
            self._connection_count -= 1

    def _on_accept(self, sock: socket.socket, addr: tuple) -> None:
        loop = self._loop
        if loop is None or self._closing or self._connection_count >= self._max_connections:
            _reject_busy(sock)
            if self._connection_count >= self._max_connections:
                logger.warning("rejecting connection from %s: at capacity", addr)
            return
        protocol = HTTPProtocol(
            self,
            loop,
            sock,
            addr,
            max_header_size=self._max_header_size,
            max_body_size=self._max_body_size,
            read_size=self._read_size,
            write_chunk_size=self._write_chunk_size,
            high_water=self._high_water,
            low_water=self._low_water,
        )
        self._connections.add(protocol)
        self._connection_count += 1

    def _run(self) -> None:
        loop = self._loop
        try:
            if loop is not None:
                loop.run_forever()
        finally:
            try:
                self._close_remaining()
                if loop is not None and not loop.closed:
                    loop.close()
            finally:
                logger.info("stopped")
                self._stopped.set()

    def _close_remaining(self) -> None:
        self._closing = True
        tcp = self._tcp
        self._tcp = None
        if tcp is not None:
            tcp.close()
        for protocol in list(self._connections):
            try:
                protocol.abort()
            except Exception:
                logger.exception("error closing connection")
        self._connections.clear()
        self._connection_count = 0


def _reject_busy(sock: socket.socket) -> None:
    try:
        sock.send(_BUSY)
    except OSError:
        pass
    try:
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _require_positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _require_non_negative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be an integer >= 0")


def _require_port(port: int) -> None:
    if isinstance(port, bool) or not isinstance(port, int) or port < 0 or port > 65535:
        raise ValueError("port must be an integer from 0 to 65535")
