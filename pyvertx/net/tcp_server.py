"""Listening socket that accepts connections onto an event loop."""

from __future__ import annotations

import errno
import logging
import selectors
import socket
from collections.abc import Callable

from pyvertx.core.event_loop import EventLoop, TimerHandle

logger = logging.getLogger(__name__)

_RESOURCE_ERRORS = {errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM}
AcceptCallback = Callable[[socket.socket, tuple], None]


class TCPServer:
    """Bind a nonblocking listening socket and accept on the event loop.

    ``on_connection`` is invoked on the loop thread with an already
    nonblocking socket. The listener stays independent of any protocol.
    """

    def __init__(
        self,
        loop: EventLoop,
        host: str,
        port: int,
        on_connection: AcceptCallback,
        *,
        backlog: int = 128,
    ) -> None:
        if backlog <= 0:
            raise ValueError("backlog must be a positive integer")
        self._loop = loop
        self._on_connection = on_connection
        self._closed = False
        self._pause_handle: TimerHandle | None = None
        self._sock = _bind_listener(host, port, backlog)
        bound = self._sock.getsockname()
        self.host = bound[0]
        self.port = bound[1]
        loop.register(self._sock, selectors.EVENT_READ, self._on_readable)

    def close(self) -> None:
        """Unregister and close the listening socket. Idempotent."""
        if self._closed:
            return
        self._closed = True
        handle = self._pause_handle
        self._pause_handle = None
        if handle is not None:
            handle.cancel()
        try:
            self._loop.unregister(self._sock)
        except RuntimeError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass

    def _on_readable(self, _events: int) -> None:
        if self._closed:
            return
        while True:
            try:
                conn, addr = self._sock.accept()
            except BlockingIOError:
                return
            except InterruptedError:
                continue
            except OSError as exc:
                if exc.errno == errno.ECONNABORTED:
                    continue
                if exc.errno in _RESOURCE_ERRORS:
                    # The listening socket stays readable if we return without
                    # accepting, which would spin the loop. Pause and retry.
                    logger.error("accept failed (%s); pausing new connections", exc.errno)
                    self._pause_accepts()
                    return
                logger.error("accept failed: %s", exc)
                return
            _prepare_accepted(conn)
            try:
                self._on_connection(conn, addr)
            except Exception:
                logger.exception("connection setup failed")
                try:
                    conn.close()
                except OSError:
                    pass

    def _pause_accepts(self) -> None:
        if self._closed:
            return
        try:
            self._loop.unregister(self._sock)
        except RuntimeError:
            return
        self._pause_handle = self._loop.call_later(0.2, self._resume_accepts)

    def _resume_accepts(self) -> None:
        self._pause_handle = None
        if self._closed:
            return
        try:
            self._loop.register(self._sock, selectors.EVENT_READ, self._on_readable)
        except RuntimeError:
            return


def _bind_listener(host: str, port: int, backlog: int) -> socket.socket:
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise OSError(f"cannot resolve {host!r}") from exc
    if not infos:
        raise OSError(f"cannot resolve {host!r}")
    family, socktype, proto, _canon, sockaddr = infos[0]
    sock = socket.socket(family, socktype, proto)
    try:
        if family == socket.AF_INET6:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(sockaddr)
        sock.listen(backlog)
        sock.setblocking(False)
    except Exception:
        sock.close()
        raise
    return sock


def _prepare_accepted(sock: socket.socket) -> None:
    sock.setblocking(False)
    try:
        # Small responses otherwise sit in Nagle's algorithm until an ack
        # arrives. That shows up as multi-millisecond latency, not throughput.
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass
