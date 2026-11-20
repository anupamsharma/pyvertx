"""HTTP/1.1 connection state machine on top of a TCP connection."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyvertx.core.event_loop import EventLoop
from pyvertx.http.parser import HTTPError, HTTPParser
from pyvertx.http.response import Response, plain
from pyvertx.net.connection import Connection

if TYPE_CHECKING:
    import socket

    from pyvertx.http.server import HTTPServer

logger = logging.getLogger(__name__)


class HTTPProtocol:
    """Read requests from one connection and write the responses in order."""

    __slots__ = ("_server", "_conn", "_parser", "_remote", "_closed")

    def __init__(
        self,
        server: HTTPServer,
        loop: EventLoop,
        sock: socket.socket,
        addr: tuple,
        *,
        max_header_size: int,
        max_body_size: int,
        read_size: int,
        write_chunk_size: int | None,
        high_water: int,
        low_water: int,
    ) -> None:
        self._server = server
        self._parser = HTTPParser(max_header_size, max_body_size)
        self._remote = _format_addr(addr)
        self._closed = False
        self._conn = Connection(
            loop,
            sock,
            self._on_data,
            self._on_close,
            read_size=read_size,
            max_send=write_chunk_size,
            high_water=high_water,
            low_water=low_water,
        )

    def abort(self) -> None:
        self._conn.abort()

    def _on_data(self, data: memoryview) -> None:
        try:
            requests = self._parser.feed(data)
        except HTTPError as exc:
            self._reject(exc.status, exc.body)
            return
        except Exception:
            logger.exception("HTTP parser failed from %s", self._remote)
            self._reject(400, b"Bad Request")
            return
        for request in requests:
            if self._conn.closed:
                return
            request.remote_addr = self._remote
            response, keep_alive = self._server.handle_request(request)
            self._write_response(response, request.version, keep_alive)
            if not keep_alive or self._conn.closed:
                return
        if self._conn.closing:
            return
        version = self._parser.consume_continue()
        if version is not None:
            token = b"HTTP/1.0" if version == "HTTP/1.0" else b"HTTP/1.1"
            self._conn.write(token + b" 100 Continue\r\n\r\n")

    def _write_response(self, response: Response, version: str, keep_alive: bool) -> None:
        self._conn.write(response.header_bytes(version, keep_alive))
        body = response.transmission_body()
        if body:
            self._conn.write(body)
        if not keep_alive:
            self._conn.close()

    def _reject(self, status: int, body: bytes) -> None:
        self._write_response(plain(status, body), "HTTP/1.1", keep_alive=False)

    def _on_close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._server.connection_closed(self)


def _format_addr(addr: tuple) -> str:
    if len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"
    return str(addr)
