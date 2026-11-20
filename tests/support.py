"""Helpers for HTTP integration tests."""

from __future__ import annotations

import socket
from contextlib import contextmanager

from pyvertx.http.server import HTTPServer


def make_app(**kwargs) -> HTTPServer:
    """Server with the routes the integration tests share."""
    app = HTTPServer(**kwargs)

    @app.get("/")
    def home(request):
        return "Hello World"

    @app.get("/hello")
    def hello(request):
        return {"message": "Hello from PyVertx"}

    @app.get("/unicode")
    def unicode_text(request):
        return "héllo"

    @app.post("/echo")
    def echo(request):
        return request.body

    @app.get("/query")
    def query(request):
        names = request.query.get("name", [])
        return {"name": names[0] if names else ""}

    @app.post("/info")
    def info(request):
        return {
            "host": request.header("Host"),
            "remote": request.remote_addr,
            "n": len(request.body),
        }

    @app.get("/boom")
    def boom(request):
        raise RuntimeError("boom")

    @app.post("/created")
    def created(request):
        from pyvertx import Response

        return Response({"ok": True}, status=201)

    return app


@contextmanager
def serve(app: HTTPServer, host: str = "127.0.0.1", port: int = 0):
    app.listen(host, port, blocking=False)
    try:
        yield app
    finally:
        app.stop()


def request_bytes(
    method: str,
    path: str,
    *,
    body: bytes = b"",
    version: str = "HTTP/1.1",
    headers: dict[str, str] | None = None,
) -> bytes:
    fields = {"Host": "localhost"}
    if body:
        fields["Content-Length"] = str(len(body))
    if headers:
        fields.update(headers)
    if version == "HTTP/1.1" and "Connection" not in fields:
        fields["Connection"] = "keep-alive"
    lines = [f"{method} {path} {version}"]
    lines.extend(f"{name}: {value}" for name, value in fields.items())
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
    return raw + body


class Reader:
    """Read HTTP/1.x responses from a blocking socket, one at a time."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.buf = bytearray()

    def read_response(self) -> tuple[int, dict[str, str], bytes]:
        while True:
            status, headers, body = self._read_message()
            if status >= 200:
                return status, headers, body

    def read_message(self) -> tuple[int, dict[str, str], bytes]:
        """Read one message, including an interim 1xx response."""
        return self._read_message()

    def _read_message(self) -> tuple[int, dict[str, str], bytes]:
        while b"\r\n\r\n" not in self.buf:
            self._pull()
        end = self.buf.find(b"\r\n\r\n")
        head = bytes(self.buf[:end])
        del self.buf[: end + 4]
        lines = head.split(b"\r\n")
        status = int(lines[0].split(b" ")[1])
        version = lines[0].split(b" ")[0].decode("ascii")
        headers: dict[str, str] = {"_version": version}
        for line in lines[1:]:
            name, value = line.split(b":", 1)
            headers[name.decode("latin-1").lower()] = value.strip().decode("latin-1")
        if status < 200:
            return status, headers, b""
        length = int(headers["content-length"])
        while len(self.buf) < length:
            self._pull()
        body = bytes(self.buf[:length])
        del self.buf[:length]
        return status, headers, body

    def _pull(self) -> None:
        chunk = self.sock.recv(8192)
        if not chunk:
            raise ConnectionError("connection closed before the response finished")
        self.buf.extend(chunk)


def open_socket(app: HTTPServer, timeout: float = 2.0) -> socket.socket:
    sock = socket.create_connection((app.host, app.port), timeout=timeout)
    sock.settimeout(timeout)
    return sock


def exchange(
    app: HTTPServer, raw: bytes, timeout: float = 2.0
) -> tuple[int, dict[str, str], bytes]:
    sock = open_socket(app, timeout)
    try:
        sock.sendall(raw)
        return Reader(sock).read_response()
    finally:
        sock.close()
