"""HTTP response objects and the small set of handler return types."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping
from email.utils import formatdate

_REASONS: dict[int, bytes] = {
    200: b"OK",
    201: b"Created",
    204: b"No Content",
    400: b"Bad Request",
    404: b"Not Found",
    405: b"Method Not Allowed",
    413: b"Payload Too Large",
    417: b"Expectation Failed",
    431: b"Request Header Fields Too Large",
    500: b"Internal Server Error",
    501: b"Not Implemented",
    503: b"Service Unavailable",
}

_TOKEN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_MANAGED = {
    "content-length",
    "connection",
    "transfer-encoding",
    "date",
    "server",
    "content-type",
}
_NO_BODY = {204, 304}
_SERVER = b"pyvertx"

# Rebuilt at most once per second. Every response on this process shares it.
_DATE = b""
_DATE_STAMP = -1


def http_date() -> bytes:
    """Return the current IMF-fix HTTP date, cached for one second."""
    global _DATE, _DATE_STAMP
    stamp = int(time.time())
    if stamp != _DATE_STAMP:
        _DATE_STAMP = stamp
        _DATE = formatdate(stamp, usegmt=True).encode("ascii")
    return _DATE


class Response:
    """A buffered HTTP response.

    The constructor accepts the same bodies a handler may return: ``str``,
    ``bytes``, ``bytearray``, or ``dict``. Dictionaries are encoded as JSON.
    """

    __slots__ = ("status", "body", "content_type", "_extra")

    def __init__(
        self,
        body: str | bytes | bytearray | memoryview | dict = b"",
        *,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        content_type: str | None = None,
    ) -> None:
        if isinstance(status, bool) or not isinstance(status, int):
            raise TypeError("status must be an integer")
        if status < 100 or status > 599:
            raise ValueError("status must be between 100 and 599")
        self.status = status
        self.body, default_type = _encode_body(body)
        self.content_type = content_type or default_type
        _check_header_value("content-type", self.content_type)
        extra: list[tuple[str, str]] = []
        if headers:
            for name, value in headers.items():
                field = str(name).strip()
                if field.lower() in _MANAGED:
                    continue
                if not _TOKEN.fullmatch(field):
                    raise ValueError(f"invalid header name: {name!r}")
                text = str(value)
                _check_header_value(field, text)
                extra.append((field, text))
        self._extra = tuple(extra)

    def header_bytes(self, version: str, keep_alive: bool) -> bytes:
        """Serialize the status line and headers, including the final CRLF."""
        reason = _REASONS.get(self.status, b"Unknown")
        http_version = b"HTTP/1.0" if version == "HTTP/1.0" else b"HTTP/1.1"
        connection = b"keep-alive" if keep_alive else b"close"
        body = self.transmission_body()
        parts = [
            http_version,
            b" ",
            str(self.status).encode("ascii"),
            b" ",
            reason,
            b"\r\nContent-Type: ",
            self.content_type.encode("ascii"),
            b"\r\nContent-Length: ",
            str(len(body)).encode("ascii"),
            b"\r\nConnection: ",
            connection,
            b"\r\nServer: ",
            _SERVER,
            b"\r\nDate: ",
            http_date(),
            b"\r\n",
        ]
        for name, value in self._extra:
            parts.append(name.encode("ascii"))
            parts.append(b": ")
            parts.append(value.encode("latin-1"))
            parts.append(b"\r\n")
        parts.append(b"\r\n")
        return b"".join(parts)

    def transmission_body(self) -> bytes:
        """Body bytes that follow the header block."""
        if self.status in _NO_BODY:
            return b""
        return self.body


def plain(status: int, body: bytes) -> Response:
    """Build a ``text/plain`` response."""
    return Response(body, status=status, content_type="text/plain; charset=utf-8")


def coerce_response(result: object) -> Response:
    """Turn a handler return value into a :class:`Response`."""
    if isinstance(result, Response):
        return result
    if isinstance(result, (str, bytes, bytearray, memoryview, dict)):
        return Response(result)
    raise TypeError(
        "handler must return str, bytes, dict, or Response, "
        f"got {type(result).__name__}"
    )


def _encode_body(
    body: str | bytes | bytearray | memoryview | dict,
) -> tuple[bytes, str]:
    if isinstance(body, str):
        return body.encode("utf-8"), "text/plain; charset=utf-8"
    if isinstance(body, dict):
        encoded = json.dumps(
            body,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return encoded, "application/json"
    if isinstance(body, bytes):
        return body, "application/octet-stream"
    if isinstance(body, bytearray):
        return bytes(body), "application/octet-stream"
    if isinstance(body, memoryview):
        return body.tobytes(), "application/octet-stream"
    raise TypeError(f"unsupported response body type: {type(body).__name__}")


def _check_header_value(name: str, value: str) -> None:
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"invalid header value for {name}")
