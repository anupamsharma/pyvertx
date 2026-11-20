"""Incremental HTTP/1.1 request parser.

The parser keeps only the incomplete tail of the byte stream. A completed
request owns its own body copy, so the read buffer can be reused.
"""

from __future__ import annotations

import re

from pyvertx.http.request import Request

_HEAD = 0
_BODY = 1

_TOKEN = re.compile(rb"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_VERSIONS = {b"HTTP/1.0": "HTTP/1.0", b"HTTP/1.1": "HTTP/1.1"}
_REJECT_DUPLICATES = {"content-length", "host", "transfer-encoding"}


class HTTPError(Exception):
    """A request that should be answered with ``status`` and then closed."""

    status = 400
    body = b"Bad Request"


class ParseError(HTTPError):
    """The request is not valid HTTP/1.0 or HTTP/1.1."""


class HeaderTooLarge(HTTPError):
    status = 431
    body = b"Request Header Fields Too Large"


class PayloadTooLarge(HTTPError):
    status = 413
    body = b"Payload Too Large"


class UnsupportedTransferEncoding(HTTPError):
    status = 501
    body = b"Not Implemented"


class ExpectationFailed(HTTPError):
    status = 417
    body = b"Expectation Failed"


class HTTPParser:
    """Parse one or more requests from a byte stream.

    ``max_header_size`` bounds the request line plus headers, including the
    terminating blank line. ``max_body_size`` bounds ``Content-Length``.
    Chunked bodies are rejected.
    """

    def __init__(self, max_header_size: int = 8192, max_body_size: int = 1_048_576) -> None:
        if max_header_size <= 0:
            raise ValueError("max_header_size must be positive")
        if max_body_size < 0:
            raise ValueError("max_body_size must be >= 0")
        self.max_header_size = max_header_size
        self.max_body_size = max_body_size
        self._buf = bytearray()
        self._pos = 0
        self._state = _HEAD
        self._pending: Request | None = None
        self._need = 0
        self._continue_pending = False
        self._continue_version = "HTTP/1.1"

    def feed(self, data: bytes | bytearray | memoryview) -> list[Request]:
        """Append ``data`` and return every request that is now complete.

        ``data`` is copied into the parser buffer. Callers may reuse the
        object they passed in, including a memoryview over a read buffer.
        """
        if data:
            self._buf.extend(data)
        completed: list[Request] = []
        while True:
            if self._state == _HEAD:
                outcome = self._read_head()
                if outcome == "wait":
                    break
                if outcome == "body":
                    self._state = _BODY
                    continue
                completed.append(self._take_request())
                continue
            if not self._read_body():
                break
            completed.append(self._take_request())
        self._compact()
        return completed

    def consume_continue(self) -> str | None:
        """Return the HTTP version if a ``100 Continue`` should be sent now."""
        if not self._continue_pending:
            return None
        self._continue_pending = False
        return self._continue_version

    def _take_request(self) -> Request:
        request = self._pending
        if request is None:
            raise ParseError("internal parser state")
        self._pending = None
        self._state = _HEAD
        self._need = 0
        return request

    def _read_head(self) -> str:
        buf = self._buf
        pos = self._pos
        end = buf.find(b"\r\n\r\n", pos)
        if end == -1:
            if len(buf) - pos > self.max_header_size:
                raise HeaderTooLarge()
            return "wait"
        header_end = end + 4
        if header_end - pos > self.max_header_size:
            raise HeaderTooLarge()
        head = bytes(buf[pos:end])
        self._pos = header_end
        request, length, expect_continue = _parse_head(head, self.max_body_size)
        self._pending = request
        if length == 0:
            request.body = b""
            return "request"
        available = len(buf) - self._pos
        if available >= length:
            request.body = bytes(buf[self._pos : self._pos + length])
            self._pos += length
            return "request"
        self._need = length
        if expect_continue:
            self._continue_pending = True
            self._continue_version = request.version
        return "body"

    def _read_body(self) -> bool:
        if len(self._buf) - self._pos < self._need:
            return False
        request = self._pending
        if request is None:
            raise ParseError("internal parser state")
        request.body = bytes(self._buf[self._pos : self._pos + self._need])
        self._pos += self._need
        self._need = 0
        return True

    def _compact(self) -> None:
        if self._pos == 0:
            return
        if self._pos == len(self._buf):
            self._buf.clear()
            self._pos = 0
            return
        if self._pos >= 4096:
            del self._buf[: self._pos]
            self._pos = 0


def _parse_head(head: bytes, max_body_size: int) -> tuple[Request, int, bool]:
    if not head:
        raise ParseError("empty request")
    lines = head.split(b"\r\n")
    method_b, target, version_b = _split_request_line(lines[0])
    version = _VERSIONS.get(version_b)
    if version is None:
        raise ParseError("unsupported HTTP version")
    if not _TOKEN.fullmatch(method_b):
        raise ParseError("invalid method")
    path, query = _split_target(target)
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, value = _split_header(line)
        if name in headers:
            if name in _REJECT_DUPLICATES:
                raise ParseError(f"duplicate {name} header")
            headers[name] = f"{headers[name]}, {value}"
        else:
            headers[name] = value
    if "transfer-encoding" in headers:
        if "content-length" in headers:
            raise ParseError("both transfer-encoding and content-length")
        raise UnsupportedTransferEncoding()
    length = 0
    if "content-length" in headers:
        length = _content_length(headers["content-length"], max_body_size)
    expect_continue = False
    expect = headers.get("expect")
    if expect is not None:
        if expect.lower() != "100-continue":
            raise ExpectationFailed()
        expect_continue = length > 0
    if version == "HTTP/1.1" and not headers.get("host"):
        raise ParseError("missing host")
    request = Request(
        method=method_b.decode("ascii"),
        path=path,
        query_string=query,
        version=version,
        headers=headers,
        body=b"",
        keep_alive=_keep_alive(version, headers),
    )
    return request, length, expect_continue


def _split_request_line(line: bytes) -> tuple[bytes, bytes, bytes]:
    parts = line.split(b" ")
    if len(parts) != 3 or any(part == b"" for part in parts):
        raise ParseError("malformed request line")
    return parts[0], parts[1], parts[2]


def _split_target(target: bytes) -> tuple[str, str]:
    if not target.startswith(b"/"):
        raise ParseError("only origin-form targets are supported")
    if any(byte < 32 or byte == 127 for byte in target):
        raise ParseError("invalid request target")
    query_at = target.find(b"?")
    if query_at == -1:
        path_b, query_b = target, b""
    else:
        path_b, query_b = target[:query_at], target[query_at + 1 :]
    return path_b.decode("latin-1"), query_b.decode("latin-1")


def _split_header(line: bytes) -> tuple[str, str]:
    if not line or line[:1] in (b" ", b"\t"):
        raise ParseError("malformed header")
    colon = line.find(b":")
    if colon <= 0:
        raise ParseError("malformed header")
    name_b = line[:colon]
    if not _TOKEN.fullmatch(name_b):
        raise ParseError("invalid header name")
    value_b = line[colon + 1 :].strip(b" \t")
    if any(byte < 32 or byte == 127 for byte in value_b):
        raise ParseError("invalid header value")
    return name_b.decode("ascii").lower(), value_b.decode("latin-1")


def _content_length(raw: str, max_body_size: int) -> int:
    if not raw.isdigit():
        raise ParseError("invalid content-length")
    # A huge digit string is a cheap way to waste CPU inside int().
    if len(raw) > 18:
        raise PayloadTooLarge()
    length = int(raw)
    if length > max_body_size:
        raise PayloadTooLarge()
    return length


def _keep_alive(version: str, headers: dict[str, str]) -> bool:
    tokens = {
        part.strip().lower()
        for part in headers.get("connection", "").split(",")
        if part.strip()
    }
    if version == "HTTP/1.0":
        return "keep-alive" in tokens
    return "close" not in tokens
