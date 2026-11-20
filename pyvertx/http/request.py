"""An HTTP request delivered to an application handler."""

from __future__ import annotations

import json
from urllib.parse import parse_qs


class Request:
    """A fully buffered HTTP/1.1 request.

    ``headers`` uses lowercase names. ``path`` does not include the query
    string. ``body`` is exactly the ``Content-Length`` bytes, or empty when
    the request has no body.
    """

    __slots__ = (
        "method",
        "path",
        "query_string",
        "version",
        "headers",
        "body",
        "keep_alive",
        "remote_addr",
        "_query",
    )

    def __init__(
        self,
        method: str,
        path: str,
        query_string: str,
        version: str,
        headers: dict[str, str],
        body: bytes,
        keep_alive: bool,
    ) -> None:
        self.method = method
        self.path = path
        self.query_string = query_string
        self.version = version
        self.headers = headers
        self.body = body
        self.keep_alive = keep_alive
        self.remote_addr = ""
        self._query: dict[str, list[str]] | None = None

    def header(self, name: str, default: str | None = None) -> str | None:
        """Return a header value. Lookup is case-insensitive."""
        return self.headers.get(name.lower(), default)

    @property
    def query(self) -> dict[str, list[str]]:
        """Query parameters parsed from ``query_string``.

        Values are lists because a key may appear more than once.
        """
        cached = self._query
        if cached is None:
            cached = parse_qs(self.query_string, keep_blank_values=True)
            self._query = cached
        return cached

    @property
    def text(self) -> str:
        """Decode ``body`` as UTF-8."""
        return self.body.decode("utf-8")

    def json(self) -> object:
        """Parse ``body`` as JSON."""
        if not self.body:
            raise ValueError("request body is empty")
        return json.loads(self.body)

    def __repr__(self) -> str:
        return f"Request({self.method} {self.path} {self.version})"
