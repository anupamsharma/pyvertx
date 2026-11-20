"""Exact-match HTTP routing."""

from __future__ import annotations

from collections.abc import Callable

from pyvertx.http.request import Request

Handler = Callable[[Request], object]


class Router:
    """Map ``(method, path)`` pairs to handlers.

    Paths are matched exactly, without the query string. Registering the same
    pair again replaces the handler. The router is read from the event-loop
    thread and is not locked; register routes before listening.
    """

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], Handler] = {}
        self._methods: dict[str, set[str]] = {}

    def add(self, method: str, path: str, handler: Handler) -> None:
        verb = method.upper()
        if not verb or any(char.isspace() for char in verb):
            raise ValueError("method must be a non-empty token")
        if not path.startswith("/"):
            raise ValueError("path must start with '/'")
        self._routes[(verb, path)] = handler
        self._methods.setdefault(path, set()).add(verb)

    def handler(self, method: str, path: str) -> Handler | None:
        return self._routes.get((method, path))

    def allowed(self, path: str) -> tuple[str, ...]:
        """Methods registered for ``path``, in alphabetical order."""
        methods = self._methods.get(path)
        if not methods:
            return ()
        return tuple(sorted(methods))
