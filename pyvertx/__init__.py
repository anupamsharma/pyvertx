"""PyVertx, a small event-driven networking framework.

Release 1 is a single-process HTTP/1.1 server. The event loop and TCP layer
do not import the HTTP package; the names below are loaded on first use so
``import pyvertx.core.event_loop`` stays protocol-agnostic.
"""

from __future__ import annotations

import logging
from typing import Any

__all__ = ["EventLoop", "HTTPServer", "Request", "Response", "__version__"]

__version__ = "0.1.0"

logging.getLogger(__name__).addHandler(logging.NullHandler())


def __getattr__(name: str) -> Any:
    if name == "EventLoop":
        from pyvertx.core.event_loop import EventLoop

        value = EventLoop
    elif name == "HTTPServer":
        from pyvertx.http.server import HTTPServer

        value = HTTPServer
    elif name == "Request":
        from pyvertx.http.request import Request

        value = Request
    elif name == "Response":
        from pyvertx.http.response import Response

        value = Response
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
