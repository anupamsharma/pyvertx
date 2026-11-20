"""A single nonblocking TCP connection with read and write buffers.

Write interest is registered only while bytes are waiting to be sent, so an
idle connection blocks in the selector instead of spinning. If the peer stops
reading, the write buffer grows to a high-water mark and reads are paused.
"""

from __future__ import annotations

import logging
import selectors
import socket
from collections.abc import Callable

from pyvertx.core.event_loop import EventLoop

logger = logging.getLogger(__name__)

Buffer = bytes | bytearray | memoryview
DataCallback = Callable[[memoryview], None]
CloseCallback = Callable[[], None]


class Connection:
    """Move bytes for one accepted socket.

    ``on_data`` receives a memoryview into a buffer that is reused on the next
    read. The callback must copy anything it wants to keep. ``on_data`` and
    ``on_close`` run on the event-loop thread.
    """

    __slots__ = (
        "_loop",
        "_sock",
        "_on_data",
        "_on_close",
        "_read_size",
        "_max_send",
        "_high_water",
        "_low_water",
        "_read_buf",
        "_read_view",
        "_out",
        "_out_pos",
        "_events",
        "_read_enabled",
        "_write_enabled",
        "_closed",
        "_closing",
        "_paused",
    )

    def __init__(
        self,
        loop: EventLoop,
        sock: socket.socket,
        on_data: DataCallback,
        on_close: CloseCallback,
        *,
        read_size: int = 65536,
        max_send: int | None = None,
        high_water: int = 1_048_576,
        low_water: int = 262_144,
    ) -> None:
        if read_size <= 0:
            raise ValueError("read_size must be positive")
        if max_send is not None and max_send <= 0:
            raise ValueError("max_send must be positive")
        if low_water >= high_water:
            raise ValueError("low_water must be lower than high_water")
        sock.setblocking(False)
        self._loop = loop
        self._sock = sock
        self._on_data = on_data
        self._on_close = on_close
        self._read_size = read_size
        self._max_send = max_send
        self._high_water = high_water
        self._low_water = low_water
        self._read_buf = bytearray(read_size)
        self._read_view = memoryview(self._read_buf)
        self._out = bytearray()
        self._out_pos = 0
        self._events = selectors.EVENT_READ
        self._read_enabled = True
        self._write_enabled = False
        self._closed = False
        self._closing = False
        self._paused = False
        loop.register(sock, self._events, self._on_ready)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def closing(self) -> bool:
        return self._closing or self._closed

    @property
    def pending(self) -> int:
        """Bytes accepted by ``write`` that have not reached the kernel yet."""
        return len(self._out) - self._out_pos

    @property
    def read_paused(self) -> bool:
        return self._paused

    def write(self, data: Buffer) -> None:
        """Queue ``data`` and send as much as the socket will take.

        A short write stays in the connection buffer. The socket is then
        watched for writability; the loop does not poll.
        """
        if self._closed or not data:
            return
        if self._pending():
            self._out.extend(data)
            self._reclaim_out(force=False)
            self._write_enabled = True
            self._apply_backpressure()
            self._update_interest()
            return
        view = memoryview(data)
        offset = 0
        try:
            while offset < len(view):
                try:
                    sent = self._send_memory(view[offset:])
                except BlockingIOError:
                    break
                if sent <= 0:
                    break
                offset += sent
        except OSError:
            self.abort()
            return
        if offset < len(view):
            # Copy the tail before returning. Callers may reuse `data`
            # (the read buffer is a memoryview that the next recv overwrites).
            self._out.extend(view[offset:])
            self._write_enabled = True
            self._apply_backpressure()
            self._update_interest()

    def close(self) -> None:
        """Finish pending writes, then close. Further reads are ignored."""
        if self._closed or self._closing:
            return
        self._closing = True
        self._read_enabled = False
        if self._pending():
            self._write_enabled = True
            self._update_interest()
            return
        self.abort()

    def abort(self) -> None:
        """Drop the socket immediately and invoke ``on_close`` once."""
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._read_enabled = False
        self._write_enabled = False
        self._paused = False
        self._out.clear()
        self._out_pos = 0
        # Drop bytes already queued in the kernel. Closing with unread data
        # can abort the connection (RST) and discard a response we just wrote.
        self._discard_unread()
        try:
            self._loop.unregister(self._sock)
        except RuntimeError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        callback = self._on_close
        self._on_close = _noop_close
        try:
            callback()
        except Exception:
            logger.exception("connection close callback failed")

    def _on_ready(self, events: int) -> None:
        if self._closed:
            return
        if events & selectors.EVENT_WRITE:
            self._flush()
            if self._closed:
                return
        if (
            events & selectors.EVENT_READ
            and self._read_enabled
            and not self._closing
            and not self._closed
        ):
            self._read()

    def _read(self) -> None:
        while not self._closed and not self._closing and self._read_enabled:
            try:
                count = self._sock.recv_into(self._read_view)
            except BlockingIOError:
                return
            except InterruptedError:
                continue
            except OSError:
                self.abort()
                return
            if count == 0:
                self._peer_closed()
                return
            try:
                self._on_data(self._read_view[:count])
            except Exception:
                logger.exception("connection data callback failed")
                self.abort()
                return

    def _discard_unread(self) -> None:
        try:
            while True:
                chunk = self._sock.recv(65536)
                if not chunk:
                    return
        except (BlockingIOError, OSError):
            return

    def _peer_closed(self) -> None:
        # The peer sent FIN. If a response is still buffered, flush it before
        # dropping the socket so a client that half-closes can still read.
        if self._pending():
            self._closing = True
            self._read_enabled = False
            self._flush()
            return
        self.abort()

    def _flush(self) -> None:
        if not self._pending():
            self._write_enabled = False
            if self._closing:
                self.abort()
                return
            self._apply_backpressure()
            self._update_interest()
            return
        try:
            while self._pending():
                try:
                    sent = self._send_out()
                except BlockingIOError:
                    break
                if sent <= 0:
                    break
                self._out_pos += sent
            self._reclaim_out(force=False)
        except OSError:
            self.abort()
            return
        if self._closed:
            return
        if self._pending():
            self._write_enabled = True
            self._apply_backpressure()
            self._update_interest()
            return
        self._write_enabled = False
        self._apply_backpressure()
        if self._closing:
            self.abort()
            return
        self._update_interest()

    def _send_out(self) -> int:
        start = self._out_pos
        end = len(self._out)
        limit = self._max_send
        if limit is not None and end - start > limit:
            end = start + limit
        view = memoryview(self._out)
        try:
            return self._sock.send(view[start:end])
        finally:
            view.release()

    def _send_memory(self, view: memoryview) -> int:
        limit = self._max_send
        if limit is not None and len(view) > limit:
            view = view[:limit]
        return self._sock.send(view)

    def _pending(self) -> int:
        return len(self._out) - self._out_pos

    def _reclaim_out(self, force: bool) -> None:
        if self._out_pos == 0:
            return
        if self._out_pos == len(self._out):
            self._out.clear()
            self._out_pos = 0
            return
        if force or self._out_pos >= 65536:
            del self._out[: self._out_pos]
            self._out_pos = 0

    def _apply_backpressure(self) -> None:
        pending = self._pending()
        if not self._paused and pending >= self._high_water:
            self._paused = True
            self._read_enabled = False
        elif self._paused and pending <= self._low_water and not self._closing:
            self._paused = False
            self._read_enabled = True

    def _update_interest(self) -> None:
        if self._closed:
            return
        events = 0
        if self._read_enabled:
            events |= selectors.EVENT_READ
        if self._write_enabled:
            events |= selectors.EVENT_WRITE
        if events == 0:
            events = selectors.EVENT_READ
        if events == self._events:
            return
        self._events = events
        try:
            self._loop.modify(self._sock, events, self._on_ready)
        except (KeyError, ValueError, RuntimeError):
            self.abort()


def _noop_close() -> None:
    return None
