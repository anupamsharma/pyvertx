"""Selector-based event loop.

The loop is deliberately protocol-agnostic. HTTP, and any later protocol,
registers sockets and receives readiness callbacks. A self-pipe wakes the
loop so another thread can stop it or schedule a callback without polling.
"""

from __future__ import annotations

import heapq
import logging
import queue
import selectors
import socket
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

_READY_BATCH = 1024
Callback = Callable[..., Any]


class TimerHandle:
    """A cancellable timer created by :meth:`EventLoop.call_later`."""

    __slots__ = ("when", "seq", "callback", "args", "cancelled")

    def __init__(
        self,
        when: float,
        seq: int,
        callback: Callback | None,
        args: tuple[Any, ...],
    ) -> None:
        self.when = when
        self.seq = seq
        self.callback = callback
        self.args = args
        self.cancelled = False

    def cancel(self) -> None:
        """Prevent the callback from running. Safe to call more than once."""
        self.cancelled = True
        self.callback = None
        self.args = ()

    def __lt__(self, other: TimerHandle) -> bool:
        return (self.when, self.seq) < (other.when, other.seq)


class EventLoop:
    """Run readiness callbacks for nonblocking sockets.

    All ``register``, ``modify``, and ``unregister`` calls must happen on the
    loop thread (or before :meth:`run_forever` starts). :meth:`call_soon`,
    :meth:`call_later`, and :meth:`stop` are safe to call from another thread.
    """

    def __init__(self) -> None:
        self._selector = selectors.DefaultSelector()
        self._ready: deque[tuple[Callback, tuple[Any, ...]]] = deque()
        self._incoming: queue.SimpleQueue[tuple[Callback, tuple[Any, ...]]] = (
            queue.SimpleQueue()
        )
        self._timers: list[TimerHandle] = []
        self._timer_seq = 0
        self._stop_event = threading.Event()
        self._started = threading.Event()
        self._closed = False
        self._thread_id: int | None = None
        self._iterations = 0

        wake_read, wake_write = socket.socketpair()
        wake_read.setblocking(False)
        wake_write.setblocking(False)
        self._wake_r = wake_read
        self._wake_w = wake_write
        self._selector.register(wake_read, selectors.EVENT_READ, self._on_wakeup)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def running(self) -> bool:
        return self._thread_id is not None and not self._stop_event.is_set()

    @property
    def thread_id(self) -> int | None:
        return self._thread_id

    @property
    def iterations(self) -> int:
        """How many loop turns have started. Useful for idle-spin tests."""
        return self._iterations

    def wait_started(self, timeout: float | None = None) -> bool:
        """Block until :meth:`run_forever` has entered the loop thread."""
        return self._started.wait(timeout)

    def register(self, fileobj: socket.socket, events: int, callback: Callback) -> None:
        """Watch ``fileobj`` and call ``callback(events)`` when it is ready."""
        self._check_loop_thread()
        if events == 0:
            raise ValueError("events must include read, write, or both")
        self._selector.register(fileobj, events, callback)

    def modify(self, fileobj: socket.socket, events: int, callback: Callback) -> None:
        """Replace the event mask and callback for a registered socket."""
        self._check_loop_thread()
        if events == 0:
            raise ValueError("events must include read, write, or both")
        self._selector.modify(fileobj, events, callback)

    def unregister(self, fileobj: socket.socket) -> None:
        """Stop watching ``fileobj``. Missing registrations are ignored."""
        if self._closed:
            return
        self._check_loop_thread()
        try:
            self._selector.unregister(fileobj)
        except (KeyError, ValueError, OSError):
            return

    def call_soon(self, callback: Callback, *args: Any) -> None:
        """Schedule ``callback`` to run on the next loop turn."""
        if self._closed:
            return
        item = (callback, args)
        if threading.get_ident() == self._thread_id:
            self._ready.append(item)
            return
        self._incoming.put(item)
        self._wakeup()

    def call_later(self, delay: float, callback: Callback, *args: Any) -> TimerHandle:
        """Schedule ``callback`` after ``delay`` seconds and return its handle."""
        if delay < 0:
            raise ValueError("delay must be >= 0")
        when = time.monotonic() + delay
        if threading.get_ident() == self._thread_id:
            handle = self._new_timer(when, callback, args)
            heapq.heappush(self._timers, handle)
            return handle
        handle = TimerHandle(when, -1, callback, args)
        self.call_soon(self._schedule_timer, handle)
        return handle

    def run_forever(self) -> None:
        """Block until :meth:`stop` is called or the thread is interrupted."""
        if self._closed:
            raise RuntimeError("event loop is closed")
        if self._thread_id is not None:
            raise RuntimeError("event loop is already running")
        self._stop_event.clear()
        self._thread_id = threading.get_ident()
        self._started.set()
        try:
            while not self._stop_event.is_set():
                self._run_once(None)
        except KeyboardInterrupt:
            self._stop_event.set()
        finally:
            self._thread_id = None

    def run_once(self, timeout: float | None = None) -> None:
        """Process scheduled work and at most one selector wait.

        ``timeout`` caps how long to wait for I/O. ``None`` waits until the
        next timer or socket event, which may block.
        """
        if self._closed:
            raise RuntimeError("event loop is closed")
        own_thread = self._thread_id is None
        if own_thread:
            self._thread_id = threading.get_ident()
        try:
            self._run_once(timeout)
        finally:
            if own_thread:
                self._thread_id = None

    def stop(self) -> None:
        """Ask the loop to exit. Safe to call from another thread."""
        if self._closed:
            return
        self._stop_event.set()
        self._wakeup()

    def close(self) -> None:
        """Release the selector and the wakeup sockets.

        The loop must not be running. Sockets registered by callers are not
        closed here; the owner (for example the HTTP server) closes those.
        """
        if self._closed:
            return
        if self._thread_id is not None:
            raise RuntimeError("cannot close a running event loop")
        self._closed = True
        self._stop_event.set()
        try:
            self._selector.close()
        finally:
            for sock in (self._wake_r, self._wake_w):
                try:
                    sock.close()
                except OSError:
                    pass

    def _run_once(self, poll_timeout: float | None) -> None:
        self._iterations += 1
        self._drain_incoming()
        self._collect_due_timers()
        self._run_ready()
        if self._stop_event.is_set():
            return
        timeout = self._compute_timeout()
        if poll_timeout is not None:
            timeout = poll_timeout if timeout is None else min(timeout, poll_timeout)
        try:
            ready = self._selector.select(timeout)
        except InterruptedError:
            return
        except OSError:
            logger.exception("selector failure")
            return
        for key, mask in ready:
            callback = key.data
            try:
                callback(mask)
            except Exception:
                logger.exception(
                    "error in I/O callback %r",
                    getattr(callback, "__name__", callback),
                )

    def _run_ready(self) -> None:
        ready = self._ready
        ran = 0
        while ready and ran < _READY_BATCH:
            if self._stop_event.is_set():
                return
            callback, args = ready.popleft()
            ran += 1
            try:
                callback(*args)
            except Exception:
                logger.exception(
                    "error in callback %r",
                    getattr(callback, "__name__", callback),
                )

    def _drain_incoming(self) -> None:
        incoming = self._incoming
        ready = self._ready
        while True:
            try:
                ready.append(incoming.get_nowait())
            except queue.Empty:
                return

    def _collect_due_timers(self) -> None:
        now = time.monotonic()
        timers = self._timers
        ready = self._ready
        while timers and timers[0].when <= now:
            handle = heapq.heappop(timers)
            if not handle.cancelled and handle.callback is not None:
                ready.append((handle.callback, handle.args))

    def _compute_timeout(self) -> float | None:
        if self._ready:
            return 0.0
        while self._timers and self._timers[0].cancelled:
            heapq.heappop(self._timers)
        if not self._timers:
            return None
        return max(0.0, self._timers[0].when - time.monotonic())

    def _schedule_timer(self, handle: TimerHandle) -> None:
        if handle.cancelled:
            return
        handle.seq = self._next_seq()
        heapq.heappush(self._timers, handle)

    def _new_timer(
        self, when: float, callback: Callback, args: tuple[Any, ...]
    ) -> TimerHandle:
        return TimerHandle(when, self._next_seq(), callback, args)

    def _next_seq(self) -> int:
        seq = self._timer_seq
        self._timer_seq += 1
        return seq

    def _on_wakeup(self, _events: int) -> None:
        sock = self._wake_r
        try:
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    return
        except BlockingIOError:
            pass
        except OSError:
            return
        self._drain_incoming()

    def _wakeup(self) -> None:
        if self._closed:
            return
        try:
            self._wake_w.send(b"\x00")
        except (BlockingIOError, OSError):
            # The socket is already readable, or the loop has gone away.
            return

    def _check_loop_thread(self) -> None:
        if self._closed:
            raise RuntimeError("event loop is closed")
        thread_id = self._thread_id
        if thread_id is not None and thread_id != threading.get_ident():
            raise RuntimeError("socket registration must run on the event loop thread")
