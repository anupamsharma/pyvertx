"""Event loop behavior that does not depend on HTTP."""

from __future__ import annotations

import selectors
import socket
import subprocess
import sys
import threading
import time
import unittest

from pyvertx.core.event_loop import EventLoop


def _close_loop(loop: EventLoop, thread: threading.Thread | None = None) -> None:
    loop.stop()
    if thread is not None:
        thread.join(2)
    if not loop.closed:
        loop.close()


class EventLoopTest(unittest.TestCase):
    def test_read_readiness_and_unregister(self) -> None:
        loop = EventLoop()
        left, right = socket.socketpair()
        self.addCleanup(right.close)
        self.addCleanup(loop.close)
        left.setblocking(False)
        right.setblocking(False)
        seen: list[bytes] = []

        def on_ready(events: int) -> None:
            self.assertTrue(events & selectors.EVENT_READ)
            seen.append(left.recv(64))
            loop.unregister(left)

        loop.register(left, selectors.EVENT_READ, on_ready)
        right.send(b"ping")
        loop.run_once(timeout=1)
        self.assertEqual(seen, [b"ping"])

        right.send(b"pong")
        loop.run_once(timeout=0)
        self.assertEqual(seen, [b"ping"])
        left.close()

    def test_callback_error_does_not_stop_the_loop(self) -> None:
        loop = EventLoop()
        first, first_peer = socket.socketpair()
        second, second_peer = socket.socketpair()
        for sock in (first, first_peer, second, second_peer):
            sock.setblocking(False)
            self.addCleanup(sock.close)
        self.addCleanup(loop.close)
        seen: list[str] = []

        def bad(_events: int) -> None:
            loop.unregister(first)
            seen.append("bad")
            raise RuntimeError("boom")

        def good(_events: int) -> None:
            loop.unregister(second)
            seen.append("good")

        loop.register(first, selectors.EVENT_READ, bad)
        loop.register(second, selectors.EVENT_READ, good)
        first_peer.send(b"a")
        second_peer.send(b"b")
        loop.run_once(timeout=1)
        self.assertEqual(set(seen), {"bad", "good"})

    def test_stop_from_another_thread_unblocks(self) -> None:
        loop = EventLoop()
        seen = threading.Event()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        self.addCleanup(_close_loop, loop, thread)
        self.assertTrue(loop.wait_started(1))
        loop.call_soon(seen.set)
        self.assertTrue(seen.wait(1))
        loop.stop()
        thread.join(1)
        self.assertFalse(thread.is_alive())

    def test_idle_loop_blocks_instead_of_spinning(self) -> None:
        loop = EventLoop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        self.addCleanup(_close_loop, loop, thread)
        self.assertTrue(loop.wait_started(1))
        time.sleep(0.25)
        loop.stop()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        # A busy loop would iterate thousands of times in a quarter second.
        self.assertLess(loop.iterations, 10)

    def test_call_later_and_cancel(self) -> None:
        loop = EventLoop()
        fired: list[str] = []

        def arm() -> None:
            loop.call_later(0.05, fired.append, "yes")
            loop.call_later(0.05, loop.stop)
            cancelled = loop.call_later(0.01, fired.append, "no")
            cancelled.cancel()

        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        self.addCleanup(_close_loop, loop, thread)
        self.assertTrue(loop.wait_started(1))
        started = time.monotonic()
        loop.call_soon(arm)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(fired, ["yes"])
        self.assertGreaterEqual(time.monotonic() - started, 0.04)

    def test_event_loop_and_tcp_do_not_import_http(self) -> None:
        code = (
            "import sys\n"
            "import pyvertx.core.event_loop\n"
            "import pyvertx.net\n"
            "names = [name for name in sys.modules if name.startswith('pyvertx.http')]\n"
            "raise SystemExit('imported ' + ', '.join(names) if names else 0)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
