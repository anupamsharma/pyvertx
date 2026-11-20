"""TCP connection buffering, independent of HTTP."""

from __future__ import annotations

import socket
import threading
import time
import unittest

from pyvertx.core.event_loop import EventLoop
from pyvertx.net.connection import Connection


def _stop_loop(loop, thread, conn, peer) -> None:
    loop.stop()
    thread.join(2)
    conn.abort()
    try:
        peer.close()
    except OSError:
        pass
    if not loop.closed:
        loop.close()


class ConnectionTest(unittest.TestCase):
    def test_echo_copies_out_of_the_read_buffer(self) -> None:
        loop = EventLoop()
        local, peer = socket.socketpair()
        peer.setblocking(False)
        received = bytearray()
        conn_box: list[Connection] = []

        def on_data(data: memoryview) -> None:
            # The read buffer is reused. The copy has to happen before return.
            conn_box[0].write(bytes(data))

        def on_close() -> None:
            return None

        conn = Connection(loop, local, on_data, on_close, max_send=8)
        conn_box.append(conn)
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        self.addCleanup(_stop_loop, loop, thread, conn, peer)
        self.assertTrue(loop.wait_started(1))
        peer.setblocking(True)
        peer.settimeout(2)
        payload = b"abcdefghijklmnopqrstuvwxyz"
        peer.sendall(payload)
        while len(received) < len(payload):
            chunk = peer.recv(64)
            if not chunk:
                break
            received.extend(chunk)
        self.assertEqual(bytes(received), payload)

    def test_partial_write_resumes_after_the_kernel_buffer_fills(self) -> None:
        loop = EventLoop()
        local, peer = socket.socketpair()
        peer.setblocking(False)
        local.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        observed: list[tuple[int, bool]] = []

        def on_data(_data: memoryview) -> None:
            return None

        def on_close() -> None:
            return None

        conn = Connection(
            loop,
            local,
            on_data,
            on_close,
            high_water=4096,
            low_water=1024,
        )
        payload = b"z" * 65536

        def start() -> None:
            conn.write(payload)
            observed.append((conn.pending, conn.read_paused))

        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        self.addCleanup(_stop_loop, loop, thread, conn, peer)
        self.assertTrue(loop.wait_started(1))
        loop.call_soon(start)
        deadline = time.monotonic() + 2
        while not observed and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(observed, "write did not run")
        pending, paused = observed[0]
        self.assertGreater(pending, 0)
        self.assertTrue(paused)

        peer.setblocking(True)
        peer.settimeout(2)
        received = bytearray()
        while len(received) < len(payload):
            chunk = peer.recv(8192)
            if not chunk:
                break
            received.extend(chunk)
        self.assertEqual(received, payload)
        deadline = time.monotonic() + 1
        while conn.pending and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(conn.pending, 0)
        self.assertFalse(conn.read_paused)

    def test_data_callback_error_closes_the_connection_only(self) -> None:
        loop = EventLoop()
        local, peer = socket.socketpair()
        peer.setblocking(False)
        closed = threading.Event()

        def on_data(_data: memoryview) -> None:
            raise RuntimeError("handler failed")

        conn = Connection(loop, local, on_data, closed.set)
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        self.addCleanup(_stop_loop, loop, thread, conn, peer)
        self.assertTrue(loop.wait_started(1))
        peer.send(b"x")
        self.assertTrue(closed.wait(1))
        self.assertTrue(conn.closed)
        self.assertTrue(loop.running)


if __name__ == "__main__":
    unittest.main()
