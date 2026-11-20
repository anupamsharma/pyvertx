"""HTTP server integration tests."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
import unittest

from tests.support import Reader, exchange, make_app, open_socket, request_bytes, serve


def _wait_until(predicate, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class BasicHTTPTest(unittest.TestCase):
    def setUp(self) -> None:
        self.app = make_app()
        self.addCleanup(self.app.stop)
        self.app.listen("127.0.0.1", 0, blocking=False)

    def test_get_text_and_json(self) -> None:
        status, headers, body = exchange(self.app, request_bytes("GET", "/"))
        self.assertEqual(status, 200)
        self.assertEqual(body, b"Hello World")
        self.assertEqual(headers["content-type"], "text/plain; charset=utf-8")
        self.assertEqual(headers["content-length"], "11")
        self.assertEqual(headers["connection"], "keep-alive")
        self.assertEqual(headers["server"], "pyvertx")
        self.assertEqual(headers["_version"], "HTTP/1.1")
        self.assertRegex(headers["date"], r"^[A-Za-z]{3}, ")

        status, headers, body = exchange(self.app, request_bytes("GET", "/hello"))
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(json.loads(body), {"message": "Hello from PyVertx"})

        status, headers, body = exchange(self.app, request_bytes("GET", "/unicode"))
        self.assertEqual(body, "héllo".encode())
        self.assertEqual(headers["content-length"], str(len(body)))

    def test_post_echo_and_request_metadata(self) -> None:
        status, headers, body = exchange(
            self.app, request_bytes("POST", "/echo", body=b"ping")
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, b"ping")
        self.assertEqual(headers["content-type"], "application/octet-stream")

        status, headers, body = exchange(
            self.app, request_bytes("POST", "/info", body=b"abcdef")
        )
        payload = json.loads(body)
        self.assertEqual(payload["host"], "localhost")
        self.assertEqual(payload["n"], 6)
        self.assertTrue(payload["remote"].startswith("127.0.0.1:"))

        status, headers, body = exchange(
            self.app, request_bytes("GET", "/query?name=pyvertx")
        )
        self.assertEqual(json.loads(body), {"name": "pyvertx"})

        status, headers, body = exchange(
            self.app, request_bytes("POST", "/created", body=b"{}")
        )
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(body), {"ok": True})

    def test_stdlib_client_keep_alive(self) -> None:
        conn = http.client.HTTPConnection(self.app.host, self.app.port, timeout=2)
        self.addCleanup(conn.close)
        conn.request("GET", "/")
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), b"Hello World")
        first = conn.sock
        conn.request("POST", "/echo", body=b"xyz")
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), b"xyz")
        self.assertIs(conn.sock, first)

    def test_multiple_requests_on_one_connection(self) -> None:
        sock = open_socket(self.app)
        self.addCleanup(sock.close)
        reader = Reader(sock)
        sock.sendall(request_bytes("GET", "/"))
        self.assertEqual(reader.read_response()[2], b"Hello World")
        sock.sendall(request_bytes("POST", "/echo", body=b"one"))
        self.assertEqual(reader.read_response()[2], b"one")
        sock.sendall(request_bytes("GET", "/hello"))
        self.assertEqual(
            json.loads(reader.read_response()[2]),
            {"message": "Hello from PyVertx"},
        )
        sock.sendall(request_bytes("POST", "/echo", body=b"two"))
        status, headers, body = reader.read_response()
        self.assertEqual(status, 200)
        self.assertEqual(body, b"two")
        self.assertEqual(headers["connection"], "keep-alive")

    def test_pipelined_requests(self) -> None:
        sock = open_socket(self.app)
        self.addCleanup(sock.close)
        sock.sendall(request_bytes("GET", "/") + request_bytes("GET", "/unicode"))
        reader = Reader(sock)
        self.assertEqual(reader.read_response()[2], b"Hello World")
        self.assertEqual(reader.read_response()[2], "héllo".encode())

    def test_http10_close_and_keep_alive(self) -> None:
        sock = open_socket(self.app)
        self.addCleanup(sock.close)
        sock.sendall(
            request_bytes("GET", "/", version="HTTP/1.0", headers={"Host": "localhost"})
        )
        # request_bytes adds Host twice if we pass it and also sets Host.
        # The helper always sets Host, then updates. One Host header. HTTP/1.0
        # without Connection: keep-alive must close.
        status, headers, body = Reader(sock).read_response()
        self.assertEqual(status, 200)
        self.assertEqual(body, b"Hello World")
        self.assertEqual(headers["_version"], "HTTP/1.0")
        self.assertEqual(headers["connection"], "close")
        sock.settimeout(1)
        self.assertEqual(sock.recv(64), b"")

        sock = open_socket(self.app)
        self.addCleanup(sock.close)
        reader = Reader(sock)
        keep = request_bytes(
            "GET",
            "/",
            version="HTTP/1.0",
            headers={"Connection": "keep-alive"},
        )
        sock.sendall(keep)
        self.assertEqual(reader.read_response()[1]["connection"], "keep-alive")
        sock.sendall(keep)
        self.assertEqual(reader.read_response()[0], 200)

    def test_missing_host_and_routing_errors(self) -> None:
        status, headers, body = exchange(
            self.app,
            b"GET / HTTP/1.1\r\nConnection: close\r\n\r\n",
        )
        self.assertEqual(status, 400)
        self.assertEqual(body, b"Bad Request")
        self.assertEqual(headers["connection"], "close")

        status, _headers, body = exchange(
            self.app, request_bytes("GET", "/missing")
        )
        self.assertEqual(status, 404)
        self.assertEqual(body, b"Not Found")

        status, headers, body = exchange(
            self.app, request_bytes("POST", "/")
        )
        self.assertEqual(status, 405)
        self.assertEqual(body, b"Method Not Allowed")
        self.assertIn("GET", headers["allow"])

    def test_malformed_request_then_the_server_still_accepts(self) -> None:
        sock = open_socket(self.app)
        sock.sendall(b"this is not http\r\n\r\n")
        status, _headers, body = Reader(sock).read_response()
        self.assertEqual(status, 400)
        self.assertEqual(body, b"Bad Request")
        sock.settimeout(1)
        self.assertEqual(sock.recv(32), b"")
        sock.close()

        status, _headers, body = exchange(
            self.app,
            b"GET / HTTP/9.9\r\nHost: localhost\r\n\r\n",
        )
        self.assertEqual(status, 400)
        status, _headers, body = exchange(self.app, request_bytes("GET", "/"))
        self.assertEqual((status, body), (200, b"Hello World"))

    def test_chunked_is_rejected(self) -> None:
        status, _headers, body = exchange(
            self.app,
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n",
        )
        self.assertEqual(status, 501)
        self.assertEqual(body, b"Not Implemented")

    def test_expect_continue(self) -> None:
        sock = open_socket(self.app)
        self.addCleanup(sock.close)
        sock.sendall(
            b"POST /echo HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Length: 4\r\n"
            b"Expect: 100-continue\r\n"
            b"\r\n"
        )
        reader = Reader(sock)
        status, _headers, body = reader.read_message()
        self.assertEqual(status, 100)
        self.assertEqual(body, b"")
        sock.sendall(b"pong")
        status, _headers, body = reader.read_response()
        self.assertEqual(status, 200)
        self.assertEqual(body, b"pong")

    def test_handler_exception_is_isolated(self) -> None:
        status, headers, body = exchange(self.app, request_bytes("GET", "/boom"))
        self.assertEqual(status, 500)
        self.assertEqual(body, b"Internal Server Error")
        self.assertEqual(headers["connection"], "close")
        status, _headers, body = exchange(self.app, request_bytes("GET", "/"))
        self.assertEqual((status, body), (200, b"Hello World"))

    def test_concurrent_connections(self) -> None:
        workers = 32
        barrier = threading.Barrier(workers)
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                barrier.wait(2)
                status, _headers, body = exchange(
                    self.app, request_bytes("GET", "/")
                )
                if status != 200 or body != b"Hello World":
                    raise AssertionError((status, body))
            except BaseException as exc:  # noqa: BLE001 - collect and report
                with lock:
                    errors.append(exc)

        before = threading.active_count()
        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        # The server thread already existed. Connections must not add one thread each.
        self.assertLess(threading.active_count() - before, workers)

    def test_client_disconnect_during_request(self) -> None:
        sock = open_socket(self.app)
        sock.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        self.assertTrue(
            _wait_until(lambda: self.app.connection_count >= 1),
            "server did not accept the connection",
        )
        sock.close()
        self.assertTrue(
            _wait_until(lambda: self.app.connection_count == 0),
            "closed client was not dropped",
        )
        status, _headers, body = exchange(self.app, request_bytes("GET", "/"))
        self.assertEqual((status, body), (200, b"Hello World"))

    def test_partial_request_bytes_are_buffered(self) -> None:
        sock = open_socket(self.app)
        self.addCleanup(sock.close)
        sock.sendall(b"GET / HTTP/1.1\r\n")
        sock.settimeout(0.15)
        try:
            early = sock.recv(1)
        except TimeoutError:
            early = None
        self.assertIsNone(early)
        sock.settimeout(2)
        sock.sendall(b"Host: localhost\r\n\r\n")
        status, _headers, body = Reader(sock).read_response()
        self.assertEqual(status, 200)
        self.assertEqual(body, b"Hello World")


class PartialReadSizeTest(unittest.TestCase):
    def test_one_byte_reads_still_complete_the_request(self) -> None:
        app = make_app(read_size=1)
        with serve(app):
            status, _headers, body = exchange(app, request_bytes("POST", "/echo", body=b"abcdef"))
        self.assertEqual(status, 200)
        self.assertEqual(body, b"abcdef")


class PartialWriteTest(unittest.TestCase):
    def test_small_write_chunks_still_deliver_the_response(self) -> None:
        app = make_app(write_chunk_size=5)
        with serve(app):
            status, headers, body = exchange(app, request_bytes("GET", "/unicode"))
        self.assertEqual(status, 200)
        self.assertEqual(body, "héllo".encode())
        self.assertEqual(int(headers["content-length"]), len(body))
        self.assertGreater(len(headers["date"]) + len(body), 5)


class LimitTest(unittest.TestCase):
    def test_large_body_within_the_limit(self) -> None:
        app = make_app(max_body_size=300_000)
        payload = b"abc123" * 20_000
        self.assertGreater(len(payload), 100_000)
        with serve(app):
            status, headers, body = exchange(
                app,
                request_bytes("POST", "/echo", body=payload),
                timeout=5,
            )
        self.assertEqual(status, 200)
        self.assertEqual(body, payload)
        self.assertEqual(int(headers["content-length"]), len(payload))

    def test_header_and_body_limits(self) -> None:
        app = make_app(max_header_size=128, max_body_size=32)
        with serve(app):
            status, _headers, body = exchange(
                app,
                request_bytes(
                    "GET",
                    "/",
                    headers={"X-Pad": "y" * 200},
                ),
            )
            self.assertEqual(status, 431)
            self.assertEqual(body, b"Request Header Fields Too Large")

            status, _headers, body = exchange(
                app,
                request_bytes("POST", "/echo", body=b"x" * 64),
            )
            self.assertEqual(status, 413)
            self.assertEqual(body, b"Payload Too Large")

            status, _headers, body = exchange(
                app, request_bytes("POST", "/echo", body=b"ok")
            )
            self.assertEqual((status, body), (200, b"ok"))

    def test_connection_limit(self) -> None:
        app = make_app(max_connections=1)
        with serve(app):
            held = open_socket(app)
            held.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            self.assertTrue(_wait_until(lambda: app.connection_count == 1))
            # The limit is enforced at accept time, before a request is read.
            rejected = open_socket(app)
            status, _headers, body = Reader(rejected).read_response()
            self.assertEqual(status, 503)
            self.assertEqual(body, b"Service Unavailable")
            rejected.close()
            held.close()
            self.assertTrue(_wait_until(lambda: app.connection_count == 0))
            status, _headers, body = exchange(app, request_bytes("GET", "/"))
            self.assertEqual((status, body), (200, b"Hello World"))


class ShutdownTest(unittest.TestCase):
    def test_stop_closes_listeners_and_open_connections(self) -> None:
        app = make_app()
        app.listen("127.0.0.1", 0, blocking=False)
        host, port = app.host, app.port
        sock = open_socket(app)
        sock.sendall(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n")
        self.assertTrue(_wait_until(lambda: app.connection_count >= 1))
        thread = app._thread
        loop = app._loop
        app.stop()
        self.assertIsNotNone(thread)
        assert thread is not None
        self.assertFalse(thread.is_alive())
        self.assertIsNotNone(loop)
        assert loop is not None
        self.assertTrue(loop.closed)
        self.assertFalse(app.running)
        self.assertEqual(app.connection_count, 0)
        sock.settimeout(1)
        try:
            leftover = bytearray()
            while True:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                leftover.extend(chunk)
        except OSError:
            leftover = bytearray()
        sock.close()
        self.assertNotIn(b"HTTP/1.1 200", leftover)
        with self.assertRaises(OSError):
            socket.create_connection((host, port), timeout=0.5)

    def test_restart_after_stop(self) -> None:
        app = make_app()
        self.addCleanup(app.stop)
        app.listen("127.0.0.1", 0, blocking=False)
        first = app.port
        self.assertEqual(exchange(app, request_bytes("GET", "/"))[2], b"Hello World")
        app.stop()
        self.assertFalse(app.running)
        app.listen("127.0.0.1", 0, blocking=False)
        self.assertNotEqual(app.port, first)
        self.assertEqual(exchange(app, request_bytes("GET", "/"))[2], b"Hello World")
        app.stop()
        app.stop()


if __name__ == "__main__":
    unittest.main()
