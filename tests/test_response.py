"""Response encoding."""

from __future__ import annotations

import json
import unittest

from pyvertx.http.response import Response, coerce_response


class ResponseTest(unittest.TestCase):
    def test_text_json_and_bytes(self) -> None:
        text = Response("héllo")
        self.assertEqual(text.body, "héllo".encode())
        self.assertEqual(text.content_type, "text/plain; charset=utf-8")

        document = Response({"message": "Hello from PyVertx"})
        self.assertEqual(document.body, b'{"message":"Hello from PyVertx"}')
        self.assertEqual(json.loads(document.body), {"message": "Hello from PyVertx"})
        self.assertEqual(document.content_type, "application/json")

        binary = Response(b"\x00\xff")
        self.assertEqual(binary.content_type, "application/octet-stream")
        self.assertEqual(coerce_response(bytearray(b"ab")).body, b"ab")

    def test_header_block_and_keep_alive(self) -> None:
        response = Response("Hello World", headers={"X-Test": "1"})
        head = response.header_bytes("HTTP/1.1", keep_alive=True)
        self.assertTrue(head.startswith(b"HTTP/1.1 200 OK\r\n"))
        self.assertIn(b"Content-Length: 11\r\n", head)
        self.assertIn(b"Connection: keep-alive\r\n", head)
        self.assertIn(b"Server: pyvertx\r\n", head)
        self.assertIn(b"X-Test: 1\r\n", head)
        self.assertTrue(head.endswith(b"\r\n\r\n"))
        self.assertNotIn(b"Hello World", head)

        closed = response.header_bytes("HTTP/1.0", keep_alive=False)
        self.assertTrue(closed.startswith(b"HTTP/1.0 200 OK\r\n"))
        self.assertIn(b"Connection: close\r\n", closed)

    def test_empty_status_and_rejected_values(self) -> None:
        empty = Response(b"ignored", status=204)
        head = empty.header_bytes("HTTP/1.1", True)
        self.assertIn(b"Content-Length: 0\r\n", head)
        self.assertEqual(empty.transmission_body(), b"")
        with self.assertRaises(TypeError):
            coerce_response(12)
        with self.assertRaises(ValueError):
            Response("x", headers={"Bad\r\nName": "1"})
        with self.assertRaises(TypeError):
            Response({"nope": object()})


if __name__ == "__main__":
    unittest.main()
