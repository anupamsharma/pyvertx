"""Incremental HTTP parser."""

from __future__ import annotations

import unittest

from pyvertx.http.parser import (
    ExpectationFailed,
    HeaderTooLarge,
    HTTPParser,
    ParseError,
    PayloadTooLarge,
    UnsupportedTransferEncoding,
)


def get(path: str = "/", *, version: str = "HTTP/1.1", extra: bytes = b"") -> bytes:
    host = b"" if version == "HTTP/1.0" else b"Host: localhost\r\n"
    return f"GET {path} {version}\r\n".encode() + host + extra + b"\r\n"


class ParserTest(unittest.TestCase):
    def test_get_and_query_split(self) -> None:
        parser = HTTPParser()
        requests = parser.feed(get("/hello?name=Py+Vertx&name=two"))
        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.path, "/hello")
        self.assertEqual(request.query_string, "name=Py+Vertx&name=two")
        self.assertEqual(request.query["name"], ["Py Vertx", "two"])
        self.assertEqual(request.body, b"")
        self.assertTrue(request.keep_alive)
        self.assertEqual(request.header("HOST"), "localhost")

    def test_post_body_and_byte_at_a_time(self) -> None:
        raw = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Length: 5\r\n"
            b"\r\n"
            b"hello"
            b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        parser = HTTPParser()
        done = []
        view = memoryview(raw)
        for index in range(len(view)):
            done.extend(parser.feed(view[index : index + 1]))
        self.assertEqual(len(done), 2)
        self.assertEqual(done[0].body, b"hello")
        self.assertEqual(done[0].text, "hello")
        self.assertEqual(done[1].path, "/")

    def test_body_is_owned_by_the_request(self) -> None:
        payload = bytearray(
            b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nBODY"
        )
        requests = HTTPParser().feed(payload)
        payload[:] = b"Z" * len(payload)
        self.assertEqual(requests[0].body, b"BODY")

    def test_body_split_across_feeds_after_header_compact(self) -> None:
        parser = HTTPParser(max_header_size=8192, max_body_size=100_000)
        pad = b"a" * 5000
        head = (
            b"POST / HTTP/1.1\r\nHost: h\r\nX-Pad: "
            + pad
            + b"\r\nContent-Length: 10000\r\n\r\n"
        )
        body = b"b" * 10000
        self.assertEqual(parser.feed(head + body[:4000]), [])
        requests = parser.feed(body[4000:])
        self.assertEqual(requests[0].body, body)

    def test_two_requests_in_one_buffer(self) -> None:
        parser = HTTPParser()
        requests = parser.feed(get("/") + get("/next"))
        self.assertEqual([request.path for request in requests], ["/", "/next"])

    def test_header_limit_boundary(self) -> None:
        prefix = b"GET / HTTP/1.1\r\nHost: h\r\nX-Pad: "
        suffix = b"\r\n\r\n"
        limit = 128
        exact_pad = b"x" * (limit - len(prefix) - len(suffix))
        parser = HTTPParser(max_header_size=limit)
        requests = parser.feed(prefix + exact_pad + suffix)
        self.assertEqual(len(requests), 1)
        with self.assertRaises(HeaderTooLarge):
            HTTPParser(max_header_size=limit).feed(prefix + exact_pad + b"y" + suffix)

    def test_incomplete_headers_do_not_grow_without_bound(self) -> None:
        parser = HTTPParser(max_header_size=64)
        with self.assertRaises(HeaderTooLarge):
            parser.feed(b"GET / HTTP/1.1\r\nHost: " + (b"a" * 80))

    def test_body_limit(self) -> None:
        parser = HTTPParser(max_body_size=4)
        with self.assertRaises(PayloadTooLarge):
            parser.feed(b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\n")
        requests = HTTPParser(max_body_size=4).feed(
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\n\r\nabcd"
        )
        self.assertEqual(requests[0].body, b"abcd")
        with self.assertRaises(PayloadTooLarge):
            HTTPParser().feed(
                b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: "
                + b"9" * 19
                + b"\r\n\r\n"
            )

    def test_malformed_requests(self) -> None:
        samples = [
            b"\r\n\r\n",
            b"GET / HTTP/2.0\r\nHost: h\r\n\r\n",
            b"GET /\r\nHost: h\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nBad Header: x\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nX-Fold: a\r\n b\r\n\r\n",
            b"GET / HTTP/1.1\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length: no\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nHost: other\r\n\r\n",
            b"GET / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\n",
            b"GET http://example/ HTTP/1.1\r\nHost: h\r\n\r\n",
        ]
        for sample in samples:
            with self.subTest(sample=sample):
                with self.assertRaises(ParseError):
                    HTTPParser().feed(sample)

    def test_chunked_and_expect(self) -> None:
        with self.assertRaises(UnsupportedTransferEncoding):
            HTTPParser().feed(
                b"POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"
            )
        with self.assertRaises(ParseError):
            HTTPParser().feed(
                b"POST / HTTP/1.1\r\nHost: h\r\n"
                b"Transfer-Encoding: chunked\r\nContent-Length: 1\r\n\r\n"
            )
        with self.assertRaises(ExpectationFailed):
            HTTPParser().feed(
                b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\n"
                b"Expect: magic\r\n\r\n"
            )

    def test_continue_only_when_the_body_is_not_buffered(self) -> None:
        parser = HTTPParser()
        head = (
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\n"
            b"Expect: 100-continue\r\n\r\n"
        )
        self.assertEqual(parser.feed(head), [])
        self.assertEqual(parser.consume_continue(), "HTTP/1.1")
        self.assertIsNone(parser.consume_continue())
        requests = parser.feed(b"data")
        self.assertEqual(requests[0].body, b"data")

        full = HTTPParser()
        self.assertEqual(full.feed(head + b"data")[0].body, b"data")
        self.assertIsNone(full.consume_continue())

    def test_connection_tokens(self) -> None:
        close_req = HTTPParser().feed(
            get(extra=b"Connection: Keep-Alive, close\r\n")
        )[0]
        self.assertFalse(close_req.keep_alive)
        old = HTTPParser().feed(get(version="HTTP/1.0"))[0]
        self.assertFalse(old.keep_alive)
        kept = HTTPParser().feed(
            get(version="HTTP/1.0", extra=b"Connection: keep-alive\r\n")
        )[0]
        self.assertTrue(kept.keep_alive)


if __name__ == "__main__":
    unittest.main()
