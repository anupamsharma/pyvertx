"""Exact-match router."""

from __future__ import annotations

import unittest

from pyvertx.http.router import Router


class RouterTest(unittest.TestCase):
    def test_match_and_allowed_methods(self) -> None:
        router = Router()
        router.add("get", "/hello", lambda request: "a")
        router.add("POST", "/hello", lambda request: "b")
        self.assertIsNotNone(router.handler("GET", "/hello"))
        self.assertIsNone(router.handler("PUT", "/hello"))
        self.assertIsNone(router.handler("GET", "/hello/"))
        self.assertEqual(router.allowed("/hello"), ("GET", "POST"))
        self.assertEqual(router.allowed("/missing"), ())

    def test_replace_handler_and_reject_bad_paths(self) -> None:
        router = Router()
        router.add("GET", "/", lambda request: "first")
        router.add("GET", "/", lambda request: "second")
        self.assertEqual(router.handler("GET", "/")(None), "second")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            router.add("GET", "relative", lambda request: None)


if __name__ == "__main__":
    unittest.main()
