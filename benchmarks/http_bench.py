#!/usr/bin/env python3
"""Load-test an HTTP/1.1 server with the standard library only.

The client keeps one connection per worker and sends one request at a time.
Numbers include this client, the kernel, and the server. They are a local
measurement, not a comparison with other frameworks.

Example, with ``python examples/hello.py`` already listening:

    python benchmarks/http_bench.py --port 8080 --concurrency 32 --duration 5
"""

from __future__ import annotations

import argparse
import math
import socket
import sys
import threading
import time


class WorkerResult:
    def __init__(self) -> None:
        self.successes = 0
        self.errors = 0
        self.latency: list[float] = []
        self.started = 0.0
        self.finished = 0.0


class Client:
    def __init__(self, host: str, port: int, request: bytes, timeout: float) -> None:
        self.host = host
        self.port = port
        self.request = request
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self.buf = bytearray()

    def connect(self) -> None:
        self.close()
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.settimeout(self.timeout)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        self.sock = sock
        self.buf.clear()

    def close(self) -> None:
        sock = self.sock
        self.sock = None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def request_once(self) -> int:
        if self.sock is None:
            self.connect()
        assert self.sock is not None
        self.sock.sendall(self.request)
        return self._read_status()

    def _read_status(self) -> int:
        while b"\r\n\r\n" not in self.buf:
            self._pull()
        end = self.buf.find(b"\r\n\r\n")
        head = bytes(self.buf[:end])
        del self.buf[: end + 4]
        lines = head.split(b"\r\n")
        status = int(lines[0].split(b" ")[1])
        length = None
        for line in lines[1:]:
            name, _, value = line.partition(b":")
            if name.lower() == b"content-length":
                length = int(value.strip())
        if length is None:
            raise ConnectionError("response has no content-length")
        while len(self.buf) < length:
            self._pull()
        del self.buf[:length]
        return status

    def _pull(self) -> None:
        assert self.sock is not None
        chunk = self.sock.recv(65536)
        if not chunk:
            raise ConnectionError("connection closed")
        self.buf.extend(chunk)


def build_request(host: str, port: int, method: str, path: str, body: bytes) -> bytes:
    lines = [
        f"{method} {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Connection: keep-alive",
        "User-Agent: pyvertx-bench",
    ]
    if body:
        lines.append(f"Content-Length: {len(body)}")
        lines.append("Content-Type: application/octet-stream")
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")
    return raw + body


def percentile(samples: list[float], fraction: float) -> float:
    if not samples:
        return 0.0
    if len(samples) == 1:
        return samples[0]
    rank = (len(samples) - 1) * fraction
    low = math.floor(rank)
    high = min(low + 1, len(samples) - 1)
    weight = rank - low
    return samples[low] * (1.0 - weight) + samples[high] * weight


def run_worker(
    index: int,
    args: argparse.Namespace,
    request: bytes,
    barrier: threading.Barrier,
    results: list[WorkerResult],
) -> None:
    client = Client(args.host, args.port, request, args.timeout)
    result = results[index]
    try:
        try:
            client.connect()
            deadline = time.perf_counter() + args.warmup
            while time.perf_counter() < deadline:
                client.request_once()
        except Exception:
            try:
                client.connect()
            except Exception:
                pass
        barrier.wait(args.timeout + args.warmup + 5)
        result.started = time.perf_counter()
        end = result.started + args.duration
        while time.perf_counter() < end:
            started = time.perf_counter()
            try:
                status = client.request_once()
            except Exception:
                result.errors += 1
                try:
                    client.connect()
                except Exception:
                    time.sleep(0.01)
                continue
            elapsed = time.perf_counter() - started
            if status != args.expect_status:
                result.errors += 1
                continue
            result.successes += 1
            result.latency.append(elapsed)
        result.finished = time.perf_counter()
    except threading.BrokenBarrierError:
        result.errors += 1
        result.finished = time.perf_counter()
    finally:
        client.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HTTP/1.1 load test (standard library only)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--path", default="/")
    parser.add_argument("--method", default="GET")
    parser.add_argument("--body", default="", help="Request body. Empty for GET.")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--duration", type=float, default=5.0, help="Measured seconds per worker.")
    parser.add_argument("--warmup", type=float, default=0.5, help="Unmeasured warmup seconds.")
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--expect-status", type=int, default=200)
    args = parser.parse_args(argv)
    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1")
    if args.duration <= 0:
        parser.error("--duration must be > 0")
    if args.warmup < 0:
        parser.error("--warmup must be >= 0")
    args.method = args.method.upper()
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    request = build_request(
        args.host,
        args.port,
        args.method,
        args.path,
        args.body.encode("utf-8"),
    )
    barrier = threading.Barrier(args.concurrency)
    results = [WorkerResult() for _ in range(args.concurrency)]
    threads = [
        threading.Thread(
            target=run_worker,
            args=(index, args, request, barrier, results),
            name=f"bench-{index}",
        )
        for index in range(args.concurrency)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    successes = sum(item.successes for item in results)
    errors = sum(item.errors for item in results)
    samples = [value for item in results for value in item.latency]
    samples.sort()
    starts = [item.started for item in results if item.started]
    finishes = [item.finished for item in results if item.finished]
    wall = (max(finishes) - min(starts)) if starts and finishes else 0.0
    rps = successes / wall if wall else 0.0
    mean = (sum(samples) / len(samples)) if samples else 0.0

    def ms(seconds: float) -> float:
        return seconds * 1000.0

    print("pyvertx http benchmark")
    print(f"target: {args.method} http://{args.host}:{args.port}{args.path}")
    print(f"concurrency: {args.concurrency}")
    print(f"duration_s: {args.duration:.2f}")
    print(f"warmup_s: {args.warmup:.2f}")
    print(f"wall_s: {wall:.3f}")
    print(f"requests: {successes}")
    print(f"errors: {errors}")
    print(f"rps: {rps:.1f}")
    if samples:
        print(
            "latency_ms:"
            f" min={ms(samples[0]):.3f}"
            f" mean={ms(mean):.3f}"
            f" p50={ms(percentile(samples, 0.50)):.3f}"
            f" p90={ms(percentile(samples, 0.90)):.3f}"
            f" p99={ms(percentile(samples, 0.99)):.3f}"
            f" max={ms(samples[-1]):.3f}"
        )
    else:
        print("latency_ms: n/a")
    print("note: rps and latency include this stdlib client and the kernel")
    return 0 if successes and errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
