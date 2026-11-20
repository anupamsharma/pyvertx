# PyVertx

PyVertx is a small event-driven networking framework inspired by [Eclipse Vert.x](https://vertx.io/). Release 1 is a single-process HTTP/1.1 server built only on the Python standard library. It does not use asyncio, third-party packages, or C extensions.

Python 3.12 or newer is required.

## Run the example

From the repository root:

```bash
python examples/hello.py
```

The process listens on `127.0.0.1:8080` until you stop it with Ctrl+C.

```bash
curl -s http://127.0.0.1:8080/
curl -s http://127.0.0.1:8080/hello
curl -s -X POST --data 'ping' http://127.0.0.1:8080/echo
```

## Run the tests

```bash
python -m unittest discover -s tests -t . -v
```

No packages need to be installed. The command above puts the repository root on `sys.path`.

## Run a load test

Start the example server, then in another shell:

```bash
python benchmarks/http_bench.py --port 8080 --concurrency 32 --duration 5
```

The benchmark is a standard-library client. It opens one keep-alive connection per worker and reports requests per second, latency, and errors. See [Benchmark](#benchmark) for what those numbers include.

## Application API

```python
from pyvertx import HTTPServer

app = HTTPServer()

@app.get("/")
def home(request):
    return "Hello World"

@app.get("/hello")
def hello(request):
    return {"message": "Hello from PyVertx"}

@app.post("/echo")
def echo(request):
    return request.body

app.listen(host="127.0.0.1", port=8080)
```

Handlers run on the event-loop thread. They must not block. A return value may be:

| Return value | Status | Content-Type |
| --- | --- | --- |
| `str` | 200 | `text/plain; charset=utf-8` |
| `bytes` or `bytearray` | 200 | `application/octet-stream` |
| `dict` | 200 | `application/json` |
| `Response` | chosen by you | chosen by you, with the defaults above |

`Response` is useful when the status or an extra header is not the default:

```python
from pyvertx import Response

@app.post("/items")
def create(request):
    return Response({"ok": True}, status=201, headers={"X-Request-Id": "1"})
```

`route(method, path)` registers any method. Paths are matched exactly. The query string is not part of the path.

`request` carries `method`, `path`, `query_string`, `query`, `version`, `headers`, `body`, `keep_alive`, and `remote_addr`. Header lookup is case-insensitive through `request.header(name)`. `request.text` decodes the body as UTF-8, and `request.json()` parses it as JSON.

Unknown paths return 404. A known path with an unregistered method returns 405 and an `Allow` header. A handler that raises, or returns a value that is not one of the types above, becomes a 500 response and the connection is closed. The loop keeps serving other connections.

### Limits

`HTTPServer` accepts these keyword arguments:

| Argument | Default | Meaning |
| --- | --- | --- |
| `max_header_size` | 8192 | Maximum bytes for the request line and headers, including the blank line |
| `max_body_size` | 1048576 | Maximum `Content-Length` |
| `max_connections` | 10000 | Open connections. Further clients receive 503 and the socket is closed |
| `backlog` | 128 | Listening socket backlog |
| `read_size` | 65536 | Bytes requested per `recv` |
| `write_chunk_size` | `None` | Optional cap on bytes per `send`, mainly for tests |
| `high_water` / `low_water` | 1 MiB / 256 KiB | Pause and resume reads based on the pending write buffer |

`listen(host="127.0.0.1", port=8080)` blocks. `port` may be `0`. The first `getaddrinfo` result is used, so pass `127.0.0.1` when you want IPv4. `listen(..., blocking=False)` runs the loop on a background thread; `stop()` from another thread closes the listener and the open connections.

Register routes before `listen`. The router is not locked.

## Architecture

One thread owns every socket. The thread blocks inside `selectors.DefaultSelector.select` until a socket is readable, writable, a timer is due, or another thread wakes the loop. There is no thread per connection and no busy loop while the process is idle.

```text
HTTPServer
    routes and handler calls
        HTTPProtocol          one request/response state machine per connection
            HTTPParser        incremental bytes to Request objects
            Connection        nonblocking reads and a write buffer
        TCPServer             accept loop
    EventLoop                 selector, timers, cross-thread wakeup
```

`EventLoop` and `Connection` do not know about HTTP. A later protocol can register sockets with the same loop. Importing `pyvertx.core.event_loop` does not import the HTTP package; the names re-exported from `pyvertx` are loaded on first use.

Flow for one request:

1. The listening socket becomes readable. `TCPServer` accepts, sets the socket nonblocking, and disables Nagle (`TCP_NODELAY`) so small responses are not delayed.
2. `Connection` registers for reads. Each `recv` fills a reused buffer and hands the callback a `memoryview`. The parser copies what it keeps, so the next read can reuse the buffer.
3. `HTTPParser` waits until it has a full request line, headers, and `Content-Length` body. It can complete several requests from one read and can pause in the middle of any of them.
4. The router calls the handler. The return value is serialized to a header block plus the original body bytes, so the body is not copied into a second combined buffer when the kernel accepts the write.
5. A short `send` leaves the tail in a `bytearray`. The socket is watched for writability only while that buffer is non-empty. If the buffer passes the high-water mark, reads are paused until it drains.
6. HTTP/1.1 connections stay open unless the request says `Connection: close` or the response is an error that might have left the stream unaligned. HTTP/1.0 closes unless the client sent `Connection: keep-alive`.

`stop()` is safe from another thread. It sets a flag and writes one byte to a wakeup socket, which unblocks `select`. The loop thread then closes the listener, aborts open connections, and closes the selector. A lock is not taken around socket registration. The standard library's queue and event, used only to cross threads, have their own internal locks.

## Benchmark

`benchmarks/http_bench.py` measures a running server. Each worker keeps a connection open, warms up, then issues requests until the duration elapses. Reported requests per second use the wall clock from the first worker start to the last worker finish. Latency is the time from `send` of one request to the end of its response, on that client. Errors are failed reads, resets, and statuses other than the expected one.

The client is ordinary Python sockets, so the rate includes client overhead and the kernel, not only server time. Treat the figures as a local sample.

One local run against `examples/hello.py` on Python 3.14, measured for 3 seconds after a short warmup, with keep-alive GET `/` and zero errors:

| Concurrency | Requests/sec | Mean latency | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 33767 | 0.029 ms | 0.030 ms | 0.048 ms |
| 32 | 44095 | 0.725 ms | 0.639 ms | 2.114 ms |
| 64 | 44106 | 1.450 ms | 1.253 ms | 4.341 ms |

Throughput levels off once the single thread is busy. Extra connections add latency instead of requests per second.

## Decisions

- **One visible reactor.** Release 1 does not adopt asyncio. Handlers are ordinary functions, and the wait point is `selector.select`.
- **Protocol split.** The loop and the TCP connection can host another protocol without rewriting accept, buffering, or shutdown.
- **Incremental parsing with hard limits.** Header and body caps are checked against the declared sizes. A `Content-Length` larger than the cap is rejected without allocating that body. Chunked bodies are answered with 501 because they are out of scope for this release.
- **Write interest only when needed.** An idle keep-alive connection is registered for read and blocks in `select`.
- **Backpressure.** A client that stops reading does not grow the write buffer without bound; reads pause at the high-water mark.
- **Best-effort `100 Continue`.** If `Expect: 100-continue` arrives before the body, the server sends `100 Continue` and then the final response. If the body is already buffered, only the final response is sent.
- **Date header cached for one second.** Every response shares that cached value. It can be up to a second old.

## Limitations

- One process and one event loop. A blocking handler stalls every connection.
- Request bodies are buffered in memory, up to `max_body_size`. There is no streaming body API.
- No chunked request or response bodies, TLS, HTTP/2, or WebSockets.
- Only origin-form targets (`/path`). Absolute-form URLs are rejected.
- Line endings must be CRLF. Bare LF is a 400.
- `HEAD` is not implied by a `GET` route.
- No idle timeout. A client can hold a connection open until the process stops or `max_connections` is reached.
- Pipelined requests already in the buffer are answered in order. The server does not otherwise schedule pipelines specially.
- Routes are exact strings. There are no path parameters.
- Duplicate `Content-Length`, `Host`, or `Transfer-Encoding` headers are rejected. Other repeated headers are joined with a comma.
- Closing a connection that still has unread bytes can still produce a TCP reset if more data arrives after the socket is closed. Oversized requests that were fully read are answered with 413 or 431 before the close.

## Layout

```text
pyvertx/
    core/event_loop.py     selector loop, timers, wakeup
    net/tcp_server.py      listening socket
    net/connection.py      per-socket read and write buffers
    http/parser.py         incremental HTTP/1.1 parser
    http/protocol.py       connection state machine
    http/server.py         HTTPServer and routing decorators
    http/request.py
    http/response.py
    http/router.py
tests/                     unittest suite
examples/hello.py
benchmarks/http_bench.py
```

`http/protocol.py` is the extra module beyond a minimal sketch. It keeps the per-connection HTTP state out of the application class so `HTTPServer` stays the public API and the TCP types stay free of HTTP.
