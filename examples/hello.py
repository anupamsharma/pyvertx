"""Minimal HTTP server.

Run from the repository root:

    python examples/hello.py

Then, in another shell:

    curl -s http://127.0.0.1:8080/
    curl -s http://127.0.0.1:8080/hello
    curl -s -X POST --data 'ping' http://127.0.0.1:8080/echo
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow `python examples/hello.py` without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        app.listen(host="127.0.0.1", port=8080)
    except KeyboardInterrupt:
        pass
