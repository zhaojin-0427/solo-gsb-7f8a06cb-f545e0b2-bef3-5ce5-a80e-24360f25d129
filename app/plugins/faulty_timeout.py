"""Fault-injection plugin: hangs forever on documents containing [[timeout]].

Behaves exactly like line_text otherwise, so it passes candidate validation
and serves normal documents — only the marked document triggers the fault.
The host must time the task out, kill this process and keep serving others.
"""
from __future__ import annotations

import time

from app.plugins.harness import serve
from app.plugins.line_text import parse_lines

TIMEOUT_MARKER = "[[timeout]]"


def parse(content: str, emit, options: dict):
    if TIMEOUT_MARKER in content:
        while True:  # simulate a hung plugin: never responds, never exits
            time.sleep(3600)
    parse_lines(content, emit, options)


if __name__ == "__main__":
    serve("faulty_timeout", parse)
