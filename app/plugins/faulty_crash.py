"""Fault-injection plugin: dies mid-parse on documents containing [[crash]].

Emits one valid batch and then exits hard (like a segfault): no `done`
message, no cleanup.  The host must fail the task — the partial result must
NOT be published — and the pool must recover for subsequent tasks.
Behaves exactly like line_text for documents without the marker.
"""
from __future__ import annotations

import os
import sys

from app.plugins.harness import serve
from app.plugins.line_text import parse_lines

CRASH_MARKER = "[[crash]]"


def parse(content: str, emit, options: dict):
    if CRASH_MARKER in content:
        first_line = content.split("\n", 1)[0]
        emit([{"start": 0, "end": len(first_line), "text": first_line}])
        sys.stdout.flush()
        os._exit(1)  # simulate a hard crash (segfault-style, no atexit)
    parse_lines(content, emit, options)


if __name__ == "__main__":
    serve("faulty_crash", parse)
