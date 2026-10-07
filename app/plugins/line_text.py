"""Trusted plugin: split a document into lines.

Each block is one logical line (without its line terminator); start/end are
codepoint offsets into the original document.  Options:
    batch_size       lines per batch (default 50)
    batch_delay_ms   artificial delay between batches (default 0, for drills)
"""
from __future__ import annotations

import time

from app.plugins.harness import serve


def parse_lines(content: str, emit, options: dict):
    batch_size = max(1, int(options.get("batch_size", 50)))
    delay_ms = float(options.get("batch_delay_ms", 0))
    batch = []
    pos, n = 0, len(content)
    while pos < n:
        nl = content.find("\n", pos)
        end = n if nl == -1 else nl
        if end > pos and content[end - 1] == "\r":
            end -= 1
        batch.append({"start": pos, "end": end, "text": content[pos:end]})
        pos = n if nl == -1 else nl + 1
        if len(batch) >= batch_size:
            emit(batch)
            batch = []
            if delay_ms:
                time.sleep(delay_ms / 1000)
    if batch:
        emit(batch)


if __name__ == "__main__":
    serve("line_text", parse_lines)
