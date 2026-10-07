"""Trusted plugin: split a Markdown document into sections.

A section runs from one ATX heading (`#` .. `######`) up to the next
heading; content before the first heading becomes a leading section.  Each
section is emitted as its own batch.  start/end are codepoint offsets into
the original document.  Options:
    batch_delay_ms   artificial delay between sections (default 0, for drills)
"""
from __future__ import annotations

import re
import time

from app.plugins.harness import serve

HEADING_RE = re.compile(r"^#{1,6}\s")


def _line_spans(content: str) -> list[tuple[int, int]]:
    """(start, end) codepoint offsets of each line, excluding the newline."""
    spans = []
    pos, n = 0, len(content)
    while pos < n:
        nl = content.find("\n", pos)
        end = n if nl == -1 else nl
        if end > pos and content[end - 1] == "\r":
            end -= 1
        spans.append((pos, end))
        pos = n if nl == -1 else nl + 1
    return spans


def parse_sections(content: str, emit, options: dict):
    delay_ms = float(options.get("batch_delay_ms", 0))
    sec_start = None
    last_end = None
    for line_start, line_end in _line_spans(content):
        if HEADING_RE.match(content[line_start:line_end]) and sec_start is not None:
            emit([{"start": sec_start, "end": last_end,
                   "text": content[sec_start:last_end]}])
            if delay_ms:
                time.sleep(delay_ms / 1000)
            sec_start = None
        if sec_start is None:
            sec_start = line_start
        last_end = line_end
    if sec_start is not None:
        emit([{"start": sec_start, "end": last_end,
               "text": content[sec_start:last_end]}])


if __name__ == "__main__":
    serve("markdown_sections", parse_sections)
