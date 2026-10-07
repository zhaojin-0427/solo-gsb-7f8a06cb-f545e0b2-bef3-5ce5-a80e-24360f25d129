"""Wire protocol between the service (host) and plugin subprocesses.

Framing: one JSON object per line (JSON Lines) on stdin/stdout.
Plugins must keep stdout protocol-clean and log to stderr only.

Host -> plugin:
    {"type": "init", "protocol_version": 1, "plugin": {"name": ..., "version": ...}, "config": {...}}
    {"type": "parse", "job_id": ..., "content": ..., "options": {...}}
    {"type": "shutdown"}

Plugin -> host:
    {"type": "init_ok", "protocol_version": 1, "implementation": ...} | {"type": "init_error", "error": ...}
    {"type": "batch", "job_id": ..., "seq": 0, "blocks": [{"start": i, "end": j, "text": ...}]}
    {"type": "done", "job_id": ..., "batches": N}
    {"type": "error", "job_id": ..., "error": ...}

A parse result is a *stream* of batches.  A result is only complete (and
therefore publishable) when a `done` message arrives whose `batches` count
matches the number of batches actually received.  Every batch is validated
for order (seq), ranges (codepoint offsets inside the document) and
structure (text must equal the document slice it claims to cover).
"""
from __future__ import annotations

import json

PROTOCOL_VERSION = 1
MAX_MESSAGE_BYTES = 16 * 1024 * 1024


def encode(msg: dict) -> bytes:
    return (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")


def init_message(name: str, version: str, config: dict) -> dict:
    return {
        "type": "init",
        "protocol_version": PROTOCOL_VERSION,
        "plugin": {"name": name, "version": version},
        "config": config or {},
    }


def parse_message(job_id: str, content: str, options: dict) -> dict:
    return {
        "type": "parse",
        "job_id": job_id,
        "content": content,
        "options": options or {},
    }


def shutdown_message() -> dict:
    return {"type": "shutdown"}


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


class BatchValidator:
    """Stateful validator for one parse job's batch stream.

    Returns human-readable error strings; any non-None result is a protocol
    violation and must fail the job (the partial result is discarded).
    """

    def __init__(self, document: str):
        self.document = document
        self.next_seq = 0
        self.last_end = 0
        self.blocks: list[dict] = []
        self.done = False

    def validate_batch(self, msg: dict) -> str | None:
        if self.done:
            return "received batch after done"
        seq = msg.get("seq")
        if not _is_int(seq):
            return "batch.seq must be an integer"
        if seq != self.next_seq:
            return f"out-of-order batch: expected seq {self.next_seq}, got {seq}"
        blocks = msg.get("blocks")
        if not isinstance(blocks, list):
            return "batch.blocks must be a list"
        doc_len = len(self.document)
        for i, block in enumerate(blocks):
            if not isinstance(block, dict):
                return f"block {i} of batch {seq} is not an object"
            start, end, text = block.get("start"), block.get("end"), block.get("text")
            if not _is_int(start) or not _is_int(end) or not isinstance(text, str):
                return f"block {i} of batch {seq} must have integer start/end and string text"
            if not (0 <= start <= end <= doc_len):
                return (
                    f"block {i} of batch {seq} has range [{start},{end}) "
                    f"outside document of length {doc_len}"
                )
            if start < self.last_end:
                return (
                    f"block {i} of batch {seq} overlaps a previous block "
                    f"(start {start} < {self.last_end})"
                )
            if self.document[start:end] != text:
                return (
                    f"block {i} of batch {seq}: text does not match document "
                    f"codepoints [{start},{end})"
                )
            self.last_end = end
            self.blocks.append({"start": start, "end": end, "text": text})
        self.next_seq += 1
        return None

    def validate_done(self, msg: dict) -> str | None:
        if self.done:
            return "duplicate done message"
        batches = msg.get("batches")
        if not _is_int(batches):
            return "done.batches must be an integer"
        if batches != self.next_seq:
            return (
                f"done claims {batches} batches but {self.next_seq} were received; "
                "result is incomplete"
            )
        self.done = True
        return None
