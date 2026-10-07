"""Fault-injection plugin: emits a corrupt batch for [[corrupt]] documents.

The corrupt batch claims a codepoint range whose text does not match the
document, so the host's batch validation must reject it, fail the task and
kill this process.  Behaves exactly like line_text otherwise.
"""
from __future__ import annotations

import json
import sys

from app.plugins.line_text import parse_lines

CORRUPT_MARKER = "[[corrupt]]"


def _send(msg: dict):
    sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        mtype = msg.get("type")
        if mtype == "init":
            _send({"type": "init_ok", "protocol_version": 1,
                   "implementation": "faulty_bad_structure"})
        elif mtype == "parse":
            job_id = msg.get("job_id")
            content = msg.get("content") or ""
            if CORRUPT_MARKER in content:
                # text "XXXXX" does not match document[0:5]
                _send({"type": "batch", "job_id": job_id, "seq": 0,
                       "blocks": [{"start": 0, "end": 5, "text": "XXXXX"}]})
                _send({"type": "done", "job_id": job_id, "batches": 1})
                continue
            seq = 0

            def emit(blocks):
                nonlocal seq
                _send({"type": "batch", "job_id": job_id, "seq": seq,
                       "blocks": blocks})
                seq += 1

            parse_lines(content, emit, {})
            _send({"type": "done", "job_id": job_id, "batches": seq})
        elif mtype == "shutdown":
            break


if __name__ == "__main__":
    main()
