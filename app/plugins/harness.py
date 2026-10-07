"""Shared harness for trusted in-repo plugins.

Speaks the JSON-Lines protocol from app.protocol on stdin/stdout.
Logs go to stderr only — stdout is reserved for protocol messages.
"""
from __future__ import annotations

import json
import sys
import traceback

PROTOCOL_VERSION = 1


def send(msg: dict):
    sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def log(*args):
    print("[plugin]", *args, file=sys.stderr, flush=True)


def serve(implementation: str, parse_fn):
    """Run the protocol loop.

    parse_fn(content, emit, options) emits blocks via
    emit([{"start": i, "end": j, "text": ...}, ...]) — one call per batch.
    """
    config: dict = {}
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            send({"type": "error", "job_id": None, "error": "invalid JSON from host"})
            continue
        mtype = msg.get("type")
        if mtype == "init":
            if msg.get("protocol_version") != PROTOCOL_VERSION:
                send({
                    "type": "init_error",
                    "error": f"unsupported protocol version "
                             f"{msg.get('protocol_version')!r}",
                })
                continue
            config = msg.get("config") or {}
            send({
                "type": "init_ok",
                "protocol_version": PROTOCOL_VERSION,
                "implementation": implementation,
                "capabilities": {"streaming": True},
            })
        elif mtype == "parse":
            job_id = msg.get("job_id")
            content = msg.get("content") or ""
            options = dict(config)
            options.update(msg.get("options") or {})
            seq = 0

            def emit(blocks):
                nonlocal seq
                send({"type": "batch", "job_id": job_id, "seq": seq, "blocks": blocks})
                seq += 1

            try:
                parse_fn(content, emit, options)
            except Exception as exc:  # noqa: BLE001 - reported over the protocol
                traceback.print_exc(file=sys.stderr)
                send({"type": "error", "job_id": job_id,
                      "error": f"{type(exc).__name__}: {exc}"})
            else:
                send({"type": "done", "job_id": job_id, "batches": seq})
        elif mtype == "shutdown":
            break
