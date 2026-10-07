"""Fault-injection plugin: answers the init handshake with a wrong protocol
version.  Candidate validation must reject it and keep the current default.
"""
from __future__ import annotations

import json
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        msg = json.loads(line)
        mtype = msg.get("type")
        if mtype == "init":
            sys.stdout.write(json.dumps({
                "type": "init_ok",
                "protocol_version": 999,  # incompatible on purpose
                "implementation": "faulty_bad_init",
            }) + "\n")
            sys.stdout.flush()
        elif mtype == "shutdown":
            break


if __name__ == "__main__":
    main()
