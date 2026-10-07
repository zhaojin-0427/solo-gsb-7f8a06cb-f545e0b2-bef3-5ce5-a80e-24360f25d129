#!/usr/bin/env python3
"""Restart drill: published results survive a restart, in-flight tasks get a
clear terminal state and are never published twice.

Steps:
  1. ensure the line-text plugin is registered and default
  2. run a quick task to completion (this result must survive the restart)
  3. start a slow task, then restart the service mid-flight
  4. after the service is back: the slow task is FAILED('interrupted ...'),
     the quick task is still SUCCEEDED with its original result

Usage:
  python scripts/restart_drill.py [--base http://localhost:8000] [--no-docker]

With --no-docker the script asks you to restart the service manually
(e.g. Ctrl-C the uvicorn process and start it again).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time

import httpx

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELED")


def wait_health(c: httpx.Client, timeout_s: float = 90) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if c.get("/health", timeout=5).json().get("status") == "ok":
                return True
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return False


def wait_task(c: httpx.Client, task_id: str, timeout_s: float = 60) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        task = c.get(f"/tasks/{task_id}").json()["task"]
        if task["status"] in TERMINAL:
            return task
        time.sleep(0.2)
    raise TimeoutError(task_id)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--no-docker", action="store_true",
                    help="do not run 'docker compose restart'; prompt instead")
    args = ap.parse_args()
    c = httpx.Client(base_url=args.base, timeout=30)

    if not wait_health(c, 30):
        print("service is not reachable")
        sys.exit(1)

    key = f"restart-drill-{int(time.time())}"
    c.post("/plugins", json={"name": "line-text", "version": "1.0.0",
                             "entry": "app.plugins.line_text",
                             "config": {"batch_size": 10, "batch_delay_ms": 200}})
    r = c.post("/plugins/line-text/default", json={"version": "1.0.0"})
    r.raise_for_status()

    quick = c.post("/tasks", json={"request_key": key + "-quick",
                                   "plugin": "line-text",
                                   "document": "published\nbefore\nrestart\n"}).json()["task"]
    quick = wait_task(c, quick["id"])
    assert quick["status"] == "SUCCEEDED", quick
    print(f"quick task {quick['id'][:8]} SUCCEEDED with "
          f"{quick['result']['block_count']} blocks")

    slow = c.post("/tasks", json={"request_key": key + "-slow",
                                  "plugin": "line-text",
                                  "document": "".join(f"slow line {i}\n" for i in range(200)),
                                  }).json()["task"]
    print(f"slow task {slow['id'][:8]} submitted (status {slow['status']}); restarting service...")
    time.sleep(0.5)

    if args.no_docker:
        input("restart the service now, then press Enter...")
    else:
        subprocess.run(["docker", "compose", "restart", "app"], check=True)

    if not wait_health(c, 120):
        print("service did not come back")
        sys.exit(1)
    print("service is back")

    slow_after = c.get(f"/tasks/{slow['id']}").json()["task"]
    quick_after = c.get(f"/tasks/{quick['id']}").json()["task"]

    ok = True
    if slow_after["status"] == "FAILED" and "interrupted" in (slow_after["error"] or ""):
        print(f"  ok   slow task is FAILED with clear reason: {slow_after['error']}")
    else:
        print(f" FAIL  slow task state: {slow_after['status']} / {slow_after['error']}")
        ok = False
    if slow_after["result"] is None:
        print("  ok   slow task has no published result")
    else:
        print(" FAIL  slow task unexpectedly has a result")
        ok = False
    if (quick_after["status"] == "SUCCEEDED"
            and quick_after["result"] == quick["result"]):
        print("  ok   published result survived the restart intact")
    else:
        print(" FAIL  published result changed or was lost")
        ok = False

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
