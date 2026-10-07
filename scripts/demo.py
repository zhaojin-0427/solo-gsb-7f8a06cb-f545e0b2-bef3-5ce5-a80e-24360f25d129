#!/usr/bin/env python3
"""End-to-end demo and fault drill for the document parsing plugin service.

Covers, against a running service:
  1. plugin registration + default switch (with candidate validation)
  2. line and markdown parsing with codepoint ranges
  3. idempotent submission (same key+doc -> same task, other doc -> 409)
  4. switching the default version while a task is in flight on the old one
  5. cancellation (cancel vs complete race)
  6. fault isolation: timeout / crash / corrupt output plugins
  7. failed candidate validation keeps the current default

Usage:  python scripts/demo.py [--base http://localhost:8000]
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import httpx

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELED")
FAILURES = []


def step(title: str):
    print(f"\n=== {title} " + "=" * max(0, 60 - len(title)))


def check(cond: bool, label: str):
    print(("  ok  " if cond else " FAIL ") + label)
    if not cond:
        FAILURES.append(label)


def brief(obj, limit=420) -> str:
    s = json.dumps(obj, ensure_ascii=False, default=str)
    return s if len(s) <= limit else s[:limit] + "..."


def wait_task(c: httpx.Client, task_id: str, timeout_s: float = 60) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        task = c.get(f"/tasks/{task_id}").json()["task"]
        if task["status"] in TERMINAL:
            return task
        time.sleep(0.2)
    raise TimeoutError(f"task {task_id} did not finish in {timeout_s}s")


def submit(c: httpx.Client, **kw) -> dict:
    r = c.post("/tasks", json=kw)
    assert r.status_code == 201, f"submit failed: {r.status_code} {r.text}"
    return r.json()["task"]


def register(c: httpx.Client, name, version, entry, config=None):
    r = c.post("/plugins", json={
        "name": name, "version": version, "entry": entry, "config": config or {},
    })
    assert r.status_code == 201, f"register failed: {r.status_code} {r.text}"
    return r.json()


def set_default(c: httpx.Client, name, version) -> dict:
    r = c.post(f"/plugins/{name}/default", json={"version": version})
    assert r.status_code == 200, f"switch failed: {r.status_code} {r.text}"
    return r.json()


def big_doc(lines=400) -> str:
    return "".join(
        f"line {i:04d}: the quick brown fox jumps over the lazy dog\n"
        for i in range(lines)
    )


MD_DOC = """# Intro

Some intro text.

## Details

- point one
- point two

# Conclusion

All done.
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    args = ap.parse_args()
    c = httpx.Client(base_url=args.base, timeout=30)

    step("0. wait for service")
    for _ in range(60):
        try:
            if c.get("/health").json().get("status") == "ok":
                break
        except httpx.HTTPError:
            pass
        time.sleep(1)
    else:
        print("service did not become healthy")
        sys.exit(1)
    print("  service is healthy")

    step("1. register line-text 1.0.0 and make it default")
    register(c, "line-text", "1.0.0", "app.plugins.line_text",
             {"batch_size": 10, "batch_delay_ms": 100})
    r = set_default(c, "line-text", "1.0.0")
    print(" ", brief(r))

    step("2. basic parse (line-text + markdown-sections)")
    t = submit(c, request_key="demo-basic", plugin="line-text",
               document="hello\nworld\n")
    t = wait_task(c, t["id"])
    check(t["status"] == "SUCCEEDED", f"line parse succeeded ({t['status']})")
    check(t["result"]["blocks"][0] == {"start": 0, "end": 5, "text": "hello"},
          "first block carries codepoint range [0,5)")
    register(c, "markdown-sections", "1.0.0", "app.plugins.markdown_sections")
    set_default(c, "markdown-sections", "1.0.0")
    tm = submit(c, request_key="demo-md", plugin="markdown-sections", document=MD_DOC)
    tm = wait_task(c, tm["id"])
    sections = [b["text"].splitlines()[0] for b in tm["result"]["blocks"]]
    print("  sections:", sections)
    check(tm["status"] == "SUCCEEDED" and sections == ["# Intro", "## Details", "# Conclusion"],
          "markdown split into 3 sections")

    step("3. idempotency: same key+doc -> same task, other doc -> 409")
    again = submit(c, request_key="demo-basic", plugin="line-text",
                   document="hello\nworld\n")
    check(again["id"] == t["id"], "same request_key + same document returns original task")
    r = c.post("/tasks", json={"request_key": "demo-basic", "plugin": "line-text",
                               "document": "different\n"})
    check(r.status_code == 409, f"same key + different document rejected ({r.status_code})")

    step("4. switch default while a task is in flight on the old version")
    register(c, "line-text", "2.0.0", "app.plugins.line_text", {"batch_size": 100})
    a = submit(c, request_key="demo-inflight", plugin="line-text",
               document=big_doc(400))  # ~4s on v1 (40 batches x 100ms)
    print(f"  task A {a['id'][:8]} bound to {a['version']}, status {a['status']}")
    r = set_default(c, "line-text", "2.0.0")
    check(r["previous_default"] == "1.0.0" and r["default"] == "2.0.0",
          "default switched to 2.0.0 while A is running")
    b = submit(c, request_key="demo-after-switch", plugin="line-text",
               document="new\ntask\n")
    a = wait_task(c, a["id"], timeout_s=90)
    b = wait_task(c, b["id"])
    check(a["status"] == "SUCCEEDED" and a["version"] == "1.0.0",
          f"in-flight task A finished on old version {a['version']}")
    check(b["status"] == "SUCCEEDED" and b["version"] == "2.0.0",
          f"task B after switch ran on new version {b['version']}")
    plugins = {p["name"]: p for p in c.get("/plugins").json()["plugins"]}
    v1 = next(v for v in plugins["line-text"]["versions"] if v["version"] == "1.0.0")
    check(v1["status"] == "RETIRED" and v1["workers"] == 0,
          f"old version retired after drain (status={v1['status']}, workers={v1['workers']})")

    step("5. cancel a running task (cancel vs complete race)")
    t = submit(c, request_key="demo-cancel", plugin="line-text",
               document=big_doc(400), options={"batch_delay_ms": 300})
    time.sleep(0.5)
    r = c.post(f"/tasks/{t['id']}/cancel")
    check(r.status_code == 200, f"cancel accepted ({r.status_code})")
    t = wait_task(c, t["id"])
    check(t["status"] == "CANCELED" and t["result"] is None,
          f"task is CANCELED with no published result ({t['status']})")
    r = c.post(f"/tasks/{t['id']}/cancel")
    check(r.status_code == 409, f"second cancel rejected ({r.status_code})")

    step("6. fault isolation: timeout / crash / corrupt plugins")
    register(c, "fault-timeout", "1.0.0", "app.plugins.faulty_timeout")
    set_default(c, "fault-timeout", "1.0.0")
    t = submit(c, request_key="demo-timeout", plugin="fault-timeout",
               document="[[timeout]]\nhung plugin\n", timeout_ms=2500)
    t = wait_task(c, t["id"], timeout_s=30)
    check(t["status"] == "FAILED" and "timed out" in (t["error"] or ""),
          f"timeout task FAILED: {t['error']}")
    t = submit(c, request_key="demo-timeout-ok", plugin="fault-timeout",
               document="clean\ndocument\n")
    t = wait_task(c, t["id"])
    check(t["status"] == "SUCCEEDED", "pool recovered after timeout (clean doc ok)")

    register(c, "fault-crash", "1.0.0", "app.plugins.faulty_crash")
    set_default(c, "fault-crash", "1.0.0")
    t = submit(c, request_key="demo-crash", plugin="fault-crash",
               document="[[crash]]\npartial output then death\n")
    t = wait_task(c, t["id"], timeout_s=30)
    check(t["status"] == "FAILED" and "exited" in (t["error"] or ""),
          f"crash task FAILED: {t['error']}")
    check(t["result"] is None, "partial result was NOT published")
    t = submit(c, request_key="demo-crash-ok", plugin="fault-crash",
               document="clean\ndocument\n")
    t = wait_task(c, t["id"])
    check(t["status"] == "SUCCEEDED", "pool recovered after crash (clean doc ok)")

    register(c, "fault-corrupt", "1.0.0", "app.plugins.faulty_bad_structure")
    set_default(c, "fault-corrupt", "1.0.0")
    t = submit(c, request_key="demo-corrupt", plugin="fault-corrupt",
               document="[[corrupt]] please break the protocol\n")
    t = wait_task(c, t["id"], timeout_s=30)
    check(t["status"] == "FAILED" and "protocol violation" in (t["error"] or ""),
          f"corrupt task FAILED: {t['error']}")
    check(c.get("/health").json()["status"] == "ok", "service still healthy after faults")

    step("7. failed candidate validation keeps the current default")
    register(c, "line-text", "9.9.9-badinit", "app.plugins.faulty_bad_init")
    r = c.post("/plugins/line-text/default", json={"version": "9.9.9-badinit"})
    check(r.status_code == 400, f"incompatible candidate rejected ({r.status_code})")
    print(" ", brief(r.json().get("detail"), 160))
    register(c, "line-text", "9.9.9-missing", "app.plugins.no_such_module")
    r = c.post("/plugins/line-text/default", json={"version": "9.9.9-missing"})
    check(r.status_code == 400, f"non-startable candidate rejected ({r.status_code})")
    plugins = {p["name"]: p for p in c.get("/plugins").json()["plugins"]}
    check(plugins["line-text"]["default"] == "2.0.0",
          f"default unchanged: {plugins['line-text']['default']}")

    step("summary")
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED:")
        for f in FAILURES:
            print("  -", f)
        sys.exit(1)
    print("all checks passed")
    print("\nNext: restart drill -> python scripts/restart_drill.py "
          "(or see README '故障演练')")


if __name__ == "__main__":
    main()
