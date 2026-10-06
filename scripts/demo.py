#!/usr/bin/env python3
"""本地故障演练脚本（仅用标准库，便于在容器内直接运行）。

对一个已运行的服务（默认 http://127.0.0.1:8000）依次演示：

1. 可信插件正常解析（逐行 / Markdown 分节），展示码点区间；
2. 在途任务期间切换默认版本（新任务绑定新版本，旧任务钉在旧版本）；
3. 取消一个在途的超时任务，验证取消成功、重复取消幂等；
4. 子进程崩溃 / 输出截断 / 结构化错误 / 超时：只有对应任务失败，
   同版本进程池补员后后续任务成功；
5. 同请求键同文档返回原任务、异文档 409；
6. 迟到消息不会污染复用槽位的新任务。

每一步都会打印期望与实际结果，最终给出汇总。重启恢复演练由
``scripts/restart_demo.sh`` 配合 Compose 完成（需外部重启服务）。
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"
TIMEOUT_VERSION = "1.0"


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def submit(plugin: str, key: str, document: str, version: str | None = None) -> dict:
    body: dict = {"plugin": plugin, "request_key": key, "document": document}
    if version:
        body["version"] = version
    status, resp = call("POST", "/parse", body)
    return {"http": status, **resp}


def wait_terminal(task_id: str, timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, t = call("GET", f"/tasks/{task_id}")
        if t["status"] in ("COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
            return t
        time.sleep(0.1)
    raise RuntimeError(f"task {task_id} 未在 {timeout}s 内结束")


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def check(name: str, ok: bool, detail: str = "") -> None:
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        check.failed += 1
check.failed = 0


def main() -> int:
    section("0. 健康检查")
    status, health = call("GET", "/health")
    check("服务在线", status == 200, f"{status}")
    print("当前默认版本:", health.get("defaults"))

    section("1. 可信插件：逐行文本（码点区间覆盖全文）")
    doc = "alpha\n第二行\n\n尾行"
    r = submit("line-text", "demo-line", doc)
    t = wait_terminal(r["task_id"])
    check("逐行任务 COMPLETED", t["status"] == "COMPLETED", t["status"])
    _, blocks_resp = call("GET", f"/tasks/{r['task_id']}/blocks")
    blocks = blocks_resp["blocks"]
    covered = "".join(b["text"] for b in blocks)
    check("块文本拼接 == 原文", covered == doc)
    check("区间紧密无重叠",
          all(blocks[i]["end"] == blocks[i + 1]["start"] for i in range(len(blocks) - 1))
          and blocks[0]["start"] == 0 and blocks[-1]["end"] == len(doc))
    check("每块 text == document[start:end]",
          all(doc[b["start"]:b["end"]] == b["text"] for b in blocks))
    print(f"块数={len(blocks)}，类型样例={[b['type'] for b in blocks][:6]}")

    section("1b. 可信插件：Markdown 分节")
    md = "# 标题一\n正文 A\n## 小节\n正文 B\n"
    r = submit("markdown-sections", "demo-md", md)
    t = wait_terminal(r["task_id"])
    _, br = call("GET", f"/tasks/{r['task_id']}/blocks")
    headings = [(b["text"], b.get("level")) for b in br["blocks"] if b["type"] == "heading"]
    check("Markdown 任务 COMPLETED", t["status"] == "COMPLETED", t["status"])
    check("识别出两级标题", headings == [("# 标题一", 1), ("## 小节", 2)], str(headings))

    section("2. 幂等键：同键同文档 / 同键异文档")
    a = submit("line-text", "demo-idem", "同一份\n")
    b = submit("line-text", "demo-idem", "同一份\n")
    check("同键同文档返回原任务", a["task_id"] == b["task_id"] and b.get("reused") is True)
    c = submit("line-text", "demo-idem", "不一样\n")
    check("同键异文档 409 拒绝", c["http"] == 409,
          c.get("detail", {}).get("error", ""))

    section("3. 在途任务期间切换默认版本")
    call("POST", "/plugins/default", {"plugin": "fault-timeout", "version": "1.0"})
    inflight = submit("fault-timeout", "demo-pinned", "挂起 [[timeout]] 旧版本",
                      version=TIMEOUT_VERSION)
    print("旧版本在途任务:", inflight["task_id"], inflight["version"])
    time.sleep(0.8)
    s, sw = call("POST", "/plugins/default",
                 {"plugin": "fault-timeout", "version": "2.0"})
    check("默认切换 2.0 成功", s == 200, str(sw))
    new_task = submit("fault-timeout", "demo-after-switch", "切换后新任务\n")
    check("新任务绑定 2.0", new_task.get("version") == "2.0", str(new_task.get("version")))
    tnew = wait_terminal(new_task["task_id"])
    check("新版本任务 COMPLETED", tnew["status"] == "COMPLETED", tnew["status"])
    _, old = call("GET", f"/tasks/{inflight['task_id']}")
    check("旧任务仍钉在 1.0 且仍在运行",
          old["version"] == "1.0" and old["status"] in ("PENDING", "RUNNING"),
          f"{old['version']}/{old['status']}")
    _, hp = call("GET", "/health")
    old_pool = next((p for p in hp["pools"]
                     if p["plugin"] == "fault-timeout" and p["version"] == "1.0"), None)
    check("旧版本进入 DRAINING（未强制退出）",
          bool(old_pool) and old_pool["status"] == "DRAINING",
          str(old_pool))

    section("4. 取消在途任务（取消/完成竞争）")
    s, cancel = call("POST", f"/tasks/{inflight['task_id']}/cancel")
    tc = wait_terminal(inflight["task_id"], timeout=8)
    check("取消成功 200 且任务 CANCELLED", s == 200 and tc["status"] == "CANCELLED",
          f"{s}/{tc['status']}")
    s2, _ = call("POST", f"/tasks/{inflight['task_id']}/cancel")
    check("重复取消幂等 200", s2 == 200, str(s2))
    s3, blocks409 = call("GET", f"/tasks/{inflight['task_id']}/blocks")
    check("被取消任务无结果可取(409)", s3 == 409, str(s3))

    # 已完成任务再取消必须失败——竞争只有一方成功。
    done_task = submit("line-text", "demo-cancel-race", "很快\n")
    tdone = wait_terminal(done_task["task_id"])
    s4, _ = call("POST", f"/tasks/{tdone['task_id']}/cancel")
    check("完成后取消返回 409", s4 == 409, str(s4))
    _, still = call("GET", f"/tasks/{tdone['task_id']}")
    check("结果仍为 COMPLETED（无双终态）", still["status"] == "COMPLETED")

    # 排水结束，旧版本退役。
    retired = False
    for _ in range(50):
        _, vers = call("GET", "/plugins")
        v1 = next(v for v in vers["versions"]
                  if v["plugin"] == "fault-timeout" and v["version"] == "1.0")
        if v1["status"] == "RETIRED":
            retired = True
            break
        time.sleep(0.2)
    check("旧版本在在途任务结束后 RETIRED", retired)
    rejected = submit("fault-timeout", "demo-after-retire", "x\n", version="1.0")
    check("退役版本拒绝新任务 409", rejected["http"] == 409, str(rejected["http"]))

    section("5. 子进程崩溃 / 截断 / 结构化错误 / 超时 —— 只影响本任务")
    crash = submit("fault-crash", "demo-crash", "前文\n[[crash]]\n", version="1.0")
    tcrash = wait_terminal(crash["task_id"])
    check("崩溃任务 FAILED(WORKER_CRASHED)",
          tcrash["status"] == "FAILED" and tcrash["error_code"] == "WORKER_CRASHED",
          f"{tcrash['status']}/{tcrash.get('error_code')}")

    partial = submit("fault-crash", "demo-partial",
                     "半个结果 [[crash_after_blocks]] 尾巴", version="1.0")
    tp = wait_terminal(partial["task_id"])
    sp, _ = call("GET", f"/tasks/{partial['task_id']}/blocks")
    check("部分结果后崩溃：任务 FAILED 且块绝不发布(409)",
          tp["status"] == "FAILED" and sp == 409,
          f"{tp['status']}/{sp}")

    recover = submit("fault-crash", "demo-recover", "崩溃后恢复任务\n", version="1.0")
    trec = wait_terminal(recover["task_id"], timeout=20)
    check("崩溃槽位补员后后续任务 COMPLETED", trec["status"] == "COMPLETED",
          trec["status"])

    err = submit("fault-error", "demo-error", "带 [[error]] 标记\n", version="1.0")
    te = wait_terminal(err["task_id"])
    check("结构化错误 -> FAILED/PLUGIN_ERROR",
          te["status"] == "FAILED" and te["error_code"] == "PLUGIN_ERROR",
          f"{te['status']}/{te.get('error_code')}")
    err_ok = submit("fault-error", "demo-error-ok", "健康任务\n", version="1.0")
    check("返回结构化错误的 worker 仍存活，后续任务 COMPLETED",
          wait_terminal(err_ok["task_id"])["status"] == "COMPLETED")

    tmo = submit("fault-timeout", "demo-timeout", "挂 [[timeout]]", version="2.0")
    tt = wait_terminal(tmo["task_id"], timeout=40)
    check("超时任务 FAILED(TIMEOUT)",
          tt["status"] == "FAILED" and tt["error_code"] == "TIMEOUT",
          f"{tt['status']}/{tt.get('error_code')}")
    tmo_ok = submit("fault-timeout", "demo-timeout-ok", "超时后恢复\n", version="2.0")
    check("超时杀进程后同版本其他任务 COMPLETED",
          wait_terminal(tmo_ok["task_id"], timeout=30)["status"] == "COMPLETED")

    section("6. 损坏输出：乱序/空洞/重叠/文本不符/坏 JSON")
    cases = [
        ("demo-bad-json", "xx [[bad_json]]", "CORRUPT_OUTPUT"),
        ("demo-bad-order", "xx [[bad_order]]", "OUT_OF_ORDER"),
        ("demo-gap", "ab\n[[gap]]", "GAP_IN_RANGES"),
        ("demo-overlap", "abcd\n[[overlap]]", "OVERLAPPING_RANGES"),
        ("demo-wrongtext", "abcd\n[[wrong_text]]", "TEXT_MISMATCH"),
        ("demo-truncated", "abcd\n[[truncated]]", "WORKER_CRASHED"),
    ]
    for key, doc_text, code in cases:
        rr = submit("fault-bad", key, doc_text, version="1.0")
        ttt = wait_terminal(rr["task_id"], timeout=20)
        check(f"{code} 被拒：任务 FAILED",
              ttt["status"] == "FAILED" and ttt.get("error_code") == code,
              f"{ttt['status']}/{ttt.get('error_code')}")
    after = submit("fault-bad", "demo-bad-after", "全部坏任务之后的健康任务\n",
                   version="1.0")
    check("坏输出槽位被替换，后续健康任务 COMPLETED",
          wait_terminal(after["task_id"], timeout=20)["status"] == "COMPLETED")

    section("7. 迟到消息不得写入复用槽位的新任务")
    late = submit("fault-bad", "demo-late",
                  "本任务正常 [[late:0000000000000000deadbeefdeadbeef]]", version="1.0")
    tlate = wait_terminal(late["task_id"])
    check("携带迟到帧的当前任务正常 COMPLETED", tlate["status"] == "COMPLETED",
          tlate["status"])
    time.sleep(0.3)
    reuse = submit("fault-bad", "demo-late-reuse", "复用槽位的新任务必须干净\n",
                   version="1.0")
    treuse = wait_terminal(reuse["task_id"], timeout=20)
    _, reuse_blocks = call("GET", f"/tasks/{reuse['task_id']}/blocks")
    no_stale = all(b.get("text") != "STALE" for b in reuse_blocks["blocks"])
    check("新任务 COMPLETED 且不含迟到块 STALE",
          treuse["status"] == "COMPLETED" and no_stale)

    section("汇总")
    if check.failed == 0:
        print("全部演练断言通过 ✔")
        return 0
    print(f"有 {check.failed} 项断言失败 ✗")
    return 1


if __name__ == "__main__":
    sys.exit(main())
