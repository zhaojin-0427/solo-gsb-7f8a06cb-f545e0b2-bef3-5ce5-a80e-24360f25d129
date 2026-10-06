"""端到端集成测试：通过 ASGITransport 直接驱动 FastAPI，
插件子进程使用与生产相同的独立子进程通道。
"""

from __future__ import annotations

import asyncio
import importlib
import os
import sys
import time

import pytest
from httpx import ASGITransport, AsyncClient

# 确保仓库根在 sys.path（pytest 从仓库根运行时通常已经满足）。
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 每个测试用独立的临时数据库；worker 池保持最小规模加快测试。
os.environ.setdefault("PARSER_WORKERS_PER_VERSION", "2")
os.environ.setdefault("PARSER_TASK_TIMEOUT_SECONDS", "8")


@pytest.fixture()
async def client(tmp_path):
    db_path = tmp_path / "test.db"
    os.environ["PARSER_DB"] = str(db_path)

    # 配置在导入时读取环境变量，需要重载。
    from app import config as config_mod
    importlib.reload(config_mod)

    from app import main as main_mod
    importlib.reload(main_mod)

    transport = ASGITransport(app=main_mod.app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # lifespan 需要显式触发；AsyncClient 的 ASGITransport 在
        # httpx 0.28 上不自动跑 lifespan，手动管理。
        async with main_mod.lifespan(main_mod.app):
            yield ac

    # lifespan 退出即完成关停。


async def _submit(ac, **kw):
    r = await ac.post("/parse", json=kw)
    assert r.status_code in (202, 409, 404, 503), r.text
    return r


async def _wait_terminal(ac, task_id, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = await ac.get(f"/tasks/{task_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] in ("COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} 未在 {timeout}s 内结束")


# ---- 基础解析 -------------------------------------------------------------


@pytest.mark.asyncio
async def test_line_text_blocks_cover_document(client):
    doc = "alpha\nbeta gamma\n\n尾行"
    r = await _submit(client, plugin="line-text", request_key="k-line", document=doc)
    assert r.status_code == 202
    task_id = r.json()["task_id"]
    assert r.json()["version"] == "1.0"

    final = await _wait_terminal(client, task_id)
    assert final["status"] == "COMPLETED"

    r = await client.get(f"/tasks/{task_id}/blocks")
    blocks = r.json()["blocks"]
    # 区间严格覆盖原文。
    assert "".join(b["text"] for b in blocks) == doc
    cursor = 0
    for b in blocks:
        assert b["start"] == cursor
        assert doc[b["start"]:b["end"]] == b["text"]
        cursor = b["end"]
    assert cursor == len(doc)


@pytest.mark.asyncio
async def test_markdown_sections_headings(client):
    doc = "# 标题一\n正文\n## 小节\n更多\n"
    r = await _submit(client, plugin="markdown-sections", request_key="k-md",
                      document=doc)
    task_id = r.json()["task_id"]
    final = await _wait_terminal(client, task_id)
    assert final["status"] == "COMPLETED"
    blocks = (await client.get(f"/tasks/{task_id}/blocks")).json()["blocks"]
    headings = [b for b in blocks if b["type"] == "heading"]
    assert [(h["text"], h["level"]) for h in headings] == [
        ("# 标题一", 1), ("## 小节", 2)
    ]
    assert "".join(b["text"] for b in blocks) == doc


@pytest.mark.asyncio
async def test_empty_document_completes_with_zero_blocks(client):
    r = await _submit(client, plugin="line-text", request_key="k-empty", document="")
    task_id = r.json()["task_id"]
    final = await _wait_terminal(client, task_id)
    assert final["status"] == "COMPLETED"
    blocks = (await client.get(f"/tasks/{task_id}/blocks")).json()["blocks"]
    assert blocks == []


# ---- 幂等键 ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_key_same_document_returns_same_task(client):
    payload = dict(plugin="line-text", request_key="idem-1", document="hello\n")
    r1 = await _submit(client, **payload)
    r2 = await _submit(client, **payload)
    assert r1.json()["task_id"] == r2.json()["task_id"]
    assert r2.json()["reused"] is True


@pytest.mark.asyncio
async def test_same_key_different_document_rejected(client):
    r1 = await _submit(client, plugin="line-text", request_key="idem-conflict",
                       document="version A\n")
    assert r1.status_code == 202
    r2 = await _submit(client, plugin="line-text", request_key="idem-conflict",
                       document="version B\n")
    assert r2.status_code == 409
    assert r2.json()["detail"]["error"] == "REQUEST_KEY_DOCUMENT_MISMATCH"


# ---- 故障插件 -------------------------------------------------------------


@pytest.mark.asyncio
async def test_structured_error_task_fails_but_worker_lives(client):
    r = await _submit(client, plugin="fault-error", version="1.0", request_key="e1",
                      document="before [[error]] after\n")
    task_id = r.json()["task_id"]
    final = await _wait_terminal(client, task_id)
    assert final["status"] == "FAILED"
    assert final["error_code"] == "PLUGIN_ERROR"

    # 同池后续任务应正常（结构化 error 不杀 worker）。
    r2 = await _submit(client, plugin="fault-error", version="1.0", request_key="e2",
                       document="healthy\n")
    final2 = await _wait_terminal(client, r2.json()["task_id"])
    assert final2["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_crash_kills_only_that_task_and_pool_replaces_worker(client):
    r = await _submit(client, plugin="fault-crash", version="1.0", request_key="c1",
                      document="data\n[[crash]]\n")
    final = await _wait_terminal(client, r.json()["task_id"])
    assert final["status"] == "FAILED"
    assert final["error_code"] in ("WORKER_CRASHED",)

    # 崩溃后池补员：同版本后续任务仍可完成。
    r2 = await _submit(client, plugin="fault-crash", version="1.0", request_key="c2",
                       document="recovered\n")
    final2 = await _wait_terminal(client, r2.json()["task_id"], timeout=15)
    assert final2["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_crash_after_partial_blocks_is_not_published(client):
    r = await _submit(client, plugin="fault-crash", version="1.0", request_key="c3",
                      document="partial\n[[crash_after_blocks]] tail")
    final = await _wait_terminal(client, r.json()["task_id"])
    assert final["status"] == "FAILED"
    assert final["block_count"] is None
    # 完成查询必须 409（不存在“COMPLETED 但缺块”）。
    rb = await client.get(f"/tasks/{final['task_id']}/blocks")
    assert rb.status_code == 409


@pytest.mark.asyncio
async def test_timeout_fails_task_and_other_workers_continue(client):
    r = await _submit(client, plugin="fault-timeout", version="1.0", request_key="t1",
                      document="hang [[timeout]]\n")
    task_id = r.json()["task_id"]
    # 超时阈值 8s；给足余量。
    final = await _wait_terminal(client, task_id, timeout=20)
    assert final["status"] == "FAILED"
    assert final["error_code"] == "TIMEOUT"

    r2 = await _submit(client, plugin="fault-timeout", version="1.0", request_key="t2",
                       document="after timeout ok\n")
    final2 = await _wait_terminal(client, r2.json()["task_id"], timeout=20)
    assert final2["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_corrupt_output_variants_fail_current_task_only(client):
    cases = [
        ("b1", "x [[bad_json]]\n", "CORRUPT_OUTPUT"),
        ("b2", "x [[bad_order]]\n", "OUT_OF_ORDER"),
        ("b3", "abc\n", "GAP_IN_RANGES"),  # 需要 gap 标记，见下
    ]
    # gap/overlap/wrong_text 用明确标记。
    marked = [
        ("gap-k", "ab\ncd\n[[gap]]", {"GAP_IN_RANGES"}),
        ("ov-k", "abcd\n[[overlap]]", {"OVERLAPPING_RANGES"}),
        ("wt-k", "abcd\n[[wrong_text]]", {"TEXT_MISMATCH"}),
        ("tr-k", "abcd\n[[truncated]]", {"WORKER_CRASHED"}),
    ]
    _ = cases
    for key, doc, codes in marked:
        r = await _submit(client, plugin="fault-bad", version="1.0", request_key=key, document=doc)
        final = await _wait_terminal(client, r.json()["task_id"])
        assert final["status"] == "FAILED", (key, final)
        assert final["error_code"] in codes, (key, final["error_code"])

    # 坏任务之后同版本健康任务照常完成（槽位被替换/隔离）。
    r = await _submit(client, plugin="fault-bad", version="1.0", request_key="after-bad",
                      document="clean doc\n")
    final = await _wait_terminal(client, r.json()["task_id"])
    assert final["status"] == "COMPLETED"


# ---- 取消 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_inflight_timeout_task(client):
    r = await _submit(client, plugin="fault-timeout", version="1.0", request_key="cancel-1",
                      document="will hang [[timeout]] zzz")
    task_id = r.json()["task_id"]
    # 等任务进入 RUNNING 再取消。
    await asyncio.sleep(1.0)
    rc = await client.post(f"/tasks/{task_id}/cancel")
    assert rc.status_code == 200
    final = await _wait_terminal(client, task_id, timeout=5)
    assert final["status"] == "CANCELLED"
    # 重复取消幂等返回 200。
    rc2 = await client.post(f"/tasks/{task_id}/cancel")
    assert rc2.status_code == 200
    # 已取消任务不能再查到块。
    rb = await client.get(f"/tasks/{task_id}/blocks")
    assert rb.status_code == 409


@pytest.mark.asyncio
async def test_cancel_after_complete_conflict(client):
    r = await _submit(client, plugin="line-text", request_key="cancel-2",
                      document="fast\n")
    final = await _wait_terminal(client, r.json()["task_id"])
    assert final["status"] == "COMPLETED"
    rc = await client.post(f"/tasks/{final['task_id']}/cancel")
    assert rc.status_code == 409
    # 结果仍可查，未被取消覆盖。
    blocks = (await client.get(f"/tasks/{final['task_id']}/blocks")).json()["blocks"]
    assert "".join(b["text"] for b in blocks) == "fast\n"


# ---- 迟到帧 ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_late_frame_after_finish_dropped_and_slot_reused(client):
    # 用不存在的“旧任务”标识制造迟到帧；当前任务必须正常完成，
    # worker 被隔离并替换，后续任务也必须正常。
    doc = "ok\n[[late:0123456789abcdef0123456789abcdef]]"
    r = await _submit(client, plugin="fault-bad", version="1.0", request_key="late-1", document=doc)
    final = await _wait_terminal(client, r.json()["task_id"])
    assert final["status"] == "COMPLETED"
    events = (await client.get(f"/tasks/{final['task_id']}/events")).json()["events"]
    # 给迟到帧一点到达时间。
    await asyncio.sleep(0.3)
    r2 = await _submit(client, plugin="fault-bad", version="1.0", request_key="late-2",
                       document="second task on possibly reused slot\n")
    final2 = await _wait_terminal(client, r2.json()["task_id"], timeout=15)
    assert final2["status"] == "COMPLETED", final2
    blocks = (await client.get(f"/tasks/{r2.json()['task_id']}/blocks")).json()["blocks"]
    assert all("STALE" != b["text"] for b in blocks)
    _ = events


# ---- 版本生命周期 ---------------------------------------------------------


@pytest.mark.asyncio
async def test_version_pinning_survives_default_switch_and_drain(client):
    # 0) 先令 1.0 成为默认。
    r0 = await client.post("/plugins/default",
                           json={"plugin": "fault-timeout", "version": "1.0"})
    assert r0.status_code == 200, r0.text

    # 1) 起一个在途的超时任务（提交时绑定当时的默认 1.0）。
    r = await _submit(client, plugin="fault-timeout",
                      request_key="pin-1", document="hang [[timeout]]")
    pinned = r.json()["task_id"]
    assert r.json()["version"] == "1.0"
    await asyncio.sleep(0.8)

    # 2) 在途期间把默认切到 2.0：新任务绑定 2.0，旧任务仍绑定 1.0。
    rs = await client.post("/plugins/default",
                           json={"plugin": "fault-timeout", "version": "2.0"})
    assert rs.status_code == 200, rs.text
    r_new = await _submit(client, plugin="fault-timeout", request_key="after-switch",
                          document="healthy new default\n")
    assert r_new.json()["version"] == "2.0"
    final_new = await _wait_terminal(client, r_new.json()["task_id"])
    assert final_new["version"] == "2.0"
    assert final_new["status"] == "COMPLETED"

    # 旧版本必须排水而不是立即消失；在途任务仍可取消（终态后版本才能退役）。
    health = (await client.get("/health")).json()
    old_pool = next(p for p in health["pools"]
                    if p["plugin"] == "fault-timeout" and p["version"] == "1.0")
    assert old_pool["status"] == "DRAINING"
    assert old_pool["refcount"] >= 1

    rc = await client.post(f"/tasks/{pinned}/cancel")
    assert rc.status_code == 200
    await _wait_terminal(client, pinned, timeout=5)

    # 等排水完成。
    deadline = time.time() + 10
    retired = False
    while time.time() < deadline:
        vers = (await client.get("/plugins")).json()["versions"]
        v1 = next(v for v in vers if v["plugin"] == "fault-timeout" and v["version"] == "1.0")
        if v1["status"] == "RETIRED":
            retired = True
            break
        await asyncio.sleep(0.2)
    assert retired, "旧版本未在在途任务结束后退役"

    # 退役版本拒绝新任务。
    r_old = await _submit(client, plugin="fault-timeout", version="1.0",
                          request_key="after-retire", document="x\n")
    assert r_old.status_code == 409


@pytest.mark.asyncio
async def test_register_immutable_version_rejects_mutation(client):
    body = {"plugin": "line-text", "version": "1.0",
            "entrypoint": "app.plugins.line_text", "config": {"batch_size": 64}}
    r = await client.post("/plugins/register", json=body)
    assert r.status_code == 200
    assert r.json()["registered"] is False

    body["entrypoint"] = "app.plugins.fault_crash"
    r2 = await client.post("/plugins/register", json=body)
    assert r2.status_code == 409
    assert r2.json()["detail"]["error"] == "VERSION_IMMUTABLE"


@pytest.mark.asyncio
async def test_register_unknown_entrypoint_fails_validation(client):
    r = await client.post("/plugins/register", json={
        "plugin": "line-text", "version": "9.9",
        "entrypoint": "app.plugins.does_not_exist", "config": {},
        "make_default": True,
    })
    assert r.status_code == 422
    # 原默认保持不变。
    defaults = (await client.get("/plugins")).json()["defaults"]
    assert defaults["line-text"] == "1.0"


@pytest.mark.asyncio
async def test_register_outside_whitelist_rejected(client):
    r = await client.post("/plugins/register", json={
        "plugin": "evil", "version": "1.0",
        "entrypoint": "os", "config": {},
    })
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_init_failure_keeps_original_default(client):
    # 登记一个初始化即退出的版本并试图设为默认：
    # 校验失败 -> 422，且不落库、不改默认。
    r = await client.post("/plugins/register", json={
        "plugin": "line-text", "version": "broken-1",
        "entrypoint": "app.plugins.fault_initfail", "config": {},
        "make_default": True,
    })
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "VALIDATION_FAILED"
    info = (await client.get("/plugins")).json()
    assert not any(v["plugin"] == "line-text" and v["version"] == "broken-1"
                   for v in info["versions"])
    assert info["defaults"]["line-text"] == "1.0"

    # 未登记的坏版本也不能通过 set_default 影响现状。
    r2 = await client.post("/plugins/default",
                           json={"plugin": "line-text", "version": "broken-1"})
    assert r2.status_code == 404
    assert (await client.get("/plugins")).json()["defaults"]["line-text"] == "1.0"


# ---- 重启 -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_restart_publishes_results_remain_unfinished_interrupted(tmp_path):
    """模拟同一 DB 文件上的服务重启：不复用任何内存状态。"""
    from app import config as config_mod
    from app import main as main_mod

    db_path = str(tmp_path / "restart.db")
    os.environ["PARSER_DB"] = db_path
    importlib.reload(config_mod)
    importlib.reload(main_mod)

    # ---- 第一次启动 ----
    async with main_mod.lifespan(main_mod.app):
        tr = ASGITransport(app=main_mod.app)
        async with AsyncClient(transport=tr, base_url="http://test") as c1:
            rd = await c1.post("/parse", json={
                "plugin": "line-text", "request_key": "persist-ok",
                "document": "persisted\n"})
            done_id = rd.json()["task_id"]
            await _wait_terminal(c1, done_id)

            rh = await c1.post("/parse", json={
                "plugin": "fault-timeout", "version": "1.0",
                "request_key": "persist-hang", "document": "hang [[timeout]] forever"})
            hang_id = rh.json()["task_id"]
            await asyncio.sleep(1.0)
            assert (await c1.get(f"/tasks/{hang_id}")).json()["status"] == "RUNNING"

    # lifespan 退出即旧进程死亡：在途子进程一并终止。

    # ---- 全新启动（新进程视角）----
    importlib.reload(config_mod)
    importlib.reload(main_mod)
    async with main_mod.lifespan(main_mod.app):
        tr = ASGITransport(app=main_mod.app)
        async with AsyncClient(transport=tr, base_url="http://test") as c2:
            # 已发布结果仍可查询。
            r = await c2.get(f"/tasks/{done_id}")
            assert r.status_code == 200
            assert r.json()["status"] == "COMPLETED"
            blocks = (await c2.get(f"/tasks/{done_id}/blocks")).json()["blocks"]
            assert "".join(b["text"] for b in blocks) == "persisted\n"

            # 未完成者有明确终态，而不是悬挂的 RUNNING。
            r2 = await c2.get(f"/tasks/{hang_id}")
            assert r2.json()["status"] == "INTERRUPTED"

            # 不会出现两份发布：块仍只有一份。
            rows = (await c2.get(f"/tasks/{done_id}/blocks")).json()
            assert rows["count"] == len(blocks)

            # 重启后新任务正常。
            r3 = await c2.post("/parse", json={
                "plugin": "line-text", "request_key": "after-restart",
                "document": "new\n",
            })
            final = await _wait_terminal(c2, r3.json()["task_id"])
            assert final["status"] == "COMPLETED"
