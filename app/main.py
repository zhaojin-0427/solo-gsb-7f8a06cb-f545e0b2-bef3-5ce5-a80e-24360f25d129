"""HTTP API 与应用装配。"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.config import settings
from app.db import (
    CANCELLED,
    COMPLETED,
    RETIRED,
    PENDING,
    Database,
    document_fingerprint,
)
from app.runtime.supervisor import HandshakeFailed, Supervisor

logging.basicConfig(
    level=os.environ.get("PARSER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("api")


# ---- 请求/响应模型 --------------------------------------------------------


class RegisterVersionIn(BaseModel):
    plugin: str = Field(..., min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.\-]+$")
    version: str = Field(..., min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.\-]+$")
    entrypoint: str = Field(..., min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)
    make_default: bool = False


class SetDefaultIn(BaseModel):
    plugin: str
    version: str


class ParseIn(BaseModel):
    plugin: str = Field(..., min_length=1)
    version: str | None = None
    request_key: str = Field(..., min_length=1, max_length=200)
    document: str = Field("", description="文档原文（Unicode 字符串）")


class TaskOut(BaseModel):
    task_id: str
    plugin: str
    version: str
    status: str
    request_key: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: float
    updated_at: float
    blocks: list[dict[str, Any]] | None = None
    block_count: int | None = None


# ---- 仓库内随服务交付的可信插件种子 ---------------------------------------

SEED_VERSIONS = [
    {
        "plugin": "line-text",
        "version": "1.0",
        "entrypoint": "app.plugins.line_text",
        "config": {"batch_size": 64},
        "default": True,
    },
    {
        "plugin": "markdown-sections",
        "version": "1.0",
        "entrypoint": "app.plugins.markdown_sections",
        "config": {"batch_size": 32},
        "default": True,
    },
    # —— 故障样例（仓库内可信代码；默认提交冒烟文档，可通过校验）——
    {
        "plugin": "fault-timeout",
        "version": "1.0",
        "entrypoint": "app.plugins.fault_timeout",
        "config": {},
    },
    {
        "plugin": "fault-timeout",
        "version": "2.0",
        "entrypoint": "app.plugins.fault_timeout",
        "config": {},
    },
    {
        "plugin": "fault-crash",
        "version": "1.0",
        "entrypoint": "app.plugins.fault_crash",
        "config": {},
    },
    {
        "plugin": "fault-error",
        "version": "1.0",
        "entrypoint": "app.plugins.fault_error",
        "config": {},
    },
    {
        "plugin": "fault-bad",
        "version": "1.0",
        "entrypoint": "app.plugins.fault_bad",
        "config": {},
    },
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(os.path.abspath(settings.db_path)) or ".", exist_ok=True)
    db = Database(settings.db_path)
    supervisor = Supervisor(db)

    # 幂等播种仓库内插件版本；随后校验并设定默认。
    # 关键规则：校验失败时保持原默认版本不动。
    for seed in SEED_VERSIONS:
        inserted = db.insert_version(
            seed["plugin"], seed["version"], seed["entrypoint"], seed["config"]
        )
        if inserted:
            log.info("seeded plugin version %s@%s", seed["plugin"], seed["version"])

    for seed in SEED_VERSIONS:
        if not seed.get("default"):
            continue
        plugin, version = seed["plugin"], seed["version"]
        if db.get_default(plugin) is not None:
            continue
        try:
            await supervisor.validate_candidate(seed["entrypoint"], seed["config"])
            db.set_default(plugin, version)
            log.info("default set: %s@%s", plugin, version)
        except HandshakeFailed as exc:
            log.error("种子默认版本校验失败，%s 暂无默认版本：%s", plugin, exc)

    await supervisor.start_from_db()

    app.state.db = db
    app.state.supervisor = supervisor
    try:
        yield
    finally:
        await supervisor.shutdown()
        db.close()


app = FastAPI(title="Local Document Parser Plugin Service", lifespan=lifespan)


def db_of(request: Request) -> Database:
    return request.app.state.db


def svc_of(request: Request) -> Supervisor:
    return request.app.state.supervisor


def _task_out(t: dict[str, Any], blocks: list[dict[str, Any]] | None = None) -> TaskOut:
    return TaskOut(
        task_id=t["id"],
        plugin=t["plugin"],
        version=t["version"],
        status=t["status"],
        request_key=t.get("request_key"),
        error_code=t.get("error_code"),
        error_message=t.get("error_message"),
        created_at=t["created_at"],
        updated_at=t["updated_at"],
        blocks=blocks,
        block_count=len(blocks) if blocks is not None else None,
    )


# ---- 健康/元数据 ----------------------------------------------------------


@app.get("/health")
async def health(request: Request) -> dict[str, Any]:
    db = db_of(request)
    return {
        "status": "ok",
        "defaults": db.list_defaults(),
        "pools": [
            {"plugin": p.plugin, "version": p.version, "status": p.status,
             "workers": len(p.workers), "refcount": p.refcount}
            for p in svc_of(request).pools.values()
        ],
    }


@app.get("/plugins")
async def list_plugins(request: Request) -> dict[str, Any]:
    db = db_of(request)
    versions = db.list_versions()
    defaults = db.list_defaults()
    for v in versions:
        v["is_default"] = defaults.get(v["plugin"]) == v["version"]
        v.pop("config", None)
    return {"versions": versions, "defaults": defaults}


# ---- 版本登记与切换 -------------------------------------------------------


@app.post("/plugins/register")
async def register_version(request: Request, response: Response, body: RegisterVersionIn) -> dict[str, Any]:
    db = db_of(request)
    supervisor = svc_of(request)

    existing = db.get_version(body.plugin, body.version)
    if existing is not None:
        # 版本不可变：entrypoint/config 任何差异都拒绝；完全相同则幂等。
        if existing["entrypoint"] != body.entrypoint or existing["config"] != body.config:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "VERSION_IMMUTABLE",
                    "message": f"{body.plugin}@{body.version} 已登记为不可变版本，"
                              "entrypoint/config 不得修改",
                },
            )
        return {"plugin": body.plugin, "version": body.version, "registered": False,
                "message": "版本已存在（不可变），内容一致，幂等返回"}

    # 先验证候选：初始化握手 + 协议兼容 + 冒烟解析，任何失败都不落库、
    # 不影响当前默认版本。
    try:
        await supervisor.validate_candidate(body.entrypoint, body.config)
    except HandshakeFailed as exc:
        raise HTTPException(
            status_code=422,
            detail={"error": "VALIDATION_FAILED", "message": str(exc)},
        )

    response.status_code = 201
    db.insert_version(body.plugin, body.version, body.entrypoint, body.config)

    became_default = False
    if body.make_default:
        old_default = db.get_default(body.plugin)
        db.set_default(body.plugin, body.version)
        await supervisor.activate_version(body.plugin, body.version)
        if old_default:
            await supervisor.switch_default(body.plugin, old_default, body.version)
        became_default = True

    return {
        "plugin": body.plugin,
        "version": body.version,
        "registered": True,
        "became_default": became_default,
        "default_version": db.get_default(body.plugin),
    }


@app.post("/plugins/default")
async def set_default(request: Request, body: SetDefaultIn) -> dict[str, Any]:
    db = db_of(request)
    supervisor = svc_of(request)
    info = db.get_version(body.plugin, body.version)
    if info is None:
        raise HTTPException(404, detail={"error": "UNKNOWN_VERSION",
                                        "message": f"{body.plugin}@{body.version} 未登记"})
    if info["status"] == RETIRED:
        raise HTTPException(409, detail={"error": "VERSION_RETIRED",
                                        "message": "该版本已退出，不能设为默认"})

    old_default = db.get_default(body.plugin)
    if old_default == body.version:
        return {"plugin": body.plugin, "default_version": body.version, "changed": False}

    # 切默认之前再做一次兼容校验；失败则原默认保持不变。
    try:
        await supervisor.validate_candidate(info["entrypoint"], info["config"])
        await supervisor.activate_version(body.plugin, body.version)
    except HandshakeFailed as exc:
        raise HTTPException(
            422,
            detail={"error": "VALIDATION_FAILED", "message": str(exc),
                    "default_unchanged": old_default},
        )

    db.set_default(body.plugin, body.version)
    await supervisor.switch_default(body.plugin, old_default, body.version)
    return {"plugin": body.plugin, "default_version": body.version,
            "previous_default": old_default, "changed": True}


# ---- 解析任务 -------------------------------------------------------------


@app.post("/parse", status_code=202)
async def submit_parse(request: Request, body: ParseIn) -> dict[str, Any]:
    db = db_of(request)
    supervisor = svc_of(request)

    if len(body.document) > settings.max_document_codepoints:
        raise HTTPException(
            413, detail={"error": "DOCUMENT_TOO_LARGE",
                         "message": f"文档超过 {settings.max_document_codepoints} 码点上限"}
        )

    # 1) 解析目标版本：显式指定 > 当前默认。版本在任务接收这一刻冻结。
    version = body.version or db.get_default(body.plugin)
    if not version:
        raise HTTPException(
            404, detail={"error": "NO_DEFAULT_VERSION",
                         "message": f"插件 {body.plugin} 没有可用默认版本，"
                                    "且请求未指定 version"}
        )
    info = db.get_version(body.plugin, version)
    if info is None:
        raise HTTPException(404, detail={"error": "UNKNOWN_VERSION",
                                        "message": f"{body.plugin}@{version} 未登记"})
    if info["status"] == RETIRED:
        raise HTTPException(409, detail={"error": "VERSION_RETIRED",
                                        "message": "该插件版本已退出"}
                            )

    fingerprint = document_fingerprint(body.document)

    # 2) 幂等键：同键同文档返回原任务；同键异文档拒绝（不新建任务）。
    existing_task_id, existing_hash = db.register_request(
        body.plugin, body.request_key, fingerprint
    )
    if existing_hash is not None:
        if existing_hash != fingerprint:
            raise HTTPException(
                409,
                detail={
                    "error": "REQUEST_KEY_DOCUMENT_MISMATCH",
                    "message": "该 request_key 已绑定另一份文档；拒绝复用",
                    "existing_task_id": existing_task_id or None,
                },
            )
        task = db.get_task(existing_task_id)
        if task is None:
            # 理论不可能（占位回填失败）；保守报 500。
            raise HTTPException(500, detail={"error": "INCONSISTENT_REQUEST_KEY"})
        return {
            "task_id": task["id"],
            "status": task["status"],
            "reused": True,
            "plugin": body.plugin,
            "version": task["version"],
            "message": "同请求键同文档，返回原任务",
        }

    # 3) 新任务：创建后回填幂等键，再投递到“该冻结版本”的进程池。
    task_id = db.create_task(body.plugin, version, body.document, body.request_key)
    db.bind_request_task(body.plugin, body.request_key, task_id)
    try:
        await supervisor.submit(task_id, body.plugin, version)
    except HandshakeFailed as exc:
        db.fail_task(task_id, "POOL_UNAVAILABLE", str(exc))
        raise HTTPException(503, detail={"error": "POOL_UNAVAILABLE",
                                         "message": str(exc), "task_id": task_id})

    return {"task_id": task_id, "status": PENDING, "reused": False,
            "plugin": body.plugin, "version": version,
            "message": "任务已接收并绑定到具体版本"}


@app.get("/tasks/{task_id}")
async def get_task(request: Request, task_id: str, include_blocks: bool = True) -> TaskOut:
    db = db_of(request)
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(404, detail={"error": "TASK_NOT_FOUND"})
    blocks = None
    if include_blocks and task["status"] == COMPLETED:
        blocks = db.get_blocks(task_id)
    return _task_out(task, blocks)


@app.get("/tasks/{task_id}/blocks")
async def get_task_blocks(request: Request, task_id: str) -> dict[str, Any]:
    db = db_of(request)
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(404, detail={"error": "TASK_NOT_FOUND"})
    if task["status"] != COMPLETED:
        raise HTTPException(409, detail={"error": "TASK_NOT_COMPLETED",
                                        "status": task["status"]})
    blocks = db.get_blocks(task_id)
    return {"task_id": task_id, "count": len(blocks), "blocks": blocks}


@app.get("/tasks/{task_id}/events")
async def get_task_events(request: Request, task_id: str) -> dict[str, Any]:
    db = db_of(request)
    if db.get_task(task_id) is None:
        raise HTTPException(404, detail={"error": "TASK_NOT_FOUND"})
    return {"task_id": task_id, "events": db.get_events(task_id)}


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: str) -> dict[str, Any]:
    db = db_of(request)
    supervisor = svc_of(request)
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(404, detail={"error": "TASK_NOT_FOUND"})

    cancelled, is_terminal = db.cancel_task(task_id)
    if cancelled:
        # 通知执行器终止 PENDING 等待或杀掉 RUNNING 子进程。
        supervisor.request_cancel(task_id)
        return {"task_id": task_id, "status": CANCELLED, "cancelled": True}

    # 取消与完成竞争：只有一方成功。
    current = db.get_task(task_id)
    if current["status"] == CANCELLED:
        return {"task_id": task_id, "status": CANCELLED, "cancelled": False,
                "message": "任务此前已取消（幂等）"}
    raise HTTPException(
        409,
        detail={"error": "TASK_ALREADY_TERMINAL", "status": current["status"],
                "message": "任务已处于其他终态，取消失败"},
    )
