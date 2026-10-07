"""FastAPI surface for the document parsing plugin service."""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .db import Database, TERMINAL_STATES, ConflictError
from .runtime import ProbeError, Runtime
from .schemas import ENTRY_RE, DefaultSwitch, PluginRegister, TaskSubmit

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("api")

DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "app.db")
)
MAX_WORKERS = int(os.environ.get("POOL_MAX_WORKERS_PER_VERSION", "4"))
DEFAULT_TIMEOUT_MS = int(os.environ.get("TASK_DEFAULT_TIMEOUT_MS", "30000"))

db = Database(DB_PATH)
runtime = Runtime(db, max_workers_per_version=MAX_WORKERS,
                  default_timeout_ms=DEFAULT_TIMEOUT_MS)
STARTED_AT = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Recovery: anything left non-terminal by a previous run was interrupted
    # (crash, kill, restart).  It gets a clear FAILED state; published
    # results are terminal and remain untouched and queryable.
    n = db.fail_unfinished("interrupted: service restarted")
    if n:
        log.warning("recovery: marked %d interrupted task(s) FAILED", n)
    db.retire_draining_versions()
    yield
    await runtime.stop()


app = FastAPI(title="document-parsing-plugin-service", lifespan=lifespan)


# ------------------------------------------------------------------ helpers

def _task_view(row: dict) -> dict:
    return {
        "id": row["id"],
        "request_key": row["request_key"],
        "plugin": row["plugin_name"],
        "version": row["plugin_version"],
        "status": row["status"],
        "error": row["error"],
        "result": row["result"],
        "doc_hash": row["doc_hash"],
        "document_length": len(row["document"]),
        "timeout_ms": row["timeout_ms"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _plugin_view(row: dict) -> dict:
    return {
        **row,
        "is_default": db.get_default(row["name"]) == row["version"],
        **runtime.version_stats(row["name"], row["version"]),
    }


# ------------------------------------------------------------------ health

@app.get("/health")
async def health():
    return {"status": "ok", "uptime_s": round(time.time() - STARTED_AT, 3)}


# ----------------------------------------------------------------- plugins

@app.post("/plugins", status_code=201)
async def register_plugin(body: PluginRegister):
    if not ENTRY_RE.fullmatch(body.entry):
        raise HTTPException(
            422,
            "entry must be a module under app.plugins.* "
            "(only trusted in-repo plugins are accepted)",
        )
    try:
        row, created = db.register_plugin(body.name, body.version,
                                          body.entry, body.config)
    except ConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"created": created, "plugin": _plugin_view(row)}


@app.get("/plugins")
async def list_plugins():
    grouped: dict[str, dict] = {}
    for row in db.list_plugins():
        entry = grouped.setdefault(row["name"], {
            "name": row["name"],
            "default": db.get_default(row["name"]),
            "versions": [],
        })
        entry["versions"].append(_plugin_view(row))
    return {"plugins": list(grouped.values())}


@app.post("/plugins/{name}/default")
async def switch_default(name: str, body: DefaultSwitch):
    row = db.get_plugin(name, body.version)
    if row is None:
        raise HTTPException(404, f"plugin {name}@{body.version} is not registered")
    try:
        old = await runtime.switch_default(name, body.version)
    except ProbeError as exc:
        # Candidate failed init/compat/smoke validation: keep the old default.
        raise HTTPException(
            400, f"candidate validation failed; default unchanged: {exc}"
        ) from exc
    return {"name": name, "previous_default": old, "default": body.version}


# -------------------------------------------------------------------- tasks

@app.post("/tasks", status_code=201)
async def submit_task(body: TaskSubmit):
    default = db.get_default(body.plugin)
    if default is None:
        if not db.list_plugins(name=body.plugin):
            raise HTTPException(404, f"plugin {body.plugin!r} is not registered")
        raise HTTPException(409, f"plugin {body.plugin!r} has no default version yet")
    if body.version is not None and body.version != default:
        raise HTTPException(
            409,
            f"version {body.version!r} is not the active default "
            f"({default!r}); new tasks bind to the current default version",
        )
    doc_hash = hashlib.sha256(body.document.encode("utf-8")).hexdigest()
    task_id = uuid.uuid4().hex
    try:
        db.insert_task({
            "id": task_id,
            "request_key": body.request_key,
            "doc_hash": doc_hash,
            "plugin_name": body.plugin,
            "plugin_version": default,  # version is bound at receipt time
            "document": body.document,
            "options": body.options,
            "timeout_ms": body.timeout_ms,
        })
    except sqlite3.IntegrityError:
        existing = db.get_task_by_key(body.request_key)
        if existing["doc_hash"] != doc_hash:
            raise HTTPException(
                409, "request_key was already used with a different document"
            ) from None
        return {"deduplicated": True, "task": _task_view(existing)}
    runtime.enqueue_task(task_id)
    return {"deduplicated": False, "task": _task_view(db.get_task(task_id))}


@app.get("/tasks")
async def list_tasks(limit: int = 50):
    return {"tasks": [_task_view(r) for r in db.list_tasks(limit=limit)]}


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    row = db.get_task(task_id)
    if row is None:
        raise HTTPException(404, "task not found")
    return {"task": _task_view(row)}


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str):
    row = db.get_task(task_id)
    if row is None:
        raise HTTPException(404, "task not found")
    if row["status"] in TERMINAL_STATES:
        raise HTTPException(409, f"task is already {row['status']}")
    # The CAS is the arbiter of the cancel/complete race: exactly one wins.
    if not db.cas_status(task_id, ("PENDING", "RUNNING"), "CANCELED",
                         error="canceled by client"):
        row = db.get_task(task_id)
        raise HTTPException(
            409, f"task reached terminal state {row['status']} before cancel"
        )
    runtime.abort(task_id)
    return {"task": _task_view(db.get_task(task_id))}
