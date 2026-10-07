"""SQLite persistence layer.

The database is the single source of truth for plugin registrations,
default-version pointers and task state.  Every transition into a terminal
task state is a compare-and-swap UPDATE guarded by the current status, so
exactly one of {succeed, fail, cancel} can win for a given task — this is
what makes the cancel/complete race safe and guarantees a task never gets
two published results.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS plugins (
    name        TEXT NOT NULL,
    version     TEXT NOT NULL,
    entry       TEXT NOT NULL,
    config_json TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE TABLE IF NOT EXISTS defaults (
    name    TEXT PRIMARY KEY,
    version TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    request_key     TEXT NOT NULL UNIQUE,
    doc_hash        TEXT NOT NULL,
    plugin_name     TEXT NOT NULL,
    plugin_version  TEXT NOT NULL,
    document        TEXT NOT NULL,
    options_json    TEXT NOT NULL,
    timeout_ms      INTEGER,
    status          TEXT NOT NULL,
    result_json     TEXT,
    error           TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
"""

# Task states
PENDING = "PENDING"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
CANCELED = "CANCELED"
TERMINAL_STATES = (SUCCEEDED, FAILED, CANCELED)
ACTIVE_STATES = (PENDING, RUNNING)

# Plugin version lifecycle: REGISTERED -> ACTIVE -> DRAINING -> RETIRED


class ConflictError(Exception):
    """Raised when an immutable record is re-registered with a different spec."""


class Database:
    def __init__(self, path: str):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # RLock: register_plugin() re-enters via get_plugin()
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute("PRAGMA journal_mode=WAL")

    # ------------------------------------------------------------------ plugins

    @staticmethod
    def _plugin_row(row: sqlite3.Row) -> dict:
        return {
            "name": row["name"],
            "version": row["version"],
            "entry": row["entry"],
            "config": json.loads(row["config_json"]),
            "status": row["status"],
            "created_at": row["created_at"],
        }

    def register_plugin(self, name: str, version: str, entry: str, config: dict):
        """Insert an immutable plugin version.  Re-registering the identical
        spec is a no-op; a different spec for the same (name, version) is a
        conflict."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM plugins WHERE name=? AND version=?", (name, version)
            ).fetchone()
            if row is not None:
                existing = self._plugin_row(row)
                if existing["entry"] == entry and existing["config"] == config:
                    return existing, False
                raise ConflictError(
                    f"plugin {name}@{version} is already registered with a "
                    "different spec; versions are immutable"
                )
            self._conn.execute(
                "INSERT INTO plugins(name, version, entry, config_json, status, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (name, version, entry, json.dumps(config), "REGISTERED", time.time()),
            )
            self._conn.commit()
            return self.get_plugin(name, version), True

    def get_plugin(self, name: str, version: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM plugins WHERE name=? AND version=?", (name, version)
            ).fetchone()
            return self._plugin_row(row) if row else None

    def list_plugins(self, name: str | None = None) -> list[dict]:
        with self._lock:
            if name is None:
                rows = self._conn.execute(
                    "SELECT * FROM plugins ORDER BY name, created_at"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM plugins WHERE name=? ORDER BY created_at", (name,)
                ).fetchall()
            return [self._plugin_row(r) for r in rows]

    def get_default(self, name: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT version FROM defaults WHERE name=?", (name,)
            ).fetchone()
            return row["version"] if row else None

    def set_default(self, name: str, version: str) -> str | None:
        """Atomically move the default pointer.  Returns the previous default."""
        with self._lock:
            row = self._conn.execute(
                "SELECT version FROM defaults WHERE name=?", (name,)
            ).fetchone()
            old = row["version"] if row else None
            if old == version:
                return old
            self._conn.execute(
                "UPDATE plugins SET status='ACTIVE' WHERE name=? AND version=?",
                (name, version),
            )
            if old is not None:
                self._conn.execute(
                    "UPDATE plugins SET status='DRAINING'"
                    " WHERE name=? AND version=? AND status='ACTIVE'",
                    (name, old),
                )
            self._conn.execute(
                "INSERT INTO defaults(name, version) VALUES (?,?)"
                " ON CONFLICT(name) DO UPDATE SET version=excluded.version",
                (name, version),
            )
            self._conn.commit()
            return old

    def set_version_status(self, name: str, version: str, status: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE plugins SET status=? WHERE name=? AND version=?",
                (status, name, version),
            )
            self._conn.commit()

    def retire_draining_versions(self) -> None:
        """Startup sweep: after a restart no version has live workers, so any
        version still marked DRAINING can be retired immediately."""
        with self._lock:
            self._conn.execute(
                "UPDATE plugins SET status='RETIRED' WHERE status='DRAINING'"
            )
            self._conn.commit()

    # -------------------------------------------------------------------- tasks

    @staticmethod
    def _task_row(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"],
            "request_key": row["request_key"],
            "doc_hash": row["doc_hash"],
            "plugin_name": row["plugin_name"],
            "plugin_version": row["plugin_version"],
            "document": row["document"],
            "options": json.loads(row["options_json"]),
            "timeout_ms": row["timeout_ms"],
            "status": row["status"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def insert_task(self, task: dict) -> None:
        """Insert a PENDING task.  Raises sqlite3.IntegrityError if the
        request_key is already taken (idempotency handled by the caller)."""
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO tasks(id, request_key, doc_hash, plugin_name,"
                " plugin_version, document, options_json, timeout_ms, status,"
                " created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task["id"],
                    task["request_key"],
                    task["doc_hash"],
                    task["plugin_name"],
                    task["plugin_version"],
                    task["document"],
                    json.dumps(task.get("options") or {}),
                    task.get("timeout_ms"),
                    PENDING,
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def get_task(self, task_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            return self._task_row(row) if row else None

    def get_task_by_key(self, request_key: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE request_key=?", (request_key,)
            ).fetchone()
            return self._task_row(row) if row else None

    def list_tasks(self, limit: int = 100) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [self._task_row(r) for r in rows]

    def cas_status(
        self,
        task_id: str,
        from_states: tuple,
        to: str,
        result: dict | None = None,
        error: str | None = None,
    ) -> bool:
        """Compare-and-swap a task into state `to`.  Returns True exactly once
        per task for terminal transitions — the loser of any race sees False."""
        placeholders = ",".join("?" for _ in from_states)
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE tasks SET status=?, result_json=?, error=?, updated_at=?"
                f" WHERE id=? AND status IN ({placeholders})",
                (
                    to,
                    json.dumps(result) if result is not None else None,
                    error,
                    time.time(),
                    task_id,
                    *from_states,
                ),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def fail_unfinished(self, reason: str) -> int:
        """Mark every non-terminal task FAILED.  Used on shutdown and on
        startup (recovery) so unfinished tasks always have a clear state and
        can never be published twice."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE tasks SET status=?, error=?, updated_at=?"
                " WHERE status IN (?,?)",
                (FAILED, reason, time.time(), PENDING, RUNNING),
            )
            self._conn.commit()
            return cur.rowcount
