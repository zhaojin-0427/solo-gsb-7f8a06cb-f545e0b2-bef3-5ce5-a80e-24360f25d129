"""SQLite 持久化层。

关键并发/生命周期约定：

* ``requests`` 表对 (plugin, request_key) 建唯一索引，并记录文档指纹——
  同键同文档返回原任务，同键异文档由 ``register_request`` 以
  ``IntegrityError`` 形式拒绝；
* 任务状态迁移一律使用带 ``from_status`` 前置条件的 CAS UPDATE，
  因此“取消与完成竞争”只有一方能成功；
* 块与 COMPLETED 状态在同一个 IMMEDIATE 事务内提交，外部永远观察不到
  “已完成但没有块”的中间态；
* 已发布的结果只追加不覆盖，重启后仍可查询；启动时把遗留在
  非终态的任务统一置为 INTERRUPTED。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from typing import Any, Iterable

# ---- 任务状态 -------------------------------------------------------------

PENDING = "PENDING"        # 已登记，等待空闲 worker
RUNNING = "RUNNING"        # 已绑定 worker 子进程，正在解析
COMPLETED = "COMPLETED"    # 终态：结果已发布
FAILED = "FAILED"          # 终态：插件报错 / 崩溃 / 输出损坏 / 超时
CANCELLED = "CANCELLED"    # 终态：被取消
INTERRUPTED = "INTERRUPTED"  # 终态：服务重启时任务仍未结束

NON_TERMINAL = (PENDING, RUNNING)
TERMINAL = (COMPLETED, FAILED, CANCELLED, INTERRUPTED)

# ---- 插件版本状态 ---------------------------------------------------------

ACTIVE = "ACTIVE"          # 可接收新任务
DRAINING = "DRAINING"      # 不再接收新任务，等待已绑定任务结束
RETIRED = "RETIRED"        # 已绑定任务全部结束，可以退出进程池


class Database:
    def __init__(self, path: str):
        self.path = path
        # SQLite 连接默认 check_same_thread=True；本服务集中在事件循环
        # 线程的同步函数里访问它，另用一把锁串行化所有写事务。
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    # --- 基础 --------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS plugin_versions (
                    plugin        TEXT NOT NULL,
                    version       TEXT NOT NULL,
                    entrypoint    TEXT NOT NULL,
                    config_json   TEXT NOT NULL DEFAULT '{}',
                    status        TEXT NOT NULL DEFAULT 'ACTIVE',
                    registered_at REAL NOT NULL,
                    PRIMARY KEY (plugin, version)
                );

                CREATE TABLE IF NOT EXISTS plugin_defaults (
                    plugin     TEXT PRIMARY KEY,
                    version    TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (plugin, version)
                        REFERENCES plugin_versions(plugin, version)
                );

                CREATE TABLE IF NOT EXISTS requests (
                    plugin        TEXT NOT NULL,
                    request_key   TEXT NOT NULL,
                    task_id       TEXT NOT NULL,
                    document_hash TEXT NOT NULL,
                    created_at    REAL NOT NULL,
                    PRIMARY KEY (plugin, request_key)
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id           TEXT PRIMARY KEY,
                    plugin       TEXT NOT NULL,
                    version      TEXT NOT NULL,
                    request_key  TEXT,
                    document     TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    created_at   REAL NOT NULL,
                    updated_at   REAL NOT NULL,
                    started_at   REAL,
                    finished_at  REAL,
                    error_code   TEXT,
                    error_message TEXT,
                    FOREIGN KEY (plugin, version)
                        REFERENCES plugin_versions(plugin, version)
                );

                CREATE TABLE IF NOT EXISTS task_blocks (
                    task_id  TEXT NOT NULL,
                    ordinal  INTEGER NOT NULL,
                    type     TEXT NOT NULL,
                    text     TEXT NOT NULL,
                    start    INTEGER NOT NULL,
                    "end"    INTEGER NOT NULL,
                    extra_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (task_id, ordinal),
                    FOREIGN KEY (task_id) REFERENCES tasks(id)
                );

                CREATE TABLE IF NOT EXISTS task_events (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id   TEXT NOT NULL,
                    at        REAL NOT NULL,
                    event     TEXT NOT NULL,
                    detail    TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_plugin_version
                    ON tasks(plugin, version);
                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, id);
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- 事件 --------------------------------------------------------------

    def add_event(self, task_id: str, event: str, detail: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO task_events(task_id, at, event, detail) VALUES (?,?,?,?)",
                (task_id, time.time(), event, detail),
            )
            self._conn.commit()

    def get_events(self, task_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT at, event, detail FROM task_events WHERE task_id=? ORDER BY id",
                (task_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- 插件版本 ----------------------------------------------------------

    def get_version(self, plugin: str, version: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM plugin_versions WHERE plugin=? AND version=?",
                (plugin, version),
            ).fetchone()
        return _version_row(row) if row else None

    def list_versions(self, plugin: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if plugin is None:
                rows = self._conn.execute(
                    "SELECT * FROM plugin_versions ORDER BY plugin, registered_at"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM plugin_versions WHERE plugin=? ORDER BY registered_at",
                    (plugin,),
                ).fetchall()
        return [_version_row(r) for r in rows]

    def insert_version(
        self, plugin: str, version: str, entrypoint: str, config: dict[str, Any]
    ) -> bool:
        """登记不可变版本。返回 False 表示该 (plugin, version) 已存在。

        已存在时即使 entrypoint/config 不同也拒绝修改——版本不可变。
        """
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM plugin_versions WHERE plugin=? AND version=?",
                (plugin, version),
            ).fetchone()
            if exists:
                return False
            self._conn.execute(
                "INSERT INTO plugin_versions(plugin, version, entrypoint, config_json,"
                " status, registered_at) VALUES (?,?,?,?,?,?)",
                (plugin, version, entrypoint, json.dumps(config, ensure_ascii=False),
                 ACTIVE, time.time()),
            )
            self._conn.commit()
        return True

    def set_default(self, plugin: str, version: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO plugin_defaults(plugin, version, updated_at) VALUES (?,?,?)\n"
                "ON CONFLICT(plugin) DO UPDATE SET version=excluded.version,"
                " updated_at=excluded.updated_at",
                (plugin, version, time.time()),
            )

    def get_default(self, plugin: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT version FROM plugin_defaults WHERE plugin=?", (plugin,)
            ).fetchone()
        return row["version"] if row else None

    def list_defaults(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute("SELECT plugin, version FROM plugin_defaults").fetchall()
        return {r["plugin"]: r["version"] for r in rows}

    def set_version_status(self, plugin: str, version: str, status: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE plugin_versions SET status=? WHERE plugin=? AND version=?",
                (status, plugin, version),
            )
            self._conn.commit()

    def count_active_tasks(self, plugin: str, version: str) -> int:
        """该版本上仍占用生命周期的任务数（非终态）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(1) AS n FROM tasks WHERE plugin=? AND version=? "
                "AND status IN (?,?)",
                (plugin, version, PENDING, RUNNING),
            ).fetchone()
        return int(row["n"])

    # --- 请求幂等 ----------------------------------------------------------

    def register_request(
        self, plugin: str, request_key: str, document_hash: str
    ) -> tuple[str | None, str | None]:
        """登记幂等键。

        返回 ``(task_id, existing_hash)``：
        * 键不存在时插入占位，task_id/existing_hash 均为 None，调用方随后建任务；
        * 键已存在且文档一致时返回原 task_id；
        * 键已存在但文档指纹不同时 existing_hash 为另一个值（调用方据此 409）。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT task_id, document_hash FROM requests WHERE plugin=? AND request_key=?",
                (plugin, request_key),
            ).fetchone()
            if row:
                return row["task_id"], row["document_hash"]
            # 先占住键，task_id 在任务行插入后回填，避免同键并发请求各自建任务。
            self._conn.execute(
                "INSERT INTO requests(plugin, request_key, task_id, document_hash, created_at)"
                " VALUES (?,?,?,?,?)",
                (plugin, request_key, "", document_hash, time.time()),
            )
            self._conn.commit()
        return None, None

    def bind_request_task(self, plugin: str, request_key: str, task_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE requests SET task_id=? WHERE plugin=? AND request_key=?",
                (task_id, plugin, request_key),
            )
            self._conn.commit()

    # --- 任务 --------------------------------------------------------------

    def create_task(
        self,
        plugin: str,
        version: str,
        document: str,
        request_key: str | None,
    ) -> str:
        task_id = uuid.uuid4().hex
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO tasks(id, plugin, version, request_key, document,"
                " status, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                (task_id, plugin, version, request_key, document, PENDING, now, now),
            )
            self._conn.execute(
                "INSERT INTO task_events(task_id, at, event, detail) VALUES (?,?,?,?)",
                (task_id, now, "created", f"bound to {plugin}@{version}"),
            )
        return task_id

    def get_task(self, task_id: str, include_document: bool = False) -> dict[str, Any] | None:
        cols = "id, plugin, version, request_key, status, created_at, updated_at," \
               " started_at, finished_at, error_code, error_message"
        if include_document:
            cols += ", document"
        with self._lock:
            row = self._conn.execute(f"SELECT {cols} FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(row) if row else None

    def get_document(self, task_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT document FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
        return row["document"] if row else None

    def mark_running(self, task_id: str) -> bool:
        return self._cas_status(task_id, (PENDING,), RUNNING, start_timer=True)

    def _cas_status(
        self,
        task_id: str,
        from_statuses: Iterable[str],
        to_status: str,
        *,
        start_timer: bool = False,
        error: tuple[str, str] | None = None,
    ) -> bool:
        now = time.time()
        placeholders = ",".join("?" for _ in list(from_statuses))
        sets = ["status=?", "updated_at=?"]
        params: list[Any] = [to_status, now]
        if start_timer:
            sets.append("started_at=?")
            params.append(now)
        if to_status in TERMINAL:
            sets.append("finished_at=?")
            params.append(now)
        if error is not None:
            sets.append("error_code=?")
            sets.append("error_message=?")
            params.extend([error[0], error[1][:2000]])
        sql = (
            f"UPDATE tasks SET {', '.join(sets)} WHERE id=? AND status IN ({placeholders})"
        )
        params.extend([task_id, *from_statuses])
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur.rowcount == 1

    def complete_task(
        self, task_id: str, blocks: list[dict[str, Any]]
    ) -> bool:
        """终态提交：CAS 到 COMPLETED + 块写入在同一个 IMMEDIATE 事务内。

        若取消方先行把状态改成 CANCELLED，UPDATE 命中 0 行，事务回滚，
        块不会落库——两份终态发布在物理上不可能同时发生。
        """
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            cur = self._conn.execute(
                "UPDATE tasks SET status=?, updated_at=?, finished_at=?, "
                "error_code=NULL, error_message=NULL WHERE id=? AND status IN (?,?)",
                (COMPLETED, now, now, task_id, PENDING, RUNNING),
            )
            if cur.rowcount != 1:
                self._conn.rollback()
                return False
            self._conn.executemany(
                "INSERT INTO task_blocks(task_id, ordinal, type, text, start, \"end\","
                " extra_json) VALUES (?,?,?,?,?,?,?)",
                [
                    (task_id, i, b["type"], b["text"], b["start"], b["end"],
                     json.dumps({k: v for k, v in b.items()
                                 if k not in ("type", "text", "start", "end")},
                                ensure_ascii=False))
                    for i, b in enumerate(blocks)
                ],
            )
            self._conn.execute(
                "INSERT INTO task_events(task_id, at, event, detail) VALUES (?,?,?,?)",
                (task_id, now, "completed", f"{len(blocks)} blocks published"),
            )
        return True

    def fail_task(
        self, task_id: str, code: str, message: str, *, from_statuses: Iterable[str] = NON_TERMINAL
    ) -> bool:
        ok = self._cas_status(task_id, tuple(from_statuses), FAILED, error=(code, message))
        if ok:
            self.add_event(task_id, "failed", f"{code}: {message}")
        return ok

    def cancel_task(self, task_id: str) -> tuple[bool, bool]:
        """尝试取消。返回 (是否本次取消成功, 任务当前是否已终态)。

        对已处于 CANCELLED 的任务返回 (False, True)——取消是幂等的，
        API 层据此给出 200；其他终态则 409。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if row is None:
                return (False, False)
            current = row["status"]
            if current in (PENDING, RUNNING):
                ok = self._cas_status(task_id, (PENDING, RUNNING), CANCELLED)
                if ok:
                    self.add_event(task_id, "cancelled", None)
                # CAS 成功即终态；理论上不会失败（同行锁内），失败时重查。
                return ok, ok or self._is_terminal(task_id)
            return False, True

    def _is_terminal(self, task_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
        return row is not None and row["status"] in TERMINAL

    def get_blocks(self, task_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ordinal, type, text, start, \"end\", extra_json FROM task_blocks "
                "WHERE task_id=? ORDER BY ordinal",
                (task_id,),
            ).fetchall()
        result = []
        for r in rows:
            block = {"type": r["type"], "text": r["text"], "start": r["start"],
                     "end": r["end"]}
            extra = json.loads(r["extra_json"] or "{}")
            # 插件元数据平铺在前、结构字段在后，避免被覆盖。
            merged = {**extra, **block}
            result.append(merged)
        return result

    def recover_on_startup(self) -> list[str]:
        """重启恢复：非终态任务 -> INTERRUPTED；返回受影响任务 id 列表。"""
        now = time.time()
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT id FROM tasks WHERE status IN (?,?)", (PENDING, RUNNING)
            ).fetchall()
            ids = [r["id"] for r in rows]
            for tid in ids:
                self._conn.execute(
                    "UPDATE tasks SET status=?, updated_at=?, finished_at=? WHERE id=?",
                    (INTERRUPTED, now, now, tid),
                )
                self._conn.execute(
                    "INSERT INTO task_events(task_id, at, event, detail) VALUES (?,?,?,?)",
                    (tid, now, "interrupted", "service restarted before task finished"),
                )
        return ids

    def iter_non_retired_versions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM plugin_versions WHERE status != ? ORDER BY plugin, version",
                (RETIRED,),
            ).fetchall()
        return [_version_row(r) for r in rows]


def _version_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "plugin": row["plugin"],
        "version": row["version"],
        "entrypoint": row["entrypoint"],
        "config": json.loads(row["config_json"]),
        "status": row["status"],
        "registered_at": row["registered_at"],
    }


def document_fingerprint(document: str) -> str:
    return hashlib.sha256(document.encode("utf-8")).hexdigest()
