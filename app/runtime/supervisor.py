"""插件进程池与任务执行器（父进程侧）。

设计要点
========

* 每个已登记且未退役的 (plugin, version) 对应一个 :class:`VersionPool`，
  池内维持固定数量的常驻 worker 子进程；
* 一个 worker 同一时刻只服务一个任务，任务结束（完成/失败/取消/超时）
  后才可复用；崩溃的 worker 由池补一个全新进程，因此一个任务的子进程
  崩溃只终止该任务；
* 每条 worker 协议消息都必须携带任务标识；服务于旧任务的迟到帧在
  worker 空闲后到达时按“过期槽位消息”丢弃并隔离该 worker，绝不写入
  已复用槽位的新任务；
* 版本切换后旧版本进入 DRAINING：不再领取新任务，仅等待已绑定任务
  结束；引用计数归零后子进程退出、版本置 RETIRED；
* 任务在提交瞬间即写入 ``tasks.plugin/version``（DB 中冻结），
  与进程池当前默认版本完全解耦。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

from app.config import settings
from app.db import (
    ACTIVE,
    DRAINING,
    PENDING,
    RETIRED,
    RUNNING,
    Database,
)
from app.protocol import (
    MAX_FRAME_BYTES,
    PROTOCOL,
    ProtocolError,
    init_message,
    parse_message,
    shutdown_message,
    validate_blocks_frame,
    validate_complete_blocks,
)

log = logging.getLogger("supervisor")

CHILD_WORKDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


class TaskTimeout(Exception):
    pass


class HandshakeFailed(Exception):
    pass


# ---- 单个 worker ----------------------------------------------------------


@dataclass
class Worker:
    pool: "VersionPool"
    slot: int
    proc: Optional[asyncio.subprocess.Process] = None
    # 当前绑定的任务；None 表示空闲可领取。
    current_task: Optional[str] = None
    # 每次绑定新任务自增，用于识别迟到帧（逻辑槽位代号）。
    era: int = 0
    # 收到属于陌生任务的迟到帧后隔离，当前任务结束后销毁，不再复用。
    quarantined: bool = False
    reader_task: Optional[asyncio.Task] = None
    stderr_task: Optional[asyncio.Task] = None
    stdin_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def busy(self) -> bool:
        return self.current_task is not None

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def start(self) -> None:
        argv = [sys.executable, "-u", "-m", "app.runtime.child", self.pool.entrypoint]
        self.proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            cwd=CHILD_WORKDIR,
        )
        self.reader_task = asyncio.create_task(self._reader_loop())
        self.stderr_task = asyncio.create_task(self._drain_stderr())
        await self._write(init_message(self.pool.config))

    async def _write(self, frame: dict[str, Any]) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        async with self.stdin_lock:
            if self.proc.stdin.is_closing():
                return
            self.proc.stdin.write((json.dumps(frame, ensure_ascii=False) + "\n").encode("utf-8"))
            await self.proc.stdin.drain()

    async def send_parse(self, task_id: str, document: str) -> None:
        await self._write(parse_message(task_id, document))

    async def send_shutdown(self) -> None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._write(shutdown_message()), timeout=1.0)

    async def _drain_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        while True:
            line = await self.proc.stderr.readline()
            if not line:
                return
            log.debug(
                "worker %s#%s stderr: %s",
                self.pool.name,
                self.slot,
                line.decode("utf-8", "replace").rstrip(),
            )

    async def _reader_loop(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        stream = self.proc.stdout
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                if len(line) > MAX_FRAME_BYTES:
                    await self.pool.supervisor.on_protocol_fault(
                        self, "FRAME_TOO_LARGE", "单行报文超过上限"
                    )
                    continue
                try:
                    frame = json.loads(line.decode("utf-8"))
                    if not isinstance(frame, dict):
                        raise ValueError("frame is not an object")
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    await self.pool.supervisor.on_protocol_fault(
                        self, "CORRUPT_OUTPUT", f"无法解析的输出: {exc}"
                    )
                    continue
                await self.pool.supervisor.dispatch(self, frame)
        finally:
            await self.pool.supervisor.on_worker_exit(self)

    async def terminate(self, *, kill: bool = False) -> None:
        if not self.alive:
            return
        if kill:
            with contextlib.suppress(ProcessLookupError):
                self.proc.kill()
        else:
            await self.send_shutdown()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=settings.shutdown_grace_seconds)
                return
            except asyncio.TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    self.proc.kill()
        with contextlib.suppress(Exception):
            await self.proc.wait()


# ---- 单个插件版本的进程池 -------------------------------------------------


class VersionPool:
    """一个不可变插件版本的生命周期容器。"""

    def __init__(
        self,
        supervisor: "Supervisor",
        plugin: str,
        version: str,
        entrypoint: str,
        config: dict[str, Any],
        size: int,
    ):
        self.supervisor = supervisor
        self.plugin = plugin
        self.version = version
        self.entrypoint = entrypoint
        self.config = config
        self.size = size
        self.name = f"{plugin}@{version}"
        self.workers: list[Worker] = []
        # 等待空闲 worker 的 PENDING 任务 FIFO。
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        # 在途（已绑定 worker、含即将完成）的任务数；含 PENDING 引用。
        self.refcount = 0
        self.status = ACTIVE
        self.started = False
        self._runner: Optional[asyncio.Task] = None
        self._wake = asyncio.Event()
        self._stopping = False

    # --- 生命周期 ----------------------------------------------------------

    async def start(self) -> None:
        if self.started:
            return
        self.started = True
        for slot in range(self.size):
            w = Worker(self, slot)
            self.workers.append(w)
            await w.start()
        # 等待所有 worker 完成协议握手。
        try:
            await asyncio.wait_for(
                asyncio.gather(*(asyncio.create_task(w.ready_event.wait()) for w in self.workers)),
                timeout=settings.validate_timeout_seconds,
            )
        except asyncio.TimeoutError:
            await self._force_shutdown()
            raise HandshakeFailed(f"{self.name}: worker 握手超时")
        dead = [w for w in self.workers if not w.alive]
        if dead:
            await self._force_shutdown()
            raise HandshakeFailed(f"{self.name}: worker 握手前退出")
        self._runner = asyncio.create_task(self._schedule_loop())
        log.info("pool %s started with %d workers", self.name, self.size)

    async def _force_shutdown(self) -> None:
        self._stopping = True
        for w in self.workers:
            await w.terminate(kill=True)
        self.workers.clear()

    async def drain(self) -> None:
        """版本切换：不再接新任务，等存量任务结束后退出。"""
        if self.status != ACTIVE:
            return
        self.status = DRAINING
        self.supervisor.db.set_version_status(self.plugin, self.version, DRAINING)
        log.info("pool %s -> DRAINING (refcount=%d)", self.name, self.refcount)

    async def retire(self) -> None:
        self.status = RETIRED
        self.supervisor.db.set_version_status(self.plugin, self.version, RETIRED)
        self._stopping = True
        if self._runner:
            self._runner.cancel()
        # 此时不应有在途任务；保险起见等待。
        for w in list(self.workers):
            await w.terminate()
        self.workers.clear()
        log.info("pool %s retired, workers exited", self.name)

    # --- 调度 --------------------------------------------------------------

    def submit(self, task_id: str) -> None:
        self.refcount += 1
        self.queue.put_nowait(task_id)
        self._wake.set()

    def _free_workers(self) -> list[Worker]:
        return [w for w in self.workers if w.alive and not w.busy and not w.quarantined]

    async def _schedule_loop(self) -> None:
        while not self._stopping:
            try:
                task_id = await self.queue.get()
            except asyncio.CancelledError:
                return
            # DRAINING 时不允许进入新绑定；理论上 drain 后无新入队，
            # 这里做双保险（已在队列里的 PENDING 任务仍可继续——
            # 它们在提交时已绑定本版本）。
            worker = await self._acquire_worker()
            asyncio.create_task(self.supervisor.run_task(self, worker, task_id))

    async def _acquire_worker(self) -> Worker:
        while True:
            free = self._free_workers()
            if free:
                return free[0]
            # worker 释放时会 set wake
            await self.supervisor.wait_worker_free(self)

    async def on_worker_released(self, worker: Worker) -> None:
        """任务终态处理完后调用：决定复用、替换或退役。"""
        if worker.quarantined or not worker.alive:
            await self._replace_worker(worker)
        self.supervisor.notify_worker_free(self)

    async def _replace_worker(self, old: Worker) -> None:
        with contextlib.suppress(ValueError):
            self.workers.remove(old)
        await old.terminate(kill=not old.alive)
        if self.status == RETIRED or self._stopping:
            return
        if not old.alive:
            self.supervisor.db.add_event(
                old.current_task or "",
                "worker_replaced",
                f"{self.name} 崩溃 worker 已替换，不影响同版本其他任务",
            ) if old.current_task else None
        w = Worker(self, old.slot)
        try:
            await w.start()
            await asyncio.wait_for(w.ready_event.wait(), timeout=settings.validate_timeout_seconds)
        except Exception as exc:
            log.error("replace worker for %s failed: %r", self.name, exc)
            if w.alive:
                await w.terminate(kill=True)
            return
        self.workers.append(w)

    def maybe_retire(self) -> bool:
        if self.status == DRAINING and self.refcount == 0:
            asyncio.create_task(self.retire())
            return True
        return False


# ---- 在途任务运行时状态 ---------------------------------------------------


@dataclass
class _Inflight:
    pool: VersionPool
    worker: Worker
    era: int
    blocks: list[dict[str, Any]] = field(default_factory=list)
    next_batch: int = 0
    done_event: asyncio.Event = field(default_factory=asyncio.Event)


class Supervisor:
    """所有版本池的总管：建池/退役、分发帧、执行任务、处理故障。"""

    def __init__(self, db: Database, pool_size: int | None = None):
        self.db = db
        self.pool_size = pool_size or settings.workers_per_version
        self.pools: dict[tuple[str, str], VersionPool] = {}
        self.inflight: dict[str, _Inflight] = {}
        # 等待被取消的任务集合（PENDING 在队列中或 RUNNING 在 worker 上）。
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._free_cond = asyncio.Condition()
        # 优雅关停中：此时 worker 退出属于预期，不再把任务记为崩溃。
        self.shutting_down = False

    # --- 池的装配 ----------------------------------------------------------

    async def start_from_db(self) -> None:
        """启动时：恢复任务状态，并为所有未退役版本建池。"""
        interrupted = self.db.recover_on_startup()
        for tid in interrupted:
            log.info("task %s marked INTERRUPTED after restart", tid)
        for v in self.db.iter_non_retired_versions():
            pool = self._build_pool(v["plugin"], v["version"], v["entrypoint"], v["config"])
            try:
                await pool.start()
            except HandshakeFailed as exc:
                # 已登记版本的常驻池启动失败：保留登记，不设默认切换，
                # 标记 pool 为不可用（提交时显式报错）。
                log.error("pool start failed on startup: %s", exc)
                pool._stopping = True
                pool.status = DRAINING

    def _build_pool(self, plugin: str, version: str, entrypoint: str,
                    config: dict[str, Any]) -> VersionPool:
        key = (plugin, version)
        pool = VersionPool(self, plugin, version, entrypoint, config, self.pool_size)
        self.pools[key] = pool
        return pool

    async def validate_candidate(
        self, entrypoint: str, config: dict[str, Any]
    ) -> None:
        """登记/切默认前的兼容校验：白名单 + 握手 + 冒烟文档解析。

        使用一次性短命子进程，手工驱动整条协议（不挂全局 reader），
        校验结束即退出，不加入常驻池。
        """
        if not entrypoint.startswith("app.plugins."):
            raise HandshakeFailed("entrypoint 必须位于仓库内 app.plugins 包下")
        module_path = os.path.join(
            CHILD_WORKDIR, entrypoint.replace(".", os.sep) + ".py"
        )
        if not os.path.isfile(module_path):
            raise HandshakeFailed(f"entrypoint 文件不存在: {entrypoint}")

        argv = [sys.executable, "-u", "-m", "app.runtime.child", entrypoint]
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            cwd=CHILD_WORKDIR,
        )
        try:
            async def write(frame: dict[str, Any]) -> None:
                assert proc.stdin is not None
                proc.stdin.write((json.dumps(frame, ensure_ascii=False) + "\n").encode())
                await proc.stdin.drain()

            async def readline() -> bytes:
                assert proc.stdout is not None
                return await asyncio.wait_for(
                    proc.stdout.readline(), timeout=settings.validate_timeout_seconds
                )

            await write(init_message(config))
            line = await readline()
            if not line:
                raise HandshakeFailed("子进程在握手前退出（初始化失败）")
            ready = json.loads(line.decode("utf-8"))
            if ready.get("kind") != "ready" or ready.get("protocol") != PROTOCOL:
                raise HandshakeFailed(f"握手失败：{ready!r}")

            smoke_doc = "smoke line\n"
            await write(parse_message("__smoke__", smoke_doc))
            blocks: list[dict[str, Any]] = []
            batch = 0
            while True:
                line = await readline()
                if not line:
                    raise HandshakeFailed("冒烟解析期间子进程退出")
                frame = json.loads(line.decode("utf-8"))
                kind = frame.get("kind")
                if kind == "blocks":
                    blocks.extend(
                        validate_blocks_frame(frame, task_id="__smoke__",
                                              expected_batch=batch)
                    )
                    batch += 1
                elif kind == "error":
                    raise HandshakeFailed(f"插件冒烟返回错误: {frame.get('message')}")
                elif kind == "finish":
                    if frame.get("batches") != batch:
                        raise HandshakeFailed("冒烟 finish 批次数不符")
                    validate_complete_blocks(
                        blocks, smoke_doc,
                        expected_count=frame.get("blocks")
                        if isinstance(frame.get("blocks"), int) else None,
                    )
                    break
                else:
                    raise HandshakeFailed(f"冒烟阶段收到意外帧: {kind!r}")

            with contextlib.suppress(Exception):
                await write(shutdown_message())
        except asyncio.TimeoutError:
            raise HandshakeFailed("候选插件初始化或冒烟校验超时")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HandshakeFailed(f"候选插件输出损坏: {exc}")
        finally:
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()

    # --- 版本管理 ----------------------------------------------------------

    def pool_for(self, plugin: str, version: str) -> VersionPool | None:
        return self.pools.get((plugin, version))

    async def ensure_pool(self, plugin: str, version: str) -> VersionPool:
        key = (plugin, version)
        pool = self.pools.get(key)
        if pool is not None:
            if pool.status == RETIRED or pool._stopping:
                raise HandshakeFailed(f"{plugin}@{version} 已退出，拒绝新任务")
            return pool
        info = self.db.get_version(plugin, version)
        if info is None:
            raise HandshakeFailed(f"未知版本 {plugin}@{version}")
        pool = self._build_pool(plugin, version, info["entrypoint"], info["config"])
        try:
            await pool.start()
        except Exception:
            self.pools.pop(key, None)
            raise
        return pool

    async def activate_version(self, plugin: str, version: str) -> None:
        """确保目标版本池在运行（切默认前已 validate 通过）。"""
        pool = await self.ensure_pool(plugin, version)
        pool.status = ACTIVE
        self.db.set_version_status(plugin, version, ACTIVE)

    async def switch_default(self, plugin: str, old_version: str | None,
                             new_version: str) -> None:
        """默认版本切换：新版本即刻接新任务；旧版本排水退出。"""
        if old_version and old_version != new_version:
            old = self.pools.get((plugin, old_version))
            if old is not None and old.status == ACTIVE:
                await old.drain()
                # 若恰好没有在途任务，立即退役。
                old.maybe_retire()

    async def submit(self, task_id: str, plugin: str, version: str) -> None:
        pool = await self.ensure_pool(plugin, version)
        if pool.status == DRAINING:
            # 显式提交到排水版本只发生在“登记后尚未成为默认又被手动指定”
            # 等罕见路径；语义上允许已冻结绑定继续，不允许新进入。
            raise HandshakeFailed(f"{plugin}@{version} 正在退出，拒绝新任务")
        pool.submit(task_id)
        self.db.add_event(task_id, "queued", f"on pool {pool.name}")

    def request_cancel(self, task_id: str) -> None:
        ev = self._cancel_events.get(task_id)
        if ev is not None:
            ev.set()

    async def wait_worker_free(self, pool: VersionPool) -> None:
        async with self._free_cond:
            await self._free_cond.wait()

    def notify_worker_free(self, pool: VersionPool | None = None) -> None:
        async def _poke():
            async with self._free_cond:
                self._free_cond.notify_all()
        asyncio.create_task(_poke())

    # --- 任务执行 ----------------------------------------------------------

    async def run_task(self, pool: VersionPool, worker: Worker, task_id: str) -> None:
        # 取消事件在状态翻转前登记，消除“PENDING 已取消但调度循环
        # 刚好把它翻成 RUNNING”的窗口。
        cancel_event = asyncio.Event()
        self._cancel_events[task_id] = cancel_event

        # 取消可能在任务尚未绑定 worker 时发生（PENDING -> CANCELLED）。
        if not self.db.mark_running(task_id):
            self._cancel_events.pop(task_id, None)
            await self._release(pool, worker)
            return

        document = self.db.get_document(task_id)
        if document is None:
            self.db.fail_task(task_id, "INTERNAL", "文档丢失")
            self._cancel_events.pop(task_id, None)
            await self._release(pool, worker)
            return

        # mark_running 之后、真正绑定 worker 之前可能已有取消请求落入。
        if cancel_event.is_set():
            self._cancel_events.pop(task_id, None)
            await self._release(pool, worker)
            return

        worker.current_task = task_id
        worker.era += 1
        inflight = _Inflight(pool=pool, worker=worker, era=worker.era)
        self.inflight[task_id] = inflight
        self.db.add_event(task_id, "dispatched", f"slot={worker.slot} era={worker.era}")

        timed_out = False
        cancel_flag = False
        done_waiter = asyncio.create_task(inflight.done_event.wait())
        cancel_waiter = asyncio.create_task(cancel_event.wait())
        try:
            await worker.send_parse(task_id, document)
            try:
                done, pending = await asyncio.wait(
                    {done_waiter, cancel_waiter},
                    timeout=settings.task_timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.CancelledError:
                raise
            if not done:
                timed_out = True
            elif (cancel_waiter in done
                  and done_waiter not in done
                  and not inflight.done_event.is_set()):
                cancel_flag = True

            if timed_out:
                await self._terminate_task(pool, worker, task_id, "TIMEOUT",
                                           "插件执行超时，子进程已被杀掉",
                                           kill=True)
                return
            if cancel_flag:
                await self._terminate_task(pool, worker, task_id, "CANCELLED",
                                           "任务被取消，子进程已终止",
                                           kill=True, cancelled=True)
                return
            # done_event 置位时终态已在 dispatch 中提交，这里只清理绑定。
        finally:
            done_waiter.cancel()
            cancel_waiter.cancel()
            self.inflight.pop(task_id, None)
            self._cancel_events.pop(task_id, None)
            await self._release(pool, worker)

    async def _terminate_task(
        self, pool: VersionPool, worker: Worker, task_id: str,
        code: str, message: str, *, kill: bool, cancelled: bool = False,
    ) -> None:
        if cancelled:
            # CAS 已由 API 层完成；这里只负责杀子进程。
            pass
        else:
            self.db.fail_task(task_id, code, message)
        # 杀掉的是“这一个任务绑定的这一个 worker”；池随后补员，
        # 同版本其他 worker 与其他任务完全不受影响。
        if kill:
            await worker.terminate(kill=True)
            # reader_loop 退出后 on_worker_exit 会走崩溃路径，
            # 但终态已落库，那里只做清理。
        else:
            await worker.send_shutdown()

    async def _release(self, pool: VersionPool, worker: Worker) -> None:
        worker.current_task = None
        pool.refcount -= 1
        await pool.on_worker_released(worker)
        if pool.maybe_retire():
            return
        # 唤醒调度循环。
        pool._wake.set()
        self.notify_worker_free()

    # --- 帧分发 ------------------------------------------------------------

    async def dispatch(self, worker: Worker, frame: dict[str, Any]) -> None:
        kind = frame.get("kind")

        if kind == "ready":
            if frame.get("protocol") != PROTOCOL:
                log.error("%s 协议版本不符: %r", worker.pool.name, frame.get("protocol"))
                await worker.terminate(kill=True)
                return
            worker.ready_event.set()
            return

        task_id = frame.get("task")
        if not isinstance(task_id, str):
            await self.on_protocol_fault(worker, "BAD_FRAME", "帧缺少 task 标识")
            return

        inflight = self.inflight.get(task_id)

        # 关键防护：消息必须来自当前正绑定该任务的那个 worker，
        # 且 era 一致。任何“迟到 + 槽位已复用”的组合都在此被拦截，
        # 旧任务的迟到数据绝不可能写入新任务。
        if inflight is None or inflight.worker is not worker or inflight.era != worker.era:
            if worker.current_task == task_id:
                # 同槽位同任务但 inflight 已消失（理论不应发生）——按损坏处理。
                await self.on_protocol_fault(worker, "ORPHAN_FRAME",
                                             "任务运行时上下文缺失")
                return
            # 这是属于“别的任务”的迟到帧：记录、丢弃、隔离 worker。
            log.warning(
                "丢弃迟到帧 kind=%s task=%s（当前槽位任务=%s era=%d）",
                kind, task_id, worker.current_task, worker.era,
            )
            target = self.db.get_task(task_id)
            if target is not None:
                self.db.add_event(
                    task_id, "stale_frame_dropped",
                    f"kind={kind} 在任务终态后到达，来源池 {worker.pool.name}",
                )
            worker.quarantined = True
            return

        if kind == "blocks":
            try:
                norm = validate_blocks_frame(
                    frame, task_id=task_id, expected_batch=inflight.next_batch
                )
            except ProtocolError as exc:
                inflight.blocks.clear()  # 不完整结果绝不发布
                await self._fail_running(inflight.pool, worker, task_id,
                                         exc.code, exc.message)
                inflight.done_event.set()
                return
            inflight.blocks.extend(norm)
            inflight.next_batch += 1
            return

        if kind == "finish":
            batches = frame.get("batches")
            declared = frame.get("blocks")
            if batches != inflight.next_batch:
                await self._fail_running(inflight.pool, worker, task_id, "OUT_OF_ORDER",
                                         f"finish 声明 {batches} 批，实收 {inflight.next_batch} 批")
                inflight.done_event.set()
                return
            try:
                validate_complete_blocks(
                    inflight.blocks, self.db.get_document(task_id) or "",
                    expected_count=declared if isinstance(declared, int) else None,
                )
            except ProtocolError as exc:
                await self._fail_running(inflight.pool, worker, task_id, exc.code, exc.message)
                inflight.done_event.set()
                return
            # 终态提交：块 + COMPLETED 同一事务 CAS；若取消方先行，
            # complete_task 返回 False，本结果整体丢弃，绝不双发布。
            if self.db.complete_task(task_id, inflight.blocks):
                log.info("task %s published with %d blocks", task_id, len(inflight.blocks))
            else:
                log.warning("task %s 完成晚于取消，结果丢弃", task_id)
                self.db.add_event(task_id, "completion_lost", "完成与取消竞争，取消先行")
            inflight.done_event.set()
            return

        if kind == "error":
            code = str(frame.get("code") or "PLUGIN_ERROR")[:64]
            message = str(frame.get("message") or "plugin returned error")
            await self._fail_running(inflight.pool, worker, task_id, code, message)
            inflight.done_event.set()
            return

        await self.on_protocol_fault(worker, "BAD_FRAME", f"未知帧类型 {kind!r}")

    async def _fail_running(self, pool: VersionPool, worker: Worker, task_id: str,
                            code: str, message: str) -> None:
        # 只允许 RUNNING -> FAILED；若取消/超时先行则失败提交无效。
        if not self.db.fail_task(task_id, code, message, from_statuses=(RUNNING,)):
            log.warning("task %s 的失败提交被终态竞争吞掉 (%s)", task_id, code)

    # --- 协议损坏 / 子进程退出 ---------------------------------------------

    async def on_protocol_fault(self, worker: Worker, code: str, message: str) -> None:
        task_id = worker.current_task
        log.warning("协议故障 %s on %s: %s (task=%s)", code, worker.pool.name, message, task_id)
        if task_id is not None:
            self.db.fail_task(task_id, code, message)
            inflight = self.inflight.get(task_id)
            if inflight is not None:
                inflight.done_event.set()
        # 输出已不可信：杀掉这个 worker，由池补员；不影响其他 worker。
        worker.quarantined = True
        await worker.terminate(kill=True)

    async def on_worker_exit(self, worker: Worker) -> None:
        """reader_loop 结束（EOF / 被杀 / 崩溃）时调用。"""
        task_id = worker.current_task
        pool = worker.pool
        if task_id is not None:
            inflight = self.inflight.get(task_id)
            rc = worker.proc.returncode if worker.proc else None
            if self.shutting_down:
                # 服务关停：在途任务统一 INTERRUPTED（shutdown 已批量标记），
                # 这里仅唤醒等待者，不产生 WORKER_CRASHED 终态。
                log.info("关停期间 worker 退出，任务 %s 视为 INTERRUPTED", task_id)
            elif not self.db.fail_task(
                task_id,
                "WORKER_CRASHED",
                f"插件子进程意外退出（exit={rc}），该任务终止；进程池已隔离该槽位",
                from_statuses=(PENDING, RUNNING),
            ):
                log.info("worker 退出时任务 %s 已终态，仅清理槽位", task_id)
            if inflight is not None:
                inflight.done_event.set()
        else:
            log.info("空闲 worker %s#%s 退出（rc=%s）", pool.name, worker.slot,
                     worker.proc.returncode if worker.proc else None)
        worker.quarantined = True
        # 若该 worker 正被 run_task 等待，释放/补员由 run_task 的
        # finally（_release -> on_worker_released -> _replace_worker）完成；
        # 空闲崩溃（握手后、任务前，或迟到帧隔离后被退役）则在此直接补员。
        if task_id is None and pool.status != RETIRED and not pool._stopping:
            async def _recover_idle() -> None:
                await pool._replace_worker(worker)
                self.notify_worker_free()
            asyncio.create_task(_recover_idle())

    # --- 关停 --------------------------------------------------------------

    async def shutdown(self) -> None:
        self.shutting_down = True
        # 先给所有非终态任务一个确定终态，再终止子进程——
        # 这样 worker 退出回调不会把它们误记为崩溃。
        for tid in self.db.recover_on_startup():
            inflight = self.inflight.get(tid)
            if inflight is not None:
                inflight.done_event.set()
        for pool in list(self.pools.values()):
            pool._stopping = True
            for w in list(pool.workers):
                await w.terminate()
        self.pools.clear()


