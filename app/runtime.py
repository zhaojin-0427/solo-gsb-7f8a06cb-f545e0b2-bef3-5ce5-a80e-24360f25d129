"""Plugin process pool, version lifecycle and task execution.

Design notes
------------
* One worker process executes at most one job at a time.  Workers are pooled
  per (plugin name, version) so an old version keeps serving tasks that were
  bound to it until they finish (DRAINING), and is only torn down afterwards
  (RETIRED).
* Every job gets a fresh random ``job_id``.  The runtime only accepts
  messages addressed to the job currently assigned to a worker, so a late
  message from a killed/timed-out plugin can never leak into the next task
  that reuses the slot.
* A task's result is committed to SQLite with a compare-and-swap on the task
  status.  Completion, failure and cancellation therefore cannot both win,
  and a result is published exactly once.
* Subprocess crashes, corrupt output and timeouts only fail the task bound
  to that process; the pool discards the process and spawns replacements on
  demand.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from collections import deque
from pathlib import Path

from . import protocol
from .db import Database
from .protocol import BatchValidator

log = logging.getLogger("runtime")

REPO_ROOT = Path(__file__).resolve().parent.parent

INIT_TIMEOUT_S = 10.0
SMOKE_TIMEOUT_S = 15.0
REAP_TIMEOUT_S = 5.0


class ProbeError(Exception):
    """A candidate plugin failed init/compat/smoke validation."""


class PluginStartError(Exception):
    """A worker process failed to start or complete the init handshake."""


class VersionRuntime:
    __slots__ = (
        "name", "version", "entry", "config", "status",
        "workers", "idle", "pending", "in_flight", "pumping",
    )

    def __init__(self, name: str, version: str, entry: str, config: dict, status: str):
        self.name = name
        self.version = version
        self.entry = entry
        self.config = config
        self.status = status
        self.workers: set[Worker] = set()
        self.idle: list[Worker] = []
        self.pending: deque[str] = deque()
        self.in_flight = 0
        self.pumping = False


class Job:
    __slots__ = ("task_id", "job_id", "timeout_ms", "validator", "timeout_handle")

    def __init__(self, task_id: str, document: str, timeout_ms: int):
        self.task_id = task_id
        self.job_id = uuid.uuid4().hex
        self.timeout_ms = timeout_ms
        self.validator = BatchValidator(document)
        self.timeout_handle: asyncio.TimerHandle | None = None


class Worker:
    _seq = 0

    def __init__(self, proc: asyncio.subprocess.Process):
        Worker._seq += 1
        self.id = Worker._seq
        self.proc = proc
        self.current_job: Job | None = None
        self.init_future: asyncio.Future | None = None
        self.dead = False

    @property
    def tag(self) -> str:
        return f"worker-{self.id}(pid={self.proc.pid})"


class Runtime:
    def __init__(self, db: Database, max_workers_per_version: int = 4,
                 default_timeout_ms: int = 30_000):
        self.db = db
        self.max_workers = max_workers_per_version
        self.default_timeout_ms = default_timeout_ms
        self.versions: dict[tuple[str, str], VersionRuntime] = {}
        self.jobs: dict[str, tuple[VersionRuntime, Worker, Job]] = {}
        self._stopping = False

    # ------------------------------------------------------------ lifecycle

    async def stop(self):
        """Graceful shutdown: fail anything unfinished and kill all workers.
        Terminal states already committed in SQLite stay untouched."""
        self._stopping = True
        for vr in list(self.versions.values()):
            for w in list(vr.workers):
                self._kill_worker(vr, w)
        n = self.db.fail_unfinished("interrupted: service stopped")
        if n:
            log.info("shutdown: marked %d unfinished task(s) FAILED", n)
        await asyncio.sleep(0.1)  # let reader tasks observe EOF and reap

    # -------------------------------------------------------- version state

    def _version_runtime(self, name: str, version: str) -> VersionRuntime:
        key = (name, version)
        vr = self.versions.get(key)
        if vr is None:
            row = self.db.get_plugin(name, version)
            if row is None:
                raise KeyError(f"plugin {name}@{version} is not registered")
            vr = VersionRuntime(name, version, row["entry"], row["config"], row["status"])
            self.versions[key] = vr
        return vr

    def version_stats(self, name: str, version: str) -> dict:
        vr = self.versions.get((name, version))
        if vr is None:
            return {"workers": 0, "in_flight": 0}
        return {"workers": len(vr.workers), "in_flight": vr.in_flight}

    async def switch_default(self, name: str, version: str) -> str | None:
        """Validate the candidate, then move the default pointer.  The old
        version keeps its workers until its bound tasks drain."""
        row = self.db.get_plugin(name, version)
        if row is None:
            raise KeyError(f"plugin {name}@{version} is not registered")
        await self.probe(row["entry"], row["config"])
        old = self.db.set_default(name, version)
        vr_new = self._version_runtime(name, version)
        vr_new.status = "ACTIVE"
        if old and old != version:
            vr_old = self.versions.get((name, old))
            if vr_old is not None:
                vr_old.status = "DRAINING"
                self._maybe_retire(vr_old)
            else:
                # No in-memory runtime => no workers and no live bound tasks
                # (e.g. after a restart); it can retire immediately.
                self.db.set_version_status(name, old, "RETIRED")
        log.info("plugin %s: default %s -> %s", name, old, version)
        return old

    def _maybe_retire(self, vr: VersionRuntime):
        if vr.status != "DRAINING" or vr.in_flight > 0 or vr.pending:
            return
        vr.status = "RETIRED"
        self.db.set_version_status(vr.name, vr.version, "RETIRED")
        for w in list(vr.workers):
            self._shutdown_worker(vr, w)
        log.info("retired plugin %s@%s", vr.name, vr.version)

    # --------------------------------------------------------- task intake

    def enqueue_task(self, task_id: str):
        row = self.db.get_task(task_id)
        if row is None:
            return
        vr = self._version_runtime(row["plugin_name"], row["plugin_version"])
        vr.pending.append(task_id)
        self._schedule_pump(vr)

    def abort(self, task_id: str):
        """Best-effort abort after the DB was CAS-ed to CANCELED.  Kills the
        worker process so a stuck plugin cannot linger; the DB state is the
        authoritative record and is already terminal at this point."""
        entry = self.jobs.get(task_id)
        if entry is None:
            return  # still queued (pump will skip it) or already finished
        vr, worker, job = entry
        log.info("aborting task %s on %s", task_id, worker.tag)
        self._finish_job(vr, worker, job, reuse=False)

    # -------------------------------------------------------------- pumping

    def _schedule_pump(self, vr: VersionRuntime):
        if not self._stopping:
            asyncio.create_task(self._pump(vr))

    async def _pump(self, vr: VersionRuntime):
        if vr.pumping:
            return
        vr.pumping = True
        try:
            while vr.pending and not self._stopping:
                task_id = vr.pending[0]
                row = self.db.get_task(task_id)
                if row is None or row["status"] != "PENDING":
                    vr.pending.popleft()  # canceled (or otherwise resolved) while queued
                    continue
                status, worker, exc = await self._acquire_worker(vr)
                if status == "full":
                    return  # a worker release will re-schedule the pump
                vr.pending.popleft()
                if status == "error":
                    self.db.cas_status(
                        task_id, ("PENDING",), "FAILED",
                        error=f"plugin failed to start: {exc}",
                    )
                    continue
                if not self.db.cas_status(task_id, ("PENDING",), "RUNNING"):
                    vr.idle.append(worker)  # lost a cancel race; keep the worker
                    continue
                self._start_job(vr, worker, row)
        finally:
            vr.pumping = False

    async def _acquire_worker(self, vr: VersionRuntime):
        while vr.idle:
            w = vr.idle.pop()
            if not w.dead and w.proc.returncode is None:
                return ("ok", w, None)
        if len(vr.workers) >= self.max_workers:
            return ("full", None, None)
        try:
            return ("ok", await self._spawn_worker(vr), None)
        except Exception as exc:  # noqa: BLE001 - surfaced as task failure
            return ("error", None, exc)

    # ---------------------------------------------------------- job running

    def _start_job(self, vr: VersionRuntime, worker: Worker, row: dict):
        timeout_ms = row["timeout_ms"] or self.default_timeout_ms
        job = Job(row["id"], row["document"], timeout_ms)
        worker.current_job = job
        vr.in_flight += 1
        self.jobs[job.task_id] = (vr, worker, job)
        try:
            self._send(worker, protocol.parse_message(
                job.job_id, row["document"], dict(row["options"] or {})
            ))
        except Exception as exc:  # noqa: BLE001
            self._fail_job(vr, worker, job, f"failed to send job to plugin: {exc}",
                           reuse=False)
            return
        loop = asyncio.get_running_loop()
        job.timeout_handle = loop.call_later(
            timeout_ms / 1000, self._on_timeout, vr, worker, job
        )
        log.info("task %s running on %s (%s@%s)",
                 job.task_id, worker.tag, vr.name, vr.version)

    def _on_timeout(self, vr: VersionRuntime, worker: Worker, job: Job):
        if worker.current_job is not job:
            return
        log.warning("task %s timed out on %s", job.task_id, worker.tag)
        self._fail_job(vr, worker, job,
                       f"plugin timed out after {job.timeout_ms} ms", reuse=False)

    def _succeed_job(self, vr: VersionRuntime, worker: Worker, job: Job):
        result = {
            "blocks": job.validator.blocks,
            "block_count": len(job.validator.blocks),
        }
        committed = self.db.cas_status(job.task_id, ("RUNNING",), "SUCCEEDED",
                                       result=result)
        log.info("task %s %s", job.task_id,
                 "SUCCEEDED" if committed else "completed but was already terminal")
        self._finish_job(vr, worker, job, reuse=True)

    def _fail_job(self, vr: VersionRuntime, worker: Worker, job: Job,
                  reason: str, reuse: bool):
        committed = self.db.cas_status(job.task_id, ("RUNNING",), "FAILED",
                                       error=reason)
        log.info("task %s %s", job.task_id,
                 f"FAILED ({reason})" if committed else "failure dropped; already terminal")
        self._finish_job(vr, worker, job, reuse=reuse)

    def _finish_job(self, vr: VersionRuntime, worker: Worker, job: Job, reuse: bool):
        if job.timeout_handle is not None:
            job.timeout_handle.cancel()
            job.timeout_handle = None
        if worker.current_job is job:
            worker.current_job = None
        self.jobs.pop(job.task_id, None)
        vr.in_flight -= 1
        if reuse and not worker.dead and not self._stopping:
            vr.idle.append(worker)
            self._schedule_pump(vr)
        else:
            self._kill_worker(vr, worker)
        self._maybe_retire(vr)

    # -------------------------------------------------------- worker processes

    def _send(self, worker: Worker, msg: dict):
        worker.proc.stdin.write(protocol.encode(msg))

    async def _spawn_worker(self, vr: VersionRuntime) -> Worker:
        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", "-m", vr.entry,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(REPO_ROOT),
            env=env,
            limit=protocol.MAX_MESSAGE_BYTES,
        )
        worker = Worker(proc)
        worker.init_future = asyncio.get_running_loop().create_future()
        asyncio.create_task(self._reader_loop(vr, worker))
        asyncio.create_task(self._stderr_loop(worker))
        try:
            self._send(worker, protocol.init_message(vr.name, vr.version, vr.config))
            msg = await asyncio.wait_for(worker.init_future, timeout=INIT_TIMEOUT_S)
        except Exception as exc:
            self._kill_worker(vr, worker)
            if isinstance(exc, asyncio.TimeoutError):
                raise PluginStartError("plugin did not answer the init handshake") from exc
            raise
        if msg.get("type") == "init_error":
            self._kill_worker(vr, worker)
            raise PluginStartError(f"plugin rejected init: {msg.get('error')}")
        if msg.get("type") != "init_ok":
            self._kill_worker(vr, worker)
            raise PluginStartError(f"unexpected init response: {msg.get('type')!r}")
        if msg.get("protocol_version") != protocol.PROTOCOL_VERSION:
            self._kill_worker(vr, worker)
            raise PluginStartError(
                f"incompatible protocol version {msg.get('protocol_version')!r}"
            )
        vr.workers.add(worker)
        log.info("spawned %s for %s@%s", worker.tag, vr.name, vr.version)
        return worker

    def _kill_worker(self, vr: VersionRuntime, worker: Worker):
        worker.dead = True
        self._discard_worker(vr, worker)
        if worker.proc.returncode is None:
            try:
                worker.proc.kill()
            except ProcessLookupError:
                pass

    def _shutdown_worker(self, vr: VersionRuntime, worker: Worker):
        """Graceful stop for retiring versions: ask nicely, kill on ignore."""
        try:
            self._send(worker, protocol.shutdown_message())
        except Exception:  # noqa: BLE001
            self._kill_worker(vr, worker)
            return

        async def grace():
            try:
                await asyncio.wait_for(worker.proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                self._kill_worker(vr, worker)

        asyncio.create_task(grace())

    def _discard_worker(self, vr: VersionRuntime, worker: Worker):
        vr.workers.discard(worker)
        if worker in vr.idle:
            vr.idle.remove(worker)
        if vr.pending:
            self._schedule_pump(vr)  # pool shrank; a replacement may be needed

    # ------------------------------------------------------------ messaging

    async def _reader_loop(self, vr: VersionRuntime, worker: Worker):
        try:
            stdout = worker.proc.stdout
            while True:
                try:
                    line = await stdout.readline()
                except ValueError:
                    self._protocol_violation(vr, worker,
                                             "message exceeds protocol size limit")
                    break
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    self._protocol_violation(vr, worker, "message is not valid JSON")
                    break
                self._on_message(vr, worker, msg)
        except Exception:  # noqa: BLE001
            log.exception("reader loop failed for %s", worker.tag)
        finally:
            job = worker.current_job
            if job is not None:
                log.warning("%s exited with task %s in flight", worker.tag, job.task_id)
                self._fail_job(vr, worker, job,
                               "plugin process exited unexpectedly", reuse=False)
            fut = worker.init_future
            if fut is not None and not fut.done():
                fut.set_exception(PluginStartError("plugin process exited during init"))
            worker.dead = True
            self._discard_worker(vr, worker)
            try:
                await asyncio.wait_for(worker.proc.wait(), timeout=REAP_TIMEOUT_S)
            except asyncio.TimeoutError:
                try:
                    worker.proc.kill()
                except ProcessLookupError:
                    pass

    async def _stderr_loop(self, worker: Worker):
        try:
            while True:
                line = await worker.proc.stderr.readline()
                if not line:
                    break
                log.info("%s stderr: %s", worker.tag,
                         line.decode("utf-8", errors="replace").rstrip())
        except Exception:  # noqa: BLE001
            pass

    def _on_message(self, vr: VersionRuntime, worker: Worker, msg):
        if not isinstance(msg, dict):
            self._protocol_violation(vr, worker, "message is not a JSON object")
            return
        mtype = msg.get("type")
        if mtype in ("init_ok", "init_error"):
            fut = worker.init_future
            if fut is not None and not fut.done():
                fut.set_result(msg)
            return
        job = worker.current_job
        if job is None:
            # Late message from a job that already ended (timeout/cancel/kill).
            # It must not affect whatever this slot does next.
            log.info("%s: ignoring %s with no active job", worker.tag, mtype)
            return
        if msg.get("job_id") != job.job_id:
            log.warning("%s: ignoring stale message for job %s (active job is %s)",
                        worker.tag, msg.get("job_id"), job.job_id)
            return
        if mtype == "batch":
            err = job.validator.validate_batch(msg)
            if err:
                self._protocol_violation(vr, worker, err)
        elif mtype == "done":
            err = job.validator.validate_done(msg)
            if err:
                self._protocol_violation(vr, worker, err)
            else:
                self._succeed_job(vr, worker, job)
        elif mtype == "error":
            self._fail_job(vr, worker, job,
                           f"plugin reported error: {msg.get('error')}", reuse=True)
        else:
            self._protocol_violation(vr, worker, f"unexpected message type {mtype!r}")

    def _protocol_violation(self, vr: VersionRuntime, worker: Worker, reason: str):
        log.warning("%s protocol violation: %s", worker.tag, reason)
        job = worker.current_job
        if job is not None:
            self._fail_job(vr, worker, job,
                           f"plugin protocol violation: {reason}", reuse=False)
        else:
            self._kill_worker(vr, worker)

    # ------------------------------------------------------- candidate probe

    async def probe(self, entry: str, config: dict):
        """Validate a candidate plugin version before it may become default:
        spawn it, require a compatible init handshake, then run a smoke parse
        whose batch stream is fully validated.  Raises ProbeError on failure."""
        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-u", "-m", entry,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(REPO_ROOT),
                env=env,
                limit=protocol.MAX_MESSAGE_BYTES,
            )
        except OSError as exc:
            raise ProbeError(f"failed to spawn plugin: {exc}") from exc

        async def drain_stderr():
            try:
                while await proc.stderr.readline():
                    pass
            except Exception:  # noqa: BLE001
                pass

        stderr_task = asyncio.create_task(drain_stderr())
        try:
            try:
                await self._probe_session(proc, config)
            except ProbeError:
                raise
            except Exception as exc:  # noqa: BLE001 - e.g. broken pipe on write
                raise ProbeError(f"plugin probe failed: {exc}") from exc
        finally:
            stderr_task.cancel()
            if proc.returncode is None:
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except asyncio.TimeoutError:
                    proc.kill()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=2)
                    except asyncio.TimeoutError:
                        pass

    async def _probe_session(self, proc, config: dict):
        proc.stdin.write(protocol.encode(protocol.init_message("probe", "probe", config)))
        msg = await self._read_probe_message(proc, INIT_TIMEOUT_S)
        if msg.get("type") == "init_error":
            raise ProbeError(f"plugin rejected init: {msg.get('error')}")
        if msg.get("type") != "init_ok":
            raise ProbeError(f"expected init_ok, got {msg.get('type')!r}")
        if msg.get("protocol_version") != protocol.PROTOCOL_VERSION:
            raise ProbeError(
                f"incompatible protocol version {msg.get('protocol_version')!r}; "
                f"service requires {protocol.PROTOCOL_VERSION}"
            )
        # Smoke parse: the streamed result must pass the same validation
        # as a real job (order, ranges, structure, completeness).
        doc = "# Smoke Test\n\nfirst line\nsecond line\n"
        job_id = "probe-" + uuid.uuid4().hex
        proc.stdin.write(protocol.encode(protocol.parse_message(job_id, doc, {})))
        validator = BatchValidator(doc)
        while True:
            msg = await self._read_probe_message(proc, SMOKE_TIMEOUT_S)
            if msg.get("job_id") != job_id:
                raise ProbeError("plugin used a wrong job_id during smoke test")
            mtype = msg.get("type")
            if mtype == "batch":
                err = validator.validate_batch(msg)
                if err:
                    raise ProbeError(f"smoke test batch invalid: {err}")
            elif mtype == "done":
                err = validator.validate_done(msg)
                if err:
                    raise ProbeError(f"smoke test completion invalid: {err}")
                break
            elif mtype == "error":
                raise ProbeError(f"plugin failed the smoke parse: {msg.get('error')}")
            else:
                raise ProbeError(f"unexpected message during smoke test: {mtype!r}")
        proc.stdin.write(protocol.encode(protocol.shutdown_message()))

    @staticmethod
    async def _read_probe_message(proc, timeout: float) -> dict:
        try:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            raise ProbeError(f"plugin did not respond within {timeout:.0f}s") from None
        if not line:
            raise ProbeError("plugin process exited unexpectedly")
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            raise ProbeError("plugin emitted invalid JSON") from None
        if not isinstance(msg, dict):
            raise ProbeError("plugin message is not a JSON object")
        return msg
