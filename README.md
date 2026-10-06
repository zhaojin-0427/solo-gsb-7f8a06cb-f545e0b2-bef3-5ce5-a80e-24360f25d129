# 本地文档解析插件运行服务

一个基于 **Python / FastAPI + SQLite + 独立子进程** 的本地文档解析服务。插件是
仓库内的可信 Python 代码，服务为每个**不可变插件版本**维护独立的常驻子进程池，
通过明确的 NDJSON 行协议通信，并严格处理分批结果校验、版本生命周期、终态竞争、
子进程故障隔离与重启恢复。

> 隔离目标是**运行故障与进程生命周期**（崩溃、超时、输出损坏、版本退出），
> 不是安全沙箱——所有插件都来自本仓库 `app/plugins/`，父进程只加载该白名单
> 路径下的入口模块。

---

## 目录结构

```
app/
  config.py                 # 环境变量配置
  protocol.py               # 父子进程通信协议 + 分批/区间/结构校验
  db.py                     # SQLite 持久化（幂等键、终态 CAS、重启恢复）
  main.py                   # FastAPI：登记/切换/提交/查询/取消
  runtime/
    child.py                # 子进程侧运行时（握手、主循环、Client）
    supervisor.py           # 父进程侧：版本进程池、调度、帧分发、故障隔离
  plugins/
    line_text.py            # 可信插件：逐行文本
    markdown_sections.py    # 可信插件：Markdown 分节
    fault_timeout.py        # 故障样例：超时挂起
    fault_crash.py          # 故障样例：崩溃 / 部分结果后崩溃 / 非零退出
    fault_error.py          # 故障样例：结构化 error 帧
    fault_bad.py            # 故障样例：坏 JSON、乱序、空洞、重叠、文本不符、截断、迟到帧
scripts/
  demo.py                   # 本地故障演练（在途切换/取消/崩溃恢复/迟到帧）
  restart_demo.sh           # Compose 重启恢复演练
tests/test_service.py       # 端到端集成测试（真实子进程）
compose.yaml
Dockerfile
```

---

## 快速开始（Docker Compose）

```bash
docker compose up -d --build
curl -s http://127.0.0.1:8000/health | python3 -m json.tool
```

数据落在命名卷 `parser-data`（容器内 `/data/parser.db`，SQLite WAL）。

本地无 Docker 时：

```bash
pip install -r requirements.txt
export PARSER_DB=./data/parser.db
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

服务首次启动会幂等播种：

| 插件 | 版本 | 入口 | 是否默认 |
|---|---|---|---|
| `line-text` | `1.0` | `app.plugins.line_text` | 是 |
| `markdown-sections` | `1.0` | `app.plugins.markdown_sections` | 是 |
| `fault-timeout` | `1.0` / `2.0` | `app.plugins.fault_timeout` | 否（用于切换演练） |
| `fault-crash` | `1.0` | `app.plugins.fault_crash` | 否 |
| `fault-error` | `1.0` | `app.plugins.fault_error` | 否 |
| `fault-bad` | `1.0` | `app.plugins.fault_bad` | 否 |

播种默认版本前会跑一次真实握手 + 冒烟解析；**校验失败则该插件保持无默认**，
不会影响其他插件。

---

## 进程通信协议

stdin/stdout 上的 **行分隔 JSON（NDJSON，UTF-8，单行上限 64 MiB）**；
stderr 仅用于日志。

父 → 子：

```jsonc
{"kind":"init",  "protocol":"doc-parser/1", "config":{...}}
{"kind":"parse", "task":"<task_id>", "document":"原文（Unicode 字符串）"}
{"kind":"shutdown"}
```

子 → 父：

```jsonc
{"kind":"ready",  "protocol":"doc-parser/1"}
{"kind":"blocks", "task":"<task_id>", "batch":0,
 "blocks":[{"type":"line","text":"…","start":0,"end":12, ...元数据]}
{"kind":"finish", "task":"<task_id>", "batches":3, "blocks":42}
{"kind":"error",  "task":"<task_id>", "code":"PLUGIN_ERROR", "message":"…"}
```

* 区间是**码点偏移**，语义等同 Python 切片：`text == document[start:end]`。
* `batch` 必须从 0 开始连续递增；`finish.batches` 必须等于实收批次数，
  `finish.blocks` 必须等于实收块数。
* 收齐后做终态校验：块按 `start` 严格有序、互不重叠、不留空洞、不越界，
  且整体恰好覆盖 `[0, len(document))`。空文档允许零块。
* 任一批次乱序、结构非法、或终态校验不过，任务立即 `FAILED`，
  **已收到的不完整结果一律丢弃，绝不作为成功发布**；输出流被判定不可信时
  只杀掉该 worker，由池补一个新进程。

可信插件只需实现 `parse(document, config, emit)`，由 `app.runtime.child.serve`
统一握手、计数和补发 `finish`（插件无法伪造批次号/块数）；插件抛异常会被
转成结构化 `error` 帧且 worker 继续存活。故障插件直接使用底层 `Client`
精确制造故障。

---

## 版本生命周期与并发语义

* **不可变登记**：`(plugin, version)` 唯一；重复登记且 entrypoint/config 完全
  一致时幂等返回，任何字段差异返回 `409 VERSION_IMMUTABLE`。登记/切默认前用
  一次性短命子进程做初始化握手 + 冒烟解析，**校验失败不落库、不改默认**。
* **接收即绑定**：任务在 `/parse` 接收的瞬间把 `(plugin, version)` 冻结进任务行。
  之后切换默认版本只影响**之后的**任务，在途任务始终跑在旧版本上。
* **排水退出**：默认从 A 切到 B 后，A 变为 `DRAINING`，不再领取新任务，但
  保留进程直到所有已绑定任务结束；引用计数归零才 `RETIRED` 并退出子进程。
  退役版本拒绝新任务（`409 VERSION_RETIRED`）。
* **终态提交（CAS）**：所有状态迁移都是带前置状态的条件 UPDATE。
  “完成”与“取消”竞争时只有一方成功：`COMPLETED + 全部块` 与 `CANCELLED`
  在同一个 `BEGIN IMMEDIATE` 事务里二选一，物理上不可能出现两份终态发布。
* **迟到消息**：每条帧必须来自“当前正绑定该任务的 worker + era”。槽位复用后
  到达的旧任务帧会被丢弃、记录事件并隔离该 worker，**绝不会写入复用槽位的新任务**。
* **故障隔离**：崩溃、输出损坏、超时只杀掉对应任务绑定的那一个 worker；
  同版本其他 worker 与其他版本的任务不受影响，池随后补员。
* **幂等键**：`(plugin, request_key)` 唯一并记录文档 SHA-256：
  同键同文档返回原任务（含其终态与结果），同键异文档 `409`。
* **重启恢复**：启动时把所有遗留的 `PENDING/RUNNING` 任务统一置为
  `INTERRUPTED`（明确终态）；已 `COMPLETED` 的结果只追加不覆盖，重启后照常查询，
  块仍只有一份。

任务状态机：

```
PENDING ──dispatch──▶ RUNNING ──finish+校验通过──▶ COMPLETED（终态，块同事务发布）
   │                    ├─结构化 error/崩溃/坏帧──▶ FAILED（终态）
   │                    ├─超时──────────────────▶ FAILED(TIMEOUT)（终态）
   │                    └─取消（CAS 先行）──────▶ CANCELLED（终态）
   └─取消/重启──────────▶ CANCELLED / INTERRUPTED（终态）
```

---

## HTTP API

### 元数据

`GET /health` — 服务状态、默认版本表、各版本池状态（ACTIVE/DRAINING/RETIRED、
worker 数、引用计数）。

`GET /plugins` — 所有已登记版本、是否默认、版本池生命周期状态。

### 登记不可变版本

`POST /plugins/register`

```jsonc
{
  "plugin": "line-text",
  "version": "2.0",
  "entrypoint": "app.plugins.line_text",   // 必须位于 app.plugins 包下
  "config": {"batch_size": 128},
  "make_default": false
}
```

* `201` 新登记；`200` 内容一致的幂等重放；
* `409 VERSION_IMMUTABLE` 同版本号试图改 entrypoint/config；
* `422 VALIDATION_FAILED` 初始化/握手/冒烟校验失败（原默认保持不变）。

### 验证后切换默认版本

`POST /plugins/default`

```jsonc
{"plugin": "fault-timeout", "version": "2.0"}
```

切换前重新做一次兼容校验，失败返回 `422` 且 `detail.default_unchanged` 指向前版本。
成功后响应给出 `previous_default`，旧版本进入排水。

### 提交解析

`POST /parse` → `202`

```jsonc
{
  "plugin": "line-text",
  "version": "1.0",        // 可省略：省略时使用当前默认版本
  "request_key": "doc-001",
  "document": "alpha\nbeta\n"
}
```

```jsonc
{"task_id":"…","status":"PENDING","reused":false,"plugin":"line-text","version":"1.0"}
```

错误：`404 NO_DEFAULT_VERSION/UNKNOWN_VERSION`、`409 REQUEST_KEY_DOCUMENT_MISMATCH`
/`VERSION_RETIRED`、`413 DOCUMENT_TOO_LARGE`、`503 POOL_UNAVAILABLE`。

### 查询

* `GET /tasks/{id}` — 状态、绑定版本、错误码/信息；完成时内联 `blocks`。
* `GET /tasks/{id}/blocks` — 仅终态 COMPLETED 可取，否则 `409 TASK_NOT_COMPLETED`。
* `GET /tasks/{id}/events` — 生命周期事件（created/queued/dispatched/completed/
  failed/cancelled/interrupted/worker_replaced/stale_frame_dropped…）。

### 取消

`POST /tasks/{id}/cancel`

* 取消抢先：`200` 且任务 `CANCELLED`，对应 worker 被杀；
* 重复取消：`200`（取消幂等）；
* 完成抢先：`409 TASK_ALREADY_TERMINAL`，结果保持 COMPLETED 可查。

---

## 故障演练

### 一键演练（建议把超时调小后观看）

```bash
# 终端 A
PARSER_TASK_TIMEOUT_SECONDS=8 uvicorn app.main:app --port 8000

# 终端 B
python3 scripts/demo.py
```

`demo.py` 用纯标准库驱动，逐步断言并打印 `PASS/FAIL`：

1. 两个可信插件的区间严格覆盖与 Markdown 标题级别；
2. 同键同文档返回原任务、同键异文档 409；
3. **在途任务期间切换默认版本**：新任务绑定 2.0、旧任务钉在 1.0 且旧池
   `DRAINING/refcount=1`；
4. **取消竞争**：取消在途超时任务成功、重复取消幂等、已完成任务取消 409；
   旧版本随后 `RETIRED` 且拒绝新任务；
5. **崩溃恢复**：`[[crash]]` 任务 `FAILED(WORKER_CRASHED)`；
   `[[crash_after_blocks]]` 的部分结果绝不发布（取块 409）；池补员后后续任务
   COMPLETED；结构化 error 不杀 worker；超时只杀该 worker，同版本其他任务继续；
6. 坏 JSON / 批次乱序 / 区间空洞 / 重叠 / text 不符 / 无 finish 截断
   全部精确失败且槽位被替换；
7. `[[late:<旧任务id>]]` 迟到帧被丢弃，复用槽位的新任务结果中没有 `STALE` 块。

### 重启恢复演练（Compose）

```bash
bash scripts/restart_demo.sh
```

该脚本会：提交一个完成任务与一个挂起任务 → `docker compose restart` →
断言已发布结果仍可查询、在途任务变为 `INTERRUPTED`、重启后新任务 COMPLETED。

### 故障触发标记速查

| 插件 | 文档中包含 | 行为 |
|---|---|---|
| `fault-timeout` | `[[timeout]]` | 先发一批合法块后永久挂起，等待超时被杀 |
| `fault-crash` | `[[crash]]` | 立即硬退出（无输出） |
| `fault-crash` | `[[crash_after_blocks]]` | 发部分合法块后硬退出 |
| `fault-crash` | `[[exit17]]` | 非零退出码 |
| `fault-error` | `[[error]]` | 返回结构化 error 帧，worker 存活 |
| `fault-bad` | `[[bad_json]]` / `[[bad_order]]` / `[[gap]]` / `[[overlap]]` / `[[wrong_text]]` / `[[truncated]]` / `[[late:<task>]]` | 各类输出损坏与迟到帧 |
| `fault-initfail` | （无标记） | 收到 init 即退出，登记/切默认校验必失败，原默认保持不变 |

不含任何标记时，故障插件表现为正常逐行解析（因此登记前的冒烟校验可以通过，
同时仍保留制造运行故障的能力）。

---

## 测试

```bash
pip install -r requirements.txt pytest pytest-asyncio httpx
pytest -q
```

`tests/test_service.py` 通过 ASGI in-process 驱动 FastAPI，但插件全部运行在
**真实独立子进程**中，覆盖：区间覆盖与码点一致、空文档、幂等键冲突、结构化错误、
崩溃与池补员、部分结果不发布、超时隔离、全部坏帧类别、取消竞争、迟到帧与槽位
复用、在途切换/排水/退役、不可变登记、校验失败保持原默认，以及同库重启恢复。

---

## 关键环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `PARSER_DB` | `data/parser.db` | SQLite 路径 |
| `PARSER_WORKERS_PER_VERSION` | `2` | 每个在役版本常驻 worker 数 |
| `PARSER_TASK_TIMEOUT_SECONDS` | `30` | 单任务超时 |
| `PARSER_VALIDATE_TIMEOUT_SECONDS` | `10` | 登记/切默认校验超时 |
| `PARSER_SHUTDOWN_GRACE_SECONDS` | `3` | 优雅关停宽限 |
| `PARSER_MAX_DOCUMENT_CODEPOINTS` | `1000000` | 文档码点上限 |
| `PARSER_LOG_LEVEL` | `INFO` | 日志级别 |
