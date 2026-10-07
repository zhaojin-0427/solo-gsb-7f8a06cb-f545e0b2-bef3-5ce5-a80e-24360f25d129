# 文档解析插件运行服务

基于 **Python / FastAPI + SQLite + 独立子进程** 的本地文档解析插件运行时。插件以
独立子进程运行，通过明确的 JSON-Lines 进程协议通信；服务提供插件版本登记 /
切换、解析任务提交 / 查询 / 取消的 HTTP API，并对子进程崩溃、输出损坏、
超时等运行故障做任务级隔离。

## 目录结构

```
├── app/
│   ├── main.py                  # FastAPI HTTP 层
│   ├── runtime.py               # 进程池、版本生命周期、任务执行
│   ├── protocol.py              # 进程通信协议 + 分批结果校验器
│   ├── db.py                    # SQLite 持久化（终态 CAS 提交）
│   ├── schemas.py               # 请求模型
│   └── plugins/
│       ├── harness.py           # 可信插件共用 harness（协议循环）
│       ├── line_text.py         # 可信插件：逐行文本
│       ├── markdown_sections.py # 可信插件：Markdown 分节
│       ├── faulty_timeout.py    # 故障插件：[[timeout]] 触发挂起
│       ├── faulty_crash.py      # 故障插件：[[crash]] 触发崩溃
│       ├── faulty_bad_structure.py # 故障插件：[[corrupt]] 触发错误结构
│       └── faulty_bad_init.py   # 故障插件：协议版本不兼容
├── scripts/demo.py              # 端到端演示 + 故障演练
├── scripts/restart_drill.py     # 重启演练
├── Dockerfile / compose.yaml    # 一体化部署
└── requirements.txt
```

## 快速开始

```bash
docker compose up -d --build
curl localhost:8000/health
python scripts/demo.py                 # 端到端演示（需要 pip install httpx）
python scripts/restart_drill.py        # 重启演练（容器内重启服务）
```

本地开发（不用 Docker）：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --port 8000
.venv/bin/python scripts/demo.py
```

数据落在 SQLite（容器卷 `app-data` → `/data/app.db`，本地默认 `./data/app.db`），
重启后已发布结果仍可查询。服务按设计以**单 worker 进程**运行（插件池在进程内）。

## 插件协议（JSON Lines，stdin/stdout）

每行一个 JSON 对象；插件日志只能写 stderr，stdout 专用于协议。

宿主 → 插件：

| 消息 | 说明 |
|---|---|
| `{"type":"init","protocol_version":1,"plugin":{"name","version"},"config":{}}` | 初始化握手，携带登记时的 config |
| `{"type":"parse","job_id":"...","content":"...","options":{}}` | 解析请求；options 为任务级覆盖 |
| `{"type":"shutdown"}` | 请求退出（版本退役时） |

插件 → 宿主：

| 消息 | 说明 |
|---|---|
| `{"type":"init_ok","protocol_version":1,...}` / `{"type":"init_error","error"}` | 握手应答；协议版本必须兼容 |
| `{"type":"batch","job_id","seq":N,"blocks":[{"start","end","text"}]}` | 分批结果，seq 从 0 严格递增 |
| `{"type":"done","job_id","batches":N}` | 完结；N 必须等于实际批次数 |
| `{"type":"error","job_id","error"}` | 任务级错误 |

**分批结果校验**（`app/protocol.py` 的 `BatchValidator`，探测与正式任务共用）：

- 顺序：`seq` 必须严格等于期望序号；
- 区间：`0 <= start <= end <= len(文档)`，块间有序不重叠（均为**码点**偏移）；
- 结构：`text` 必须与文档 `[start,end)` 切片逐字一致；
- 完整性：只有收到 `done` 且批次数吻合才提交成功——崩溃/超时留下的
  不完整结果**不会**被发布。

## HTTP API

| 方法/路径 | 说明 |
|---|---|
| `GET /health` | 健康检查 |
| `POST /plugins` | 登记插件版本（不可变）。`{"name","version","entry","config"}`；entry 限 `app.plugins.*` 仓库内可信模块。同版本不同规格 → 409；同规格 → 幂等返回 |
| `GET /plugins` | 列出插件、默认版本、各版本状态/工人数/在途数 |
| `POST /plugins/{name}/default` | **验证后**切换默认版本：`{"version"}`。候选须通过初始化握手 + 协议兼容校验 + 冒烟解析，失败 → 400 且**保持原默认** |
| `POST /tasks` | 提交解析：`{"request_key","plugin","document","version"?,"timeout_ms"?,"options"?}`。任务在接收时**绑定当前默认版本**；显式指定非默认版本 → 409 |
| `GET /tasks/{id}` | 查询任务状态与结果 |
| `GET /tasks` | 最近任务列表 |
| `POST /tasks/{id}/cancel` | 取消；已终态 → 409 |

**幂等**：`request_key` 唯一。同键同文档 → 返回原任务（`"deduplicated":true`）；
同键异文档 → 409。

**任务状态机**：`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELED`。
成功结果形如 `{"blocks":[{"start","end","text"}...],"block_count":N}`。

示例：

```bash
curl -X POST localhost:8000/plugins -H 'Content-Type: application/json' -d '{
  "name":"line-text","version":"1.0.0",
  "entry":"app.plugins.line_text","config":{"batch_size":50}}'
curl -X POST localhost:8000/plugins/line-text/default -d '{"version":"1.0.0"}' \
  -H 'Content-Type: application/json'
curl -X POST localhost:8000/tasks -H 'Content-Type: application/json' -d '{
  "request_key":"doc-1","plugin":"line-text","document":"hello\nworld\n"}'
curl localhost:8000/tasks/<task_id>
```

## 关键设计

**版本生命周期**：`REGISTERED → ACTIVE（默认）→ DRAINING（被取代，仍有在途
任务）→ RETIRED（在途清零，进程退出）`。任务在接收时绑定具体版本；切换默认
只影响之后的任务。旧版本的工人进程为已绑定任务保留，全部结束后才退役退出；
新任务一律绑定当前默认版本（显式请求旧版本 → 409）。

**进程池**：按 `(name, version)` 分组，上限 `POOL_MAX_WORKERS_PER_VERSION`
（默认 4），按需spawn、空闲复用、一进程同时只跑一个任务。崩溃/超时/协议违规
只终止对应任务并废弃该进程，后续任务由新进程承接——故障隔离目标是运行故障
与进程生命周期（插件均为仓库内可信代码，登记时 entry 限定 `app.plugins.*`）。

**终态提交与竞争**：所有终态转移都是对 SQLite 的 compare-and-swap
（`UPDATE ... WHERE status IN (非终态)`），取消与完成**只有一个能成功**；
结果因此**只发布一次**。取消运行中任务会杀掉对应插件进程，保证真正终止。

**迟到消息防护**：每个任务分配随机 `job_id`，运行时只接受当前绑定该槽位的
`job_id` 的消息。被杀/超时插件的迟到消息（或复用槽位上的上一任务消息）会被
识别并丢弃，**不会写入新任务**。

**重启持久化**：已发布结果随 SQLite 持久化，重启后仍可查询。启动恢复把上一
运行遗留的 `PENDING/RUNNING` 任务标记为 `FAILED("interrupted: service
restarted")`（优雅停机时标记 `"interrupted: service stopped"`）——未完成者
有明确状态，且不会出现两份发布结果。

## 故障演练

`scripts/demo.py` 覆盖（全部自动断言）：

1. 登记 + 验证后切换默认版本；
2. 逐行 / Markdown 分节解析，校验码点区间；
3. 幂等：同键同文档返回原任务，异文档 409；
4. **在途任务期间切换默认版本**：慢任务 A 绑定 1.0.0 运行中切到 2.0.0，
   A 仍在旧版本完成，新任务 B 用新版本，旧版本随后 RETIRED；
5. **取消竞争**：运行中取消 → CANCELED 且无结果，重复取消 409；
6. **故障隔离**：`[[timeout]]` 文档触发挂起 → 超时 FAILED；`[[crash]]` 触发
   崩溃 → FAILED 且部分结果未发布；`[[corrupt]]` 触发错误结构 → 协议违规
   FAILED；每次故障后干净文档照常成功，服务保持健康；
7. **候选校验失败**：协议版本不兼容 / 模块不存在 → 400，原默认不变。

重启演练（已发布结果保留、在途任务明确失败）：

```bash
python scripts/restart_drill.py            # 自动 docker compose restart app
python scripts/restart_drill.py --no-docker # 或手动重启本地 uvicorn
```

故障插件对无标记文档表现正常（因此能通过候选验证），只有含对应标记
（`[[timeout]]` / `[[crash]]` / `[[corrupt]]`）的文档才触发故障。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DB_PATH` | `/data/app.db`（容器）/ `./data/app.db`（本地） | SQLite 路径 |
| `POOL_MAX_WORKERS_PER_VERSION` | `4` | 每个版本的进程池上限 |
| `TASK_DEFAULT_TIMEOUT_MS` | `30000` | 任务默认超时（提交时可用 `timeout_ms` 覆盖，上限 120s） |
| `LOG_LEVEL` | `INFO` | 日志级别 |
