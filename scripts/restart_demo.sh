#!/usr/bin/env bash
# 重启恢复演练（需要 docker compose 环境）。
#
# 流程：
#   1. compose 拉起服务（parser-data 卷持久化 SQLite）；
#   2. 提交一个很快完成的任务、一个一直挂起的超时任务；
#   3. 重启容器（模拟服务重启/崩溃恢复）；
#   4. 验证：已发布结果仍可查询；在途任务变为 INTERRUPTED；重启后新任务正常。
#
# 用法：bash scripts/restart_demo.sh
set -euo pipefail

BASE=${PARSER_BASE:-http://127.0.0.1:8000}
BODY=/tmp/rd_body.json

# api METHOD PATH [JSON_BODY] -> 仅把响应体写入 $BODY
api() {
  local method=$1 path=$2 body=${3:-}
  if [ -n "$body" ]; then
    curl -sS -o "$BODY" -X "$method" "$BASE$path" \
      -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -o "$BODY" -X "$method" "$BASE$path"
  fi
}

json_field() {
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' \
    "$BODY" "$1"
}

wait_status() {
  local tid=$1 deadline=$((SECONDS + 30)) status
  while [ $SECONDS -lt $deadline ]; do
    api GET "/tasks/$tid"
    status=$(json_field status)
    case "$status" in
      COMPLETED|FAILED|CANCELLED|INTERRUPTED) echo "$status"; return 0;;
    esac
    sleep 0.3
  done
  echo "TIMEOUT_WAITING"; return 1
}

echo "==> 1) 拉起服务"
docker compose up -d --build
for _ in $(seq 1 30); do
  curl -fsS "$BASE/health" >/dev/null && break
  sleep 1
done

echo "==> 2) 提交一个正常任务并等待完成"
api POST /parse '{"plugin":"line-text","request_key":"rd-done","document":"落库结果\n"}'
DONE_ID=$(json_field task_id)
[ "$(wait_status "$DONE_ID")" = COMPLETED ]

echo "==> 3) 提交一个在途挂起任务"
api POST /parse '{"plugin":"fault-timeout","version":"1.0","request_key":"rd-hang","document":"hang [[timeout]]"}'
HANG_ID=$(json_field task_id)
sleep 1
api GET "/tasks/$HANG_ID"
echo "    重启前状态: $(json_field status)"

echo "==> 4) 重启容器（数据卷保留）"
docker compose restart
for _ in $(seq 1 30); do
  curl -fsS "$BASE/health" >/dev/null && break
  sleep 1
done

echo "==> 5) 验证恢复语义"
api GET "/tasks/$DONE_ID/blocks"
python3 - "$BODY" <<'PY'
import json, sys
b = json.load(open(sys.argv[1]))
assert b["count"] >= 1, b
assert "".join(x["text"] for x in b["blocks"]) == "落库结果\n"
print("    [OK] 已发布结果重启后仍可查询, blocks =", b["count"])
PY

api GET "/tasks/$HANG_ID"
python3 - "$BODY" <<'PY'
import json, sys
t = json.load(open(sys.argv[1]))
assert t["status"] == "INTERRUPTED", t
print("    [OK] 在途任务重启后状态 =", t["status"])
PY

api POST /parse '{"plugin":"line-text","request_key":"rd-after","document":"重启后新任务\n"}'
NEW_ID=$(json_field task_id)
st=$(wait_status "$NEW_ID")
[ "$st" = COMPLETED ] && echo "    [OK] 重启后新任务 $st" || { echo "    [FAIL] $st"; exit 1; }

echo "==> 演练完成。可用 'docker compose logs -f' 查看恢复日志。"
