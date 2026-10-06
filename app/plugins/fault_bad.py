"""故障插件：输出损坏 / 迟到消息。

通过文档标记制造各种违反协议的输出：

* ``[[bad_json]]``      —— 写出无法解析的一行；
* ``[[bad_order]]``     —— 批次号从 1 开始（缺少批次 0）；
* ``[[gap]]``           —— 块区间之间留有空洞；
* ``[[overlap]]``       —— 块区间互相重叠；
* ``[[wrong_text]]``    —— text 与原文区间不一致；
* ``[[truncated]]``     —— 发出批次后直接退出，从不发送 finish；
* ``[[late:<task>]]``   —— 正常完成当前任务后，再冒用旧 task 标识
                           发送一帧，用于验证“已退出/已复用槽位的
                           迟到消息不得写入新任务”。

这些情况都只能让**当前任务**失败，且不得污染同进程后续任务。
"""

from __future__ import annotations

from typing import Any

from app.runtime.child import Client

from . import fault_common


def handle(document: str, config: dict[str, Any], client: Client, task_id: str) -> str:
    if "[[bad_json]]" in document:
        client.stdout.write("{this is not json\n")
        client.stdout.flush()
        return "handled"

    if "[[bad_order]]" in document:
        blocks = fault_common._line_blocks(document)
        client.send_blocks(task_id, 1, blocks)  # 故意跳过批次 0
        return "handled"

    if "[[gap]]" in document:
        n = len(document)
        client.send_blocks(
            task_id,
            0,
            [
                {"type": "line", "text": document[:1], "start": 0, "end": 1},
                {"type": "line", "text": document[2:n], "start": 2, "end": n},
            ],
        )
        client.send_finish(task_id, 1, 2)
        return "handled"

    if "[[overlap]]" in document:
        n = len(document)
        mid = max(1, n // 2)
        client.send_blocks(
            task_id,
            0,
            [
                {"type": "line", "text": document[: mid + 1], "start": 0, "end": mid + 1},
                {"type": "line", "text": document[mid:n], "start": mid, "end": n},
            ],
        )
        client.send_finish(task_id, 1, 2)
        return "handled"

    if "[[wrong_text]]" in document:
        n = len(document)
        client.send_blocks(
            task_id,
            0,
            [{"type": "line", "text": "NOT-THE-DOCUMENT", "start": 0, "end": n}],
        )
        client.send_finish(task_id, 1, 1)
        return "handled"

    if "[[truncated]]" in document:
        client.send_blocks(task_id, 0, fault_common._line_blocks(document))
        client.stdout.flush()
        # 既不 finish 也不 error，直接结束（模拟输出截断后进程消失）。
        fault_common.hard_exit(2)

    if "[[late:" in document:
        import re

        m = re.search(r"\[\[late:([^\]]+)\]\]", document)
        old_task = m.group(1) if m else "unknown"
        blocks = fault_common._line_blocks(document)
        client.send_blocks(task_id, 0, blocks)
        client.send_finish(task_id, 1, len(blocks))
        client.stdout.flush()
        # 当前任务已正常结束；随后冒用另一个（可能已被复用的）槽位任务
        # 标识再发一帧迟到的 blocks。
        client.send_blocks(
            old_task,
            0,
            [{"type": "line", "text": "STALE", "start": 0, "end": 5}],
        )
        client.stdout.flush()
        return "handled"

    # 未使用标记：正常路径。
    blocks = fault_common._line_blocks(document)
    client.send_blocks(task_id, 0, blocks)
    client.send_finish(task_id, 1, len(blocks))
    return "handled"


def main(config: dict[str, Any]) -> None:
    fault_common.run(handle)
