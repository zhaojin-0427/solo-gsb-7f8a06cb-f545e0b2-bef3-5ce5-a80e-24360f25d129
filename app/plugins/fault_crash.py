"""故障插件：子进程崩溃。

* ``[[crash]]``  —— 不输出任何结果直接硬退出（段错误式崩溃）；
* ``[[crash_after_blocks]]`` —— 先输出一个合法批次再崩溃，
  用于验证“不完整结果绝不能作为成功发布”；
* ``[[exit17]]`` —— 以非零退出码退出。
"""

from __future__ import annotations

from typing import Any

from app.runtime.child import Client

from . import fault_common


def handle(document: str, config: dict[str, Any], client: Client, task_id: str) -> str:
    if "[[crash_after_blocks]]" in document:
        idx = document.find("[[crash_after_blocks]]")
        prefix = document[:idx]
        client.send_blocks(task_id, 0, fault_common._line_blocks(prefix))
        client.stdout.flush()
        fault_common.hard_exit(2)

    if "[[crash]]" in document:
        client.stdout.flush()
        fault_common.hard_exit(9)

    if "[[exit17]]" in document:
        client.stdout.flush()
        raise SystemExit(17)

    blocks = fault_common._line_blocks(document)
    client.send_blocks(task_id, 0, blocks)
    client.send_finish(task_id, 1, len(blocks))
    return "handled"


def main(config: dict[str, Any]) -> None:
    fault_common.run(handle)
