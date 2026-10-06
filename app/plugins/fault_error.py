"""故障插件：返回结构化错误。

文档含 ``[[error]]`` 时，不输出块，直接发送协议规定的 ``error`` 帧
（code/message 齐全）。父进程应把任务终态置为 FAILED 且错误码为
``PLUGIN_ERROR``，子进程自身保持存活并继续服务后续任务。
"""

from __future__ import annotations

from typing import Any

from app.runtime.child import Client

from . import fault_common


def handle(document: str, config: dict[str, Any], client: Client, task_id: str) -> str:
    if "[[error]]" in document:
        client.send_error(
            task_id,
            "PLUGIN_ERROR",
            "plugin refused document: intentional structured error sample",
        )
        return "handled"

    blocks = fault_common._line_blocks(document)
    client.send_blocks(task_id, 0, blocks)
    client.send_finish(task_id, 1, len(blocks))
    return "handled"


def main(config: dict[str, Any]) -> None:
    fault_common.run(handle)
