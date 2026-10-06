"""故障插件共用驱动。

故障插件不走 :func:`app.runtime.child.serve` 的安全主循环，而是直接
操作底层 :class:`~app.runtime.child.Client`，以便精确制造各类运行故障。
各故障模块通过 ``config['mode']`` 或文档中的 ``[[标记]]`` 触发故障；
无标记时正常输出，保证登记版本时的冒烟校验可以通过（故障插件同样
是仓库内“可信代码”，隔离目标是运行故障而非恶意代码）。
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Callable

from app.runtime.child import Client, bootstrap


def _line_blocks(document: str, start: int = 0) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    pos = start
    n = len(document)
    while pos < n:
        nl = document.find("\n", pos)
        line_end = n if nl == -1 else nl
        if line_end > pos:
            blocks.append(
                {"type": "line", "text": document[pos:line_end], "start": pos, "end": line_end}
            )
        if nl != -1:
            blocks.append({"type": "newline", "text": "\n", "start": nl, "end": nl + 1})
            pos = nl + 1
        else:
            pos = line_end
    return blocks


def run(handle: Callable[[str, dict[str, Any], Client, str], str | None]) -> None:
    """通用主循环。

    ``handle(document, config, client, task_id)`` 返回值：
    * ``"ok"`` 或 None —— 由驱动自动补 finish（块数自动统计）；
    * ``"handled"`` —— 插件已自行处理帧序列（不再补发 finish）。
    """
    config, client = bootstrap()
    while True:
        frame = client.read_frame()
        if frame is None:
            return
        if frame.get("kind") == "shutdown":
            return
        if frame.get("kind") != "parse":
            continue
        task_id = frame["task"]
        document = frame.get("document", "")
        try:
            result = handle(document, config, client, task_id)
        except Exception as exc:  # 兜底：故障插件自身的意外异常
            client.send_error(task_id, "PLUGIN_ERROR", repr(exc))
            continue
        if result == "handled":
            continue


def hard_exit(code: int = 2) -> None:
    """模拟段错误/被 SIGKILL：立即退出，不走 finally、不刷缓冲。"""
    sys.stderr.flush()
    os._exit(code)


def sleep_until_killed(seconds: float = 3600.0) -> None:
    # 忙等/长睡眠，确保只能由父进程超时杀进程来结束。
    time.sleep(seconds)
