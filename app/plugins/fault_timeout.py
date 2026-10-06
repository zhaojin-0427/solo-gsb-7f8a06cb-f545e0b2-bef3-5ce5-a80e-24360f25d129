"""故障插件：超时。

文档中含 ``[[timeout]]`` 时，输出第一批块后永久挂起，只能由父进程
按任务超时杀死。注意：冒烟文档不含标记，因此该版本**能够**通过
登记校验并被设为默认——校验通过不代表后续任务不会故障。
"""

from __future__ import annotations

import time
from typing import Any

from app.runtime.child import Client

from . import fault_common

MARKER = "[[timeout]]"


def handle(document: str, config: dict[str, Any], client: Client, task_id: str) -> str:
    if MARKER in document:
        idx = document.find(MARKER)
        # 先吐出标记之前的内容（合法第一批），再挂起。
        prefix = document[:idx]
        blocks = fault_common._line_blocks(prefix) if prefix else []
        client.send_blocks(task_id, 0, blocks)
        # flush 后阻塞，制造“在途任务超时”。
        client.stdout.flush()
        while True:
            time.sleep(60)
            # 即便被外部唤醒也不退出。
            if os.getppid() == 1:
                os._exit(2)
    # 正常路径：一批发完。
    client.send_blocks(task_id, 0, fault_common._line_blocks(document))
    client.send_finish(task_id, 1, len(fault_common._line_blocks(document)))
    return "handled"


def main(config: dict[str, Any]) -> None:
    fault_common.run(handle)
