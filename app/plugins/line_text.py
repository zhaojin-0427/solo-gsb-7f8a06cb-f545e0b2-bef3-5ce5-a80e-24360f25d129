"""可信插件：逐行文本切分。

每个输出块对应原文档中的一行（不含换行符）；换行符本身也作为
``newline`` 块输出，因此块序列严格、无重叠地覆盖整个文档。

配置项 ``emit_empty``（默认 True）是否输出空行块；即便关闭，
区间覆盖仍保持完整（换行块照常输出）。
"""

from __future__ import annotations

from typing import Any

from app.runtime.child import serve


def parse(document: str, config: dict[str, Any], emit) -> None:
    batch: list[dict[str, Any]] = []
    max_batch = int(config.get("batch_size", 64))

    pos = 0
    n = len(document)
    while pos < n:
        nl = document.find("\n", pos)
        if nl == -1:
            line_end = n
            next_pos = n
        else:
            line_end = nl
            next_pos = nl + 1

        if line_end > pos:
            batch.append(
                {"type": "line", "text": document[pos:line_end], "start": pos, "end": line_end}
            )
        else:
            batch.append(
                {"type": "empty_line", "text": "", "start": pos, "end": pos}
            )
        if nl != -1:
            batch.append(
                {"type": "newline", "text": "\n", "start": nl, "end": nl + 1}
            )

        pos = next_pos

        if len(batch) >= max_batch:
            emit(batch)
            batch = []

    if n == 0:
        # 空文档：零块即完整覆盖，直接返回让运行时发 finish。
        return
    if batch:
        emit(batch)


def main(config: dict[str, Any]) -> None:
    serve(parse)
