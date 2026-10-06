"""可信插件：Markdown 分节。

按行扫描，ATX 标题行（``#``~``######``）标记为 ``heading`` 块并带
``level`` 属性；其余内容行为 ``content`` 块；换行符独立为
``newline`` 块。调用方按 heading 即可还原分节结构，同时所有块严格
覆盖完整文档。
"""

from __future__ import annotations

import re
from typing import Any

from app.runtime.child import serve

_ATX = re.compile(r"^(#{1,6})(?:[ \t]+|$)")


def _is_heading(line: str) -> int:
    m = _ATX.match(line)
    return len(m.group(1)) if m else 0


def parse(document: str, config: dict[str, Any], emit) -> None:
    batch: list[dict[str, Any]] = []
    max_batch = int(config.get("batch_size", 32))
    pos = 0
    n = len(document)

    while pos < n:
        nl = document.find("\n", pos)
        line_end = n if nl == -1 else nl
        line = document[pos:line_end]
        level = _is_heading(line)
        if level:
            block = {
                "type": "heading",
                "text": line,
                "start": pos,
                "end": line_end,
                "level": level,
            }
        elif line.strip() == "":
            block = {"type": "blank", "text": line, "start": pos, "end": line_end}
        else:
            block = {"type": "content", "text": line, "start": pos, "end": line_end}
        batch.append(block)

        if nl != -1:
            batch.append({"type": "newline", "text": "\n", "start": nl, "end": nl + 1})
            pos = nl + 1
        else:
            pos = line_end

        if len(batch) >= max_batch:
            emit(batch)
            batch = []

    if batch:
        emit(batch)


def main(config: dict[str, Any]) -> None:
    serve(parse)
