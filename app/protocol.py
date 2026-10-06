"""进程间通信协议定义。

父进程（FastAPI 服务）与子进程（插件）之间通过 stdin/stdout 交换
行分隔的 JSON（NDJSON），每行一个对象，UTF-8 编码。

父 -> 子::

    {"kind": "init", "protocol": "doc-parser/1", "config": {...}}
    {"kind": "parse", "task": "<task_id>", "document": "..."}
    {"kind": "shutdown"}

子 -> 父::

    {"kind": "ready", "protocol": "doc-parser/1"}
    {"kind": "blocks", "task": "<task_id>", "batch": 0,
     "blocks": [{"type": "...", "text": "...", "start": 0, "end": 12}]}
    {"kind": "finish", "task": "<task_id>", "batches": 3, "blocks": 42}
    {"kind": "error", "task": "<task_id>", "code": "PLUGIN_ERROR",
     "message": "human readable"}

区间约定：``start`` 为起始 Unicode 码点偏移（含），``end`` 为结束
码点偏移（不含），即 Python 字符串切片语义 ``document[start:end]``。
"""

from __future__ import annotations

from typing import Any

PROTOCOL = "doc-parser/1"

# 单行报文最大长度，防止故障插件输出无界数据撑爆内存。
MAX_FRAME_BYTES = 64 * 1024 * 1024

# 块类型（插件可在 kind 中使用 type 字符串，父进程做结构校验）。
BlockType = str

ParentMessage = dict[str, Any]
ChildMessage = dict[str, Any]


def ready_message() -> ChildMessage:
    return {"kind": "ready", "protocol": PROTOCOL}


def blocks_message(task_id: str, batch: int, blocks: list[dict[str, Any]]) -> ChildMessage:
    return {"kind": "blocks", "task": task_id, "batch": batch, "blocks": blocks}


def finish_message(task_id: str, batches: int, blocks: int) -> ChildMessage:
    return {"kind": "finish", "task": task_id, "batches": batches, "blocks": blocks}


def error_message(task_id: str, code: str, message: str) -> ChildMessage:
    return {"kind": "error", "task": task_id, "code": code, "message": message}


def init_message(config: dict[str, Any] | None = None) -> ParentMessage:
    return {"kind": "init", "protocol": PROTOCOL, "config": config or {}}


def parse_message(task_id: str, document: str) -> ParentMessage:
    return {"kind": "parse", "task": task_id, "document": document}


def shutdown_message() -> ParentMessage:
    return {"kind": "shutdown"}


class ProtocolError(Exception):
    """报文损坏 / 不符合协议（区别于插件主动返回的结构化 error）。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---- 父进程侧对结果分批的严格校验 ---------------------------------------


def validate_blocks_frame(
    frame: ChildMessage,
    *,
    task_id: str,
    expected_batch: int,
) -> list[dict[str, Any]]:
    """校验一帧 blocks 的结构，返回规范化后的块列表。

    只做结构校验；区间的业务校验（顺序、覆盖、与原文一致）在
    收齐所有批次后由 :func:`validate_complete_blocks` 统一进行。
    """
    if frame.get("kind") != "blocks":
        raise ProtocolError("BAD_FRAME", f"期望 blocks 帧，收到 {frame.get('kind')!r}")
    if frame.get("task") != task_id:
        raise ProtocolError("TASK_MISMATCH", "blocks 帧的任务标识与当前任务不一致")
    batch = frame.get("batch")
    if not isinstance(batch, int) or isinstance(batch, bool) or batch < 0:
        raise ProtocolError("BAD_FRAME", "batch 必须为非负整数")
    if batch != expected_batch:
        raise ProtocolError(
            "OUT_OF_ORDER", f"批次乱序：期望 {expected_batch}，收到 {batch}"
        )
    raw_blocks = frame.get("blocks")
    if not isinstance(raw_blocks, list):
        raise ProtocolError("BAD_FRAME", "blocks 必须为数组")

    normalised: list[dict[str, Any]] = []
    for i, b in enumerate(raw_blocks):
        if not isinstance(b, dict):
            raise ProtocolError("BAD_BLOCK", f"批次 {batch} 第 {i} 块不是对象")
        text = b.get("text")
        start = b.get("start")
        end = b.get("end")
        btype = b.get("type", "text")
        if not isinstance(text, str):
            raise ProtocolError("BAD_BLOCK", f"批次 {batch} 第 {i} 块缺少字符串 text")
        if (
            not isinstance(start, int)
            or isinstance(start, bool)
            or not isinstance(end, int)
            or isinstance(end, bool)
        ):
            raise ProtocolError("BAD_BLOCK", f"批次 {batch} 第 {i} 块的 start/end 必须为整数")
        if start < 0 or end < start:
            raise ProtocolError("BAD_BLOCK", f"批次 {batch} 第 {i} 块区间非法: [{start},{end})")
        if not isinstance(btype, str) or not btype:
            raise ProtocolError("BAD_BLOCK", f"批次 {batch} 第 {i} 块 type 非法")
        # 结构字段校验通过后规范化；保留插件附加的元数据（如 heading 的 level）。
        nb = {k: v for k, v in b.items() if k not in ("start", "end")}
        nb["start"] = start
        nb["end"] = end
        nb["type"] = btype
        nb["text"] = text
        normalised.append(nb)
    return normalised


def validate_complete_blocks(
    blocks: list[dict[str, Any]],
    document: str,
    *,
    expected_count: int | None = None,
) -> None:
    """收齐所有批次后做终态校验：顺序、区间与原文结构。

    要求：
    * 块按 ``start`` 严格有序、互不重叠、紧密相连（允许块间空区间吗？不允许——
      每个码点必须恰好属于一个块，空文档允许零块）；
    * 每块 ``text`` 必须等于 ``document[start:end]``；
    * 整体恰好覆盖 ``[0, len(document))``。
    """
    if expected_count is not None and len(blocks) != expected_count:
        raise ProtocolError(
            "BLOCK_COUNT_MISMATCH",
            f"finish 声明 {expected_count} 块，实际收到 {len(blocks)} 块",
        )

    cursor = 0
    doc_len = len(document)
    for i, b in enumerate(blocks):
        start, end, text = b["start"], b["end"], b["text"]
        if start != cursor:
            if start < cursor:
                raise ProtocolError("OVERLAPPING_RANGES", f"第 {i} 块与前块重叠")
            raise ProtocolError("GAP_IN_RANGES", f"第 {i} 块前存在未覆盖区间 [{cursor},{start})")
        if end > doc_len:
            raise ProtocolError("RANGE_OUT_OF_BOUNDS", f"第 {i} 块越过文档末尾: [{start},{end})")
        if document[start:end] != text:
            raise ProtocolError("TEXT_MISMATCH", f"第 {i} 块 text 与原文码点区间不一致")
        cursor = end
    if cursor != doc_len:
        raise ProtocolError("INCOMPLETE_COVERAGE", f"结果未覆盖完整文档，止于 {cursor}/{doc_len}")
