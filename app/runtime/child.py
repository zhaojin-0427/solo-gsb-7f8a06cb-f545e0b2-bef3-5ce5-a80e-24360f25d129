"""插件子进程侧运行时。

以 ``python -m app.runtime.child <entrypoint>`` 启动。``entrypoint`` 是
仓库内模块路径（必须位于 ``app.plugins`` 包下），由父进程通过白名单
校验后传入。

提供两层 API：

* :func:`serve` —— 可信插件用高层接口，只需提供
  ``parse(document, config, emit)`` 回调；握手、分帧、finish 均由本模块
  统一处理；
* :class:`Client` —— 故障插件用底层帧读写，可制造超时、崩溃、坏帧等。
"""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Callable
from typing import Any, Iterator

from app.protocol import (
    PROTOCOL,
    ready_message,
)


def _log(msg: str) -> None:
    # 调试信息一律走 stderr，stdout 只承载 NDJSON 协议帧。
    print(f"[child] {msg}", file=sys.stderr, flush=True)


class ProtocolViolation(Exception):
    pass


class Client:
    """stdin/stdout NDJSON 低层读写。"""

    def __init__(self) -> None:
        self.stdin = sys.stdin
        self.stdout = sys.stdout

    # -- 读 -----------------------------------------------------------------

    def read_frame(self) -> dict[str, Any] | None:
        line = self.stdin.readline()
        if line == "":
            return None  # EOF
        line = line.strip()
        if not line:
            return self.read_frame()
        return json.loads(line)

    def frames(self) -> Iterator[dict[str, Any]]:
        while True:
            frame = self.read_frame()
            if frame is None:
                return
            yield frame

    # -- 写 -----------------------------------------------------------------

    def send(self, frame: dict[str, Any]) -> None:
        self.stdout.write(json.dumps(frame, ensure_ascii=False) + "\n")
        self.stdout.flush()

    def send_ready(self) -> None:
        self.send(ready_message())

    def send_blocks(self, task_id: str, batch: int, blocks: list[dict[str, Any]]) -> None:
        from app.protocol import blocks_message

        self.send(blocks_message(task_id, batch, blocks))

    def send_finish(self, task_id: str, batches: int, blocks: int) -> None:
        from app.protocol import finish_message

        self.send(finish_message(task_id, batches, blocks))

    def send_error(self, task_id: str, code: str, message: str) -> None:
        from app.protocol import error_message

        self.send(error_message(task_id, code, message))


def _load_plugin(entrypoint: str):
    if not entrypoint.startswith("app.plugins."):
        raise SystemExit(f"refuse to load entrypoint outside app.plugins: {entrypoint}")
    module = importlib.import_module(entrypoint)
    main = getattr(module, "main", None)
    if not callable(main):
        raise SystemExit(f"plugin {entrypoint} does not define main(config)")
    return main


ParseFn = Callable[[str, dict[str, Any], Callable[[list[dict[str, Any]]], None]], None]


def bootstrap() -> tuple[dict[str, Any], Client]:
    """读取 init 帧、校验协议版本，回复 ready。返回 (config, client)。"""
    client = Client()
    init = client.read_frame()
    if init is None:
        raise SystemExit(0)
    if init.get("kind") != "init" or init.get("protocol") != PROTOCOL:
        # init 阶段无法用 error 帧（任务尚不存在），直接非零退出，
        # 由父进程记为握手失败。
        raise SystemExit("bad init frame")
    client.send_ready()
    return init.get("config") or {}, client


def serve(parse: ParseFn) -> None:
    """可信插件的标准主循环。

    ``parse(document, config, emit)`` 内通过 ``emit(blocks)`` 按序输出
    批次；返回后运行时自动补发 finish（批次号与块数均由运行时统计，
    插件无法伪造）。插件抛异常会被转成结构化 error 帧，子进程继续
    服务下一个任务（单任务失败不杀进程）。
    """
    config, client = bootstrap()
    try:
        for frame in client.frames():
            kind = frame.get("kind")
            if kind == "shutdown":
                return
            if kind != "parse":
                _log(f"ignoring unknown frame kind: {kind!r}")
                continue
            task_id = frame.get("task")
            document = frame.get("document", "")
            batch_index = 0
            block_count = 0

            def emit(blocks: list[dict[str, Any]]) -> None:
                nonlocal batch_index, block_count
                client.send_blocks(task_id, batch_index, blocks)
                batch_index += 1
                block_count += len(blocks)

            try:
                parse(document, config, emit)
            except Exception as exc:  # 插件业务错误 -> 结构化 error，不崩溃
                _log(f"plugin raised on task {task_id}: {exc!r}")
                client.send_error(task_id, "PLUGIN_ERROR", str(exc) or repr(exc))
                continue
            client.send_finish(task_id, batch_index, block_count)
    except (BrokenPipeError, OSError):
        return


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m app.runtime.child <entrypoint>")
    entrypoint = sys.argv[1]
    # 插件 main(config) 自行从 stdin 读取 init 帧并驱动主循环：
    # 可信插件调用 ``serve(parse_fn)``；故障插件用 Client 自行制造故障。
    plugin_main = _load_plugin(entrypoint)
    plugin_main({})


if __name__ == "__main__":
    main()
