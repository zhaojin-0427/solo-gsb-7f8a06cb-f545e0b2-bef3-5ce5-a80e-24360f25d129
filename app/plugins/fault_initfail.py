"""故障样例：初始化失败。

收到 init 帧后不回 ready 直接退出（模拟依赖缺失/配置非法导致的启动失败）。
该版本在登记/切换默认的兼容校验阶段就会被拒绝，从而**不会改变原默认版本**。
"""

from __future__ import annotations

import os

from typing import Any


def main(config: dict[str, Any]) -> None:
    # 不调用 bootstrap()：直接失败退出，父进程把它记为握手失败。
    os._exit(3)
