"""服务运行配置（全部可通过环境变量覆盖）。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    db_path: str = os.environ.get("PARSER_DB", "data/parser.db")
    # 每个（仍在服役的）插件版本常驻 worker 子进程数。
    workers_per_version: int = _int_env("PARSER_WORKERS_PER_VERSION", 2)
    # 单个解析任务的默认超时。
    task_timeout_seconds: float = _float_env("PARSER_TASK_TIMEOUT_SECONDS", 30.0)
    # 登记版本时初始化 + 冒烟校验超时。
    validate_timeout_seconds: float = _float_env("PARSER_VALIDATE_TIMEOUT_SECONDS", 10.0)
    # 停服时给子进程优雅退出的时间。
    shutdown_grace_seconds: float = _float_env("PARSER_SHUTDOWN_GRACE_SECONDS", 3.0)
    max_document_codepoints: int = _int_env("PARSER_MAX_DOCUMENT_CODEPOINTS", 1_000_000)
    host: str = os.environ.get("PARSER_HOST", "0.0.0.0")
    port: int = _int_env("PARSER_PORT", 8000)


settings = Settings()
