"""Request/response schemas for the HTTP API."""
from __future__ import annotations

import re
from typing import Optional

from pydantic import BaseModel, Field

# Plugins are trusted in-repo code; entries must live under app.plugins.
ENTRY_RE = re.compile(r"^app\.plugins\.[a-z0-9_]+$")

MAX_DOCUMENT_CHARS = 1_000_000


class PluginRegister(BaseModel):
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    entry: str = Field(min_length=1, max_length=200)
    config: dict = Field(default_factory=dict)


class DefaultSwitch(BaseModel):
    version: str = Field(min_length=1, max_length=64)


class TaskSubmit(BaseModel):
    request_key: str = Field(min_length=1, max_length=200)
    plugin: str = Field(min_length=1, max_length=64)
    version: Optional[str] = Field(default=None, max_length=64)
    document: str = Field(max_length=MAX_DOCUMENT_CHARS)
    timeout_ms: Optional[int] = Field(default=None, ge=100, le=120_000)
    options: dict = Field(default_factory=dict)
