"""Structured job event models for append-only persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

_UTC_Z_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_utc_z(value: str) -> datetime:
    return datetime.strptime(value, _UTC_Z_FORMAT).replace(tzinfo=timezone.utc)


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime(_UTC_Z_FORMAT)


class JobEventType(str, Enum):
    job_created = "job_created"
    job_status_changed = "job_status_changed"
    group_ready = "group_ready"
    group_started = "group_started"
    group_completed = "group_completed"
    group_failed = "group_failed"
    task_ready = "task_ready"
    task_started = "task_started"
    task_progress = "task_progress"
    task_completed = "task_completed"
    task_failed = "task_failed"
    task_cancelled = "task_cancelled"
    command_started = "command_started"
    command_completed = "command_completed"
    codex_started = "codex_started"
    codex_completed = "codex_completed"
    log_message = "log_message"


class JobEvent(BaseModel):
    timestamp: str = Field(default_factory=_utc_now_z)
    event_type: JobEventType
    job_id: str
    group_id: str | None = None
    task_id: str | None = None
    message: str = ""
    data: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        _parse_utc_z(value)
        return value

