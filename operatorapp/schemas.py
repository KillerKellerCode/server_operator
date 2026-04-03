"""Core schemas for Operator records and command outcomes."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing_extensions import Literal

_UTC_Z_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_utc_z(value: str) -> datetime:
    """Parse a UTC Z timestamp and raise ValueError on invalid input."""
    return datetime.strptime(value, _UTC_Z_FORMAT).replace(tzinfo=timezone.utc)


def utc_now_z() -> str:
    """Return current UTC time in ISO-8601 Z form."""
    return datetime.now(timezone.utc).replace(microsecond=0).strftime(_UTC_Z_FORMAT)


def make_job_id() -> str:
    """Return a short opaque job identifier."""
    return uuid4().hex[:8]


def make_task_id(parent_job_id: str, task_index: int) -> str:
    """Return a stable task id derived from parent_job_id and task_index."""
    if not parent_job_id:
        raise ValueError("parent_job_id must not be empty")
    if task_index < 0:
        raise ValueError("task_index must be >= 0")
    return f"{parent_job_id}_t{task_index:03d}"


class JobStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    complete = "complete"
    failed = "failed"
    interrupted = "interrupted"


class TaskStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    complete = "complete"
    failed = "failed"


class TaskKind(str, Enum):
    shell = "shell"
    codex = "codex"


class TestPolicy(str, Enum):
    auto = "auto"
    always = "always"
    never = "never"


class ActionRecord(BaseModel):
    action_type: Literal["shell_command"]
    shell_command: str = Field(min_length=1)
    output: str = ""
    stderr: str = ""
    exit_code: int | None = None

    model_config = ConfigDict(extra="forbid")


class TaskExchange(BaseModel):
    timestamp: str
    sequential: bool
    actions: list[ActionRecord]

    model_config = ConfigDict(extra="forbid")

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        _parse_utc_z(value)
        return value


class TaskRecord(BaseModel):
    id: str = Field(min_length=1)
    parent_job_id: str = Field(min_length=1)
    task_index: int = Field(ge=0)
    title: str = Field(min_length=1)
    kind: TaskKind
    instructions: str = Field(min_length=1)
    workdir: str | None = None
    test_policy: TestPolicy = TestPolicy.auto
    status: TaskStatus = TaskStatus.pending
    history: list[TaskExchange] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class JobRecord(BaseModel):
    id: str = Field(min_length=1)
    user_prompt: str = Field(min_length=1)
    summary: str = ""
    status: JobStatus = JobStatus.pending
    tasks: list[TaskRecord] = Field(default_factory=list)
    created_at: str
    updated_at: str

    model_config = ConfigDict(extra="forbid")

    @field_validator("created_at", "updated_at")
    @classmethod
    def validate_timestamps(cls, value: str) -> str:
        _parse_utc_z(value)
        return value

    @model_validator(mode="after")
    def validate_time_order(self) -> "JobRecord":
        created_at = _parse_utc_z(self.created_at)
        updated_at = _parse_utc_z(self.updated_at)
        if updated_at < created_at:
            raise ValueError("updated_at must be >= created_at")
        return self


class CommandResult(BaseModel):
    command: str = Field(min_length=1)
    cwd: str | None = None
    stdout: str = ""
    stderr: str = ""
    exit_code: int
    started_at: str
    finished_at: str
    duration_seconds: float = Field(ge=0.0)

    model_config = ConfigDict(extra="forbid")

    @field_validator("started_at", "finished_at")
    @classmethod
    def validate_timestamps(cls, value: str) -> str:
        _parse_utc_z(value)
        return value

    @model_validator(mode="after")
    def validate_time_order(self) -> "CommandResult":
        started_at = _parse_utc_z(self.started_at)
        finished_at = _parse_utc_z(self.finished_at)
        if finished_at < started_at:
            raise ValueError("finished_at must be >= started_at")
        return self

