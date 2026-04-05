"""Core schemas for Operator V2 job, group, and task records."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from operatorapp.task_payloads import TaskPayload

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


def make_group_id(parent_job_id: str, group_index: int) -> str:
    """Return a stable group id derived from parent_job_id and group_index."""
    if not parent_job_id:
        raise ValueError("parent_job_id must not be empty")
    if group_index < 0:
        raise ValueError("group_index must be >= 0")
    return f"{parent_job_id}_g{group_index:03d}"


def make_task_id(parent_job_id: str, group_index: int, task_index: int) -> str:
    """Return a stable task id derived from job/group/task indexes."""
    if not parent_job_id:
        raise ValueError("parent_job_id must not be empty")
    if group_index < 0:
        raise ValueError("group_index must be >= 0")
    if task_index < 0:
        raise ValueError("task_index must be >= 0")
    return f"{parent_job_id}_g{group_index:03d}_t{task_index:03d}"


class JobStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    complete = "complete"
    failed = "failed"
    interrupted = "interrupted"


class TaskGroupStatus(str, Enum):
    pending = "pending"
    ready = "ready"
    in_progress = "in_progress"
    complete = "complete"
    failed = "failed"
    blocked = "blocked"


class TaskStatus(str, Enum):
    pending = "pending"
    ready = "ready"
    in_progress = "in_progress"
    complete = "complete"
    failed = "failed"
    blocked = "blocked"
    cancelled = "cancelled"


class TaskKind(str, Enum):
    shell_command = "shell_command"
    codex_project = "codex_project"
    codex_modify = "codex_modify"
    download_file = "download_file"
    filesystem_create = "filesystem_create"
    filesystem_mutate = "filesystem_mutate"
    read_file = "read_file"
    write_file = "write_file"
    append_file = "append_file"
    patch_file = "patch_file"
    extract_archive = "extract_archive"
    git_repo = "git_repo"
    process_wait = "process_wait"


class TestPolicy(str, Enum):
    auto = "auto"
    always = "always"
    never = "never"


class FailurePolicy(str, Enum):
    fail_fast = "fail_fast"
    continue_group = "continue_group"
    continue_job = "continue_job"


class TaskProgress(BaseModel):
    state_message: str = ""
    percent: float | None = Field(default=None, ge=0.0, le=100.0)
    bytes_completed: int | None = Field(default=None, ge=0)
    bytes_total: int | None = Field(default=None, ge=0)
    current_operation: str = ""
    updated_at: str = Field(default_factory=utc_now_z)

    model_config = ConfigDict(extra="forbid")

    @field_validator("updated_at")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        _parse_utc_z(value)
        return value

    @model_validator(mode="after")
    def validate_byte_progress(self) -> "TaskProgress":
        if self.bytes_completed is not None and self.bytes_total is not None:
            if self.bytes_completed > self.bytes_total:
                raise ValueError("bytes_completed must be <= bytes_total")
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
    timed_out: bool = False
    timeout_seconds: int | None = Field(default=None, ge=1)

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
        if self.timed_out and self.timeout_seconds is None:
            raise ValueError("timeout_seconds is required when timed_out is true")
        return self


class TaskRecord(BaseModel):
    id: str = Field(min_length=1)
    parent_job_id: str = Field(min_length=1)
    parent_group_id: str = Field(min_length=1)
    group_index: int = Field(ge=0)
    task_index: int = Field(ge=0)
    title: str = Field(min_length=1)
    kind: TaskKind
    status: TaskStatus = TaskStatus.pending
    failure_policy: FailurePolicy = FailurePolicy.fail_fast
    workdir: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)
    test_policy: TestPolicy = TestPolicy.auto
    payload: TaskPayload
    progress: TaskProgress | None = None
    error_message: str | None = None
    created_at: str = Field(default_factory=utc_now_z)
    updated_at: str = Field(default_factory=utc_now_z)

    model_config = ConfigDict(extra="forbid")

    @field_validator("created_at", "updated_at")
    @classmethod
    def validate_timestamps(cls, value: str) -> str:
        _parse_utc_z(value)
        return value

    @model_validator(mode="after")
    def validate_consistency(self) -> "TaskRecord":
        if self.parent_group_id != make_group_id(self.parent_job_id, self.group_index):
            raise ValueError("parent_group_id does not match parent_job_id/group_index")
        if self.id != make_task_id(self.parent_job_id, self.group_index, self.task_index):
            raise ValueError("id does not match parent_job_id/group_index/task_index")
        if self.payload.kind != self.kind.value:
            raise ValueError("payload kind must match task kind")
        created_at = _parse_utc_z(self.created_at)
        updated_at = _parse_utc_z(self.updated_at)
        if updated_at < created_at:
            raise ValueError("updated_at must be >= created_at")
        return self


class TaskGroupRecord(BaseModel):
    id: str = Field(min_length=1)
    parent_job_id: str = Field(min_length=1)
    group_index: int = Field(ge=0)
    title: str = Field(min_length=1)
    status: TaskGroupStatus = TaskGroupStatus.pending
    depends_on_group_ids: list[str] = Field(default_factory=list)
    tasks: list[TaskRecord] = Field(default_factory=list)
    created_at: str = Field(default_factory=utc_now_z)
    updated_at: str = Field(default_factory=utc_now_z)

    model_config = ConfigDict(extra="forbid")

    @field_validator("created_at", "updated_at")
    @classmethod
    def validate_timestamps(cls, value: str) -> str:
        _parse_utc_z(value)
        return value

    @model_validator(mode="after")
    def validate_consistency(self) -> "TaskGroupRecord":
        if self.id != make_group_id(self.parent_job_id, self.group_index):
            raise ValueError("id does not match parent_job_id/group_index")
        created_at = _parse_utc_z(self.created_at)
        updated_at = _parse_utc_z(self.updated_at)
        if updated_at < created_at:
            raise ValueError("updated_at must be >= created_at")
        for task in self.tasks:
            if task.parent_group_id != self.id:
                raise ValueError("task parent_group_id does not match group id")
            if task.group_index != self.group_index:
                raise ValueError("task group_index does not match group_index")
            if task.parent_job_id != self.parent_job_id:
                raise ValueError("task parent_job_id does not match group parent_job_id")
        return self


class JobRecord(BaseModel):
    id: str = Field(min_length=1)
    user_prompt: str = Field(min_length=1)
    summary: str = ""
    status: JobStatus = JobStatus.pending
    task_groups: list[TaskGroupRecord] = Field(default_factory=list)
    created_at: str = Field(default_factory=utc_now_z)
    updated_at: str = Field(default_factory=utc_now_z)

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
        for group in self.task_groups:
            if group.parent_job_id != self.id:
                raise ValueError("group parent_job_id does not match job id")
        return self

