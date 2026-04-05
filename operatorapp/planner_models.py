"""LLM response models for V2 planning and shell-step execution."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Literal


_REQUIRED_PAYLOAD_FIELDS_BY_KIND: dict[str, tuple[str, ...]] = {
    "shell_command": ("command",),
    "codex_project": ("objective", "workspace_root"),
    "codex_modify": ("objective", "workspace_root"),
    "download_file": ("url", "destination_path"),
    "filesystem_create": ("path", "node_type"),
    "filesystem_mutate": ("operation", "source_path"),
    "read_file": ("path",),
    "write_file": ("path", "content"),
    "append_file": ("path", "content"),
    "patch_file": ("path", "patch"),
    "extract_archive": ("archive_path", "destination_dir", "format"),
    "git_repo": ("operation", "repo_path"),
    "process_wait": ("process_name", "timeout_seconds"),
}


class PlannedTaskPayload(BaseModel):
    """Strict planner payload object without schema unions for OpenAI response_format."""

    kind: str | None = None

    # shell_command
    command: str | None = None
    cwd: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)

    # codex_project / codex_modify
    objective: str | None = None
    workspace_root: str | None = None
    setup_scope: str | None = None
    test_command: str | None = None
    target_paths: list[str] | None = None

    # download_file
    url: str | None = None
    destination_path: str | None = None
    overwrite: bool | None = None

    # filesystem_create / filesystem_mutate / file_io
    path: str | None = None
    node_type: str | None = None
    exist_ok: bool | None = None
    content: str | None = None
    target: str | None = None
    operation: str | None = None
    source_path: str | None = None
    recursive: bool | None = None
    mode: str | None = None
    encoding: str | None = None
    max_bytes: int | None = Field(default=None, ge=1)
    create_parents: bool | None = None
    ensure_trailing_newline: bool | None = None
    patch: str | None = None
    patch_format: str | None = None

    # extract_archive
    archive_path: str | None = None
    destination_dir: str | None = None
    format: str | None = None

    # git_repo
    repo_url: str | None = None
    repo_path: str | None = None
    ref: str | None = None
    message: str | None = None

    # process_wait
    process_name: str | None = None
    poll_interval_seconds: float | None = Field(default=None, gt=0.0)
    success_pattern: str | None = None

    model_config = ConfigDict(extra="forbid")


class PlannedTaskInput(BaseModel):
    """Planner-produced task definition before conversion to TaskRecord."""

    title: str = Field(min_length=1)
    kind: Literal[
        "shell_command",
        "codex_project",
        "codex_modify",
        "download_file",
        "filesystem_create",
        "filesystem_mutate",
        "read_file",
        "write_file",
        "append_file",
        "patch_file",
        "extract_archive",
        "git_repo",
        "process_wait",
    ]
    workdir: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)
    test_policy: Literal["auto", "always", "never"] = "auto"
    failure_policy: Literal["fail_fast", "continue_group", "continue_job"] = "fail_fast"
    payload: PlannedTaskPayload

    @model_validator(mode="before")
    @classmethod
    def inject_payload_kind(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        kind = data.get("kind")
        payload = data.get("payload")
        if isinstance(kind, str) and isinstance(payload, dict) and "kind" not in payload:
            patched = dict(data)
            patched["payload"] = {"kind": kind, **payload}
            return patched
        return data

    @model_validator(mode="after")
    def validate_payload_fields_for_kind(self) -> "PlannedTaskInput":
        if self.payload.kind is not None and self.payload.kind != self.kind:
            raise ValueError(
                f"payload.kind '{self.payload.kind}' does not match task kind '{self.kind}'"
            )

        required_fields = _REQUIRED_PAYLOAD_FIELDS_BY_KIND.get(self.kind, ())
        missing: list[str] = []
        for field_name in required_fields:
            value = getattr(self.payload, field_name)
            if value is None:
                missing.append(field_name)
                continue
            if isinstance(value, str) and not value.strip():
                missing.append(field_name)

        if missing:
            missing_csv = ", ".join(missing)
            raise ValueError(
                f"payload missing required fields for kind '{self.kind}': {missing_csv}"
            )
        return self

    model_config = ConfigDict(extra="forbid")


class PlannedTaskGroupInput(BaseModel):
    """Planner-produced task group definition with dependency indexes."""

    title: str = Field(min_length=1)
    depends_on_group_indexes: list[int] = Field(default_factory=list)
    tasks: list[PlannedTaskInput] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class JobPlanResponse(BaseModel):
    """Top-level planner response with summary and ordered groups."""

    summary: str = Field(min_length=1)
    groups: list[PlannedTaskGroupInput] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class ShellStepAction(BaseModel):
    """Single shell command proposal for the next execution step."""

    shell_command: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    timeout_seconds: int | None = Field(default=None, ge=1)

    model_config = ConfigDict(extra="forbid")


class ShellStepResponse(BaseModel):
    """Structured response describing the next shell execution step."""

    status: Literal["in_progress", "complete", "failed"]
    summary: str = Field(min_length=1)
    sequential: bool
    actions: list[ShellStepAction] = Field(default_factory=list)
    completion_check: str = Field(min_length=1)
    failure_reason: str | None = None

    model_config = ConfigDict(extra="forbid")
