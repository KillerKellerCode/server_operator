"""Typed task payload models for Operator V2 task kinds."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import Literal


class ShellCommandPayload(BaseModel):
    kind: Literal["shell_command"] = "shell_command"
    command: str = Field(min_length=1)
    cwd: str | None = None
    timeout_seconds: int | None = Field(default=None, ge=1)

    model_config = ConfigDict(extra="forbid")


class CodexProjectPayload(BaseModel):
    kind: Literal["codex_project"] = "codex_project"
    objective: str = Field(min_length=1)
    workspace_root: str = Field(min_length=1)
    setup_scope: str = "project_setup_and_code"
    test_command: str | None = None

    model_config = ConfigDict(extra="forbid")


class CodexModifyPayload(BaseModel):
    kind: Literal["codex_modify"] = "codex_modify"
    objective: str = Field(min_length=1)
    workspace_root: str = Field(min_length=1)
    target_paths: list[str] = Field(default_factory=list)
    test_command: str | None = None

    model_config = ConfigDict(extra="forbid")


class DownloadFilePayload(BaseModel):
    kind: Literal["download_file"] = "download_file"
    url: str = Field(min_length=1)
    destination_path: str = Field(min_length=1)
    overwrite: bool = False
    timeout_seconds: int | None = Field(default=None, ge=1)

    model_config = ConfigDict(extra="forbid")

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        if not (value.startswith("http://") or value.startswith("https://")):
            raise ValueError("url must start with http:// or https://")
        return value


class FilesystemCreatePayload(BaseModel):
    kind: Literal["filesystem_create"] = "filesystem_create"
    path: str = Field(min_length=1)
    node_type: Literal["directory", "file", "symlink"]
    exist_ok: bool = True
    content: str | None = None
    target: str | None = None

    model_config = ConfigDict(extra="forbid")


class FilesystemMutatePayload(BaseModel):
    kind: Literal["filesystem_mutate"] = "filesystem_mutate"
    operation: Literal["rename", "move", "copy", "delete", "chmod"]
    source_path: str = Field(min_length=1)
    destination_path: str | None = None
    recursive: bool = False
    mode: str | None = None

    model_config = ConfigDict(extra="forbid")


class ReadFilePayload(BaseModel):
    kind: Literal["read_file"] = "read_file"
    path: str = Field(min_length=1)
    encoding: str = "utf-8"
    max_bytes: int | None = Field(default=None, ge=1)

    model_config = ConfigDict(extra="forbid")


class WriteFilePayload(BaseModel):
    kind: Literal["write_file"] = "write_file"
    path: str = Field(min_length=1)
    content: str
    encoding: str = "utf-8"
    create_parents: bool = True

    model_config = ConfigDict(extra="forbid")


class AppendFilePayload(BaseModel):
    kind: Literal["append_file"] = "append_file"
    path: str = Field(min_length=1)
    content: str
    encoding: str = "utf-8"
    ensure_trailing_newline: bool = True

    model_config = ConfigDict(extra="forbid")


class PatchFilePayload(BaseModel):
    kind: Literal["patch_file"] = "patch_file"
    path: str = Field(min_length=1)
    patch: str = Field(min_length=1)
    patch_format: Literal["unified_diff"] = "unified_diff"

    model_config = ConfigDict(extra="forbid")


class ExtractArchivePayload(BaseModel):
    kind: Literal["extract_archive"] = "extract_archive"
    archive_path: str = Field(min_length=1)
    destination_dir: str = Field(min_length=1)
    format: Literal["zip", "tar", "tar.gz", "tar.bz2", "tar.xz"]
    overwrite: bool = False

    model_config = ConfigDict(extra="forbid")


class GitRepoPayload(BaseModel):
    kind: Literal["git_repo"] = "git_repo"
    operation: Literal["clone", "checkout", "pull", "commit"]
    repo_url: str | None = None
    repo_path: str = Field(min_length=1)
    ref: str | None = None
    message: str | None = None

    model_config = ConfigDict(extra="forbid")


class ProcessWaitPayload(BaseModel):
    kind: Literal["process_wait"] = "process_wait"
    process_name: str = Field(min_length=1)
    timeout_seconds: int = Field(ge=1)
    poll_interval_seconds: float = Field(default=1.0, gt=0.0)
    success_pattern: str | None = None

    model_config = ConfigDict(extra="forbid")


TaskPayload = Annotated[
    ShellCommandPayload
    | CodexProjectPayload
    | CodexModifyPayload
    | DownloadFilePayload
    | FilesystemCreatePayload
    | FilesystemMutatePayload
    | ReadFilePayload
    | WriteFilePayload
    | AppendFilePayload
    | PatchFilePayload
    | ExtractArchivePayload
    | GitRepoPayload
    | ProcessWaitPayload,
    Field(discriminator="kind"),
]

