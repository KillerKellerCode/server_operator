"""LLM response models for planning and shell-step execution."""

from __future__ import annotations

from typing_extensions import Literal

from pydantic import BaseModel, ConfigDict, Field


class PlannedTaskInput(BaseModel):
    """Planner-produced task definition before conversion to TaskRecord."""

    title: str = Field(min_length=1)
    kind: Literal["shell", "codex"]
    instructions: str = Field(min_length=1)
    workdir: str | None = None
    test_policy: Literal["auto", "always", "never"] = "auto"

    model_config = ConfigDict(extra="forbid")


class JobPlanResponse(BaseModel):
    """Top-level planner response with summary and ordered tasks."""

    summary: str = Field(min_length=1)
    tasks: list[PlannedTaskInput]

    model_config = ConfigDict(extra="forbid")


class ShellActionPlan(BaseModel):
    """Single shell command proposal for the next execution step."""

    action_type: Literal["shell_command"] = "shell_command"
    shell_command: str = Field(min_length=1)
    reason: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class ShellStepResponse(BaseModel):
    """Structured response describing the next shell execution step."""

    status: Literal["in_progress", "complete", "failed"]
    summary: str = Field(min_length=1)
    sequential: bool
    actions: list[ShellActionPlan]
    completion_check: str = Field(min_length=1)
    failure_reason: str | None = None

    model_config = ConfigDict(extra="forbid")
