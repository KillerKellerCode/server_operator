from __future__ import annotations

import pytest
from pydantic import ValidationError

from operatorapp.planner_models import (
    JobPlanResponse,
    PlannedTaskInput,
    ShellActionPlan,
    ShellStepResponse,
)


def test_planned_task_input_defaults() -> None:
    task = PlannedTaskInput(
        title="Create src directory",
        kind="shell",
        instructions="mkdir -p src",
    )
    assert task.workdir is None
    assert task.test_policy == "auto"


def test_job_plan_response_round_trip() -> None:
    payload = {
        "summary": "Set up and implement feature.",
        "tasks": [
            {
                "title": "Prepare environment",
                "kind": "shell",
                "instructions": "python3 -m venv .venv",
                "test_policy": "auto",
            },
            {
                "title": "Implement code",
                "kind": "codex",
                "instructions": "Add parser module.",
                "workdir": "/tmp/project",
                "test_policy": "always",
            },
        ],
    }
    parsed = JobPlanResponse.model_validate(payload)
    restored = JobPlanResponse.model_validate_json(parsed.model_dump_json())
    assert restored == parsed


def test_shell_step_response_defaults_and_literals() -> None:
    response = ShellStepResponse(
        status="in_progress",
        summary="Need to create directory first.",
        sequential=True,
        actions=[
            ShellActionPlan(
                shell_command="mkdir -p data",
                reason="Create required output directory.",
            )
        ],
        completion_check="Directory exists and command succeeds.",
    )
    assert response.actions[0].action_type == "shell_command"
    assert response.failure_reason is None


def test_invalid_kind_rejected() -> None:
    with pytest.raises(ValidationError):
        PlannedTaskInput(title="Bad kind", kind="python", instructions="x")

