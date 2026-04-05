from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from operatorapp.planner_models import (
    JobPlanResponse,
    PlannedTaskGroupInput,
    PlannedTaskInput,
    ShellStepAction,
    ShellStepResponse,
)


def test_planned_task_input_defaults() -> None:
    task = PlannedTaskInput(
        title="Create src directory",
        kind="filesystem_create",
        payload={"path": "/tmp/src", "node_type": "directory"},
    )
    assert task.workdir is None
    assert task.timeout_seconds is None
    assert task.test_policy == "auto"
    assert task.failure_policy == "fail_fast"
    assert task.payload.kind == "filesystem_create"


def test_job_plan_response_with_groups_round_trip() -> None:
    payload = {
        "summary": "Prepare workspace and implement feature.",
        "groups": [
            {
                "title": "Bootstrap workspace",
                "depends_on_group_indexes": [],
                "tasks": [
                    {
                        "title": "Create project directory",
                        "kind": "filesystem_create",
                        "payload": {"path": "/tmp/proj", "node_type": "directory"},
                    },
                    {
                        "title": "Clone repository",
                        "kind": "git_repo",
                        "payload": {
                            "operation": "clone",
                            "repo_url": "https://github.com/example/repo.git",
                            "repo_path": "/tmp/proj",
                        },
                    },
                ],
            },
            {
                "title": "Implement changes",
                "depends_on_group_indexes": [0],
                "tasks": [
                    {
                        "title": "Modify parser",
                        "kind": "codex_modify",
                        "workdir": "/tmp/proj",
                        "test_policy": "always",
                        "failure_policy": "continue_group",
                        "payload": {
                            "objective": "Update parser behavior",
                            "workspace_root": "/tmp/proj",
                            "target_paths": ["parser.py"],
                        },
                    }
                ],
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
            ShellStepAction(
                shell_command="mkdir -p data",
                reason="Create required output directory.",
                timeout_seconds=120,
            )
        ],
        completion_check="Directory exists and command succeeds.",
    )
    assert response.failure_reason is None
    assert response.actions[0].timeout_seconds == 120


def test_invalid_kind_rejected() -> None:
    with pytest.raises(ValidationError):
        PlannedTaskInput(title="Bad kind", kind="python", payload={})


def test_invalid_group_dependency_type_rejected() -> None:
    with pytest.raises(ValidationError):
        PlannedTaskGroupInput(title="Bad group", depends_on_group_indexes=["x"], tasks=[])


def test_git_repo_payload_missing_operation_is_rejected() -> None:
    with pytest.raises(ValidationError, match="payload missing required fields for kind 'git_repo': operation"):
        PlannedTaskInput(
            title="Clone repository",
            kind="git_repo",
            payload={
                "repo_url": "https://github.com/example/repo.git",
                "repo_path": "/tmp/repo",
            },
        )


def _find_additional_properties_true(node: Any, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    hits: list[tuple[str, ...]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "additionalProperties" and value is True:
                hits.append(path + (key,))
            hits.extend(_find_additional_properties_true(value, path + (str(key),)))
    elif isinstance(node, list):
        for index, item in enumerate(node):
            hits.extend(_find_additional_properties_true(item, path + (str(index),)))
    return hits


def _find_key(node: Any, target_key: str, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    hits: list[tuple[str, ...]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == target_key:
                hits.append(path + (key,))
            hits.extend(_find_key(value, target_key, path + (str(key),)))
    elif isinstance(node, list):
        for index, item in enumerate(node):
            hits.extend(_find_key(item, target_key, path + (str(index),)))
    return hits


def test_llm_response_schemas_are_strict_for_openai_json_response_format() -> None:
    job_schema = JobPlanResponse.model_json_schema()
    shell_schema = ShellStepResponse.model_json_schema()

    job_plan_additional_true_hits = _find_additional_properties_true(job_schema)
    shell_additional_true_hits = _find_additional_properties_true(shell_schema)
    job_plan_oneof_hits = _find_key(job_schema, "oneOf")
    shell_oneof_hits = _find_key(shell_schema, "oneOf")

    assert job_plan_additional_true_hits == []
    assert shell_additional_true_hits == []
    assert job_plan_oneof_hits == []
    assert shell_oneof_hits == []
