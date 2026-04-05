from __future__ import annotations

from operatorapp.prompts import (
    build_codex_task_prompt,
    build_job_planner_system_prompt,
    build_job_planner_user_prompt,
    build_shell_step_system_prompt,
    build_shell_step_user_prompt,
)
from operatorapp.schemas import (
    TaskKind,
    TaskRecord,
    TestPolicy as TaskTestPolicy,
    make_group_id,
    make_task_id,
)
from operatorapp.task_payloads import (
    CodexModifyPayload,
    CodexProjectPayload,
    ShellCommandPayload,
)


def _make_shell_task() -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, 0),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=0,
        title="Run shell command",
        kind=TaskKind.shell_command,
        workdir="/tmp/project",
        timeout_seconds=900,
        payload=ShellCommandPayload(command="python -m pytest -q"),
    )


def _make_codex_project_task(test_policy: TaskTestPolicy = TaskTestPolicy.auto) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 1
    return TaskRecord(
        id=make_task_id(job_id, group_index, 0),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=0,
        title="Create new parser project",
        kind=TaskKind.codex_project,
        workdir="/tmp/project",
        test_policy=test_policy,
        payload=CodexProjectPayload(
            objective="Scaffold and implement parser package",
            workspace_root="/tmp/project",
            test_command="pytest -q",
        ),
    )


def _make_codex_modify_task(test_policy: TaskTestPolicy = TaskTestPolicy.auto) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 2
    return TaskRecord(
        id=make_task_id(job_id, group_index, 0),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=0,
        title="Modify parser behavior",
        kind=TaskKind.codex_modify,
        workdir="/tmp/project",
        test_policy=test_policy,
        payload=CodexModifyPayload(
            objective="Modify parser in place",
            workspace_root="/tmp/project",
            target_paths=["parser.py"],
            test_command="pytest tests/test_parser.py -q",
        ),
    )


def test_job_planner_system_prompt_contains_required_phrases() -> None:
    prompt = build_job_planner_system_prompt().lower()
    assert "output json matching jobplanresponse" in prompt
    assert "ordered task groups" in prompt
    assert "dependency graph" in prompt
    assert "prefer native typed task kinds" in prompt
    assert "use codex_project when creating a coding project/workspace from scratch" in prompt
    assert "use codex_modify for targeted code changes in an existing codebase" in prompt
    assert "set payload.kind and ensure it exactly matches the task kind" in prompt
    assert "required payload fields by kind" in prompt
    assert "- git_repo: operation, repo_path" in prompt
    assert "default shell timeout is 600 seconds unless overridden" in prompt
    assert "likely long-running actions" in prompt
    assert "request a larger timeout_seconds" in prompt
    assert "low-count, concrete, executable groups and tasks" in prompt


def test_shell_step_system_prompt_contains_timeout_rules() -> None:
    prompt = build_shell_step_system_prompt(default_timeout_seconds=600, max_timeout_seconds=3600).lower()
    assert "shell_command tasks only" in prompt
    assert "output json matching shellstepresponse" in prompt
    assert "default per-command timeout is 600 seconds" in prompt
    assert "never exceed 3600 seconds" in prompt
    assert "you may set action.timeout_seconds when justified" in prompt
    assert "fewer concrete commands" in prompt


def test_job_planner_user_prompt_mentions_schema() -> None:
    prompt = build_job_planner_user_prompt("Set up parser project.")
    assert "Set up parser project." in prompt
    assert "Return only JSON that conforms to JobPlanResponse." in prompt


def test_shell_step_user_prompt_includes_history_and_authoritative_reminder() -> None:
    task = _make_shell_task()
    prompt = build_shell_step_user_prompt(task)

    assert f"Task id: {task.id}" in prompt
    assert f"Task kind: {task.kind.value}" in prompt
    assert f"Task workdir: {task.workdir}" in prompt
    assert "Task timeout_seconds override: 900" in prompt
    assert "Task history (compact JSON): []" in prompt
    assert "History is authoritative and must be treated as the source of truth." in prompt


def test_codex_task_prompt_differs_for_project_vs_modify_and_respects_test_policy() -> None:
    project_prompt = build_codex_task_prompt(_make_codex_project_task(TaskTestPolicy.always))
    modify_prompt = build_codex_task_prompt(_make_codex_modify_task(TaskTestPolicy.never))
    auto_prompt = build_codex_task_prompt(_make_codex_modify_task(TaskTestPolicy.auto))

    assert "Task kind: codex_project" in project_prompt
    assert "You own coding-project workspace setup end-to-end for this task." in project_prompt
    assert "scaffold creation, implementation, dependency install, and validation" in project_prompt
    assert "Test behavior: run tests for this task before finishing." in project_prompt
    assert "Acceptance criteria:" in project_prompt

    assert "Task kind: codex_modify" in modify_prompt
    assert "Modify the existing project in place." in modify_prompt
    assert "Do not re-scaffold or restructure unrelated areas." in modify_prompt
    assert "Test behavior: do not run tests for this task." in modify_prompt
    assert "Test behavior: run only the most relevant tests for this task." in auto_prompt
