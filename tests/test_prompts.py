from __future__ import annotations

import json

from operatorapp.prompts import (
    build_codex_task_prompt,
    build_job_planner_system_prompt,
    build_job_planner_user_prompt,
    build_shell_step_system_prompt,
    build_shell_step_user_prompt,
)
from operatorapp.schemas import (
    ActionRecord,
    TaskExchange,
    TaskKind,
    TaskRecord,
    TestPolicy as TaskTestPolicy,
    utc_now_z,
)


def _make_task(test_policy: TaskTestPolicy = TaskTestPolicy.auto) -> TaskRecord:
    return TaskRecord(
        id="jobabcd1_t001",
        parent_job_id="jobabcd1",
        task_index=1,
        title="Implement parser",
        kind=TaskKind.codex,
        instructions="Create parser module and wire CLI.",
        workdir="/tmp/project",
        test_policy=test_policy,
        history=[
            TaskExchange(
                timestamp=utc_now_z(),
                sequential=True,
                actions=[
                    ActionRecord(
                        action_type="shell_command",
                        shell_command="ls -la",
                        output="total 8",
                        stderr="",
                        exit_code=0,
                    )
                ],
            )
        ],
    )


def test_job_planner_system_prompt_contains_required_phrases() -> None:
    prompt = build_job_planner_system_prompt()
    assert "convert one user request into an ordered job plan" in prompt.lower()
    assert "output JSON matching JobPlanResponse" in prompt
    assert "choose between shell and codex tasks" in prompt.lower()
    assert "keep the number of tasks low" in prompt.lower()
    assert "default to shell tasks for installs, file ops, downloads, and running software" in prompt.lower()
    assert "default to codex tasks for writing or modifying code" in prompt.lower()
    assert "sequential and dependency-aware" in prompt.lower()
    assert "set workdir when a directory is obvious" in prompt.lower()
    assert "always for code that should definitely be tested" in prompt.lower()
    assert "never for docs-only work" in prompt.lower()
    assert "auto otherwise" in prompt.lower()
    assert "valid JSON types and fields from JobPlanResponse" in prompt


def test_shell_step_system_prompt_contains_required_phrases() -> None:
    prompt = build_shell_step_system_prompt()
    assert "output JSON matching ShellStepResponse" in prompt
    assert "propose only the next commands needed" in prompt.lower()
    assert "adapt to previous command outputs" in prompt.lower()
    assert "mark complete when the task objective is satisfied" in prompt.lower()
    assert "mark failed only when the task cannot reasonably proceed" in prompt.lower()
    assert "minimal and idempotent" in prompt.lower()
    assert "avoid explanations outside json" in prompt.lower()
    assert "valid JSON types and fields from ShellStepResponse" in prompt


def test_job_planner_user_prompt_mentions_schema() -> None:
    prompt = build_job_planner_user_prompt("Set up parser project.")
    assert "Set up parser project." in prompt
    assert "Return only JSON that conforms to JobPlanResponse." in prompt


def test_shell_step_user_prompt_includes_history_and_authoritative_reminder() -> None:
    task = _make_task()
    prompt = build_shell_step_user_prompt(task)

    assert f"Task id: {task.id}" in prompt
    assert f"Task instructions: {task.instructions}" in prompt
    assert f"Task workdir: {task.workdir}" in prompt
    assert "History is authoritative and must be treated as the source of truth." in prompt

    expected_history = json.dumps(
        [exchange.model_dump(mode="json") for exchange in task.history],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert f"Task history (compact JSON): {expected_history}" in prompt


def test_codex_task_prompt_respects_test_policy() -> None:
    always_prompt = build_codex_task_prompt(_make_task(TaskTestPolicy.always))
    never_prompt = build_codex_task_prompt(_make_task(TaskTestPolicy.never))
    auto_prompt = build_codex_task_prompt(_make_task(TaskTestPolicy.auto))

    assert "Objective: Create parser module and wire CLI." in always_prompt
    assert "Stay within this task scope" in always_prompt
    assert "Use this task workdir as the project root: /tmp/project" in always_prompt
    assert "Run tests for this task before finishing." in always_prompt
    assert "Do not run tests for this task." in never_prompt
    assert "Run only the most relevant tests for this task." in auto_prompt
    assert "Summarize files changed and final validation status." in auto_prompt
