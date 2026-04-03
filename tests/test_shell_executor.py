from __future__ import annotations

from pathlib import Path
import subprocess

import pytest
from operatorapp.config import OperatorConfig
from operatorapp.planner_models import ShellActionPlan, ShellStepResponse
from operatorapp.schemas import (
    CommandResult,
    TaskKind,
    TaskRecord,
    TaskStatus,
    TestPolicy as TaskTestPolicy,
    utc_now_z,
)
from operatorapp.shell_executor import ShellTaskExecutor, append_command_to_history
from operatorapp.storage import create_run_paths, save_task


class FakeJSONLLMClient:
    def __init__(self, responses: list[ShellStepResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def complete_json(self, *, system_prompt: str, user_prompt: str, response_model: type):
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "response_model": response_model,
            }
        )
        return self._responses.pop(0)


def _make_shell_task(workdir: str | None = None) -> TaskRecord:
    return TaskRecord(
        id="jobabcd1_t000",
        parent_job_id="jobabcd1",
        task_index=0,
        title="Run shell workflow",
        kind=TaskKind.shell,
        instructions="Perform setup commands.",
        workdir=workdir,
        test_policy=TaskTestPolicy.auto,
    )


def _make_config() -> OperatorConfig:
    return OperatorConfig(
        operator_home="/tmp/operator-runs",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        command_timeout_seconds=10,
    )


def test_successful_completion_path_and_logging(tmp_path: Path) -> None:
    responses = [
        ShellStepResponse(
            status="in_progress",
            summary="Run one setup command.",
            sequential=False,
            actions=[ShellActionPlan(shell_command="echo ready", reason="Prepare environment.")],
            completion_check="Ready message present.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Task objective reached.",
            sequential=True,
            actions=[],
            completion_check="No more work needed.",
        ),
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert len(updated.history) == 1
    assert updated.history[0].actions[0].shell_command == "echo ready"
    assert "ready" in updated.history[0].actions[0].output
    assert updated.history[0].sequential is False

    run_log_path = Path(paths.job_dir) / "run.log"
    task_log_path = Path(paths.tasks_dir) / task.id / "task.log"
    prompts_path = Path(paths.tasks_dir) / task.id / "llm_prompts.jsonl"
    responses_path = Path(paths.tasks_dir) / task.id / "llm_responses.jsonl"

    assert run_log_path.is_file()
    assert task_log_path.is_file()
    assert prompts_path.is_file()
    assert responses_path.is_file()
    assert len(prompts_path.read_text(encoding="utf-8").splitlines()) == 2
    assert len(responses_path.read_text(encoding="utf-8").splitlines()) == 2


def test_adaptive_retry_after_nonzero_exit(tmp_path: Path) -> None:
    responses = [
        ShellStepResponse(
            status="in_progress",
            summary="First try fails.",
            sequential=True,
            actions=[ShellActionPlan(shell_command="false", reason="Simulate failure.")],
            completion_check="Failure captured.",
        ),
        ShellStepResponse(
            status="in_progress",
            summary="Retry with working command.",
            sequential=True,
            actions=[ShellActionPlan(shell_command="echo recovered", reason="Retry.")],
            completion_check="Recovered output present.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Now complete.",
            sequential=True,
            actions=[],
            completion_check="All good.",
        ),
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert len(updated.history) == 2
    assert updated.history[0].actions[0].exit_code != 0
    assert updated.history[1].actions[0].exit_code == 0
    assert "shell_command\":\"false\"" in str(llm.calls[1]["user_prompt"])


def test_failed_path(tmp_path: Path) -> None:
    responses = [
        ShellStepResponse(
            status="failed",
            summary="Cannot proceed.",
            sequential=True,
            actions=[],
            completion_check="Impossible precondition.",
            failure_reason="Missing permissions.",
        )
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.failed
    assert updated.history == []


def test_max_iteration_failure(tmp_path: Path) -> None:
    responses = [
        ShellStepResponse(
            status="in_progress",
            summary="Still checking.",
            sequential=True,
            actions=[],
            completion_check="Need another loop.",
        )
        for _ in range(8)
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.failed
    assert len(llm.calls) == 8
    assert updated.history == []


def test_append_command_to_history_appends_exchange() -> None:
    task = _make_shell_task()
    result = CommandResult(
        command="echo hi",
        cwd="/tmp",
        stdout="hi\n",
        stderr="",
        exit_code=0,
        started_at=utc_now_z(),
        finished_at=utc_now_z(),
        duration_seconds=0.01,
    )

    updated = append_command_to_history(task, result)

    assert updated is task
    assert len(task.history) == 1
    exchange = task.history[0]
    assert exchange.sequential is True
    assert exchange.actions[0].action_type == "shell_command"
    assert exchange.actions[0].shell_command == "echo hi"
    assert exchange.actions[0].output == "hi\n"
    assert exchange.actions[0].exit_code == 0


def test_unexpected_command_error_is_captured_and_recovery_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    responses = [
        ShellStepResponse(
            status="in_progress",
            summary="First command fails unexpectedly.",
            sequential=True,
            actions=[ShellActionPlan(shell_command="echo first", reason="Initial attempt.")],
            completion_check="Failure recorded.",
        ),
        ShellStepResponse(
            status="in_progress",
            summary="Retry with second command.",
            sequential=True,
            actions=[ShellActionPlan(shell_command="echo recovered", reason="Retry after error.")],
            completion_check="Recovered output present.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Recovered and complete.",
            sequential=True,
            actions=[],
            completion_check="Task objective satisfied.",
        ),
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    real_run = subprocess.run
    call_counter = {"value": 0}

    def fake_run(*args, **kwargs):
        call_counter["value"] += 1
        if call_counter["value"] == 1:
            raise OSError("simulated spawn failure")
        return real_run(*args, **kwargs)

    monkeypatch.setattr("operatorapp.shell_executor.subprocess.run", fake_run)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert len(updated.history) == 2
    assert updated.history[0].actions[0].exit_code == 127
    assert "simulated spawn failure" in updated.history[0].actions[0].stderr
    assert updated.history[1].actions[0].exit_code == 0
    assert "recovered" in updated.history[1].actions[0].output
    assert "simulated spawn failure" in str(llm.calls[1]["user_prompt"])


def test_complete_status_with_actions_still_executes_commands(tmp_path: Path) -> None:
    responses = [
        ShellStepResponse(
            status="complete",
            summary="Objective satisfied after command.",
            sequential=True,
            actions=[
                ShellActionPlan(
                    shell_command="echo done",
                    reason="Finalize task work.",
                )
            ],
            completion_check="Done output exists.",
        )
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert len(updated.history) == 1
    assert updated.history[0].actions[0].shell_command == "echo done"
    assert updated.history[0].actions[0].exit_code == 0


def test_missing_workdir_falls_back_and_allows_recovery_command(tmp_path: Path) -> None:
    missing_workdir = tmp_path / "missing" / "project"
    target_dir = tmp_path / "created-by-command"
    responses = [
        ShellStepResponse(
            status="in_progress",
            summary="Create missing directories first.",
            sequential=True,
            actions=[
                ShellActionPlan(
                    shell_command=f"mkdir -p {target_dir}",
                    reason="Bootstrap directories even when task workdir is missing.",
                )
            ],
            completion_check="Target directory exists.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Directory creation complete.",
            sequential=True,
            actions=[],
            completion_check="No more work required.",
        ),
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(missing_workdir))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert len(updated.history) == 1
    assert updated.history[0].actions[0].exit_code == 0
    assert target_dir.is_dir()


def test_complete_with_failed_actions_continues_instead_of_premature_complete(
    tmp_path: Path,
) -> None:
    responses = [
        ShellStepResponse(
            status="complete",
            summary="Claims complete but command fails.",
            sequential=True,
            actions=[ShellActionPlan(shell_command="false", reason="Simulate failure.")],
            completion_check="Should have passed.",
        ),
        ShellStepResponse(
            status="in_progress",
            summary="Retry after failure.",
            sequential=True,
            actions=[ShellActionPlan(shell_command="echo recovered", reason="Retry command.")],
            completion_check="Recovered output present.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Now complete.",
            sequential=True,
            actions=[],
            completion_check="All checks pass.",
        ),
    ]
    llm = FakeJSONLLMClient(responses)
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert len(updated.history) == 2
    assert updated.history[0].actions[0].exit_code != 0
    assert updated.history[1].actions[0].exit_code == 0
