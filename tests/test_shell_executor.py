from __future__ import annotations

import json
from pathlib import Path

from operatorapp.config import OperatorConfig
from operatorapp.planner_models import ShellStepAction, ShellStepResponse
from operatorapp.schemas import (
    CommandResult,
    TaskKind,
    TaskRecord,
    TaskStatus,
    make_group_id,
    make_task_id,
    utc_now_z,
)
from operatorapp.shell_executor import MAX_ITERATIONS, ShellTaskExecutor
from operatorapp.storage import create_run_paths, read_events, save_task
from operatorapp.task_payloads import ShellCommandPayload


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


def _make_shell_task(
    *,
    command: str = "echo ok",
    payload_timeout: int | None = None,
    task_timeout: int | None = None,
) -> TaskRecord:
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
        workdir=None,
        timeout_seconds=task_timeout,
        payload=ShellCommandPayload(command=command, timeout_seconds=payload_timeout),
    )


def _make_config() -> OperatorConfig:
    return OperatorConfig(
        operator_home="/tmp/operator-runs",
        openai_api_key="",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        default_command_timeout_seconds=10,
        max_command_timeout_seconds=100,
        default_codex_timeout_seconds=7200,
        max_concurrent_tasks=4,
        max_concurrent_codex_tasks=1,
    )


def _result(
    command: str,
    *,
    exit_code: int,
    timed_out: bool = False,
    timeout_seconds: int = 10,
    stdout: str = "",
    stderr: str = "",
) -> CommandResult:
    now = utc_now_z()
    return CommandResult(
        command=command,
        cwd=None,
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        started_at=now,
        finished_at=now,
        duration_seconds=0.01,
        timed_out=timed_out,
        timeout_seconds=timeout_seconds,
    )


def test_successful_shell_path_with_command_events(tmp_path: Path, monkeypatch) -> None:
    llm = FakeJSONLLMClient(
        [
            ShellStepResponse(
                status="in_progress",
                summary="Run the command.",
                sequential=True,
                actions=[ShellStepAction(shell_command="echo ready", reason="execute")],
                completion_check="ready shown",
            ),
            ShellStepResponse(
                status="complete",
                summary="done",
                sequential=True,
                actions=[],
                completion_check="complete",
            ),
        ]
    )
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task()
    save_task(paths, task)

    monkeypatch.setattr(
        "operatorapp.shell_executor.run_bash",
        lambda command, cwd=None, timeout_seconds=600: _result(
            command, exit_code=0, stdout="ready\n", timeout_seconds=timeout_seconds
        ),
    )

    updated = executor.execute(paths=paths, task=task)
    assert updated.status is TaskStatus.complete
    assert updated.progress is not None
    assert updated.progress.percent == 100.0

    events = read_events(paths.events_jsonl_path)
    event_types = [event.event_type.value for event in events]
    assert "command_started" in event_types
    assert "command_completed" in event_types
    assert event_types[-1] == "task_completed"


def test_non_zero_exit_adaptive_retry(tmp_path: Path, monkeypatch) -> None:
    llm = FakeJSONLLMClient(
        [
            ShellStepResponse(
                status="in_progress",
                summary="first attempt",
                sequential=True,
                actions=[ShellStepAction(shell_command="false", reason="attempt 1")],
                completion_check="first done",
            ),
            ShellStepResponse(
                status="in_progress",
                summary="retry",
                sequential=True,
                actions=[ShellStepAction(shell_command="echo recovered", reason="attempt 2")],
                completion_check="retry done",
            ),
            ShellStepResponse(
                status="complete",
                summary="done",
                sequential=True,
                actions=[],
                completion_check="complete",
            ),
        ]
    )
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task()
    save_task(paths, task)

    responses = iter(
        [
            _result("false", exit_code=2, stderr="bad"),
            _result("echo recovered", exit_code=0, stdout="recovered\n"),
        ]
    )
    monkeypatch.setattr(
        "operatorapp.shell_executor.run_bash",
        lambda command, cwd=None, timeout_seconds=600: next(responses),
    )

    updated = executor.execute(paths=paths, task=task)
    assert updated.status is TaskStatus.complete
    assert '"exit_code":2' in str(llm.calls[1]["user_prompt"])


def test_planner_requested_timeout_override(tmp_path: Path, monkeypatch) -> None:
    llm = FakeJSONLLMClient(
        [
            ShellStepResponse(
                status="complete",
                summary="done with one command",
                sequential=True,
                actions=[
                    ShellStepAction(
                        shell_command="echo hi",
                        reason="run",
                        timeout_seconds=55,
                    )
                ],
                completion_check="complete",
            )
        ]
    )
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(payload_timeout=20, task_timeout=30)
    save_task(paths, task)

    used_timeouts: list[int] = []

    def fake_run(command, cwd=None, timeout_seconds=600):  # type: ignore[no-untyped-def]
        used_timeouts.append(timeout_seconds)
        return _result(command, exit_code=0, timeout_seconds=timeout_seconds)

    monkeypatch.setattr("operatorapp.shell_executor.run_bash", fake_run)
    updated = executor.execute(paths=paths, task=task)

    assert updated.status is TaskStatus.complete
    assert used_timeouts == [55]
    assert "default per-command timeout is 10 seconds" in str(llm.calls[0]["system_prompt"]).lower()
    assert "never exceed 100 seconds" in str(llm.calls[0]["system_prompt"]).lower()


def test_timeout_failure_path_records_events(tmp_path: Path, monkeypatch) -> None:
    llm = FakeJSONLLMClient(
        [
            ShellStepResponse(
                status="in_progress",
                summary="try command",
                sequential=True,
                actions=[ShellStepAction(shell_command="sleep 999", reason="wait")],
                completion_check="never",
            )
        ]
    )
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task(task_timeout=12)
    save_task(paths, task)

    monkeypatch.setattr(
        "operatorapp.shell_executor.run_bash",
        lambda command, cwd=None, timeout_seconds=600: _result(
            command,
            exit_code=124,
            timed_out=True,
            timeout_seconds=timeout_seconds,
            stderr="timed out",
        ),
    )

    updated = executor.execute(paths=paths, task=task)
    assert updated.status is TaskStatus.failed
    assert "timed out" in (updated.error_message or "")

    events = read_events(paths.events_jsonl_path)
    command_completed = [event for event in events if event.event_type.value == "command_completed"][-1]
    assert command_completed.data["timed_out"] is True
    assert command_completed.data["timeout_seconds"] == 12
    assert events[-1].event_type.value == "task_failed"


def test_max_iteration_failure(tmp_path: Path) -> None:
    llm = FakeJSONLLMClient(
        [
            ShellStepResponse(
                status="in_progress",
                summary="continue",
                sequential=True,
                actions=[],
                completion_check="not yet",
            )
            for _ in range(MAX_ITERATIONS)
        ]
    )
    executor = ShellTaskExecutor(llm=llm, config=_make_config())
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_shell_task()
    save_task(paths, task)

    updated = executor.execute(paths=paths, task=task)
    assert updated.status is TaskStatus.failed
    assert updated.error_message is not None
    assert "max iterations" in updated.error_message.lower()
    task_json = Path(paths.tasks_dir) / task.id / "task.json"
    data = json.loads(task_json.read_text(encoding="utf-8"))
    assert data["status"] == "failed"

