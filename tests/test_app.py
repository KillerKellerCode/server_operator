from __future__ import annotations

from pathlib import Path

import pytest

from operatorapp.app import OperatorApp
from operatorapp.config import OperatorConfig
from operatorapp.schemas import (
    JobRecord,
    JobStatus,
    TaskGroupRecord,
    TaskKind,
    TaskRecord,
    TaskStatus,
    make_group_id,
    make_task_id,
)
from operatorapp.storage import load_job
from operatorapp.task_payloads import ShellCommandPayload


class _FakePlannerService:
    def __init__(self, job: JobRecord) -> None:
        self._job = job
        self.prompts: list[str] = []

    def create_job(self, user_prompt: str) -> JobRecord:
        self.prompts.append(user_prompt)
        return self._job


class _NoopShellExecutor:
    def execute(self, paths, task):  # type: ignore[no-untyped-def]
        task.status = TaskStatus.complete
        return task


class _NoopCodexExecutor:
    def execute(self, paths, task):  # type: ignore[no-untyped-def]
        task.status = TaskStatus.complete
        return task


def _make_config(home_dir: str) -> OperatorConfig:
    return OperatorConfig(
        operator_home=home_dir,
        openai_api_key="",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        default_command_timeout_seconds=600,
        max_command_timeout_seconds=7200,
        default_codex_timeout_seconds=7200,
        max_concurrent_tasks=4,
        max_concurrent_codex_tasks=1,
    )


def _make_job() -> JobRecord:
    job_id = "jobabcd1"
    group_0 = TaskGroupRecord(
        id=make_group_id(job_id, 0),
        parent_job_id=job_id,
        group_index=0,
        title="group-0",
        tasks=[
            TaskRecord(
                id=make_task_id(job_id, 0, 0),
                parent_job_id=job_id,
                parent_group_id=make_group_id(job_id, 0),
                group_index=0,
                task_index=0,
                title="task-0",
                kind=TaskKind.shell_command,
                payload=ShellCommandPayload(command="echo hi"),
            )
        ],
    )
    group_1 = TaskGroupRecord(
        id=make_group_id(job_id, 1),
        parent_job_id=job_id,
        group_index=1,
        title="group-1",
        depends_on_group_ids=[group_0.id],
        tasks=[
            TaskRecord(
                id=make_task_id(job_id, 1, 0),
                parent_job_id=job_id,
                parent_group_id=make_group_id(job_id, 1),
                group_index=1,
                task_index=0,
                title="task-1",
                kind=TaskKind.shell_command,
                payload=ShellCommandPayload(command="echo done"),
            )
        ],
    )
    return JobRecord(
        id=job_id,
        user_prompt="do work",
        summary="summary",
        task_groups=[group_0, group_1],
    )


def test_app_uses_scheduler_and_persists_initial_snapshots(tmp_path: Path, monkeypatch) -> None:
    planner = _FakePlannerService(_make_job())
    shell_executor = _NoopShellExecutor()
    codex_executor = _NoopCodexExecutor()
    config = _make_config(str(tmp_path))
    app = OperatorApp(planner, shell_executor, codex_executor, config)

    class _FakeScheduler:
        called = False

        def __init__(self, registry, max_concurrent_tasks: int, max_concurrent_codex_tasks: int):
            assert max_concurrent_tasks == 4
            assert max_concurrent_codex_tasks == 1

        def run(self, paths, job):  # type: ignore[no-untyped-def]
            _FakeScheduler.called = True
            job.status = JobStatus.complete
            return job

    monkeypatch.setattr("operatorapp.app.JobScheduler", _FakeScheduler)

    result = app.run("hello world")

    assert result.status is JobStatus.complete
    assert _FakeScheduler.called is True
    assert (Path(config.operator_home) / "jobabcd1" / "job.json").is_file()
    assert (Path(config.operator_home) / "jobabcd1" / "groups" / "jobabcd1_g000" / "group.json").is_file()
    assert (Path(config.operator_home) / "jobabcd1" / "tasks" / "jobabcd1_g000_t000" / "task.json").is_file()


def test_keyboard_interrupt_marks_job_interrupted_and_reraises(tmp_path: Path, monkeypatch) -> None:
    planner = _FakePlannerService(_make_job())
    shell_executor = _NoopShellExecutor()
    codex_executor = _NoopCodexExecutor()
    config = _make_config(str(tmp_path))
    app = OperatorApp(planner, shell_executor, codex_executor, config)

    class _InterruptScheduler:
        def __init__(self, registry, max_concurrent_tasks: int, max_concurrent_codex_tasks: int):
            return

        def run(self, paths, job):  # type: ignore[no-untyped-def]
            raise KeyboardInterrupt

    monkeypatch.setattr("operatorapp.app.JobScheduler", _InterruptScheduler)

    with pytest.raises(KeyboardInterrupt):
        app.run("hello world")

    saved = load_job(str(Path(config.operator_home) / "jobabcd1" / "job.json"))
    assert saved.status is JobStatus.interrupted

