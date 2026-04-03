from __future__ import annotations

from pathlib import Path

import pytest

from operatorapp.app import OperatorApp
from operatorapp.config import OperatorConfig
from operatorapp.schemas import JobRecord, JobStatus, TaskKind, TaskRecord, TaskStatus, utc_now_z
from operatorapp.storage import load_job


class _FakePlannerService:
    def __init__(self, job: JobRecord) -> None:
        self._job = job
        self.prompts: list[str] = []

    def create_job(self, user_prompt: str) -> JobRecord:
        self.prompts.append(user_prompt)
        return self._job


class _FakeShellExecutor:
    def __init__(self, *, fail: bool = False, interrupt: bool = False) -> None:
        self.fail = fail
        self.interrupt = interrupt
        self.calls: list[str] = []

    def execute(self, paths, task: TaskRecord) -> TaskRecord:
        self.calls.append(task.id)
        if self.interrupt:
            raise KeyboardInterrupt
        task.status = TaskStatus.failed if self.fail else TaskStatus.complete
        return task


class _FakeCodexExecutor:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, paths, task: TaskRecord) -> TaskRecord:
        self.calls.append(task.id)
        task.status = TaskStatus.complete
        return task


def _make_config(home_dir: str) -> OperatorConfig:
    return OperatorConfig(
        operator_home=home_dir,
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        command_timeout_seconds=600,
    )


def _make_job(task_kinds: list[TaskKind], *, out_of_order: bool = False) -> JobRecord:
    now = utc_now_z()
    tasks = []
    for idx, kind in enumerate(task_kinds):
        tasks.append(
            TaskRecord(
                id=f"jobabcd1_t{idx:03d}",
                parent_job_id="jobabcd1",
                task_index=idx,
                title=f"task-{idx}",
                kind=kind,
                instructions=f"instructions-{idx}",
            )
        )
    if out_of_order:
        tasks = [tasks[1], tasks[0]]

    return JobRecord(
        id="jobabcd1",
        user_prompt="do work",
        summary="summary",
        tasks=tasks,
        created_at=now,
        updated_at=now,
    )


def test_happy_path_sequential_execution(tmp_path: Path) -> None:
    job = _make_job([TaskKind.shell, TaskKind.codex], out_of_order=True)
    planner = _FakePlannerService(job)
    shell_executor = _FakeShellExecutor()
    codex_executor = _FakeCodexExecutor()
    app = OperatorApp(planner, shell_executor, codex_executor, _make_config(str(tmp_path)))

    result = app.run("hello world")

    assert result.status is JobStatus.complete
    assert shell_executor.calls == ["jobabcd1_t000"]
    assert codex_executor.calls == ["jobabcd1_t001"]
    assert all(task.status is TaskStatus.complete for task in result.tasks)


def test_failure_stops_later_tasks(tmp_path: Path) -> None:
    job = _make_job([TaskKind.shell, TaskKind.codex])
    planner = _FakePlannerService(job)
    shell_executor = _FakeShellExecutor(fail=True)
    codex_executor = _FakeCodexExecutor()
    app = OperatorApp(planner, shell_executor, codex_executor, _make_config(str(tmp_path)))

    result = app.run("hello world")

    assert result.status is JobStatus.failed
    assert shell_executor.calls == ["jobabcd1_t000"]
    assert codex_executor.calls == []
    assert result.tasks[0].status is TaskStatus.failed
    assert result.tasks[1].status is TaskStatus.pending


def test_keyboard_interrupt_marks_job_interrupted_and_reraises(tmp_path: Path) -> None:
    job = _make_job([TaskKind.shell])
    planner = _FakePlannerService(job)
    shell_executor = _FakeShellExecutor(interrupt=True)
    codex_executor = _FakeCodexExecutor()
    config = _make_config(str(tmp_path))
    app = OperatorApp(planner, shell_executor, codex_executor, config)

    with pytest.raises(KeyboardInterrupt):
        app.run("hello world")

    saved = load_job(str(Path(config.operator_home) / "jobabcd1" / "job.json"))
    assert saved.status is JobStatus.interrupted

