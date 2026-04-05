from __future__ import annotations

import subprocess
from pathlib import Path

from operatorapp.events import JobEventType
from operatorapp.git_executor import GitTaskExecutor
from operatorapp.schemas import JobRecord, TaskGroupRecord, TaskKind, TaskRecord, make_group_id, make_task_id
from operatorapp.storage import create_run_paths, read_events
from operatorapp.task_payloads import GitRepoPayload


def _job_group() -> tuple[JobRecord, TaskGroupRecord]:
    job = JobRecord(id="jobabcd1", user_prompt="git flow")
    group = TaskGroupRecord(
        id=make_group_id(job.id, 0),
        parent_job_id=job.id,
        group_index=0,
        title="git-group",
    )
    return job, group


def _task(payload: GitRepoPayload, task_index: int) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title=f"git-{task_index}",
        kind=TaskKind.git_repo,
        payload=payload,
    )


def test_git_clone_and_checkout_with_subprocess_fake(monkeypatch, tmp_path: Path) -> None:
    payload = GitRepoPayload(
        operation="clone",
        repo_url="https://github.com/example/repo.git",
        repo_path=str(tmp_path / "repo"),
        ref="main",
    )
    task = _task(payload, 0)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    calls: list[list[str]] = []

    def fake_run(command, text, capture_output, timeout, check):  # type: ignore[no-untyped-def]
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr("operatorapp.git_executor.subprocess.run", fake_run)

    result = GitTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "complete"
    assert calls == [
        ["git", "clone", "https://github.com/example/repo.git", str(tmp_path / "repo")],
        ["git", "-C", str(tmp_path / "repo"), "checkout", "main"],
    ]
    events = read_events(paths.events_jsonl_path)
    assert any(event.event_type is JobEventType.command_started for event in events)
    assert any(event.event_type is JobEventType.command_completed for event in events)


def test_git_pull_operation_success_with_subprocess_fake(monkeypatch, tmp_path: Path) -> None:
    payload = GitRepoPayload(
        operation="pull",
        repo_path=str(tmp_path / "repo"),
        repo_url=None,
        ref="main",
    )
    task = _task(payload, 1)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    calls: list[list[str]] = []

    def fake_run(command, text, capture_output, timeout, check):  # type: ignore[no-untyped-def]
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="updated", stderr="")

    monkeypatch.setattr("operatorapp.git_executor.subprocess.run", fake_run)

    result = GitTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "complete"
    assert calls == [["git", "-C", str(tmp_path / "repo"), "pull", "origin", "main"]]


def test_git_operation_failure_marks_task_failed(monkeypatch, tmp_path: Path) -> None:
    payload = GitRepoPayload(
        operation="pull",
        repo_path=str(tmp_path / "repo"),
        repo_url=None,
        ref=None,
    )
    task = _task(payload, 2)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    def fake_run(command, text, capture_output, timeout, check):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(
            command, 1, stdout="", stderr="fatal: not a git repository"
        )

    monkeypatch.setattr("operatorapp.git_executor.subprocess.run", fake_run)

    result = GitTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "fatal: not a git repository" in (result.error_message or "")
