from __future__ import annotations

from pathlib import Path

from operatorapp.events import JobEventType
from operatorapp.schemas import JobRecord, TaskGroupRecord, TaskKind, TaskRecord, make_group_id, make_task_id
from operatorapp.storage import create_run_paths, read_events
from operatorapp.task_payloads import ProcessWaitPayload
from operatorapp.wait_executor import WaitTaskExecutor


def _job_group() -> tuple[JobRecord, TaskGroupRecord]:
    job = JobRecord(id="jobabcd1", user_prompt="wait checks")
    group = TaskGroupRecord(
        id=make_group_id(job.id, 0),
        parent_job_id=job.id,
        group_index=0,
        title="wait-group",
    )
    return job, group


def _task(payload: ProcessWaitPayload, task_index: int) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title=f"wait-{task_index}",
        kind=TaskKind.process_wait,
        payload=payload,
    )


def test_wait_executor_success_for_multiple_condition_types(
    monkeypatch, tmp_path: Path
) -> None:
    wait_file = tmp_path / "ok.txt"
    wait_file.write_text("x", encoding="utf-8")
    wait_dir = tmp_path / "dir"
    wait_dir.mkdir()

    def fake_check(condition, success_pattern):  # type: ignore[no-untyped-def]
        if condition.kind == "port":
            return True
        if condition.kind == "http":
            return True
        if condition.kind == "file":
            return Path(condition.target).is_file()
        if condition.kind == "dir":
            return Path(condition.target).is_dir()
        return False

    monkeypatch.setattr("operatorapp.wait_executor._check_condition", fake_check)

    spec = f"file:{wait_file},dir:{wait_dir},http:http://example.local/ready,port:127.0.0.1:8080"
    payload = ProcessWaitPayload(
        process_name=spec,
        timeout_seconds=3,
        poll_interval_seconds=0.1,
        success_pattern="ready:ok",
    )
    task = _task(payload, 0)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    result = WaitTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "complete"
    assert result.progress is not None
    assert result.progress.percent == 100.0

    events = read_events(paths.events_jsonl_path)
    assert events[0].event_type is JobEventType.task_started
    assert any(event.event_type is JobEventType.task_progress for event in events)
    assert events[-1].event_type is JobEventType.task_completed


def test_wait_executor_timeout_failure(tmp_path: Path) -> None:
    missing_file = tmp_path / "missing.txt"
    payload = ProcessWaitPayload(
        process_name=f"file:{missing_file}",
        timeout_seconds=1,
        poll_interval_seconds=0.1,
    )
    task = _task(payload, 1)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    result = WaitTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "timeout after 1s" in (result.error_message or "")


def test_wait_executor_process_exit_condition(monkeypatch, tmp_path: Path) -> None:
    def fake_check(condition, success_pattern):  # type: ignore[no-untyped-def]
        return condition.kind == "process_exit"

    monkeypatch.setattr("operatorapp.wait_executor._check_condition", fake_check)

    payload = ProcessWaitPayload(
        process_name="process_exit:12345",
        timeout_seconds=3,
        poll_interval_seconds=0.1,
    )
    task = _task(payload, 2)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    result = WaitTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "complete"
