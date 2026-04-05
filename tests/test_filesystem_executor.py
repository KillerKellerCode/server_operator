from __future__ import annotations

from pathlib import Path

from operatorapp.events import JobEventType
from operatorapp.filesystem_executor import FilesystemTaskExecutor
from operatorapp.schemas import (
    JobRecord,
    TaskGroupRecord,
    TaskKind,
    TaskRecord,
    make_group_id,
    make_task_id,
)
from operatorapp.storage import create_run_paths, read_events
from operatorapp.task_payloads import FilesystemCreatePayload, FilesystemMutatePayload


def _job_group() -> tuple[JobRecord, TaskGroupRecord]:
    job = JobRecord(id="jobabcd1", user_prompt="filesystem operations")
    group = TaskGroupRecord(
        id=make_group_id(job.id, 0),
        parent_job_id=job.id,
        group_index=0,
        title="fs-group",
    )
    return job, group


def _task(kind: TaskKind, payload: object, task_index: int) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title=f"task-{task_index}",
        kind=kind,
        payload=payload,
    )


def test_filesystem_create_and_mutate_success(tmp_path: Path) -> None:
    executor = FilesystemTaskExecutor()
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    create_dir = _task(
        TaskKind.filesystem_create,
        FilesystemCreatePayload(path=str(tmp_path / "data"), node_type="directory"),
        task_index=0,
    )
    create_file = _task(
        TaskKind.filesystem_create,
        FilesystemCreatePayload(
            path=str(tmp_path / "data" / "a.txt"),
            node_type="file",
            content="hello",
        ),
        task_index=1,
    )
    move_file = _task(
        TaskKind.filesystem_mutate,
        FilesystemMutatePayload(
            operation="move",
            source_path=str(tmp_path / "data" / "a.txt"),
            destination_path=str(tmp_path / "data" / "b.txt"),
        ),
        task_index=2,
    )

    result_0 = executor.execute(paths, job, group, create_dir)
    result_1 = executor.execute(paths, job, group, create_file)
    result_2 = executor.execute(paths, job, group, move_file)

    assert result_0.status.value == "complete"
    assert result_1.status.value == "complete"
    assert result_2.status.value == "complete"
    assert (tmp_path / "data" / "b.txt").read_text(encoding="utf-8") == "hello"
    assert result_2.progress is not None
    assert result_2.progress.percent == 100.0

    events = read_events(paths.events_jsonl_path)
    assert any(event.event_type is JobEventType.task_progress for event in events)
    assert any(event.event_type is JobEventType.task_completed for event in events)


def test_filesystem_delete_requires_recursive_for_directories(tmp_path: Path) -> None:
    target_dir = tmp_path / "to-delete"
    target_dir.mkdir()
    (target_dir / "file.txt").write_text("x", encoding="utf-8")

    executor = FilesystemTaskExecutor()
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    delete_task = _task(
        TaskKind.filesystem_mutate,
        FilesystemMutatePayload(
            operation="delete",
            source_path=str(target_dir),
            recursive=False,
        ),
        task_index=3,
    )

    result = executor.execute(paths, job, group, delete_task)
    assert result.status.value == "failed"
    assert "recursive=true is required" in (result.error_message or "")
    assert target_dir.exists()


def test_filesystem_delete_refuses_root_path(tmp_path: Path) -> None:
    executor = FilesystemTaskExecutor()
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    dangerous_task = _task(
        TaskKind.filesystem_mutate,
        FilesystemMutatePayload(operation="delete", source_path="/", recursive=True),
        task_index=4,
    )

    result = executor.execute(paths, job, group, dangerous_task)
    assert result.status.value == "failed"
    assert "refusing to delete filesystem root path" in (result.error_message or "")

