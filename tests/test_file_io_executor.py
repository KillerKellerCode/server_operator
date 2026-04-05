from __future__ import annotations

from pathlib import Path

from operatorapp.events import JobEventType
from operatorapp.file_io_executor import FileIOTaskExecutor
from operatorapp.schemas import (
    JobRecord,
    TaskGroupRecord,
    TaskKind,
    TaskRecord,
    make_group_id,
    make_task_id,
)
from operatorapp.storage import create_run_paths, read_events
from operatorapp.task_payloads import (
    AppendFilePayload,
    PatchFilePayload,
    ReadFilePayload,
    WriteFilePayload,
)


def _job_group() -> tuple[JobRecord, TaskGroupRecord]:
    job = JobRecord(id="jobabcd1", user_prompt="file io operations")
    group = TaskGroupRecord(
        id=make_group_id(job.id, 0),
        parent_job_id=job.id,
        group_index=0,
        title="io-group",
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
        title=f"io-task-{task_index}",
        kind=kind,
        payload=payload,
    )


def test_file_io_write_append_patch_and_read_success(tmp_path: Path) -> None:
    executor = FileIOTaskExecutor()
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    target_file = tmp_path / "notes.txt"

    write_task = _task(
        TaskKind.write_file,
        WriteFilePayload(path=str(target_file), content="hello\nworld\n"),
        task_index=0,
    )
    append_task = _task(
        TaskKind.append_file,
        AppendFilePayload(path=str(target_file), content="tail", ensure_trailing_newline=True),
        task_index=1,
    )
    patch_task = _task(
        TaskKind.patch_file,
        PatchFilePayload(
            path=str(target_file),
            patch="@@ -1,2 +1,2 @@\n-hello\n+HELLO\n",
        ),
        task_index=2,
    )
    read_task = _task(
        TaskKind.read_file,
        ReadFilePayload(path=str(target_file), max_bytes=5),
        task_index=3,
    )

    assert executor.execute(paths, job, group, write_task).status.value == "complete"
    assert executor.execute(paths, job, group, append_task).status.value == "complete"
    assert executor.execute(paths, job, group, patch_task).status.value == "complete"
    read_result = executor.execute(paths, job, group, read_task)
    assert read_result.status.value == "complete"
    assert read_result.progress is not None
    assert read_result.progress.percent == 100.0

    content = target_file.read_text(encoding="utf-8")
    assert "HELLO" in content
    assert content.endswith("tail\n")

    events = read_events(paths.events_jsonl_path)
    assert any(event.event_type is JobEventType.task_progress for event in events)
    assert any(event.event_type is JobEventType.task_completed for event in events)
    assert any(event.data.get("preview") == "HELLO" for event in events if event.event_type is JobEventType.task_progress)


def test_file_io_write_without_parent_creation_fails(tmp_path: Path) -> None:
    executor = FileIOTaskExecutor()
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    target_file = tmp_path / "missing" / "a.txt"
    task = _task(
        TaskKind.write_file,
        WriteFilePayload(path=str(target_file), content="x", create_parents=False),
        task_index=4,
    )

    result = executor.execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "parent directory does not exist" in (result.error_message or "")


def test_file_io_patch_missing_context_fails(tmp_path: Path) -> None:
    target_file = tmp_path / "sample.txt"
    target_file.write_text("alpha\nbeta\n", encoding="utf-8")
    executor = FileIOTaskExecutor()
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    task = _task(
        TaskKind.patch_file,
        PatchFilePayload(
            path=str(target_file),
            patch="@@ -1 +1 @@\n-missing\n+updated\n",
        ),
        task_index=5,
    )
    result = executor.execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "patch context not found" in (result.error_message or "")

