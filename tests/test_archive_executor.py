from __future__ import annotations

import zipfile
from pathlib import Path

from operatorapp.archive_executor import ArchiveTaskExecutor
from operatorapp.events import JobEventType
from operatorapp.schemas import JobRecord, TaskGroupRecord, TaskKind, TaskRecord, make_group_id, make_task_id
from operatorapp.storage import create_run_paths, read_events
from operatorapp.task_payloads import ExtractArchivePayload


def _job_group() -> tuple[JobRecord, TaskGroupRecord]:
    job = JobRecord(id="jobabcd1", user_prompt="extract archive")
    group = TaskGroupRecord(
        id=make_group_id(job.id, 0),
        parent_job_id=job.id,
        group_index=0,
        title="archives",
    )
    return job, group


def _task(payload: ExtractArchivePayload, task_index: int) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title=f"extract-{task_index}",
        kind=TaskKind.extract_archive,
        payload=payload,
    )


def test_safe_zip_extraction_success_with_events_and_progress(tmp_path: Path) -> None:
    archive_path = tmp_path / "data.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("folder/a.txt", "hello")
        archive.writestr("folder/b.txt", "world")

    destination = tmp_path / "out"
    payload = ExtractArchivePayload(
        archive_path=str(archive_path),
        destination_dir=str(destination),
        format="zip",
        overwrite=False,
    )
    executor = ArchiveTaskExecutor()
    job, group = _job_group()
    task = _task(payload, 0)
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    result = executor.execute(paths, job, group, task)
    assert result.status.value == "complete"
    assert result.progress is not None
    assert result.progress.percent == 100.0
    assert (destination / "folder" / "a.txt").read_text(encoding="utf-8") == "hello"
    assert (destination / "folder" / "b.txt").read_text(encoding="utf-8") == "world"

    events = read_events(paths.events_jsonl_path)
    assert events[0].event_type is JobEventType.task_started
    assert any(event.event_type is JobEventType.task_progress for event in events)
    assert events[-1].event_type is JobEventType.task_completed


def test_archive_path_traversal_is_blocked(tmp_path: Path) -> None:
    archive_path = tmp_path / "evil.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../evil.txt", "bad")

    destination = tmp_path / "safe"
    payload = ExtractArchivePayload(
        archive_path=str(archive_path),
        destination_dir=str(destination),
        format="zip",
        overwrite=True,
    )
    executor = ArchiveTaskExecutor()
    job, group = _job_group()
    task = _task(payload, 1)
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    result = executor.execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "escapes destination" in (result.error_message or "")
    assert not (tmp_path / "evil.txt").exists()


def test_archive_overwrite_policy_enforced(tmp_path: Path) -> None:
    archive_path = tmp_path / "data.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.txt", "new-value")

    destination = tmp_path / "out"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "a.txt").write_text("existing", encoding="utf-8")

    payload = ExtractArchivePayload(
        archive_path=str(archive_path),
        destination_dir=str(destination),
        format="zip",
        overwrite=False,
    )
    executor = ArchiveTaskExecutor()
    job, group = _job_group()
    task = _task(payload, 2)
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    result = executor.execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "overwrite is false" in (result.error_message or "")
    assert (destination / "a.txt").read_text(encoding="utf-8") == "existing"
