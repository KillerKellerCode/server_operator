from __future__ import annotations

import hashlib
import json
from pathlib import Path

from operatorapp.download_executor import DownloadTaskExecutor
from operatorapp.events import JobEventType
from operatorapp.schemas import (
    JobRecord,
    TaskGroupRecord,
    TaskKind,
    TaskRecord,
    make_group_id,
    make_task_id,
)
from operatorapp.storage import create_run_paths, read_events
from operatorapp.task_payloads import DownloadFilePayload


class _FakeHTTPResponse:
    def __init__(self, body: bytes, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self._body = body
        self._offset = 0
        self.status = status
        self.headers = headers or {}

    def read(self, size: int = -1) -> bytes:
        if self._offset >= len(self._body):
            return b""
        if size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self) -> "_FakeHTTPResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def _job_group() -> tuple[JobRecord, TaskGroupRecord]:
    job = JobRecord(id="jobabcd1", user_prompt="Download a file")
    group = TaskGroupRecord(
        id=make_group_id(job.id, 0),
        parent_job_id=job.id,
        group_index=0,
        title="Downloads",
    )
    return job, group


def _task(payload: DownloadFilePayload, task_index: int = 0) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title="Download asset",
        kind=TaskKind.download_file,
        payload=payload,
    )


def test_successful_download_emits_progress_and_events(monkeypatch, tmp_path: Path) -> None:
    body = b"downloaded-bytes"
    sha = hashlib.sha256(body).hexdigest()
    payload = DownloadFilePayload(
        url=f"https://example.com/file.bin?sha256={sha}",
        destination_path=str(tmp_path / "file.bin"),
        overwrite=False,
    )
    task = _task(payload)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
        assert timeout == 600
        return _FakeHTTPResponse(body, status=200, headers={"Content-Length": str(len(body))})

    monkeypatch.setattr("operatorapp.download_executor.urllib.request.urlopen", fake_urlopen)

    result = DownloadTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "complete"
    assert result.error_message is None
    assert result.progress is not None
    assert result.progress.percent == 100.0
    assert (tmp_path / "file.bin").read_bytes() == body

    events = read_events(paths.events_jsonl_path)
    event_types = [event.event_type for event in events]
    assert JobEventType.task_started in event_types
    assert JobEventType.task_progress in event_types
    assert JobEventType.task_completed in event_types

    task_snapshot = Path(paths.tasks_dir) / task.id / "task.json"
    snapshot_data = json.loads(task_snapshot.read_text(encoding="utf-8"))
    assert snapshot_data["status"] == "complete"


def test_download_checksum_failure_marks_task_failed(monkeypatch, tmp_path: Path) -> None:
    payload = DownloadFilePayload(
        url="https://example.com/file.bin?sha256=" + ("a" * 64),
        destination_path=str(tmp_path / "bad.bin"),
        overwrite=True,
    )
    task = _task(payload)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    monkeypatch.setattr(
        "operatorapp.download_executor.urllib.request.urlopen",
        lambda request, timeout: _FakeHTTPResponse(b"mismatch", status=200),
    )

    result = DownloadTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert result.error_message is not None
    assert "sha256 mismatch" in result.error_message

    events = read_events(paths.events_jsonl_path)
    assert events[-1].event_type is JobEventType.task_failed


def test_download_overwrite_behavior(monkeypatch, tmp_path: Path) -> None:
    destination = tmp_path / "exists.bin"
    destination.write_bytes(b"existing")
    payload = DownloadFilePayload(
        url="https://example.com/file.bin",
        destination_path=str(destination),
        overwrite=False,
    )
    task = _task(payload, task_index=1)
    job, group = _job_group()
    paths = create_run_paths(str(tmp_path / "runs"), job.id)

    called = {"value": False}

    def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
        called["value"] = True
        return _FakeHTTPResponse(b"new-data", status=200)

    monkeypatch.setattr("operatorapp.download_executor.urllib.request.urlopen", fake_urlopen)

    result = DownloadTaskExecutor().execute(paths, job, group, task)
    assert result.status.value == "failed"
    assert "overwrite is false" in (result.error_message or "")
    assert called["value"] is False
    assert destination.read_bytes() == b"existing"

