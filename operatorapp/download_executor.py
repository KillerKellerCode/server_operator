"""Native executor for download_file tasks."""

from __future__ import annotations

import hashlib
import urllib.parse
import urllib.request
from pathlib import Path

from .events import JobEvent, JobEventType
from .logging_utils import append_run_log, append_task_log
from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import DownloadFilePayload

_DEFAULT_TIMEOUT_SECONDS = 600
_CHUNK_SIZE = 64 * 1024


def _event(paths: RunPaths, event: JobEvent) -> None:
    append_event(paths, event)


def _update_progress(
    *,
    task: TaskRecord,
    state_message: str,
    bytes_completed: int | None = None,
    bytes_total: int | None = None,
    current_operation: str = "",
) -> None:
    percent: float | None = None
    if bytes_completed is not None and bytes_total and bytes_total > 0:
        percent = min(100.0, (bytes_completed / bytes_total) * 100.0)
    task.progress = TaskProgress(
        state_message=state_message,
        percent=percent,
        bytes_completed=bytes_completed,
        bytes_total=bytes_total,
        current_operation=current_operation,
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


def _extract_sha256(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    fragment = urllib.parse.parse_qs(parsed.fragment)
    for key in ("sha256", "checksum_sha256", "checksum"):
        values = query.get(key) or fragment.get(key)
        if values:
            checksum = values[0].strip().lower()
            if len(checksum) != 64 or any(ch not in "0123456789abcdef" for ch in checksum):
                raise ValueError("invalid sha256 checksum format")
            return checksum
    return None


class DownloadTaskExecutor:
    kind = TaskKind.download_file

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        if task.kind is not TaskKind.download_file:
            raise ValueError("DownloadTaskExecutor only supports download_file tasks")

        payload = task.payload
        if not isinstance(payload, DownloadFilePayload):
            raise ValueError("download_file task payload must be DownloadFilePayload")

        task.status = TaskStatus.in_progress
        task.error_message = None
        _update_progress(
            task=task,
            state_message="Starting download",
            bytes_completed=0,
            current_operation="download_start",
        )
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Download task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", f"Downloading {payload.url}")
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="Download task started",
                data={"url": payload.url},
            ),
        )

        destination = Path(payload.destination_path).expanduser().resolve()
        timeout_seconds = payload.timeout_seconds or task.timeout_seconds or _DEFAULT_TIMEOUT_SECONDS
        expected_sha256 = _extract_sha256(payload.url)

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists() and destination.is_dir():
                raise IsADirectoryError(f"destination is a directory: {destination}")
            if destination.exists() and not payload.overwrite:
                raise FileExistsError(
                    f"destination exists and overwrite is false: {destination}"
                )

            temp_path = destination.with_name(f".{destination.name}.download.tmp")
            request = urllib.request.Request(payload.url, method="GET")
            hasher = hashlib.sha256() if expected_sha256 else None

            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                status = getattr(response, "status", 200)
                if status >= 400:
                    raise RuntimeError(f"HTTP request failed with status {status}")

                total_raw = response.headers.get("Content-Length") if response.headers else None
                total_bytes = int(total_raw) if total_raw and total_raw.isdigit() else None
                completed = 0
                with temp_path.open("wb") as handle:
                    while True:
                        chunk = response.read(_CHUNK_SIZE)
                        if not chunk:
                            break
                        handle.write(chunk)
                        completed += len(chunk)
                        if hasher is not None:
                            hasher.update(chunk)
                        _update_progress(
                            task=task,
                            state_message="Downloading",
                            bytes_completed=completed,
                            bytes_total=total_bytes,
                            current_operation="streaming",
                        )
                        save_task(paths, task)
                        _event(
                            paths,
                            JobEvent(
                                event_type=JobEventType.task_progress,
                                job_id=job.id,
                                group_id=group.id,
                                task_id=task.id,
                                message="Download progress",
                                data={
                                    "bytes_completed": completed,
                                    "bytes_total": total_bytes,
                                    "percent": task.progress.percent if task.progress else None,
                                },
                            ),
                        )

            if hasher is not None:
                actual_sha256 = hasher.hexdigest()
                if actual_sha256 != expected_sha256:
                    if temp_path.exists():
                        temp_path.unlink()
                    raise ValueError(
                        f"sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"
                    )

            temp_path.replace(destination)
            _update_progress(
                task=task,
                state_message="Download complete",
                bytes_completed=destination.stat().st_size if destination.exists() else None,
                bytes_total=destination.stat().st_size if destination.exists() else None,
                current_operation="complete",
            )
            if task.progress is not None:
                task.progress.percent = 100.0
            task.status = TaskStatus.complete
            task.updated_at = utc_now_z()
            append_task_log(paths, task.id, "INFO", f"Download completed: {destination}")
            _event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_completed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Download task completed",
                    data={"destination_path": str(destination)},
                ),
            )
        except Exception as exc:
            task.status = TaskStatus.failed
            task.error_message = str(exc)
            task.updated_at = utc_now_z()
            _update_progress(
                task=task,
                state_message=f"Download failed: {exc}",
                current_operation="failed",
            )
            append_run_log(paths, "ERROR", f"Download task failed: {exc}", task_id=task.id)
            append_task_log(paths, task.id, "ERROR", str(exc))
            _event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_failed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Download task failed",
                    data={"error": str(exc)},
                ),
            )
        finally:
            save_task(paths, task)

        return task

