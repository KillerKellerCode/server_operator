"""Native executor for extract_archive tasks."""

from __future__ import annotations

import shutil
import tarfile
import zipfile
from pathlib import Path

from .events import JobEvent, JobEventType
from .logging_utils import append_run_log, append_task_log
from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import ExtractArchivePayload


def _is_within_base(base: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(base)
    except ValueError:
        return False
    return True


def _safe_member_path(destination: Path, member_name: str) -> Path:
    candidate = (destination / member_name).resolve()
    if not _is_within_base(destination, candidate):
        raise ValueError(f"archive member escapes destination: {member_name}")
    return candidate


def _set_progress(task: TaskRecord, message: str, percent: float) -> None:
    task.progress = TaskProgress(
        state_message=message,
        percent=percent,
        current_operation="extract_archive",
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


class ArchiveTaskExecutor:
    kind = TaskKind.extract_archive

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        if task.kind is not TaskKind.extract_archive:
            raise ValueError("ArchiveTaskExecutor only supports extract_archive tasks")

        payload = task.payload
        if not isinstance(payload, ExtractArchivePayload):
            raise ValueError("extract_archive payload must be ExtractArchivePayload")

        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Starting archive extraction", 0.0)
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Archive task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", f"Extracting {payload.archive_path}")
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.task_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="Archive extraction started",
            ),
        )

        archive_path = Path(payload.archive_path).expanduser().resolve()
        destination = Path(payload.destination_dir).expanduser().resolve()

        try:
            if not archive_path.exists() or not archive_path.is_file():
                raise FileNotFoundError(f"archive not found: {archive_path}")
            destination.mkdir(parents=True, exist_ok=True)

            extracted = self._extract(payload, archive_path, destination, paths, job, group, task)

            _set_progress(task, "Archive extraction complete", 100.0)
            task.status = TaskStatus.complete
            append_task_log(paths, task.id, "INFO", f"Extracted {extracted} entries to {destination}")
            append_event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_completed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Archive extraction completed",
                    data={"entries_extracted": extracted, "destination_dir": str(destination)},
                ),
            )
        except Exception as exc:
            task.status = TaskStatus.failed
            task.error_message = str(exc)
            _set_progress(task, f"Archive extraction failed: {exc}", 0.0)
            append_run_log(paths, "ERROR", f"Archive task failed: {exc}", task_id=task.id)
            append_task_log(paths, task.id, "ERROR", str(exc))
            append_event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_failed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Archive extraction failed",
                    data={"error": str(exc)},
                ),
            )
        finally:
            save_task(paths, task)

        return task

    def _extract(
        self,
        payload: ExtractArchivePayload,
        archive_path: Path,
        destination: Path,
        paths: RunPaths,
        job: JobRecord,
        group: TaskGroupRecord,
        task: TaskRecord,
    ) -> int:
        if payload.format == "zip":
            return self._extract_zip(payload, archive_path, destination, paths, job, group, task)
        mode_map = {
            "tar": "r:",
            "tar.gz": "r:gz",
            "tar.bz2": "r:bz2",
            "tar.xz": "r:xz",
        }
        mode = mode_map.get(payload.format)
        if mode is None:
            raise ValueError(f"unsupported archive format: {payload.format}")
        return self._extract_tar(payload, archive_path, destination, mode, paths, job, group, task)

    def _extract_zip(
        self,
        payload: ExtractArchivePayload,
        archive_path: Path,
        destination: Path,
        paths: RunPaths,
        job: JobRecord,
        group: TaskGroupRecord,
        task: TaskRecord,
    ) -> int:
        with zipfile.ZipFile(archive_path, "r") as archive:
            members = [member for member in archive.infolist() if member.filename and member.filename != "/"]
            total = max(len(members), 1)
            for index, member in enumerate(members, start=1):
                target = _safe_member_path(destination, member.filename)
                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    if target.exists() and not payload.overwrite:
                        raise FileExistsError(f"target exists and overwrite is false: {target}")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member, "r") as source, target.open("wb") as out:
                        shutil.copyfileobj(source, out)
                percent = (index / total) * 100.0
                _set_progress(task, f"Extracted {index}/{total} entries", percent)
                save_task(paths, task)
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.task_progress,
                        job_id=job.id,
                        group_id=group.id,
                        task_id=task.id,
                        message="Archive extraction progress",
                        data={"entries_processed": index, "entries_total": total},
                    ),
                )
            return len(members)

    def _extract_tar(
        self,
        payload: ExtractArchivePayload,
        archive_path: Path,
        destination: Path,
        mode: str,
        paths: RunPaths,
        job: JobRecord,
        group: TaskGroupRecord,
        task: TaskRecord,
    ) -> int:
        with tarfile.open(archive_path, mode) as archive:
            members = [member for member in archive.getmembers() if member.name and member.name != "/"]
            total = max(len(members), 1)
            for index, member in enumerate(members, start=1):
                target = _safe_member_path(destination, member.name)
                if member.issym() or member.islnk():
                    raise ValueError(f"refusing to extract tar link member: {member.name}")

                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                elif member.isfile():
                    if target.exists() and not payload.overwrite:
                        raise FileExistsError(f"target exists and overwrite is false: {target}")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = archive.extractfile(member)
                    if source is None:
                        raise ValueError(f"unable to read tar member: {member.name}")
                    with source, target.open("wb") as out:
                        shutil.copyfileobj(source, out)
                else:
                    raise ValueError(f"unsupported tar member type: {member.name}")

                percent = (index / total) * 100.0
                _set_progress(task, f"Extracted {index}/{total} entries", percent)
                save_task(paths, task)
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.task_progress,
                        job_id=job.id,
                        group_id=group.id,
                        task_id=task.id,
                        message="Archive extraction progress",
                        data={"entries_processed": index, "entries_total": total},
                    ),
                )
            return len(members)

