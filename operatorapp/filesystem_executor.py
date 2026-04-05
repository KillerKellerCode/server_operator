"""Native executor for filesystem_create and filesystem_mutate tasks."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from .events import JobEvent, JobEventType
from .logging_utils import append_run_log, append_task_log
from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import FilesystemCreatePayload, FilesystemMutatePayload


def _event(paths: RunPaths, event: JobEvent) -> None:
    append_event(paths, event)


def _set_progress(task: TaskRecord, state_message: str, operation: str) -> None:
    task.progress = TaskProgress(
        state_message=state_message,
        percent=100.0 if operation == "complete" else 0.0,
        current_operation=operation,
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


def _resolve_path(raw_path: str) -> Path:
    return Path(raw_path).expanduser().resolve()


def _validate_delete_target(path: Path, recursive: bool) -> None:
    if path == Path("/"):
        raise ValueError("refusing to delete filesystem root path")
    if path == Path.home().resolve():
        raise ValueError("refusing to delete home directory")
    if path.is_dir() and not recursive:
        raise ValueError("recursive=true is required to delete a directory")


class FilesystemTaskExecutor:
    kind = TaskKind.filesystem_create
    supported_kinds = {TaskKind.filesystem_create, TaskKind.filesystem_mutate}

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        if task.kind not in self.supported_kinds:
            raise ValueError(
                "FilesystemTaskExecutor only supports filesystem_create and filesystem_mutate tasks"
            )

        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Starting filesystem operation", "start")
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Filesystem task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", "Filesystem task started")
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="Filesystem task started",
            ),
        )

        try:
            if task.kind is TaskKind.filesystem_create:
                payload = task.payload
                if not isinstance(payload, FilesystemCreatePayload):
                    raise ValueError("filesystem_create task payload must be FilesystemCreatePayload")
                self._execute_create(payload, task, paths)
            else:
                payload = task.payload
                if not isinstance(payload, FilesystemMutatePayload):
                    raise ValueError("filesystem_mutate task payload must be FilesystemMutatePayload")
                self._execute_mutate(payload, task, paths)

            task.status = TaskStatus.complete
            _set_progress(task, "Filesystem operation complete", "complete")
            append_task_log(paths, task.id, "INFO", "Filesystem task completed")
            _event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_completed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Filesystem task completed",
                ),
            )
        except Exception as exc:
            task.status = TaskStatus.failed
            task.error_message = str(exc)
            _set_progress(task, f"Filesystem task failed: {exc}", "failed")
            append_run_log(paths, "ERROR", f"Filesystem task failed: {exc}", task_id=task.id)
            append_task_log(paths, task.id, "ERROR", str(exc))
            _event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_failed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Filesystem task failed",
                    data={"error": str(exc)},
                ),
            )
        finally:
            save_task(paths, task)

        return task

    def _execute_create(self, payload: FilesystemCreatePayload, task: TaskRecord, paths: RunPaths) -> None:
        target_path = _resolve_path(payload.path)
        append_task_log(paths, task.id, "INFO", f"Create {payload.node_type}: {target_path}")

        if payload.node_type == "directory":
            target_path.mkdir(parents=True, exist_ok=payload.exist_ok)
        elif payload.node_type == "file":
            if target_path.exists() and not payload.exist_ok:
                raise FileExistsError(f"path already exists: {target_path}")
            if target_path.exists() and target_path.is_dir():
                raise IsADirectoryError(f"path is a directory: {target_path}")
            target_path.parent.mkdir(parents=True, exist_ok=True)
            content = payload.content if payload.content is not None else ""
            if not target_path.exists() or payload.content is not None:
                temp_path = target_path.with_name(f".{target_path.name}.tmp")
                temp_path.write_text(content, encoding="utf-8")
                temp_path.replace(target_path)
        elif payload.node_type == "symlink":
            if not payload.target:
                raise ValueError("target is required for symlink creation")
            if target_path.exists() and not payload.exist_ok:
                raise FileExistsError(f"path already exists: {target_path}")
            if target_path.exists() and payload.exist_ok:
                return
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.symlink_to(payload.target)
        else:
            raise ValueError(f"unsupported filesystem_create node_type: {payload.node_type}")

        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_progress,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="Filesystem create applied",
                data={"path": str(target_path), "node_type": payload.node_type},
            ),
        )

    def _execute_mutate(self, payload: FilesystemMutatePayload, task: TaskRecord, paths: RunPaths) -> None:
        source = _resolve_path(payload.source_path)
        destination = _resolve_path(payload.destination_path) if payload.destination_path else None
        append_task_log(paths, task.id, "INFO", f"Mutate {payload.operation}: {source}")

        if payload.operation in {"rename", "move"}:
            if destination is None:
                raise ValueError("destination_path is required for rename/move")
            destination.parent.mkdir(parents=True, exist_ok=True)
            if payload.operation == "rename":
                source.rename(destination)
            else:
                shutil.move(str(source), str(destination))
        elif payload.operation == "copy":
            if destination is None:
                raise ValueError("destination_path is required for copy")
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.is_dir():
                if not payload.recursive:
                    raise ValueError("recursive=true is required to copy a directory")
                shutil.copytree(source, destination)
            else:
                shutil.copy2(source, destination)
        elif payload.operation == "delete":
            _validate_delete_target(source, payload.recursive)
            if source.is_dir():
                shutil.rmtree(source)
            else:
                source.unlink()
        elif payload.operation == "chmod":
            if payload.mode is None:
                raise ValueError("mode is required for chmod")
            mode_string = payload.mode[2:] if payload.mode.startswith("0o") else payload.mode
            mode_value = int(mode_string, 8)
            os.chmod(source, mode_value)
        else:
            raise ValueError(f"unsupported filesystem_mutate operation: {payload.operation}")

        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_progress,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="Filesystem mutation applied",
                data={
                    "operation": payload.operation,
                    "source_path": str(source),
                    "destination_path": str(destination) if destination else None,
                },
            ),
        )

