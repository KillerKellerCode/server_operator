"""Native executor for read/write/append/patch file tasks."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from .events import JobEvent, JobEventType
from .logging_utils import append_run_log, append_task_log
from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import (
    AppendFilePayload,
    PatchFilePayload,
    ReadFilePayload,
    WriteFilePayload,
)

_PREVIEW_CHARS = 200


def _event(paths: RunPaths, event: JobEvent) -> None:
    append_event(paths, event)


def _set_progress(task: TaskRecord, state_message: str, operation: str, percent: float) -> None:
    task.progress = TaskProgress(
        state_message=state_message,
        percent=percent,
        current_operation=operation,
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


def _resolve_path(raw_path: str) -> Path:
    return Path(raw_path).expanduser().resolve()


def _atomic_write_text(path: Path, content: str, encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temp_path.write_text(content, encoding=encoding)
    temp_path.replace(path)


def _extract_patch_replace_blocks(patch: str) -> tuple[str, str]:
    removed_lines: list[str] = []
    added_lines: list[str] = []
    for line in patch.splitlines():
        if line.startswith(("---", "+++", "@@")):
            continue
        if line.startswith("-"):
            removed_lines.append(line[1:])
        elif line.startswith("+"):
            added_lines.append(line[1:])

    old_block = "\n".join(removed_lines)
    new_block = "\n".join(added_lines)
    if old_block == "" and new_block == "":
        raise ValueError("patch did not contain any replaceable lines")
    return old_block, new_block


class FileIOTaskExecutor:
    kind = TaskKind.read_file
    supported_kinds = {
        TaskKind.read_file,
        TaskKind.write_file,
        TaskKind.append_file,
        TaskKind.patch_file,
    }

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        if task.kind not in self.supported_kinds:
            raise ValueError(
                "FileIOTaskExecutor only supports read_file, write_file, append_file, patch_file tasks"
            )

        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Starting file IO task", "start", 0.0)
        save_task(paths, task)
        append_run_log(paths, "INFO", f"File IO task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", f"Running {task.kind.value}")
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="File IO task started",
                data={"kind": task.kind.value},
            ),
        )

        try:
            if task.kind is TaskKind.read_file:
                self._execute_read(paths, task)
            elif task.kind is TaskKind.write_file:
                self._execute_write(paths, task)
            elif task.kind is TaskKind.append_file:
                self._execute_append(paths, task)
            elif task.kind is TaskKind.patch_file:
                self._execute_patch(paths, task)

            task.status = TaskStatus.complete
            _set_progress(task, "File IO task complete", "complete", 100.0)
            append_task_log(paths, task.id, "INFO", "File IO task completed")
            _event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_completed,
                    job_id=task.parent_job_id,
                    group_id=task.parent_group_id,
                    task_id=task.id,
                    message="File IO task completed",
                    data={"kind": task.kind.value},
                ),
            )
        except Exception as exc:
            task.status = TaskStatus.failed
            task.error_message = str(exc)
            _set_progress(task, f"File IO task failed: {exc}", "failed", 0.0)
            append_run_log(paths, "ERROR", f"File IO task failed: {exc}", task_id=task.id)
            append_task_log(paths, task.id, "ERROR", str(exc))
            _event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_failed,
                    job_id=task.parent_job_id,
                    group_id=task.parent_group_id,
                    task_id=task.id,
                    message="File IO task failed",
                    data={"error": str(exc), "kind": task.kind.value},
                ),
            )
        finally:
            save_task(paths, task)

        return task

    def _execute_read(self, paths: RunPaths, task: TaskRecord) -> None:
        payload = task.payload
        if not isinstance(payload, ReadFilePayload):
            raise ValueError("read_file task payload must be ReadFilePayload")

        path = _resolve_path(payload.path)
        max_bytes = payload.max_bytes
        if max_bytes is None:
            raw = path.read_bytes()
            truncated = False
        else:
            with path.open("rb") as handle:
                raw = handle.read(max_bytes)
            truncated = path.stat().st_size > len(raw)

        text = raw.decode(payload.encoding, errors="replace")
        preview = text[:_PREVIEW_CHARS]
        _set_progress(task, f"Read {len(raw)} bytes", "read", 100.0)
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_progress,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="File read complete",
                data={
                    "path": str(path),
                    "bytes_read": len(raw),
                    "truncated": truncated,
                    "preview": preview,
                },
            ),
        )

    def _execute_write(self, paths: RunPaths, task: TaskRecord) -> None:
        payload = task.payload
        if not isinstance(payload, WriteFilePayload):
            raise ValueError("write_file task payload must be WriteFilePayload")

        path = _resolve_path(payload.path)
        if not payload.create_parents and not path.parent.exists():
            raise FileNotFoundError(f"parent directory does not exist: {path.parent}")
        _atomic_write_text(path, payload.content, payload.encoding)
        _set_progress(task, f"Wrote {len(payload.content)} chars", "write", 100.0)
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_progress,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="File write complete",
                data={"path": str(path), "chars_written": len(payload.content)},
            ),
        )

    def _execute_append(self, paths: RunPaths, task: TaskRecord) -> None:
        payload = task.payload
        if not isinstance(payload, AppendFilePayload):
            raise ValueError("append_file task payload must be AppendFilePayload")

        path = _resolve_path(payload.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = payload.content
        if payload.ensure_trailing_newline and not content.endswith("\n"):
            content += "\n"
        with path.open("a", encoding=payload.encoding, newline="") as handle:
            handle.write(content)
        _set_progress(task, f"Appended {len(content)} chars", "append", 100.0)
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_progress,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="File append complete",
                data={"path": str(path), "chars_appended": len(content)},
            ),
        )

    def _execute_patch(self, paths: RunPaths, task: TaskRecord) -> None:
        payload = task.payload
        if not isinstance(payload, PatchFilePayload):
            raise ValueError("patch_file task payload must be PatchFilePayload")
        if payload.patch_format != "unified_diff":
            raise ValueError("only unified_diff patch_format is supported")

        path = _resolve_path(payload.path)
        original = path.read_text(encoding="utf-8")
        old_block, new_block = _extract_patch_replace_blocks(payload.patch)

        if old_block:
            if old_block not in original:
                raise ValueError("patch context not found in target file")
            patched = original.replace(old_block, new_block, 1)
        else:
            patched = original + ("\n" if original and not original.endswith("\n") else "") + new_block

        _atomic_write_text(path, patched, "utf-8")
        _set_progress(task, "Patch applied", "patch", 100.0)
        _event(
            paths,
            JobEvent(
                event_type=JobEventType.task_progress,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="File patch complete",
                data={"path": str(path), "patched": True},
            ),
        )

