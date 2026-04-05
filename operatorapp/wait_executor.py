"""Typed executor for process_wait tasks."""

from __future__ import annotations

import os
import socket
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from .events import JobEvent, JobEventType
from .logging_utils import append_run_log, append_task_log
from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import ProcessWaitPayload


@dataclass(frozen=True)
class _Condition:
    kind: str
    target: str


def _set_progress(task: TaskRecord, message: str, percent: float) -> None:
    task.progress = TaskProgress(
        state_message=message,
        percent=percent,
        current_operation="process_wait",
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


def _parse_conditions(spec: str) -> list[_Condition]:
    parts = [part.strip() for part in spec.split(",") if part.strip()]
    if not parts:
        raise ValueError("process_name condition spec is empty")
    conditions: list[_Condition] = []
    for part in parts:
        if ":" not in part:
            raise ValueError(
                "each wait condition must use '<kind>:<target>' format; "
                "supported kinds: file, dir, port, http, process_exit"
            )
        kind, target = part.split(":", 1)
        kind = kind.strip()
        target = target.strip()
        if not target:
            raise ValueError(f"wait condition target is empty for kind '{kind}'")
        if kind not in {"file", "dir", "port", "http", "process_exit"}:
            raise ValueError(f"unsupported wait condition kind: {kind}")
        conditions.append(_Condition(kind=kind, target=target))
    return conditions


def _check_condition(condition: _Condition, success_pattern: str | None) -> bool:
    if condition.kind == "file":
        return Path(condition.target).expanduser().resolve().is_file()
    if condition.kind == "dir":
        return Path(condition.target).expanduser().resolve().is_dir()
    if condition.kind == "port":
        host, port_text = condition.target.rsplit(":", 1)
        port = int(port_text)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1.0)
            return sock.connect_ex((host, port)) == 0
    if condition.kind == "http":
        with urllib.request.urlopen(condition.target, timeout=2.0) as response:
            status = getattr(response, "status", 200)
            if status < 200 or status >= 400:
                return False
            if success_pattern:
                body = response.read(8192).decode("utf-8", errors="replace")
                return success_pattern in body
            return True
    if condition.kind == "process_exit":
        pid = int(condition.target)
        proc_entry = Path(f"/proc/{pid}")
        if proc_entry.exists():
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return not proc_entry.exists()
        return False
    raise ValueError(f"unsupported wait condition kind: {condition.kind}")


class WaitTaskExecutor:
    kind = TaskKind.process_wait

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        if task.kind is not TaskKind.process_wait:
            raise ValueError("WaitTaskExecutor only supports process_wait tasks")
        payload = task.payload
        if not isinstance(payload, ProcessWaitPayload):
            raise ValueError("process_wait payload must be ProcessWaitPayload")

        conditions = _parse_conditions(payload.process_name)

        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Waiting for readiness conditions", 0.0)
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Wait task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", f"Wait conditions: {payload.process_name}")
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.task_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="Wait task started",
                data={"conditions": payload.process_name},
            ),
        )

        deadline = time.monotonic() + payload.timeout_seconds
        poll_interval = payload.poll_interval_seconds
        total = len(conditions)

        try:
            while True:
                statuses: dict[str, bool] = {}
                for condition in conditions:
                    key = f"{condition.kind}:{condition.target}"
                    statuses[key] = _check_condition(condition, payload.success_pattern)

                ready_count = sum(1 for ready in statuses.values() if ready)
                all_ready = ready_count == total

                elapsed = payload.timeout_seconds - max(0.0, deadline - time.monotonic())
                percent = min(99.0, (elapsed / payload.timeout_seconds) * 100.0)
                if all_ready:
                    percent = 100.0

                _set_progress(
                    task,
                    f"Readiness {ready_count}/{total}",
                    percent,
                )
                save_task(paths, task)
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.task_progress,
                        job_id=job.id,
                        group_id=group.id,
                        task_id=task.id,
                        message="Wait task progress",
                        data={"ready_count": ready_count, "total": total, "statuses": statuses},
                    ),
                )

                if all_ready:
                    task.status = TaskStatus.complete
                    append_task_log(paths, task.id, "INFO", "All wait conditions satisfied")
                    append_event(
                        paths,
                        JobEvent(
                            event_type=JobEventType.task_completed,
                            job_id=job.id,
                            group_id=group.id,
                            task_id=task.id,
                            message="Wait task completed",
                            data={"statuses": statuses},
                        ),
                    )
                    break

                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"timeout after {payload.timeout_seconds}s waiting for {payload.process_name}"
                    )

                time.sleep(poll_interval)
        except Exception as exc:
            task.status = TaskStatus.failed
            task.error_message = str(exc)
            _set_progress(task, f"Wait task failed: {exc}", task.progress.percent if task.progress else 0.0)
            append_run_log(paths, "ERROR", f"Wait task failed: {exc}", task_id=task.id)
            append_task_log(paths, task.id, "ERROR", str(exc))
            append_event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_failed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Wait task failed",
                    data={"error": str(exc)},
                ),
            )
        finally:
            save_task(paths, task)

        return task
