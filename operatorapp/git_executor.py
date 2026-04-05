"""Typed executor for git_repo tasks."""

from __future__ import annotations

import subprocess
from pathlib import Path

from .events import JobEvent, JobEventType
from .logging_utils import append_run_log, append_task_log
from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import GitRepoPayload

_DEFAULT_TIMEOUT_SECONDS = 600


def _set_progress(task: TaskRecord, message: str, percent: float) -> None:
    task.progress = TaskProgress(
        state_message=message,
        percent=percent,
        current_operation="git_repo",
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


class GitTaskExecutor:
    kind = TaskKind.git_repo

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        if task.kind is not TaskKind.git_repo:
            raise ValueError("GitTaskExecutor only supports git_repo tasks")

        payload = task.payload
        if not isinstance(payload, GitRepoPayload):
            raise ValueError("git_repo payload must be GitRepoPayload")

        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Starting git operation", 0.0)
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Git task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", f"Git operation: {payload.operation}")
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.task_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="Git task started",
                data={"operation": payload.operation},
            ),
        )

        try:
            self._execute_operation(paths, job, group, task, payload)
            task.status = TaskStatus.complete
            _set_progress(task, "Git operation complete", 100.0)
            append_event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_completed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Git task completed",
                    data={"operation": payload.operation},
                ),
            )
        except Exception as exc:
            task.status = TaskStatus.failed
            task.error_message = str(exc)
            _set_progress(task, f"Git operation failed: {exc}", 0.0)
            append_run_log(paths, "ERROR", f"Git task failed: {exc}", task_id=task.id)
            append_task_log(paths, task.id, "ERROR", str(exc))
            append_event(
                paths,
                JobEvent(
                    event_type=JobEventType.task_failed,
                    job_id=job.id,
                    group_id=group.id,
                    task_id=task.id,
                    message="Git task failed",
                    data={"error": str(exc), "operation": payload.operation},
                ),
            )
        finally:
            save_task(paths, task)

        return task

    def _execute_operation(
        self,
        paths: RunPaths,
        job: JobRecord,
        group: TaskGroupRecord,
        task: TaskRecord,
        payload: GitRepoPayload,
    ) -> None:
        operation = str(payload.operation)
        repo_path = Path(payload.repo_path).expanduser().resolve()
        timeout_seconds = task.timeout_seconds or _DEFAULT_TIMEOUT_SECONDS

        if operation == "clone":
            if not payload.repo_url:
                raise ValueError("repo_url is required for clone")
            repo_path.parent.mkdir(parents=True, exist_ok=True)
            self._run_git(
                paths,
                job,
                group,
                task,
                ["git", "clone", payload.repo_url, str(repo_path)],
                timeout_seconds,
            )
            if payload.ref:
                self._run_git(
                    paths,
                    job,
                    group,
                    task,
                    ["git", "-C", str(repo_path), "checkout", payload.ref],
                    timeout_seconds,
                )
            return

        if operation == "init":
            repo_path.mkdir(parents=True, exist_ok=True)
            self._run_git(
                paths,
                job,
                group,
                task,
                ["git", "init", str(repo_path)],
                timeout_seconds,
            )
            return

        if operation == "fetch":
            self._run_git(
                paths,
                job,
                group,
                task,
                ["git", "-C", str(repo_path), "fetch"],
                timeout_seconds,
            )
            return

        if operation == "checkout":
            if not payload.ref:
                raise ValueError("ref is required for checkout")
            self._run_git(
                paths,
                job,
                group,
                task,
                ["git", "-C", str(repo_path), "checkout", payload.ref],
                timeout_seconds,
            )
            return

        if operation == "pull":
            command = ["git", "-C", str(repo_path), "pull"]
            if payload.ref:
                command.extend(["origin", payload.ref])
            self._run_git(paths, job, group, task, command, timeout_seconds)
            return

        if operation == "commit":
            if not payload.message:
                raise ValueError("message is required for commit")
            self._run_git(
                paths,
                job,
                group,
                task,
                ["git", "-C", str(repo_path), "commit", "-m", payload.message],
                timeout_seconds,
            )
            return

        raise ValueError(f"unsupported git operation: {operation}")

    def _run_git(
        self,
        paths: RunPaths,
        job: JobRecord,
        group: TaskGroupRecord,
        task: TaskRecord,
        command: list[str],
        timeout_seconds: int,
    ) -> None:
        append_task_log(paths, task.id, "INFO", f"Running: {' '.join(command)}")
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.command_started,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="git command started",
                data={"command": command},
            ),
        )
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        if result.stdout:
            append_task_log(paths, task.id, "INFO", f"stdout:\n{result.stdout.rstrip()}")
        if result.stderr:
            append_task_log(paths, task.id, "WARNING", f"stderr:\n{result.stderr.rstrip()}")

        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.command_completed,
                job_id=job.id,
                group_id=group.id,
                task_id=task.id,
                message="git command completed",
                data={
                    "command": command,
                    "exit_code": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            ),
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"git command failed ({result.returncode}): {' '.join(command)}; "
                f"stderr={result.stderr.strip()}"
            )

