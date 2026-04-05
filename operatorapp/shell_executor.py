"""Timeout-aware fallback executor for shell_command tasks."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from .config import OperatorConfig
from .events import JobEvent, JobEventType
from .llm_types import JSONLLMClient
from .logging_utils import append_llm_prompt, append_llm_response, append_run_log, append_task_log
from .planner_models import ShellStepAction, ShellStepResponse
from .prompts import build_shell_step_system_prompt, build_shell_step_user_prompt
from .schemas import CommandResult, TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task
from .task_payloads import ShellCommandPayload

MAX_ITERATIONS = 8


@dataclass
class _PromptTaskContext:
    id: str
    title: str
    kind: TaskKind
    payload: ShellCommandPayload
    workdir: str | None
    timeout_seconds: int | None
    history: list[dict[str, object]]


def run_bash(command: str, cwd: str | None = None, timeout_seconds: int = 600) -> CommandResult:
    """Execute one command via /bin/bash -lc and return structured CommandResult."""
    started_at = utc_now_z()
    started_monotonic = time.monotonic()
    stdout = ""
    stderr = ""
    exit_code = 1
    timed_out = False
    try:
        completed = subprocess.run(
            ["/bin/bash", "-lc", command],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        stdout = completed.stdout
        stderr = completed.stderr
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        exit_code = 124
        timed_out = True
    except Exception as exc:  # pragma: no cover - defensive path
        stderr = f"Operator command execution error: {type(exc).__name__}: {exc}"
        exit_code = 127

    finished_at = utc_now_z()
    duration_seconds = time.monotonic() - started_monotonic
    return CommandResult(
        command=command,
        cwd=cwd,
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        timed_out=timed_out,
        timeout_seconds=timeout_seconds,
    )


def _resolve_command_cwd(task: TaskRecord, payload: ShellCommandPayload) -> str | None:
    chosen = payload.cwd or task.workdir
    if chosen is None:
        return None
    path = Path(chosen)
    return str(path) if path.is_dir() else None


def _resolve_timeout_seconds(
    *,
    config: OperatorConfig,
    task: TaskRecord,
    payload: ShellCommandPayload,
    action: ShellStepAction,
) -> int:
    candidate = (
        action.timeout_seconds
        or payload.timeout_seconds
        or task.timeout_seconds
        or config.default_command_timeout_seconds
    )
    return min(candidate, config.max_command_timeout_seconds)


def _set_progress(task: TaskRecord, message: str, operation: str, percent: float | None = None) -> None:
    task.progress = TaskProgress(
        state_message=message,
        percent=percent,
        current_operation=operation,
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


class ShellTaskExecutor:
    def __init__(self, llm: JSONLLMClient, config: OperatorConfig) -> None:
        self._llm = llm
        self._config = config

    def execute(self, paths: RunPaths, task: TaskRecord) -> TaskRecord:
        """Execute one shell_command task with iterative planner-driven adaptation."""
        if task.kind is not TaskKind.shell_command:
            raise ValueError("ShellTaskExecutor only supports TaskKind.shell_command tasks")

        payload = task.payload
        if not isinstance(payload, ShellCommandPayload):
            raise ValueError("shell_command payload must be ShellCommandPayload")

        history: list[dict[str, object]] = []
        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Starting shell task", "start", 0.0)
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Shell task started: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", "Shell task started")

        for iteration in range(1, MAX_ITERATIONS + 1):
            _set_progress(
                task,
                f"Planning shell iteration {iteration}/{MAX_ITERATIONS}",
                "planning",
                min(95.0, (iteration - 1) * (95.0 / MAX_ITERATIONS)),
            )
            save_task(paths, task)
            append_task_log(paths, task.id, "INFO", f"Planning iteration {iteration}")

            context = _PromptTaskContext(
                id=task.id,
                title=task.title,
                kind=task.kind,
                payload=payload,
                workdir=task.workdir,
                timeout_seconds=task.timeout_seconds,
                history=history,
            )
            system_prompt = build_shell_step_system_prompt(
                default_timeout_seconds=self._config.default_command_timeout_seconds,
                max_timeout_seconds=self._config.max_command_timeout_seconds,
            )
            user_prompt = build_shell_step_user_prompt(context)  # type: ignore[arg-type]
            append_llm_prompt(
                paths,
                task.id,
                user_prompt,
                metadata={"iteration": iteration, "system_prompt": system_prompt},
            )
            step = self._llm.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=ShellStepResponse,
            )
            append_llm_response(
                paths,
                task.id,
                step.model_dump_json(),
                metadata={"iteration": iteration},
            )

            if step.status == "failed":
                reason = step.failure_reason or step.summary or "Planner reported failure."
                task.status = TaskStatus.failed
                task.error_message = reason
                _set_progress(task, f"Planner failed shell task: {reason}", "failed", 0.0)
                append_run_log(paths, "ERROR", f"Shell task failed: {reason}", task_id=task.id)
                append_task_log(paths, task.id, "ERROR", reason)
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.task_failed,
                        job_id=task.parent_job_id,
                        group_id=task.parent_group_id,
                        task_id=task.id,
                        message="Shell task failed by planner",
                        data={"reason": reason, "iteration": iteration},
                    ),
                )
                save_task(paths, task)
                return task

            if step.status == "complete" and not step.actions:
                task.status = TaskStatus.complete
                _set_progress(task, "Shell task complete", "complete", 100.0)
                append_run_log(paths, "INFO", "Shell task completed", task_id=task.id)
                append_task_log(paths, task.id, "INFO", "Shell task completed")
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.task_completed,
                        job_id=task.parent_job_id,
                        group_id=task.parent_group_id,
                        task_id=task.id,
                        message="Shell task completed",
                        data={"iteration": iteration},
                    ),
                )
                save_task(paths, task)
                return task

            all_actions_succeeded = True
            for action in step.actions:
                timeout_seconds = _resolve_timeout_seconds(
                    config=self._config, task=task, payload=payload, action=action
                )
                cwd = _resolve_command_cwd(task, payload)
                if (payload.cwd or task.workdir) and cwd is None:
                    append_task_log(
                        paths, task.id, "WARNING", f"cwd missing, running command without cwd"
                    )

                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.command_started,
                        job_id=task.parent_job_id,
                        group_id=task.parent_group_id,
                        task_id=task.id,
                        message="shell command started",
                        data={
                            "command": action.shell_command,
                            "timeout_seconds": timeout_seconds,
                            "iteration": iteration,
                        },
                    ),
                )
                append_task_log(
                    paths,
                    task.id,
                    "INFO",
                    f"Running command (timeout {timeout_seconds}s): {action.shell_command}",
                )

                result = run_bash(
                    command=action.shell_command,
                    cwd=cwd,
                    timeout_seconds=timeout_seconds,
                )
                history.append(
                    {
                        "timestamp": result.finished_at,
                        "sequential": step.sequential,
                        "command": result.command,
                        "cwd": result.cwd,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                        "exit_code": result.exit_code,
                        "timed_out": result.timed_out,
                        "timeout_seconds": result.timeout_seconds,
                    }
                )
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.command_completed,
                        job_id=task.parent_job_id,
                        group_id=task.parent_group_id,
                        task_id=task.id,
                        message="shell command completed",
                        data={
                            "command": result.command,
                            "exit_code": result.exit_code,
                            "timed_out": result.timed_out,
                            "timeout_seconds": result.timeout_seconds,
                            "duration_seconds": result.duration_seconds,
                            "iteration": iteration,
                        },
                    ),
                )

                if result.exit_code != 0:
                    all_actions_succeeded = False
                    append_task_log(
                        paths,
                        task.id,
                        "WARNING",
                        f"Command failed with exit code {result.exit_code}",
                    )

                if result.timed_out:
                    task.status = TaskStatus.failed
                    task.error_message = (
                        f"Command timed out after {result.timeout_seconds}s: {result.command}"
                    )
                    _set_progress(task, task.error_message, "failed", 0.0)
                    append_run_log(paths, "ERROR", task.error_message, task_id=task.id)
                    append_task_log(paths, task.id, "ERROR", task.error_message)
                    append_event(
                        paths,
                        JobEvent(
                            event_type=JobEventType.task_failed,
                            job_id=task.parent_job_id,
                            group_id=task.parent_group_id,
                            task_id=task.id,
                            message="Shell task failed due to timeout",
                            data={
                                "command": result.command,
                                "timeout_seconds": result.timeout_seconds,
                                "iteration": iteration,
                            },
                        ),
                    )
                    save_task(paths, task)
                    return task

                save_task(paths, task)

            if step.status == "complete" and all_actions_succeeded:
                task.status = TaskStatus.complete
                _set_progress(task, "Shell task complete", "complete", 100.0)
                append_run_log(paths, "INFO", "Shell task completed", task_id=task.id)
                append_task_log(paths, task.id, "INFO", "Shell task completed")
                append_event(
                    paths,
                    JobEvent(
                        event_type=JobEventType.task_completed,
                        job_id=task.parent_job_id,
                        group_id=task.parent_group_id,
                        task_id=task.id,
                        message="Shell task completed",
                        data={"iteration": iteration},
                    ),
                )
                save_task(paths, task)
                return task

        task.status = TaskStatus.failed
        task.error_message = (
            f"Shell task exceeded max iterations ({MAX_ITERATIONS}) without completion."
        )
        _set_progress(task, task.error_message, "failed", 0.0)
        append_run_log(paths, "ERROR", task.error_message, task_id=task.id)
        append_task_log(paths, task.id, "ERROR", task.error_message)
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.task_failed,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="Shell task failed at max iterations",
                data={"max_iterations": MAX_ITERATIONS},
            ),
        )
        save_task(paths, task)
        return task

