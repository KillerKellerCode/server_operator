"""Executor for non-codex shell tasks."""

from __future__ import annotations

from pathlib import Path
import subprocess
import time

from .config import OperatorConfig
from .llm_types import JSONLLMClient
from .logging_utils import (
    append_llm_prompt,
    append_llm_response,
    append_run_log,
    append_task_log,
)
from .planner_models import ShellStepResponse
from .prompts import build_shell_step_system_prompt, build_shell_step_user_prompt
from .schemas import ActionRecord, CommandResult, TaskExchange, TaskKind, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, save_task

MAX_ITERATIONS = 8


def run_bash(command: str, cwd: str | None = None, timeout_seconds: int = 600) -> CommandResult:
    """Execute one shell command through /bin/bash -lc and return CommandResult."""
    started_at = utc_now_z()
    started_monotonic = time.monotonic()
    stdout = ""
    stderr = ""
    exit_code = 1
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
    except Exception as exc:
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
    )


def append_command_to_history(task: TaskRecord, result: CommandResult) -> TaskRecord:
    """Append one command execution result to task history."""
    exchange = TaskExchange(
        timestamp=result.finished_at,
        sequential=True,
        actions=[
            ActionRecord(
                action_type="shell_command",
                shell_command=result.command,
                output=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
            )
        ],
    )
    task.history.append(exchange)
    return task


def _resolve_command_cwd(workdir: str | None) -> str | None:
    """Return a safe cwd for command execution, falling back when missing."""
    if workdir is None:
        return None
    if Path(workdir).is_dir():
        return workdir
    return None


class ShellTaskExecutor:
    def __init__(self, llm: JSONLLMClient, config: OperatorConfig) -> None:
        self._llm = llm
        self._config = config

    def execute(self, paths: RunPaths, task: TaskRecord) -> TaskRecord:
        """Execute one shell task with iterative model-guided command steps."""
        if task.kind is not TaskKind.shell:
            raise ValueError("ShellTaskExecutor only supports TaskKind.shell tasks")

        task.status = TaskStatus.in_progress
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Starting shell task: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", "Task started")

        for iteration in range(1, MAX_ITERATIONS + 1):
            save_task(paths, task)
            append_run_log(paths, "INFO", f"Planning shell iteration {iteration}", task_id=task.id)
            append_task_log(paths, task.id, "INFO", f"Planning iteration {iteration}")

            system_prompt = build_shell_step_system_prompt()
            user_prompt = build_shell_step_user_prompt(task)
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
            append_task_log(paths, task.id, "INFO", f"Planner status: {step.status}")

            if step.status == "complete" and not step.actions:
                task.status = TaskStatus.complete
                append_run_log(paths, "INFO", "Shell task completed", task_id=task.id)
                append_task_log(paths, task.id, "INFO", "Task completed")
                save_task(paths, task)
                return task

            if step.status == "failed":
                task.status = TaskStatus.failed
                reason = step.failure_reason or "No failure reason provided by planner."
                append_run_log(paths, "ERROR", f"Shell task failed: {reason}", task_id=task.id)
                append_task_log(paths, task.id, "ERROR", f"Task failed: {reason}")
                save_task(paths, task)
                return task

            all_actions_succeeded = True
            for action in step.actions:
                command_cwd = _resolve_command_cwd(task.workdir)
                if task.workdir and command_cwd is None:
                    append_task_log(
                        paths,
                        task.id,
                        "WARNING",
                        f"Task workdir missing, running without cwd: {task.workdir}",
                    )
                append_task_log(
                    paths,
                    task.id,
                    "INFO",
                    f"Running command: {action.shell_command}",
                )
                result = run_bash(
                    command=action.shell_command,
                    cwd=command_cwd,
                    timeout_seconds=self._config.command_timeout_seconds,
                )
                append_command_to_history(task, result)
                task.history[-1].sequential = step.sequential
                if result.exit_code != 0:
                    all_actions_succeeded = False
                append_task_log(
                    paths,
                    task.id,
                    "INFO",
                    f"Command exit code: {result.exit_code}",
                )
                append_run_log(
                    paths,
                    "INFO",
                    f"Executed command with exit code {result.exit_code}",
                    task_id=task.id,
                )

            save_task(paths, task)
            if step.status == "complete":
                if all_actions_succeeded:
                    task.status = TaskStatus.complete
                    append_run_log(paths, "INFO", "Shell task completed", task_id=task.id)
                    append_task_log(paths, task.id, "INFO", "Task completed")
                    save_task(paths, task)
                    return task
                append_task_log(
                    paths,
                    task.id,
                    "WARNING",
                    "Planner marked complete but command failures were observed; continuing.",
                )
                append_run_log(
                    paths,
                    "WARNING",
                    "Planner marked complete but command failures were observed; continuing.",
                    task_id=task.id,
                )

        task.status = TaskStatus.failed
        append_run_log(
            paths,
            "ERROR",
            f"Shell task exceeded max iterations ({MAX_ITERATIONS})",
            task_id=task.id,
        )
        append_task_log(paths, task.id, "ERROR", "Task failed: max iterations reached")
        save_task(paths, task)
        return task
