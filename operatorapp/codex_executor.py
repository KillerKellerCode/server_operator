"""Executor for delegated codex_project and codex_modify tasks."""

from __future__ import annotations

import queue
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

from .config import OperatorConfig
from .events import JobEvent, JobEventType
from .logging_utils import append_llm_prompt, append_llm_response, append_run_log, append_task_log
from .prompts import build_codex_task_prompt
from .schemas import TaskKind, TaskProgress, TaskRecord, TaskStatus, utc_now_z
from .storage import RunPaths, append_event, save_task


def build_codex_exec_command(
    config: OperatorConfig, task: TaskRecord, response_output_path: str
) -> list[str]:
    """Build explicit argv for codex exec."""
    prompt = build_codex_task_prompt(task)
    argv: list[str] = [config.codex_command, "exec"]
    if task.workdir and Path(task.workdir).is_dir():
        argv.extend(["--cd", task.workdir])
    argv.extend(["--output-last-message", response_output_path])
    if config.codex_bypass_approvals:
        argv.append("--dangerously-bypass-approvals-and-sandbox")
    argv.append(prompt)
    return argv


def _set_progress(task: TaskRecord, message: str, percent: float | None, operation: str) -> None:
    task.progress = TaskProgress(
        state_message=message,
        percent=percent,
        current_operation=operation,
        updated_at=utc_now_z(),
    )
    task.updated_at = utc_now_z()


def _reader_thread(
    pipe, stream_name: str, output_queue: queue.Queue[tuple[str, str]]
) -> None:
    if pipe is None:
        return
    try:
        while True:
            line = pipe.readline()
            if line == "":
                break
            output_queue.put((stream_name, line.rstrip("\n")))
    finally:
        pipe.close()


def _drain_output_queue(paths: RunPaths, task: TaskRecord, output_queue: queue.Queue[tuple[str, str]]) -> None:
    while True:
        try:
            stream_name, line = output_queue.get_nowait()
        except queue.Empty:
            break
        level = "INFO" if stream_name == "stdout" else "ERROR"
        if stream_name == "stdout":
            print(line, flush=True)
        else:
            print(line, file=sys.stderr, flush=True)
        append_task_log(paths, task.id, level, f"[codex {stream_name}] {line}")
        append_run_log(paths, level, f"[codex {stream_name}] {line}", task_id=task.id)


class CodexTaskExecutor:
    def __init__(self, config: OperatorConfig) -> None:
        self._config = config

    def execute(self, paths: RunPaths, task: TaskRecord) -> TaskRecord:
        """Execute codex task as black-box delegated worker."""
        if task.kind not in (TaskKind.codex_project, TaskKind.codex_modify):
            raise ValueError("CodexTaskExecutor only supports codex_project and codex_modify tasks")

        task.status = TaskStatus.in_progress
        task.error_message = None
        _set_progress(task, "Starting Codex task", 0.0, "start")
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Starting codex task: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", "Codex task started")

        timeout_seconds = task.timeout_seconds or self._config.default_codex_timeout_seconds
        prompt = build_codex_task_prompt(task)
        append_llm_prompt(
            paths,
            task.id,
            prompt,
            metadata={"type": "codex_initial_prompt", "kind": task.kind.value, "timeout_seconds": timeout_seconds},
        )

        output_path = str((Path(paths.tasks_dir) / task.id / "codex_last_message.txt").resolve())
        argv = build_codex_exec_command(self._config, task, output_path)
        append_task_log(
            paths,
            task.id,
            "INFO",
            f"Launching Codex CLI: {' '.join(shlex.quote(part) for part in argv)}",
        )
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.codex_started,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="Codex task started",
                data={"argv": argv[:-1], "timeout_seconds": timeout_seconds, "kind": task.kind.value},
            ),
        )

        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        output_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        stdout_thread = threading.Thread(
            target=_reader_thread, args=(process.stdout, "stdout", output_queue), daemon=True
        )
        stderr_thread = threading.Thread(
            target=_reader_thread, args=(process.stderr, "stderr", output_queue), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()

        timed_out = False
        exit_code: int | None = None
        deadline = time.monotonic() + timeout_seconds
        while True:
            _drain_output_queue(paths, task, output_queue)
            exit_code = process.poll()
            if exit_code is not None:
                break
            if time.monotonic() >= deadline:
                timed_out = True
                process.kill()
                exit_code = 124
                append_task_log(paths, task.id, "ERROR", f"Codex timed out after {timeout_seconds}s")
                append_run_log(
                    paths,
                    "ERROR",
                    f"Codex task timed out after {timeout_seconds}s",
                    task_id=task.id,
                )
                break
            time.sleep(0.02)

        stdout_thread.join(timeout=1.0)
        stderr_thread.join(timeout=1.0)
        _drain_output_queue(paths, task, output_queue)
        if exit_code is None:
            exit_code = process.wait(timeout=1)

        output_file = Path(output_path)
        if output_file.exists():
            final_message = output_file.read_text(encoding="utf-8")
            append_llm_response(
                paths,
                task.id,
                final_message,
                metadata={"type": "codex_final_response", "kind": task.kind.value},
            )
        else:
            append_task_log(paths, task.id, "WARNING", "No final Codex response file found")
            append_run_log(paths, "WARNING", "No final Codex response file found", task_id=task.id)

        if timed_out:
            task.status = TaskStatus.failed
            task.error_message = f"Codex execution timed out after {timeout_seconds}s"
            _set_progress(task, task.error_message, 0.0, "failed")
        elif exit_code == 0:
            task.status = TaskStatus.complete
            _set_progress(task, "Codex task complete", 100.0, "complete")
        else:
            task.status = TaskStatus.failed
            task.error_message = f"Codex process exited with code {exit_code}"
            _set_progress(task, task.error_message, 0.0, "failed")

        append_task_log(paths, task.id, "INFO", f"Codex process exit code: {exit_code}")
        append_run_log(paths, "INFO", f"Codex process exit code: {exit_code}", task_id=task.id)
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.codex_completed,
                job_id=task.parent_job_id,
                group_id=task.parent_group_id,
                task_id=task.id,
                message="Codex task completed",
                data={
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "timeout_seconds": timeout_seconds,
                    "status": task.status.value,
                    "kind": task.kind.value,
                },
            ),
        )

        save_task(paths, task)
        return task

