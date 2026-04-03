"""Executor for delegated codex tasks."""

from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

from .config import OperatorConfig
from .logging_utils import append_llm_prompt, append_llm_response, append_run_log, append_task_log
from .prompts import build_codex_task_prompt
from .schemas import TaskKind, TaskRecord, TaskStatus
from .storage import RunPaths, save_task


def build_codex_exec_command(
    config: OperatorConfig, task: TaskRecord, response_output_path: str
) -> list[str]:
    """Build argv for executing a codex task."""
    prompt = build_codex_task_prompt(task)
    argv = [config.codex_command, "exec"]
    if task.workdir:
        argv.extend(["--cd", task.workdir])
    argv.extend(["--output-last-message", response_output_path])
    if config.codex_bypass_approvals:
        argv.append("--dangerously-bypass-approvals-and-sandbox")
    argv.append(prompt)
    return argv


def _stream_pipe(
    pipe, *, stream_name: str, paths: RunPaths, task: TaskRecord
) -> None:
    """Stream one process pipe line by line to console and logs."""
    if pipe is None:
        return

    level = "INFO" if stream_name == "stdout" else "ERROR"
    for raw_line in pipe:
        line = raw_line.rstrip("\n")
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
        """Execute one codex task by delegating to Codex CLI."""
        if task.kind is not TaskKind.codex:
            raise ValueError("CodexTaskExecutor only supports TaskKind.codex tasks")

        task.status = TaskStatus.in_progress
        save_task(paths, task)
        append_run_log(paths, "INFO", f"Starting codex task: {task.title}", task_id=task.id)
        append_task_log(paths, task.id, "INFO", "Task started")

        prompt = build_codex_task_prompt(task)
        append_llm_prompt(paths, task.id, prompt, metadata={"type": "codex_initial_prompt"})

        output_path = str((Path(paths.tasks_dir) / task.id / "codex_last_message.txt").resolve())
        argv = build_codex_exec_command(self._config, task, output_path)
        append_task_log(
            paths,
            task.id,
            "INFO",
            f"Launching Codex CLI: {' '.join(shlex.quote(part) for part in argv)}",
        )

        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        _stream_pipe(process.stdout, stream_name="stdout", paths=paths, task=task)
        _stream_pipe(process.stderr, stream_name="stderr", paths=paths, task=task)
        exit_code = process.wait()

        output_file = Path(output_path)
        if output_file.exists():
            final_message = output_file.read_text(encoding="utf-8")
            append_llm_response(
                paths,
                task.id,
                final_message,
                metadata={"type": "codex_final_response"},
            )
        else:
            append_task_log(paths, task.id, "INFO", "No final Codex response file found")
            append_run_log(paths, "INFO", "No final Codex response file found", task_id=task.id)

        task.status = TaskStatus.complete if exit_code == 0 else TaskStatus.failed
        append_task_log(paths, task.id, "INFO", f"Codex task exited with status code {exit_code}")
        append_run_log(paths, "INFO", f"Codex process exit code: {exit_code}", task_id=task.id)
        save_task(paths, task)
        return task

