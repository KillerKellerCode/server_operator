"""Explicit human-readable and JSONL logging helpers for Operator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .events import JobEvent, JobEventType
from .schemas import utc_now_z
from .storage import RunPaths, append_event


def _job_id_from_paths(paths: RunPaths) -> str:
    return Path(paths.job_dir).name


def _append_text_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line + "\n")


def _append_jsonl(path: Path, record: dict[str, Any]) -> str:
    payload = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    _append_text_line(path, payload)
    return payload


def append_run_log(
    paths: RunPaths, level: str, message: str, task_id: str | None = None
) -> str:
    """Append one line to <job_dir>/run.log and return the appended line."""
    timestamp = utc_now_z()
    normalized_level = level.upper()
    resolved_task_id = task_id if task_id is not None else "-"
    line = (
        f"{timestamp} {normalized_level} "
        f"[job:{_job_id_from_paths(paths)} task:{resolved_task_id}] {message}"
    )
    log_path = Path(paths.job_dir).resolve() / "run.log"
    _append_text_line(log_path, line)
    return line


def append_task_log(paths: RunPaths, task_id: str, level: str, message: str) -> str:
    """Append one line to task log and return the appended line."""
    timestamp = utc_now_z()
    normalized_level = level.upper()
    line = f"{timestamp} {normalized_level} [task:{task_id}] {message}"
    log_path = Path(paths.tasks_dir).resolve() / task_id / "task.log"
    _append_text_line(log_path, line)
    return line


def append_llm_prompt(
    paths: RunPaths, task_id: str, content: str, metadata: dict[str, Any] | None = None
) -> str:
    """Append compact JSON line to llm_prompts.jsonl and return JSON line."""
    path = Path(paths.tasks_dir).resolve() / task_id / "llm_prompts.jsonl"
    record = {
        "timestamp": utc_now_z(),
        "task_id": task_id,
        "content": content,
        "metadata": metadata or {},
    }
    return _append_jsonl(path, record)


def append_llm_response(
    paths: RunPaths, task_id: str, content: str, metadata: dict[str, Any] | None = None
) -> str:
    """Append compact JSON line to llm_responses.jsonl and return JSON line."""
    path = Path(paths.tasks_dir).resolve() / task_id / "llm_responses.jsonl"
    record = {
        "timestamp": utc_now_z(),
        "task_id": task_id,
        "content": content,
        "metadata": metadata or {},
    }
    return _append_jsonl(path, record)


def append_structured_log_event(
    paths: RunPaths,
    level: str,
    message: str,
    job_id: str,
    group_id: str | None = None,
    task_id: str | None = None,
    data: dict[str, Any] | None = None,
) -> JobEvent:
    """Append a human log line plus a structured JobEvent and return the event."""
    normalized_level = level.upper()
    append_run_log(paths, normalized_level, message, task_id=task_id)
    if task_id is not None:
        append_task_log(paths, task_id, normalized_level, message)

    payload = dict(data or {})
    payload["level"] = normalized_level
    event = JobEvent(
        event_type=JobEventType.log_message,
        job_id=job_id,
        group_id=group_id,
        task_id=task_id,
        message=message,
        data=payload,
    )
    append_event(paths, event)
    return event
