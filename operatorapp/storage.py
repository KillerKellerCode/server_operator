"""Run storage layout utilities for Operator."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from .events import JobEvent
from .schemas import JobRecord, TaskGroupRecord, TaskRecord


class RunPaths(BaseModel):
    base_dir: str
    job_dir: str
    groups_dir: str
    tasks_dir: str
    events_jsonl_path: str

    model_config = ConfigDict(extra="forbid")


def _write_pretty_json(path: Path, data: dict) -> None:
    """Write JSON to disk atomically with stable formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    payload = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    temp_path.write_text(payload, encoding="utf-8")
    temp_path.replace(path)


def _append_jsonl(path: Path, data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(payload + "\n")
    return payload


def create_run_paths(base_dir: str, job_id: str) -> RunPaths:
    """Create and return run directory paths."""
    base_path = Path(base_dir).resolve()
    job_path = (base_path / job_id).resolve()
    groups_path = (job_path / "groups").resolve()
    tasks_path = (job_path / "tasks").resolve()
    events_path = (job_path / "events.jsonl").resolve()

    groups_path.mkdir(parents=True, exist_ok=True)
    tasks_path.mkdir(parents=True, exist_ok=True)
    events_path.touch(exist_ok=True)

    return RunPaths(
        base_dir=str(base_path),
        job_dir=str(job_path),
        groups_dir=str(groups_path),
        tasks_dir=str(tasks_path),
        events_jsonl_path=str(events_path),
    )


def save_job(paths: RunPaths, job: JobRecord) -> str:
    """Save JobRecord to <job_dir>/job.json and return file path."""
    job_path = Path(paths.job_dir).resolve() / "job.json"
    _write_pretty_json(job_path, job.model_dump(mode="json"))
    return str(job_path)


def load_job(job_json_path: str) -> JobRecord:
    """Load JobRecord from JSON file path."""
    path = Path(job_json_path).resolve()
    data = json.loads(path.read_text(encoding="utf-8"))
    return JobRecord.model_validate(data)


def save_group(paths: RunPaths, group: TaskGroupRecord) -> str:
    """Save TaskGroupRecord to <job_dir>/groups/<group_id>/group.json and return file path."""
    group_dir = Path(paths.groups_dir).resolve() / group.id
    group_path = group_dir / "group.json"
    _write_pretty_json(group_path, group.model_dump(mode="json"))
    return str(group_path)


def load_group(group_json_path: str) -> TaskGroupRecord:
    """Load TaskGroupRecord from JSON file path."""
    path = Path(group_json_path).resolve()
    data = json.loads(path.read_text(encoding="utf-8"))
    return TaskGroupRecord.model_validate(data)


def save_task(paths: RunPaths, task: TaskRecord) -> str:
    """Save TaskRecord to <job_dir>/tasks/<task_id>/task.json and return file path."""
    task_dir = Path(paths.tasks_dir).resolve() / task.id
    task_path = task_dir / "task.json"
    _write_pretty_json(task_path, task.model_dump(mode="json"))
    return str(task_path)


def load_task(task_json_path: str) -> TaskRecord:
    """Load TaskRecord from JSON file path."""
    path = Path(task_json_path).resolve()
    data = json.loads(path.read_text(encoding="utf-8"))
    return TaskRecord.model_validate(data)


def append_event(paths: RunPaths, event: JobEvent) -> str:
    """Append one compact JSON event line to <job_dir>/events.jsonl."""
    events_path = Path(paths.events_jsonl_path).resolve()
    return _append_jsonl(events_path, event.model_dump(mode="json"))


def read_events(events_jsonl_path: str) -> list[JobEvent]:
    """Read events JSONL in order and return parsed JobEvent records."""
    path = Path(events_jsonl_path).resolve()
    if not path.exists():
        return []

    events: list[JobEvent] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        events.append(JobEvent.model_validate_json(stripped))
    return events
