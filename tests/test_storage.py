from __future__ import annotations

import json
from pathlib import Path

from operatorapp.events import JobEvent, JobEventType
from operatorapp.schemas import (
    JobRecord,
    TaskGroupRecord,
    TaskKind,
    TaskRecord,
    make_group_id,
    make_task_id,
    utc_now_z,
)
from operatorapp.storage import (
    append_event,
    create_run_paths,
    load_group,
    load_job,
    load_task,
    read_events,
    save_group,
    save_job,
    save_task,
)
from operatorapp.task_payloads import ShellCommandPayload


def _make_task(job_id: str, group_index: int, task_index: int) -> TaskRecord:
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title="Create directories",
        kind=TaskKind.shell_command,
        payload=ShellCommandPayload(command="mkdir -p data"),
        created_at=utc_now_z(),
        updated_at=utc_now_z(),
    )


def test_create_run_paths_creates_groups_tasks_and_events_paths(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path / "runs"), "jobabcd1")

    assert Path(paths.base_dir).is_absolute()
    assert Path(paths.job_dir).is_absolute()
    assert Path(paths.groups_dir).is_absolute()
    assert Path(paths.tasks_dir).is_absolute()
    assert Path(paths.events_jsonl_path).is_absolute()
    assert Path(paths.job_dir).is_dir()
    assert Path(paths.groups_dir).is_dir()
    assert Path(paths.tasks_dir).is_dir()
    assert Path(paths.events_jsonl_path).is_file()


def test_save_and_load_job_group_task_round_trip(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    now = utc_now_z()

    task = _make_task("jobabcd1", 0, 0)
    group = TaskGroupRecord(
        id=make_group_id("jobabcd1", 0),
        parent_job_id="jobabcd1",
        group_index=0,
        title="Bootstrap",
        tasks=[task],
        created_at=now,
        updated_at=now,
    )
    job = JobRecord(
        id="jobabcd1",
        user_prompt="run this",
        task_groups=[group],
        created_at=now,
        updated_at=now,
    )

    job_json_path = save_job(paths, job)
    group_json_path = save_group(paths, group)
    task_json_path = save_task(paths, task)

    loaded_job = load_job(job_json_path)
    loaded_group = load_group(group_json_path)
    loaded_task = load_task(task_json_path)

    assert loaded_job.model_dump(mode="json") == job.model_dump(mode="json")
    assert loaded_group.model_dump(mode="json") == group.model_dump(mode="json")
    assert loaded_task.model_dump(mode="json") == task.model_dump(mode="json")
    assert '\n  "id": "jobabcd1"' in Path(job_json_path).read_text(encoding="utf-8")
    assert '\n  "id": "jobabcd1_g000"' in Path(group_json_path).read_text(encoding="utf-8")
    assert '\n  "id": "jobabcd1_g000_t000"' in Path(task_json_path).read_text(encoding="utf-8")


def test_append_event_and_read_events_preserve_order_and_append_only(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    event_one = JobEvent(event_type=JobEventType.job_created, job_id="jobabcd1", message="created")
    event_two = JobEvent(
        event_type=JobEventType.task_started,
        job_id="jobabcd1",
        group_id="jobabcd1_g000",
        task_id="jobabcd1_g000_t000",
        message="task running",
        data={"attempt": 1},
    )

    line_one = append_event(paths, event_one)
    line_two = append_event(paths, event_two)
    events_path = Path(paths.events_jsonl_path)

    raw_lines = events_path.read_text(encoding="utf-8").splitlines()
    parsed_raw = [json.loads(line) for line in raw_lines]
    parsed = read_events(str(events_path))

    assert len(raw_lines) == 2
    assert json.loads(line_one) == parsed_raw[0]
    assert json.loads(line_two) == parsed_raw[1]
    assert [event.event_type for event in parsed] == [
        JobEventType.job_created,
        JobEventType.task_started,
    ]
    assert parsed[1].task_id == "jobabcd1_g000_t000"

