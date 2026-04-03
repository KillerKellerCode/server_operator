from __future__ import annotations

import re
from datetime import datetime

import pytest
from pydantic import ValidationError

from operatorapp.schemas import (
    ActionRecord,
    CommandResult,
    JobRecord,
    JobStatus,
    TaskExchange,
    TaskKind,
    TaskRecord,
    TaskStatus,
    TestPolicy as TaskTestPolicy,
    make_job_id,
    make_task_id,
    utc_now_z,
)


def test_utc_now_z_format() -> None:
    value = utc_now_z()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value)
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    assert parsed.year >= 2024


def test_make_job_id_format() -> None:
    job_id = make_job_id()
    assert re.fullmatch(r"[0-9a-f]{8}", job_id)


def test_make_task_id_is_stable_and_validates() -> None:
    assert make_task_id("jobabcd1", 4) == "jobabcd1_t004"
    assert make_task_id("jobabcd1", 4) == "jobabcd1_t004"
    with pytest.raises(ValueError):
        make_task_id("", 0)
    with pytest.raises(ValueError):
        make_task_id("jobabcd1", -1)


def test_task_record_defaults() -> None:
    task = TaskRecord(
        id=make_task_id("jobabcd1", 0),
        parent_job_id="jobabcd1",
        task_index=0,
        title="Prepare workspace",
        kind=TaskKind.shell,
        instructions="Create local directory.",
    )

    assert task.workdir is None
    assert task.test_policy is TaskTestPolicy.auto
    assert task.status is TaskStatus.pending
    assert task.history == []


def test_json_round_trip_with_shell_and_codex_tasks() -> None:
    exchange = TaskExchange(
        timestamp=utc_now_z(),
        sequential=True,
        actions=[
            ActionRecord(
                action_type="shell_command",
                shell_command="mkdir -p data",
                output="",
                stderr="",
                exit_code=0,
            )
        ],
    )

    shell_task = TaskRecord(
        id=make_task_id("jobabcd1", 0),
        parent_job_id="jobabcd1",
        task_index=0,
        title="Initialize workspace",
        kind=TaskKind.shell,
        instructions="Create project directories.",
        history=[exchange],
    )
    codex_task = TaskRecord(
        id=make_task_id("jobabcd1", 1),
        parent_job_id="jobabcd1",
        task_index=1,
        title="Implement parser",
        kind=TaskKind.codex,
        instructions="Write parser module and tests.",
    )

    now = utc_now_z()
    job = JobRecord(
        id="jobabcd1",
        user_prompt="Build initial parser",
        summary="",
        status=JobStatus.in_progress,
        tasks=[shell_task, codex_task],
        created_at=now,
        updated_at=now,
    )

    payload = job.model_dump_json()
    restored = JobRecord.model_validate_json(payload)

    assert restored.id == "jobabcd1"
    assert len(restored.tasks) == 2
    assert restored.tasks[0].kind is TaskKind.shell
    assert restored.tasks[1].kind is TaskKind.codex
    assert restored.tasks[0].history[0].actions[0].action_type == "shell_command"


def test_command_result_round_trip() -> None:
    started = utc_now_z()
    finished = started
    result = CommandResult(
        command="python -m pytest",
        cwd="/tmp/project",
        stdout="ok",
        stderr="",
        exit_code=0,
        started_at=started,
        finished_at=finished,
        duration_seconds=0.0,
    )

    payload = result.model_dump_json()
    restored = CommandResult.model_validate_json(payload)
    assert restored.command == "python -m pytest"
    assert restored.exit_code == 0


def test_validation_rejects_invalid_timestamp() -> None:
    with pytest.raises(ValidationError):
        TaskExchange(timestamp="2026-04-02 18:22:10", sequential=True, actions=[])
