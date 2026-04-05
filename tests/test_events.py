from __future__ import annotations

import re

import pytest
from pydantic import ValidationError

from operatorapp.events import JobEvent, JobEventType


def test_job_event_type_contains_required_values() -> None:
    expected = {
        "job_created",
        "job_status_changed",
        "group_ready",
        "group_started",
        "group_completed",
        "group_failed",
        "task_ready",
        "task_started",
        "task_progress",
        "task_completed",
        "task_failed",
        "task_cancelled",
        "command_started",
        "command_completed",
        "codex_started",
        "codex_completed",
        "log_message",
    }
    assert {member.value for member in JobEventType} == expected


def test_job_event_defaults_and_round_trip() -> None:
    event = JobEvent(
        event_type=JobEventType.task_progress,
        job_id="jobabcd1",
        group_id="jobabcd1_g000",
        task_id="jobabcd1_g000_t000",
        message="Download progress update",
        data={"percent": 56.2},
    )
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", event.timestamp)
    payload = event.model_dump_json()
    restored = JobEvent.model_validate_json(payload)
    assert restored == event


def test_job_event_rejects_invalid_timestamp() -> None:
    with pytest.raises(ValidationError):
        JobEvent(
            timestamp="2026-04-04 10:00:00",
            event_type=JobEventType.job_created,
            job_id="jobabcd1",
        )

