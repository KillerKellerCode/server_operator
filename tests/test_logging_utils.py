from __future__ import annotations

import json
import re
from pathlib import Path

from operatorapp.events import JobEventType
from operatorapp.logging_utils import (
    append_llm_prompt,
    append_llm_response,
    append_run_log,
    append_structured_log_event,
    append_task_log,
)
from operatorapp.storage import create_run_paths, read_events


def test_append_run_log_format_and_repeated_appends(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    line_one = append_run_log(paths, "info", "started")
    line_two = append_run_log(
        paths, "warning", "step failed", task_id="jobabcd1_g000_t000"
    )

    run_log_path = Path(paths.job_dir) / "run.log"
    content = run_log_path.read_text(encoding="utf-8").splitlines()

    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z INFO \[job:jobabcd1 task:-\] started",
        line_one,
    )
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z WARNING "
        r"\[job:jobabcd1 task:jobabcd1_g000_t000\] step failed",
        line_two,
    )
    assert content == [line_one, line_two]


def test_append_task_log_format(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    line = append_task_log(
        paths, "jobabcd1_g000_t000", "debug", "created temp dir"
    )

    task_log_path = Path(paths.tasks_dir) / "jobabcd1_g000_t000" / "task.log"
    content = task_log_path.read_text(encoding="utf-8").splitlines()

    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z DEBUG "
        r"\[task:jobabcd1_g000_t000\] created temp dir",
        line,
    )
    assert content == [line]


def test_append_llm_prompt_and_response_jsonl_format_and_repeats(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task_id = "jobabcd1_g000_t001"

    prompt_line_one = append_llm_prompt(paths, task_id, "Write a parser.")
    prompt_line_two = append_llm_prompt(
        paths, task_id, "Add tests.", metadata={"attempt": 2}
    )
    response_line = append_llm_response(
        paths, task_id, "Implemented parser and tests.", metadata={"tokens": 123}
    )

    prompts_path = Path(paths.tasks_dir) / task_id / "llm_prompts.jsonl"
    responses_path = Path(paths.tasks_dir) / task_id / "llm_responses.jsonl"

    prompt_records = [
        json.loads(line)
        for line in prompts_path.read_text(encoding="utf-8").splitlines()
    ]
    response_records = [
        json.loads(line)
        for line in responses_path.read_text(encoding="utf-8").splitlines()
    ]

    assert json.loads(prompt_line_one)["metadata"] == {}
    assert json.loads(prompt_line_two)["metadata"] == {"attempt": 2}
    assert json.loads(response_line)["metadata"] == {"tokens": 123}

    assert len(prompt_records) == 2
    assert len(response_records) == 1
    assert set(prompt_records[0].keys()) == {
        "timestamp",
        "task_id",
        "content",
        "metadata",
    }
    assert set(response_records[0].keys()) == {
        "timestamp",
        "task_id",
        "content",
        "metadata",
    }
    assert prompt_records[0]["task_id"] == task_id
    assert response_records[0]["task_id"] == task_id


def test_append_structured_log_event_writes_logs_and_event(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")

    event = append_structured_log_event(
        paths=paths,
        level="info",
        message="task started",
        job_id="jobabcd1",
        group_id="jobabcd1_g000",
        task_id="jobabcd1_g000_t000",
        data={"phase": "execute"},
    )

    assert event.event_type is JobEventType.log_message
    assert event.job_id == "jobabcd1"
    assert event.group_id == "jobabcd1_g000"
    assert event.task_id == "jobabcd1_g000_t000"
    assert event.data["phase"] == "execute"
    assert event.data["level"] == "INFO"

    run_lines = (Path(paths.job_dir) / "run.log").read_text(encoding="utf-8").splitlines()
    task_lines = (
        Path(paths.tasks_dir) / "jobabcd1_g000_t000" / "task.log"
    ).read_text(encoding="utf-8").splitlines()
    events = read_events(paths.events_jsonl_path)

    assert len(run_lines) == 1
    assert len(task_lines) == 1
    assert len(events) == 1
    assert events[0].event_type is JobEventType.log_message
    assert events[0].message == "task started"

