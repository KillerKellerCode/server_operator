from __future__ import annotations

import time
from pathlib import Path

from operatorapp.codex_executor import CodexTaskExecutor, build_codex_exec_command
from operatorapp.config import OperatorConfig
from operatorapp.schemas import (
    TaskKind,
    TaskRecord,
    TaskStatus,
    TestPolicy as TaskTestPolicy,
    make_group_id,
    make_task_id,
)
from operatorapp.storage import create_run_paths, read_events, save_task
from operatorapp.task_payloads import CodexModifyPayload, CodexProjectPayload


class FakePipe:
    def __init__(self, lines: list[str]) -> None:
        self._lines = list(lines)
        self._index = 0

    def readline(self) -> str:
        if self._index >= len(self._lines):
            return ""
        line = self._lines[self._index]
        self._index += 1
        return line + "\n"

    def close(self) -> None:
        return


class FakePopen:
    def __init__(
        self,
        argv: list[str],
        *,
        stdout_lines: list[str],
        stderr_lines: list[str],
        returncode: int = 0,
        final_message: str | None = None,
        hang: bool = False,
    ) -> None:
        self.argv = argv
        self.stdout = FakePipe(stdout_lines)
        self.stderr = FakePipe(stderr_lines)
        self._returncode = returncode
        self._hang = hang
        self._killed = False

        if final_message is not None:
            output_path = self._extract_output_path(argv)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_message, encoding="utf-8")

    @staticmethod
    def _extract_output_path(argv: list[str]) -> str:
        index = argv.index("--output-last-message")
        return argv[index + 1]

    def poll(self) -> int | None:
        if self._hang and not self._killed:
            return None
        return self._returncode

    def kill(self) -> None:
        self._killed = True
        self._returncode = 124

    def wait(self, timeout: float | None = None) -> int:
        return self._returncode


def _make_config(*, bypass: bool = False) -> OperatorConfig:
    return OperatorConfig(
        operator_home="/tmp/operator-runs",
        openai_api_key="",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=bypass,
        default_command_timeout_seconds=600,
        max_command_timeout_seconds=7200,
        default_codex_timeout_seconds=7200,
        max_concurrent_tasks=4,
        max_concurrent_codex_tasks=1,
    )


def _make_codex_project_task(*, workdir: str | None = None) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, 0),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=0,
        title="Create project",
        kind=TaskKind.codex_project,
        workdir=workdir,
        test_policy=TaskTestPolicy.always,
        payload=CodexProjectPayload(
            objective="Scaffold and implement new parser project",
            workspace_root=workdir or "/tmp/project",
            test_command="pytest -q",
        ),
    )


def _make_codex_modify_task(*, workdir: str | None = None, timeout: int | None = None) -> TaskRecord:
    job_id = "jobabcd1"
    group_index = 0
    return TaskRecord(
        id=make_task_id(job_id, group_index, 1),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=1,
        title="Modify code",
        kind=TaskKind.codex_modify,
        workdir=workdir,
        timeout_seconds=timeout,
        test_policy=TaskTestPolicy.auto,
        payload=CodexModifyPayload(
            objective="Modify parser behavior in existing code",
            workspace_root=workdir or "/tmp/project",
            target_paths=["parser.py"],
            test_command="pytest tests/test_parser.py -q",
        ),
    )


def test_build_codex_exec_command_for_codex_project_with_workdir(tmp_path: Path) -> None:
    workdir = tmp_path / "project"
    workdir.mkdir()
    config = _make_config(bypass=False)
    task = _make_codex_project_task(workdir=str(workdir))

    argv = build_codex_exec_command(config, task, "/tmp/out.txt")

    assert argv[0] == "codex"
    assert argv[1] == "exec"
    assert "--cd" in argv
    assert str(workdir) in argv
    assert "--output-last-message" in argv
    assert "/tmp/out.txt" in argv
    assert "--dangerously-bypass-approvals-and-sandbox" not in argv
    assert "Task kind: codex_project" in argv[-1]
    assert "workspace setup end-to-end" in argv[-1]


def test_build_codex_exec_command_for_codex_modify_with_bypass(tmp_path: Path) -> None:
    workdir = tmp_path / "repo"
    workdir.mkdir()
    config = _make_config(bypass=True)
    task = _make_codex_modify_task(workdir=str(workdir))

    argv = build_codex_exec_command(config, task, "/tmp/out.txt")

    assert argv[0] == "codex"
    assert argv[1] == "exec"
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert "Task kind: codex_modify" in argv[-1]
    assert "Modify the existing project in place." in argv[-1]


def test_execute_success_path_streams_logs_and_emits_events(tmp_path: Path, monkeypatch, capsys) -> None:
    captured_argv: list[list[str]] = []

    def fake_popen(argv, **kwargs):  # type: ignore[no-untyped-def]
        captured_argv.append(list(argv))
        return FakePopen(
            list(argv),
            stdout_lines=["coding started", "coding done"],
            stderr_lines=["minor warning"],
            returncode=0,
            final_message="Finished implementation successfully.",
        )

    monkeypatch.setattr("operatorapp.codex_executor.subprocess.Popen", fake_popen)

    config = _make_config()
    executor = CodexTaskExecutor(config)
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_codex_project_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths, task)
    output = capsys.readouterr()

    assert updated.status is TaskStatus.complete
    assert "coding started" in output.out
    assert "coding done" in output.out
    assert "minor warning" in output.err
    assert len(captured_argv) == 1

    task_dir = Path(paths.tasks_dir) / task.id
    assert (task_dir / "llm_prompts.jsonl").is_file()
    assert (task_dir / "llm_responses.jsonl").is_file()
    events = read_events(paths.events_jsonl_path)
    assert events[0].event_type.value == "codex_started"
    assert events[-1].event_type.value == "codex_completed"
    assert events[-1].data["status"] == "complete"


def test_execute_failure_path_marks_failed(tmp_path: Path, monkeypatch) -> None:
    def fake_popen(argv, **kwargs):  # type: ignore[no-untyped-def]
        return FakePopen(
            list(argv),
            stdout_lines=[],
            stderr_lines=["fatal issue"],
            returncode=2,
            final_message="Could not complete changes.",
        )

    monkeypatch.setattr("operatorapp.codex_executor.subprocess.Popen", fake_popen)

    config = _make_config()
    executor = CodexTaskExecutor(config)
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_codex_modify_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths, task)

    assert updated.status is TaskStatus.failed
    assert "exited with code 2" in (updated.error_message or "")
    events = read_events(paths.events_jsonl_path)
    assert events[-1].data["status"] == "failed"


def test_execute_timeout_failure(tmp_path: Path, monkeypatch) -> None:
    def fake_popen(argv, **kwargs):  # type: ignore[no-untyped-def]
        return FakePopen(
            list(argv),
            stdout_lines=["still working"],
            stderr_lines=[],
            hang=True,
        )

    monkeypatch.setattr("operatorapp.codex_executor.subprocess.Popen", fake_popen)

    config = _make_config()
    executor = CodexTaskExecutor(config)
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_codex_modify_task(workdir=str(tmp_path), timeout=1)
    save_task(paths, task)

    started = time.monotonic()
    updated = executor.execute(paths, task)
    elapsed = time.monotonic() - started

    assert updated.status is TaskStatus.failed
    assert "timed out" in (updated.error_message or "")
    assert elapsed >= 1.0
    events = read_events(paths.events_jsonl_path)
    assert events[-1].data["timed_out"] is True
    assert events[-1].data["timeout_seconds"] == 1


def test_execute_missing_final_response_file(tmp_path: Path, monkeypatch) -> None:
    def fake_popen(argv, **kwargs):  # type: ignore[no-untyped-def]
        return FakePopen(
            list(argv),
            stdout_lines=["running"],
            stderr_lines=[],
            returncode=0,
            final_message=None,
        )

    monkeypatch.setattr("operatorapp.codex_executor.subprocess.Popen", fake_popen)

    config = _make_config()
    executor = CodexTaskExecutor(config)
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = _make_codex_modify_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths, task)

    assert updated.status is TaskStatus.complete
    response_log = Path(paths.tasks_dir) / task.id / "llm_responses.jsonl"
    assert not response_log.exists()

