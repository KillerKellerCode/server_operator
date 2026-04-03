from __future__ import annotations

from pathlib import Path

from operatorapp.codex_executor import CodexTaskExecutor, build_codex_exec_command
from operatorapp.config import OperatorConfig
from operatorapp.schemas import TaskKind, TaskRecord, TaskStatus, TestPolicy as TaskTestPolicy
from operatorapp.storage import create_run_paths, save_task


class FakePopen:
    def __init__(
        self,
        argv: list[str],
        *,
        stdout_lines: list[str],
        stderr_lines: list[str],
        returncode: int,
        final_message: str | None,
    ) -> None:
        self.argv = argv
        self.stdout = [f"{line}\n" for line in stdout_lines]
        self.stderr = [f"{line}\n" for line in stderr_lines]
        self._returncode = returncode

        if final_message is not None:
            output_path = self._extract_output_path(argv)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_message, encoding="utf-8")

    @staticmethod
    def _extract_output_path(argv: list[str]) -> str:
        index = argv.index("--output-last-message")
        return argv[index + 1]

    def wait(self) -> int:
        return self._returncode


def _make_config(*, bypass: bool = False) -> OperatorConfig:
    return OperatorConfig(
        operator_home="/tmp/operator-runs",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=bypass,
        command_timeout_seconds=600,
    )


def _make_task(*, workdir: str | None = None) -> TaskRecord:
    return TaskRecord(
        id="jobabcd1_t001",
        parent_job_id="jobabcd1",
        task_index=1,
        title="Implement parser",
        kind=TaskKind.codex,
        instructions="Implement parser module and tests.",
        workdir=workdir,
        test_policy=TaskTestPolicy.auto,
    )


def test_build_codex_exec_command_without_workdir() -> None:
    config = _make_config(bypass=False)
    task = _make_task(workdir=None)

    argv = build_codex_exec_command(config, task, "/tmp/out.txt")

    assert argv[0] == "codex"
    assert argv[1] == "exec"
    assert "--cd" not in argv
    assert "--output-last-message" in argv
    assert "/tmp/out.txt" in argv
    assert "--dangerously-bypass-approvals-and-sandbox" not in argv
    assert "Objective: Implement parser module and tests." in argv[-1]


def test_build_codex_exec_command_with_workdir_and_bypass() -> None:
    config = _make_config(bypass=True)
    task = _make_task(workdir="/tmp/project")

    argv = build_codex_exec_command(config, task, "/tmp/out.txt")

    assert argv[0] == "codex"
    assert argv[1] == "exec"
    assert "--cd" in argv
    assert "/tmp/project" in argv
    assert "--dangerously-bypass-approvals-and-sandbox" in argv


def test_execute_success_path_streams_and_logs(tmp_path: Path, monkeypatch, capsys) -> None:
    captured_argv: list[list[str]] = []

    def fake_popen(argv, **kwargs):
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
    task = _make_task(workdir=str(tmp_path))
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
    assert (Path(paths.job_dir) / "run.log").is_file()
    assert (task_dir / "task.log").is_file()


def test_execute_failure_path_marks_failed(tmp_path: Path, monkeypatch) -> None:
    def fake_popen(argv, **kwargs):
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
    task = _make_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths, task)

    assert updated.status is TaskStatus.failed


def test_execute_missing_final_response_file(tmp_path: Path, monkeypatch) -> None:
    def fake_popen(argv, **kwargs):
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
    task = _make_task(workdir=str(tmp_path))
    save_task(paths, task)

    updated = executor.execute(paths, task)

    assert updated.status is TaskStatus.complete
    response_log = Path(paths.tasks_dir) / task.id / "llm_responses.jsonl"
    assert not response_log.exists()

