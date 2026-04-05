from __future__ import annotations

import subprocess
import sys
from types import SimpleNamespace

from operatorapp.cli import main
from operatorapp.schemas import JobRecord, JobStatus
from operatorapp.version import __version__


def test_version_is_non_empty_string() -> None:
    assert isinstance(__version__, str)
    assert __version__


def test_main_prints_prompt_and_returns_zero(monkeypatch, capsys) -> None:
    fake_config = SimpleNamespace(
        operator_home="/tmp/runs",
        openai_api_key="test-key",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        default_command_timeout_seconds=600,
        max_command_timeout_seconds=7200,
        default_codex_timeout_seconds=7200,
        max_concurrent_tasks=4,
        max_concurrent_codex_tasks=1,
    )
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: fake_config)
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", lambda model, api_key=None: object())
    monkeypatch.setattr("operatorapp.cli.PlannerService", lambda llm: object())
    monkeypatch.setattr("operatorapp.cli.ShellTaskExecutor", lambda llm, config: object())
    monkeypatch.setattr("operatorapp.cli.CodexTaskExecutor", lambda config: object())

    class FakeOperatorApp:
        def __init__(self, planner, shell_executor, codex_executor, config) -> None:
            pass

        def run(self, user_prompt: str) -> JobRecord:
            return JobRecord(
                id="jobabcd1",
                user_prompt=user_prompt,
                status=JobStatus.complete,
                task_groups=[],
            )

    monkeypatch.setattr("operatorapp.cli.OperatorApp", FakeOperatorApp)

    exit_code = main(["hello"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "hello" in captured.out


def test_module_execution_works() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "operatorapp.cli", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
