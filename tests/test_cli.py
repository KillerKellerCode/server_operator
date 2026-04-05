from __future__ import annotations

from types import SimpleNamespace

from operatorapp.cli import main
from operatorapp.schemas import JobRecord, JobStatus


def _job(status: JobStatus) -> JobRecord:
    return JobRecord(
        id="jobabcd1",
        user_prompt="hello world",
        summary="summary",
        status=status,
        task_groups=[],
    )


def _config() -> SimpleNamespace:
    return SimpleNamespace(
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


def test_cli_wiring_happy_path(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}
    fake_config = _config()
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: fake_config)

    class FakeOpenAIJSONClient:
        def __init__(self, model: str, api_key: str | None = None) -> None:
            calls.setdefault("models", []).append(model)
            calls.setdefault("api_keys", []).append(api_key)

    class FakePlannerService:
        def __init__(self, llm) -> None:
            calls["planner_llm"] = llm

    class FakeShellTaskExecutor:
        def __init__(self, llm, config) -> None:
            calls["shell_llm"] = llm
            calls["shell_config"] = config

    class FakeCodexTaskExecutor:
        def __init__(self, config) -> None:
            calls["codex_config"] = config

    class FakeOperatorApp:
        def __init__(self, planner, shell_executor, codex_executor, config) -> None:
            calls["planner"] = planner
            calls["shell_executor"] = shell_executor
            calls["codex_executor"] = codex_executor
            calls["app_config"] = config

        def run(self, user_prompt: str) -> JobRecord:
            calls["run_prompt"] = user_prompt
            return _job(JobStatus.complete)

    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", FakeOpenAIJSONClient)
    monkeypatch.setattr("operatorapp.cli.PlannerService", FakePlannerService)
    monkeypatch.setattr("operatorapp.cli.ShellTaskExecutor", FakeShellTaskExecutor)
    monkeypatch.setattr("operatorapp.cli.CodexTaskExecutor", FakeCodexTaskExecutor)
    monkeypatch.setattr("operatorapp.cli.OperatorApp", FakeOperatorApp)

    exit_code = main(["hello world"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert calls["models"] == ["planner-model", "shell-model"]
    assert calls["api_keys"] == ["test-key", "test-key"]
    assert calls["run_prompt"] == "hello world"
    assert "received prompt" in output
    assert "job jobabcd1 finished with status complete" in output


def test_cli_keyboard_interrupt_returns_130(monkeypatch, capsys) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", _config)
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", lambda model, api_key=None: object())
    monkeypatch.setattr("operatorapp.cli.PlannerService", lambda llm: object())
    monkeypatch.setattr("operatorapp.cli.ShellTaskExecutor", lambda llm, config: object())
    monkeypatch.setattr("operatorapp.cli.CodexTaskExecutor", lambda config: object())

    class FakeOperatorApp:
        def __init__(self, planner, shell_executor, codex_executor, config) -> None:
            pass

        def run(self, user_prompt: str) -> JobRecord:
            raise KeyboardInterrupt

    monkeypatch.setattr("operatorapp.cli.OperatorApp", FakeOperatorApp)

    exit_code = main(["hello world"])
    output = capsys.readouterr().out

    assert exit_code == 130
    assert "interrupted" in output.lower()


def test_cli_failed_job_returns_1(monkeypatch) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", _config)
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", lambda model, api_key=None: object())
    monkeypatch.setattr("operatorapp.cli.PlannerService", lambda llm: object())
    monkeypatch.setattr("operatorapp.cli.ShellTaskExecutor", lambda llm, config: object())
    monkeypatch.setattr("operatorapp.cli.CodexTaskExecutor", lambda config: object())

    class FakeOperatorApp:
        def __init__(self, planner, shell_executor, codex_executor, config) -> None:
            pass

        def run(self, user_prompt: str) -> JobRecord:
            return _job(JobStatus.failed)

    monkeypatch.setattr("operatorapp.cli.OperatorApp", FakeOperatorApp)
    assert main(["hello world"]) == 1


def test_cli_missing_api_key_returns_nonzero(monkeypatch) -> None:
    config = _config()
    config.openai_api_key = "   "
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: config)
    assert main(["hello world"]) == 2


def test_cli_config_failure_returns_nonzero(monkeypatch) -> None:
    def fail_load():
        raise RuntimeError("bad config")

    monkeypatch.setattr("operatorapp.cli.load_config", fail_load)
    assert main(["hello world"]) == 2


def test_cli_runtime_exception_returns_nonzero(monkeypatch) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", _config)
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", lambda model, api_key=None: object())
    monkeypatch.setattr("operatorapp.cli.PlannerService", lambda llm: object())
    monkeypatch.setattr("operatorapp.cli.ShellTaskExecutor", lambda llm, config: object())
    monkeypatch.setattr("operatorapp.cli.CodexTaskExecutor", lambda config: object())

    class FakeOperatorApp:
        def __init__(self, planner, shell_executor, codex_executor, config) -> None:
            pass

        def run(self, user_prompt: str) -> JobRecord:
            raise RuntimeError("boom")

    monkeypatch.setattr("operatorapp.cli.OperatorApp", FakeOperatorApp)
    assert main(["hello world"]) == 1

