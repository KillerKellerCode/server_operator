from __future__ import annotations

import json
from pathlib import Path
import subprocess

from operatorapp.cli import main
from operatorapp.config import OperatorConfig
from operatorapp.planner_models import JobPlanResponse, PlannedTaskInput, ShellActionPlan, ShellStepResponse
from operatorapp.schemas import JobStatus, TaskStatus
from operatorapp.storage import load_job


class _FakeOpenAIJSONClient:
    planner_response: JobPlanResponse | None = None
    shell_responses: list[ShellStepResponse] = []

    def __init__(self, model: str, api_key: str | None = None) -> None:
        self.model = model

    def complete_json(self, *, system_prompt: str, user_prompt: str, response_model: type):
        if response_model.__name__ == "JobPlanResponse":
            if self.planner_response is None:
                raise AssertionError("planner_response not configured")
            return self.planner_response
        if response_model.__name__ == "ShellStepResponse":
            if not self.shell_responses:
                raise AssertionError("shell_responses exhausted")
            return self.shell_responses.pop(0)
        raise AssertionError(f"Unexpected response model: {response_model}")


class _FakeCodexPopen:
    def __init__(
        self,
        argv: list[str],
        *,
        stdout_lines: list[str],
        stderr_lines: list[str],
        returncode: int,
        final_message: str | None,
    ) -> None:
        self.stdout = [f"{line}\n" for line in stdout_lines]
        self.stderr = [f"{line}\n" for line in stderr_lines]
        self._returncode = returncode
        if final_message is not None:
            index = argv.index("--output-last-message")
            output_path = Path(argv[index + 1])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(final_message, encoding="utf-8")

    def wait(self) -> int:
        return self._returncode


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_e2e_pure_shell_plan_job(tmp_path: Path, monkeypatch, capsys) -> None:
    config = OperatorConfig(
        operator_home=str(tmp_path / "runs"),
        openai_api_key="test-key",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        command_timeout_seconds=30,
    )
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: config)
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)

    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="Prepare environment in two shell steps.",
        tasks=[
            PlannedTaskInput(
                title="Create data dir",
                kind="shell",
                instructions="Create a data directory.",
                workdir=str(tmp_path),
            ),
            PlannedTaskInput(
                title="Write marker file",
                kind="shell",
                instructions="Write a marker file.",
                workdir=str(tmp_path),
            ),
        ],
    )
    _FakeOpenAIJSONClient.shell_responses = [
        ShellStepResponse(
            status="in_progress",
            summary="Create directory first.",
            sequential=True,
            actions=[
                ShellActionPlan(
                    shell_command="mkdir -p data",
                    reason="Ensure destination directory exists.",
                )
            ],
            completion_check="Directory exists.",
        ),
        ShellStepResponse(
            status="complete",
            summary="First shell task complete.",
            sequential=True,
            actions=[],
            completion_check="No more commands needed.",
        ),
        ShellStepResponse(
            status="in_progress",
            summary="Write marker file.",
            sequential=True,
            actions=[
                ShellActionPlan(
                    shell_command="echo ready > data/marker.txt",
                    reason="Persist marker for next steps.",
                )
            ],
            completion_check="Marker file exists.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Second shell task complete.",
            sequential=True,
            actions=[],
            completion_check="No more commands needed.",
        ),
    ]

    exit_code = main(["prepare workspace"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Operator: running job..." in output

    run_dirs = [path for path in (tmp_path / "runs").iterdir() if path.is_dir()]
    assert len(run_dirs) == 1
    job_dir = run_dirs[0]
    job = load_job(str(job_dir / "job.json"))
    assert job.status is JobStatus.complete
    assert len(job.tasks) == 2
    assert all(task.status is TaskStatus.complete for task in job.tasks)

    run_log = (job_dir / "run.log").read_text(encoding="utf-8")
    assert run_log.index("Starting task 0: Create data dir") < run_log.index(
        "Starting task 1: Write marker file"
    )

    for task in job.tasks:
        task_dir = job_dir / "tasks" / task.id
        assert (task_dir / "task.log").is_file()
        assert (task_dir / "llm_prompts.jsonl").is_file()
        assert (task_dir / "llm_responses.jsonl").is_file()
        assert len(_read_jsonl(task_dir / "llm_prompts.jsonl")) >= 1
        assert len(_read_jsonl(task_dir / "llm_responses.jsonl")) >= 1


def test_e2e_mixed_shell_and_codex_job(tmp_path: Path, monkeypatch, capsys) -> None:
    config = OperatorConfig(
        operator_home=str(tmp_path / "runs"),
        openai_api_key="test-key",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        command_timeout_seconds=30,
    )
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: config)
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)

    real_popen = subprocess.Popen

    def fake_popen(argv, **kwargs):
        if list(argv)[:2] != ["codex", "exec"]:
            return real_popen(argv, **kwargs)
        return _FakeCodexPopen(
            list(argv),
            stdout_lines=["codex started", "codex finished"],
            stderr_lines=["codex note"],
            returncode=0,
            final_message="Implemented the requested code updates.",
        )

    monkeypatch.setattr("operatorapp.codex_executor.subprocess.Popen", fake_popen)

    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="Prepare files then delegate coding to Codex.",
        tasks=[
            PlannedTaskInput(
                title="Create src dir",
                kind="shell",
                instructions="Create source directory.",
                workdir=str(tmp_path),
            ),
            PlannedTaskInput(
                title="Implement module",
                kind="codex",
                instructions="Implement parser module.",
                workdir=str(tmp_path),
                test_policy="always",
            ),
        ],
    )
    _FakeOpenAIJSONClient.shell_responses = [
        ShellStepResponse(
            status="in_progress",
            summary="Create source directory.",
            sequential=True,
            actions=[
                ShellActionPlan(
                    shell_command="mkdir -p src",
                    reason="Prepare coding workspace.",
                )
            ],
            completion_check="src exists.",
        ),
        ShellStepResponse(
            status="complete",
            summary="Shell preparation complete.",
            sequential=True,
            actions=[],
            completion_check="No more shell commands needed.",
        ),
    ]

    exit_code = main(["build parser feature"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Operator: running job..." in captured.out
    assert "codex started" in captured.out
    assert "codex note" in captured.err

    run_dirs = [path for path in (tmp_path / "runs").iterdir() if path.is_dir()]
    assert len(run_dirs) == 1
    job_dir = run_dirs[0]
    job = load_job(str(job_dir / "job.json"))
    assert job.status is JobStatus.complete
    assert [task.status for task in job.tasks] == [TaskStatus.complete, TaskStatus.complete]

    run_log = (job_dir / "run.log").read_text(encoding="utf-8")
    assert run_log.index("Starting task 0: Create src dir") < run_log.index(
        "Starting task 1: Implement module"
    )

    shell_task_dir = job_dir / "tasks" / job.tasks[0].id
    codex_task_dir = job_dir / "tasks" / job.tasks[1].id
    assert (shell_task_dir / "task.log").is_file()
    assert (codex_task_dir / "task.log").is_file()
    assert (shell_task_dir / "llm_prompts.jsonl").is_file()
    assert (shell_task_dir / "llm_responses.jsonl").is_file()
    assert (codex_task_dir / "llm_prompts.jsonl").is_file()
    assert (codex_task_dir / "llm_responses.jsonl").is_file()
    assert len(_read_jsonl(codex_task_dir / "llm_responses.jsonl")) == 1
