from __future__ import annotations

import json
import re
import time
from pathlib import Path

from operatorapp.cli import main
from operatorapp.config import OperatorConfig
from operatorapp.planner_models import JobPlanResponse, PlannedTaskGroupInput, PlannedTaskInput, ShellStepAction, ShellStepResponse
from operatorapp.schemas import CommandResult, JobStatus, TaskGroupStatus, TaskStatus, utc_now_z
from operatorapp.storage import load_job, read_events


class _FakeOpenAIJSONClient:
    planner_response: JobPlanResponse | None = None
    shell_responses: list[ShellStepResponse] = []
    shell_response_by_command: dict[str, list[ShellStepResponse]] = {}
    _shell_command_call_counts: dict[str, int] = {}

    def __init__(self, model: str, api_key: str | None = None) -> None:
        self.model = model

    def complete_json(self, *, system_prompt: str, user_prompt: str, response_model: type):
        if response_model.__name__ == "JobPlanResponse":
            if self.planner_response is None:
                raise AssertionError("planner_response not configured")
            return self.planner_response
        if response_model.__name__ == "ShellStepResponse":
            match = re.search(r'"command":"([^"]+)"', user_prompt)
            if match and match.group(1) in self.shell_response_by_command:
                command = match.group(1)
                call_index = self._shell_command_call_counts.get(command, 0)
                responses = self.shell_response_by_command[command]
                if call_index >= len(responses):
                    raise AssertionError(f"shell responses exhausted for command {command}")
                self._shell_command_call_counts[command] = call_index + 1
                return responses[call_index]
            if not self.shell_responses:
                raise AssertionError("shell_responses exhausted")
            return self.shell_responses.pop(0)
        raise AssertionError(f"Unexpected response model: {response_model}")


class _FakePipe:
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
        self.stdout = _FakePipe(stdout_lines)
        self.stderr = _FakePipe(stderr_lines)
        self._returncode = returncode

        if final_message is not None:
            output_path = self._extract_output_path(argv)
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(final_message, encoding="utf-8")

    @staticmethod
    def _extract_output_path(argv: list[str]) -> str:
        index = argv.index("--output-last-message")
        return argv[index + 1]

    def poll(self) -> int | None:
        return self._returncode

    def kill(self) -> None:
        self._returncode = 124

    def wait(self, timeout: float | None = None) -> int:
        return self._returncode


def _config(tmp_path: Path) -> OperatorConfig:
    return OperatorConfig(
        operator_home=str(tmp_path / "runs"),
        openai_api_key="test-key",
        planner_model="planner-model",
        shell_model="shell-model",
        codex_command="codex",
        codex_bypass_approvals=False,
        default_command_timeout_seconds=30,
        max_command_timeout_seconds=600,
        default_codex_timeout_seconds=120,
        max_concurrent_tasks=4,
        max_concurrent_codex_tasks=1,
    )


def _single_job_dir(runs_dir: Path) -> Path:
    dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
    assert len(dirs) == 1
    return dirs[0]


def test_e2e_pure_native_task_job(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: _config(tmp_path))
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)
    _FakeOpenAIJSONClient.shell_response_by_command = {}
    _FakeOpenAIJSONClient._shell_command_call_counts = {}

    output_file = tmp_path / "workspace" / "hello.txt"
    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="Pure native task flow.",
        groups=[
            PlannedTaskGroupInput(
                title="Create workspace and file",
                tasks=[
                    PlannedTaskInput(
                        title="Create workspace dir",
                        kind="filesystem_create",
                        payload={"path": str(output_file.parent), "node_type": "directory"},
                    ),
                    PlannedTaskInput(
                        title="Write file",
                        kind="write_file",
                        payload={"path": str(output_file), "content": "hello"},
                    ),
                    PlannedTaskInput(
                        title="Append file",
                        kind="append_file",
                        payload={"path": str(output_file), "content": " world"},
                    ),
                ],
            )
        ],
    )
    _FakeOpenAIJSONClient.shell_responses = []

    exit_code = main(["prepare native workspace"])
    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Operator: running job..." in output

    job_dir = _single_job_dir(tmp_path / "runs")
    job = load_job(str(job_dir / "job.json"))
    assert job.status is JobStatus.complete
    assert len(job.task_groups) == 1
    assert job.task_groups[0].status is TaskGroupStatus.complete
    assert all(task.status is TaskStatus.complete for task in job.task_groups[0].tasks)
    assert output_file.read_text(encoding="utf-8") == "hello world\n"

    events_path = job_dir / "events.jsonl"
    assert events_path.is_file()
    events = read_events(str(events_path))
    assert any(event.event_type.value == "task_completed" for event in events)


def test_e2e_mixed_native_and_shell_job(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: _config(tmp_path))
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)
    _FakeOpenAIJSONClient.shell_response_by_command = {}
    _FakeOpenAIJSONClient._shell_command_call_counts = {}

    shell_out = tmp_path / "workspace" / "shell.txt"
    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="Native setup then shell fallback.",
        groups=[
            PlannedTaskGroupInput(
                title="Setup",
                tasks=[
                    PlannedTaskInput(
                        title="Create workspace",
                        kind="filesystem_create",
                        payload={"path": str(shell_out.parent), "node_type": "directory"},
                    )
                ],
            ),
            PlannedTaskGroupInput(
                title="Fallback shell step",
                depends_on_group_indexes=[0],
                tasks=[
                    PlannedTaskInput(
                        title="Run shell write",
                        kind="shell_command",
                        payload={"command": "echo fallback"},
                        timeout_seconds=45,
                    )
                ],
            ),
        ],
    )
    _FakeOpenAIJSONClient.shell_responses = [
        ShellStepResponse(
            status="in_progress",
            summary="Write shell marker",
            sequential=True,
            actions=[
                ShellStepAction(
                    shell_command=f"echo shell > {shell_out}",
                    reason="write marker",
                    timeout_seconds=30,
                )
            ],
            completion_check="file exists",
        ),
        ShellStepResponse(
            status="complete",
            summary="done",
            sequential=True,
            actions=[],
            completion_check="done",
        ),
    ]

    exit_code = main(["run native and shell"])
    assert exit_code == 0

    job_dir = _single_job_dir(tmp_path / "runs")
    job = load_job(str(job_dir / "job.json"))
    assert job.status is JobStatus.complete
    assert shell_out.read_text(encoding="utf-8").strip() == "shell"

    shell_task = job.task_groups[1].tasks[0]
    shell_task_dir = job_dir / "tasks" / shell_task.id
    assert (shell_task_dir / "llm_prompts.jsonl").is_file()
    assert (shell_task_dir / "llm_responses.jsonl").is_file()


def test_e2e_mixed_native_and_codex_job(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: _config(tmp_path))
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)
    _FakeOpenAIJSONClient.shell_response_by_command = {}
    _FakeOpenAIJSONClient._shell_command_call_counts = {}

    def fake_popen(argv, **kwargs):  # type: ignore[no-untyped-def]
        return _FakeCodexPopen(
            list(argv),
            stdout_lines=["codex started", "codex finished"],
            stderr_lines=["codex note"],
            returncode=0,
            final_message="Implemented code changes.",
        )

    monkeypatch.setattr("operatorapp.codex_executor.subprocess.Popen", fake_popen)

    workspace = tmp_path / "workspace"
    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="Native setup then codex modify.",
        groups=[
            PlannedTaskGroupInput(
                title="Create workspace",
                tasks=[
                    PlannedTaskInput(
                        title="Create workspace",
                        kind="filesystem_create",
                        payload={"path": str(workspace), "node_type": "directory"},
                    )
                ],
            ),
            PlannedTaskGroupInput(
                title="Codex modify",
                depends_on_group_indexes=[0],
                tasks=[
                    PlannedTaskInput(
                        title="Modify parser",
                        kind="codex_modify",
                        workdir=str(workspace),
                        test_policy="always",
                        payload={
                            "objective": "Modify parser behavior",
                            "workspace_root": str(workspace),
                            "target_paths": ["parser.py"],
                            "test_command": "pytest -q",
                        },
                    )
                ],
            ),
        ],
    )
    _FakeOpenAIJSONClient.shell_responses = []

    exit_code = main(["native plus codex"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "codex started" in captured.out
    assert "codex note" in captured.err

    job_dir = _single_job_dir(tmp_path / "runs")
    job = load_job(str(job_dir / "job.json"))
    assert job.status is JobStatus.complete
    codex_task = job.task_groups[1].tasks[0]
    codex_task_dir = job_dir / "tasks" / codex_task.id
    assert (codex_task_dir / "llm_prompts.jsonl").is_file()
    assert (codex_task_dir / "llm_responses.jsonl").is_file()
    events = read_events(str(job_dir / "events.jsonl"))
    assert any(event.event_type.value == "codex_started" for event in events)
    assert any(event.event_type.value == "codex_completed" for event in events)


def test_e2e_multigroup_dependency_graph_with_concurrency(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: _config(tmp_path))
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)
    _FakeOpenAIJSONClient.shell_responses = []
    _FakeOpenAIJSONClient._shell_command_call_counts = {}

    # g0 and g1 are independent shell tasks. g2 depends on g1 only.
    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="Concurrency overlap graph.",
        groups=[
            PlannedTaskGroupInput(
                title="Long shell",
                tasks=[PlannedTaskInput(title="long", kind="shell_command", payload={"command": "cmd-long"})],
            ),
            PlannedTaskGroupInput(
                title="Short shell",
                tasks=[PlannedTaskInput(title="short", kind="shell_command", payload={"command": "cmd-short"})],
            ),
            PlannedTaskGroupInput(
                title="After short",
                depends_on_group_indexes=[1],
                tasks=[PlannedTaskInput(title="after", kind="shell_command", payload={"command": "cmd-after-short"})],
            ),
        ],
    )

    # Each shell task: first response issues one command, second completes.
    _FakeOpenAIJSONClient.shell_response_by_command = {
        "cmd-long": [
            ShellStepResponse(
                status="in_progress",
                summary="run",
                sequential=True,
                actions=[ShellStepAction(shell_command="cmd-long", reason="run long")],
                completion_check="done",
            ),
            ShellStepResponse(
                status="complete",
                summary="done long",
                sequential=True,
                actions=[],
                completion_check="done",
            ),
        ],
        "cmd-short": [
            ShellStepResponse(
                status="in_progress",
                summary="run",
                sequential=True,
                actions=[ShellStepAction(shell_command="cmd-short", reason="run short")],
                completion_check="done",
            ),
            ShellStepResponse(
                status="complete",
                summary="done short",
                sequential=True,
                actions=[],
                completion_check="done",
            ),
        ],
        "cmd-after-short": [
            ShellStepResponse(
                status="in_progress",
                summary="run",
                sequential=True,
                actions=[ShellStepAction(shell_command="cmd-after-short", reason="run dependent")],
                completion_check="done",
            ),
            ShellStepResponse(
                status="complete",
                summary="done dependent",
                sequential=True,
                actions=[],
                completion_check="done",
            ),
        ],
    }

    starts: dict[str, float] = {}
    ends: dict[str, float] = {}

    def fake_run_bash(command: str, cwd=None, timeout_seconds: int = 600):  # type: ignore[no-untyped-def]
        starts[command] = time.monotonic()
        delay = {"cmd-long": 0.35, "cmd-short": 0.08, "cmd-after-short": 0.05}[command]
        time.sleep(delay)
        ends[command] = time.monotonic()
        now = utc_now_z()
        return CommandResult(
            command=command,
            cwd=cwd,
            stdout="ok",
            stderr="",
            exit_code=0,
            started_at=now,
            finished_at=now,
            duration_seconds=delay,
            timed_out=False,
            timeout_seconds=timeout_seconds,
        )

    monkeypatch.setattr("operatorapp.shell_executor.run_bash", fake_run_bash)

    exit_code = main(["run overlap graph"])
    assert exit_code == 0
    assert starts["cmd-after-short"] < ends["cmd-long"]

    job_dir = _single_job_dir(tmp_path / "runs")
    job = load_job(str(job_dir / "job.json"))
    assert job.status is JobStatus.complete
    assert all(group.status is TaskGroupStatus.complete for group in job.task_groups)


def test_e2e_cli_return_code_on_runtime_failure(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("operatorapp.cli.load_config", lambda: _config(tmp_path))
    monkeypatch.setattr("operatorapp.cli.OpenAIJSONClient", _FakeOpenAIJSONClient)
    _FakeOpenAIJSONClient.shell_response_by_command = {}
    _FakeOpenAIJSONClient._shell_command_call_counts = {}

    _FakeOpenAIJSONClient.planner_response = JobPlanResponse(
        summary="One failing shell task.",
        groups=[
            PlannedTaskGroupInput(
                title="failing shell",
                tasks=[PlannedTaskInput(title="fail", kind="shell_command", payload={"command": "x"})],
            )
        ],
    )
    _FakeOpenAIJSONClient.shell_responses = [
        ShellStepResponse(
            status="failed",
            summary="cannot proceed",
            sequential=True,
            actions=[],
            completion_check="none",
            failure_reason="forced failure",
        )
    ]

    exit_code = main(["fail job"])
    assert exit_code == 1
