from __future__ import annotations

from operatorapp.planner import PlannerService, build_job_from_plan
from operatorapp.planner_models import JobPlanResponse, PlannedTaskInput
from operatorapp.schemas import JobStatus, TaskKind, TaskStatus, TestPolicy as TaskTestPolicy


class FakeJSONLLMClient:
    def __init__(self, response: JobPlanResponse) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def complete_json(self, *, system_prompt: str, user_prompt: str, response_model: type):
        self.calls.append(
            {
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "response_model": response_model,
            }
        )
        return self.response


def _sample_plan() -> JobPlanResponse:
    return JobPlanResponse(
        summary="Prepare environment and implement feature.",
        tasks=[
            PlannedTaskInput(
                title="Install dependency",
                kind="shell",
                instructions="pip install requests",
                workdir="/tmp/proj",
                test_policy="never",
            ),
            PlannedTaskInput(
                title="Implement parser",
                kind="codex",
                instructions="Create parser and update CLI integration.",
                workdir="/tmp/proj",
                test_policy="always",
            ),
            PlannedTaskInput(
                title="Run selective checks",
                kind="shell",
                instructions="pytest tests/test_parser.py -q",
                workdir="/tmp/proj",
                test_policy="auto",
            ),
        ],
    )


def test_build_job_from_plan_maps_kind_policy_ids_and_order() -> None:
    job = build_job_from_plan("Build parser", _sample_plan())

    assert job.summary == "Prepare environment and implement feature."
    assert [task.title for task in job.tasks] == [
        "Install dependency",
        "Implement parser",
        "Run selective checks",
    ]
    assert [task.kind for task in job.tasks] == [TaskKind.shell, TaskKind.codex, TaskKind.shell]
    assert [task.test_policy for task in job.tasks] == [
        TaskTestPolicy.never,
        TaskTestPolicy.always,
        TaskTestPolicy.auto,
    ]
    assert [task.id for task in job.tasks] == [
        f"{job.id}_t000",
        f"{job.id}_t001",
        f"{job.id}_t002",
    ]
    assert [task.task_index for task in job.tasks] == [0, 1, 2]
    assert all(task.parent_job_id == job.id for task in job.tasks)


def test_create_job_uses_llm_and_returns_pending_job_and_tasks() -> None:
    fake = FakeJSONLLMClient(_sample_plan())
    service = PlannerService(fake)

    user_prompt = "Build parser"
    job = service.create_job(user_prompt)

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["response_model"] is JobPlanResponse
    assert "Convert one user request into an ordered job plan." in str(call["system_prompt"])
    assert user_prompt in str(call["user_prompt"])

    assert job.status is JobStatus.pending
    assert all(task.status is TaskStatus.pending for task in job.tasks)
    assert job.created_at == job.updated_at
