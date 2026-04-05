from __future__ import annotations

import pytest
from pydantic import ValidationError

from operatorapp.planner import PlannerService, build_job_from_plan
from operatorapp.planner_models import JobPlanResponse, PlannedTaskGroupInput, PlannedTaskInput
from operatorapp.schemas import JobStatus, TaskKind, TaskStatus, TestPolicy as TaskTestPolicy
from operatorapp.task_payloads import CodexModifyPayload, DownloadFilePayload, ShellCommandPayload


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
        summary="Fetch source then update code and run checks.",
        groups=[
            PlannedTaskGroupInput(
                title="Fetch source artifacts",
                tasks=[
                    PlannedTaskInput(
                        title="Download source archive",
                        kind="download_file",
                        timeout_seconds=900,
                        payload={
                            "url": "https://example.com/src.tar.gz",
                            "destination_path": "/tmp/src.tar.gz",
                            "overwrite": True,
                        },
                    ),
                ],
            ),
            PlannedTaskGroupInput(
                title="Modify project",
                depends_on_group_indexes=[0],
                tasks=[
                    PlannedTaskInput(
                        title="Modify parser behavior",
                        kind="codex_modify",
                        workdir="/tmp/project",
                        test_policy="always",
                        payload={
                            "objective": "Adjust parser normalization",
                            "workspace_root": "/tmp/project",
                            "target_paths": ["parser.py"],
                        },
                    ),
                    PlannedTaskInput(
                        title="Run focused tests",
                        kind="shell_command",
                        workdir="/tmp/project",
                        timeout_seconds=1200,
                        test_policy="always",
                        payload={
                            "command": "pytest tests/test_parser.py -q",
                            "cwd": "/tmp/project",
                        },
                    ),
                ],
            ),
        ],
    )


def test_build_job_from_plan_maps_groups_dependencies_kinds_payloads_and_ids() -> None:
    job = build_job_from_plan("Build parser", _sample_plan())

    assert job.summary == "Fetch source then update code and run checks."
    assert len(job.task_groups) == 2
    assert [group.group_index for group in job.task_groups] == [0, 1]
    assert job.task_groups[0].depends_on_group_ids == []
    assert job.task_groups[1].depends_on_group_ids == [f"{job.id}_g000"]

    first_group_task = job.task_groups[0].tasks[0]
    second_group_task_0 = job.task_groups[1].tasks[0]
    second_group_task_1 = job.task_groups[1].tasks[1]

    assert first_group_task.kind is TaskKind.download_file
    assert isinstance(first_group_task.payload, DownloadFilePayload)
    assert first_group_task.timeout_seconds == 900

    assert second_group_task_0.kind is TaskKind.codex_modify
    assert isinstance(second_group_task_0.payload, CodexModifyPayload)
    assert second_group_task_0.test_policy is TaskTestPolicy.always

    assert second_group_task_1.kind is TaskKind.shell_command
    assert isinstance(second_group_task_1.payload, ShellCommandPayload)
    assert second_group_task_1.timeout_seconds == 1200

    assert first_group_task.id == f"{job.id}_g000_t000"
    assert second_group_task_0.id == f"{job.id}_g001_t000"
    assert second_group_task_1.id == f"{job.id}_g001_t001"


def test_build_job_from_plan_rejects_invalid_dependency_index() -> None:
    plan = JobPlanResponse(
        summary="Bad dependency",
        groups=[
            PlannedTaskGroupInput(
                title="Only group",
                depends_on_group_indexes=[1],
                tasks=[
                    PlannedTaskInput(
                        title="Run shell",
                        kind="shell_command",
                        payload={"command": "echo hi"},
                    )
                ],
            )
        ],
    )

    with pytest.raises(ValueError):
        build_job_from_plan("Bad", plan)


def test_invalid_planned_task_payload_for_kind_is_rejected() -> None:
    with pytest.raises(ValidationError):
        JobPlanResponse(
            summary="Invalid payload",
            groups=[
                PlannedTaskGroupInput(
                    title="Group",
                    tasks=[
                        PlannedTaskInput(
                            title="Bad shell payload",
                            kind="shell_command",
                            payload={"url": "https://example.com/file.txt"},
                        )
                    ],
                )
            ],
        )


def test_create_job_uses_llm_and_returns_pending_job_groups_and_tasks() -> None:
    fake = FakeJSONLLMClient(_sample_plan())
    service = PlannerService(fake)

    user_prompt = "Build parser"
    job = service.create_job(user_prompt)

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["response_model"] is JobPlanResponse
    assert "Convert one user request into ordered task groups" in str(call["system_prompt"])
    assert user_prompt in str(call["user_prompt"])

    assert job.status is JobStatus.pending
    assert all(group.status.value == "pending" for group in job.task_groups)
    assert all(
        task.status is TaskStatus.pending
        for group in job.task_groups
        for task in group.tasks
    )
    assert job.created_at == job.updated_at
