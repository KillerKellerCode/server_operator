"""Planning layer that converts user prompts into V2 group/DAG jobs."""

from __future__ import annotations

from pydantic import TypeAdapter

from .llm_types import JSONLLMClient
from .planner_models import JobPlanResponse, PlannedTaskPayload
from .prompts import build_job_planner_system_prompt, build_job_planner_user_prompt
from .schemas import (
    FailurePolicy,
    JobRecord,
    TaskGroupRecord,
    TaskKind,
    TaskRecord,
    TestPolicy,
    make_group_id,
    make_job_id,
    make_task_id,
    utc_now_z,
)
from .task_payloads import TaskPayload

_TASK_PAYLOAD_ADAPTER = TypeAdapter(TaskPayload)


def _validate_and_build_payload(kind: TaskKind, payload: PlannedTaskPayload) -> TaskPayload:
    payload_data = payload.model_dump(exclude_none=True)
    payload_kind = payload_data.pop("kind", None)
    if payload_kind is not None and payload_kind != kind.value:
        raise ValueError(f"payload kind '{payload_kind}' does not match task kind '{kind.value}'")
    payload_data["kind"] = kind.value
    return _TASK_PAYLOAD_ADAPTER.validate_python(payload_data)


def build_job_from_plan(user_prompt: str, plan: JobPlanResponse) -> JobRecord:
    """Build a pending JobRecord from a validated V2 planner response."""
    job_id = make_job_id()
    timestamp = utc_now_z()
    total_groups = len(plan.groups)

    task_groups: list[TaskGroupRecord] = []
    for group_index, planned_group in enumerate(plan.groups):
        group_id = make_group_id(job_id, group_index)
        depends_on_group_ids: list[str] = []
        for dependency_index in planned_group.depends_on_group_indexes:
            if dependency_index < 0 or dependency_index >= total_groups:
                raise ValueError(f"invalid depends_on_group_indexes value: {dependency_index}")
            if dependency_index == group_index:
                raise ValueError("group cannot depend on itself")
            depends_on_group_ids.append(make_group_id(job_id, dependency_index))

        tasks: list[TaskRecord] = []
        for task_index, planned_task in enumerate(planned_group.tasks):
            kind = TaskKind(planned_task.kind)
            typed_payload = _validate_and_build_payload(kind, planned_task.payload)
            tasks.append(
                TaskRecord(
                    id=make_task_id(job_id, group_index, task_index),
                    parent_job_id=job_id,
                    parent_group_id=group_id,
                    group_index=group_index,
                    task_index=task_index,
                    title=planned_task.title,
                    kind=kind,
                    failure_policy=FailurePolicy(planned_task.failure_policy),
                    workdir=planned_task.workdir,
                    timeout_seconds=planned_task.timeout_seconds,
                    test_policy=TestPolicy(planned_task.test_policy),
                    payload=typed_payload,
                    created_at=timestamp,
                    updated_at=timestamp,
                )
            )

        task_groups.append(
            TaskGroupRecord(
                id=group_id,
                parent_job_id=job_id,
                group_index=group_index,
                title=planned_group.title,
                depends_on_group_ids=depends_on_group_ids,
                tasks=tasks,
                created_at=timestamp,
                updated_at=timestamp,
            )
        )

    return JobRecord(
        id=job_id,
        user_prompt=user_prompt,
        summary=plan.summary,
        task_groups=task_groups,
        created_at=timestamp,
        updated_at=timestamp,
    )


class PlannerService:
    def __init__(self, llm: JSONLLMClient) -> None:
        self._llm = llm

    def create_job(self, user_prompt: str) -> JobRecord:
        """Create a job by planning with the configured JSON-capable LLM."""
        system_prompt = build_job_planner_system_prompt()
        planner_user_prompt = build_job_planner_user_prompt(user_prompt)
        plan = self._llm.complete_json(
            system_prompt=system_prompt,
            user_prompt=planner_user_prompt,
            response_model=JobPlanResponse,
        )
        return build_job_from_plan(user_prompt=user_prompt, plan=plan)
