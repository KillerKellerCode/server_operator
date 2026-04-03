"""Planning layer that converts user prompts into structured jobs."""

from __future__ import annotations

from .llm_types import JSONLLMClient
from .planner_models import JobPlanResponse
from .prompts import build_job_planner_system_prompt, build_job_planner_user_prompt
from .schemas import JobRecord, TaskKind, TaskRecord, TestPolicy, make_job_id, make_task_id, utc_now_z


def build_job_from_plan(user_prompt: str, plan: JobPlanResponse) -> JobRecord:
    """Build a pending JobRecord from a validated plan."""
    job_id = make_job_id()
    timestamp = utc_now_z()

    tasks: list[TaskRecord] = []
    for index, planned_task in enumerate(plan.tasks):
        task = TaskRecord(
            id=make_task_id(job_id, index),
            parent_job_id=job_id,
            task_index=index,
            title=planned_task.title,
            kind=TaskKind(planned_task.kind),
            instructions=planned_task.instructions,
            workdir=planned_task.workdir,
            test_policy=TestPolicy(planned_task.test_policy),
        )
        tasks.append(task)

    return JobRecord(
        id=job_id,
        user_prompt=user_prompt,
        summary=plan.summary,
        tasks=tasks,
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

