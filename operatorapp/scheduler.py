"""Concurrent DAG scheduler for V2 job/group/task execution."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from .events import JobEvent, JobEventType
from .executor_registry import ExecutorRegistry
from .schemas import (
    FailurePolicy,
    JobRecord,
    JobStatus,
    TaskGroupRecord,
    TaskGroupStatus,
    TaskKind,
    TaskRecord,
    TaskStatus,
    utc_now_z,
)
from .storage import RunPaths, append_event, save_group, save_job, save_task

_TERMINAL_TASK_STATUSES = {
    TaskStatus.complete,
    TaskStatus.failed,
    TaskStatus.blocked,
    TaskStatus.cancelled,
}

_TERMINAL_GROUP_STATUSES = {
    TaskGroupStatus.complete,
    TaskGroupStatus.failed,
    TaskGroupStatus.blocked,
}

_CODEX_TASK_KINDS = {TaskKind.codex_project, TaskKind.codex_modify}


def group_is_ready(job: JobRecord, group: TaskGroupRecord) -> bool:
    """Return True when all dependency groups are complete."""
    statuses = {candidate.id: candidate.status for candidate in job.task_groups}
    return all(statuses.get(dep_id) is TaskGroupStatus.complete for dep_id in group.depends_on_group_ids)


def task_is_ready(group: TaskGroupRecord, task: TaskRecord) -> bool:
    """Return True when task can be scheduled in its current group state."""
    return (
        group.status in {TaskGroupStatus.ready, TaskGroupStatus.in_progress}
        and task.status in {TaskStatus.pending, TaskStatus.ready}
    )


class JobScheduler:
    def __init__(
        self, registry: ExecutorRegistry, max_concurrent_tasks: int, max_concurrent_codex_tasks: int
    ) -> None:
        self._registry = registry
        self._max_concurrent_tasks = max(1, max_concurrent_tasks)
        self._max_concurrent_codex_tasks = max(1, max_concurrent_codex_tasks)

    def run(self, paths: RunPaths, job: JobRecord) -> JobRecord:
        """Run a job by scheduling ready groups/tasks under concurrency limits."""
        group_by_index = {group.group_index: group for group in job.task_groups}

        def emit(
            event_type: JobEventType,
            *,
            message: str,
            group: TaskGroupRecord | None = None,
            task: TaskRecord | None = None,
            data: dict[str, object] | None = None,
        ) -> None:
            append_event(
                paths,
                JobEvent(
                    event_type=event_type,
                    job_id=job.id,
                    group_id=group.id if group else None,
                    task_id=task.id if task else None,
                    message=message,
                    data=data or {},
                ),
            )

        def persist_job_status(status: JobStatus, message: str) -> None:
            if job.status is status:
                return
            job.status = status
            job.updated_at = utc_now_z()
            save_job(paths, job)
            emit(
                JobEventType.job_status_changed,
                message=message,
                data={"status": status.value},
            )

        def persist_group_status(group: TaskGroupRecord, status: TaskGroupStatus, event_type: JobEventType, message: str) -> None:
            if group.status is status:
                return
            group.status = status
            group.updated_at = utc_now_z()
            save_group(paths, group)
            emit(event_type, group=group, message=message)

        def persist_task_status(task: TaskRecord, status: TaskStatus, event_type: JobEventType, message: str) -> None:
            if task.status is status:
                return
            task.status = status
            task.updated_at = utc_now_z()
            save_task(paths, task)
            group = group_by_index[task.group_index]
            emit(event_type, group=group, task=task, message=message)

        persist_job_status(JobStatus.in_progress, "Job scheduling started")

        fail_fast_triggered = False
        running_codex_tasks = 0
        future_map: dict[Future[TaskRecord], tuple[int, int, TaskKind]] = {}

        with ThreadPoolExecutor(max_workers=self._max_concurrent_tasks) as pool:
            while True:
                made_progress = False

                # Promote newly ready groups and tasks.
                for group in sorted(job.task_groups, key=lambda item: item.group_index):
                    if group.status is TaskGroupStatus.pending and group_is_ready(job, group):
                        persist_group_status(
                            group,
                            TaskGroupStatus.ready,
                            JobEventType.group_ready,
                            "Group dependencies complete",
                        )
                        made_progress = True
                        for task in sorted(group.tasks, key=lambda item: item.task_index):
                            if task.status is TaskStatus.pending:
                                persist_task_status(
                                    task,
                                    TaskStatus.ready,
                                    JobEventType.task_ready,
                                    "Task is ready to run",
                                )

                # Block groups that can never run due to failed dependencies.
                for group in sorted(job.task_groups, key=lambda item: item.group_index):
                    if group.status is not TaskGroupStatus.pending:
                        continue
                    dependency_statuses = [
                        candidate.status
                        for candidate in job.task_groups
                        if candidate.id in group.depends_on_group_ids
                    ]
                    if any(
                        status in {TaskGroupStatus.failed, TaskGroupStatus.blocked}
                        for status in dependency_statuses
                    ):
                        persist_group_status(
                            group,
                            TaskGroupStatus.blocked,
                            JobEventType.group_failed,
                            "Group blocked by failed dependency",
                        )
                        made_progress = True
                        for task in group.tasks:
                            if task.status in {TaskStatus.pending, TaskStatus.ready}:
                                persist_task_status(
                                    task,
                                    TaskStatus.blocked,
                                    JobEventType.task_cancelled,
                                    "Task blocked by failed dependency",
                                )

                # Submit ready tasks while respecting concurrency limits.
                available_slots = self._max_concurrent_tasks - len(future_map)
                if available_slots > 0 and not fail_fast_triggered:
                    for group in sorted(job.task_groups, key=lambda item: item.group_index):
                        if available_slots <= 0:
                            break
                        if group.status not in {TaskGroupStatus.ready, TaskGroupStatus.in_progress}:
                            continue

                        for task in sorted(group.tasks, key=lambda item: item.task_index):
                            if available_slots <= 0:
                                break
                            if not task_is_ready(group, task):
                                continue
                            if (
                                task.kind in _CODEX_TASK_KINDS
                                and running_codex_tasks >= self._max_concurrent_codex_tasks
                            ):
                                continue

                            if group.status is TaskGroupStatus.ready:
                                persist_group_status(
                                    group,
                                    TaskGroupStatus.in_progress,
                                    JobEventType.group_started,
                                    "Group execution started",
                                )
                            persist_task_status(
                                task,
                                TaskStatus.in_progress,
                                JobEventType.task_started,
                                "Task execution started",
                            )

                            executor = self._registry.get(task.kind)
                            future = pool.submit(executor.execute, paths, job, group, task)
                            future_map[future] = (group.group_index, task.task_index, task.kind)
                            if task.kind in _CODEX_TASK_KINDS:
                                running_codex_tasks += 1
                            available_slots -= 1
                            made_progress = True

                # Harvest completed tasks.
                done_futures: set[Future[TaskRecord]] = {
                    future for future in future_map if future.done()
                }
                if not done_futures and future_map:
                    done_futures, _ = wait(
                        set(future_map.keys()),
                        timeout=0.05,
                        return_when=FIRST_COMPLETED,
                    )

                for future in done_futures:
                    group_index, task_index, task_kind = future_map.pop(future)
                    if task_kind in _CODEX_TASK_KINDS:
                        running_codex_tasks -= 1
                    group = group_by_index[group_index]
                    task_position = next(
                        idx
                        for idx, item in enumerate(group.tasks)
                        if item.task_index == task_index
                    )
                    existing_task = group.tasks[task_position]

                    try:
                        updated_task = future.result()
                    except Exception as exc:  # pragma: no cover - defensive
                        existing_task.status = TaskStatus.failed
                        existing_task.error_message = (
                            f"Executor error ({type(exc).__name__}): {exc}"
                        )
                        existing_task.updated_at = utc_now_z()
                        updated_task = existing_task

                    group.tasks[task_position] = updated_task
                    save_task(paths, updated_task)
                    made_progress = True

                    if updated_task.status is TaskStatus.complete:
                        emit(
                            JobEventType.task_completed,
                            group=group,
                            task=updated_task,
                            message="Task execution completed",
                        )
                    elif updated_task.status is TaskStatus.failed:
                        emit(
                            JobEventType.task_failed,
                            group=group,
                            task=updated_task,
                            message="Task execution failed",
                            data={"error_message": updated_task.error_message or ""},
                        )
                        if (
                            updated_task.failure_policy is FailurePolicy.fail_fast
                            and not fail_fast_triggered
                        ):
                            fail_fast_triggered = True
                            persist_group_status(
                                group,
                                TaskGroupStatus.failed,
                                JobEventType.group_failed,
                                "Group failed due to fail_fast task failure",
                            )
                            persist_job_status(
                                JobStatus.failed,
                                "Job failed due to fail_fast task failure",
                            )
                            for candidate_group in job.task_groups:
                                if candidate_group.status in {
                                    TaskGroupStatus.pending,
                                    TaskGroupStatus.ready,
                                }:
                                    persist_group_status(
                                        candidate_group,
                                        TaskGroupStatus.blocked,
                                        JobEventType.group_failed,
                                        "Group blocked by fail_fast failure",
                                    )
                                for candidate_task in candidate_group.tasks:
                                    if candidate_task.status in {
                                        TaskStatus.pending,
                                        TaskStatus.ready,
                                    }:
                                        persist_task_status(
                                            candidate_task,
                                            TaskStatus.cancelled,
                                            JobEventType.task_cancelled,
                                            "Task cancelled by fail_fast failure",
                                        )

                # Finalize groups that have reached terminal task states.
                for group in sorted(job.task_groups, key=lambda item: item.group_index):
                    if group.status not in {TaskGroupStatus.ready, TaskGroupStatus.in_progress}:
                        continue
                    if not group.tasks:
                        persist_group_status(
                            group,
                            TaskGroupStatus.complete,
                            JobEventType.group_completed,
                            "Empty group completed",
                        )
                        made_progress = True
                        continue
                    if not all(task.status in _TERMINAL_TASK_STATUSES for task in group.tasks):
                        continue

                    failed_tasks = [task for task in group.tasks if task.status is TaskStatus.failed]
                    if not failed_tasks:
                        persist_group_status(
                            group,
                            TaskGroupStatus.complete,
                            JobEventType.group_completed,
                            "All tasks completed",
                        )
                        made_progress = True
                        continue

                    if any(
                        task.failure_policy in {FailurePolicy.fail_fast, FailurePolicy.continue_group}
                        for task in failed_tasks
                    ):
                        persist_group_status(
                            group,
                            TaskGroupStatus.failed,
                            JobEventType.group_failed,
                            "Group failed due to task failures",
                        )
                        if not fail_fast_triggered:
                            persist_job_status(
                                JobStatus.failed,
                                "Job failed due to group failure",
                            )
                        made_progress = True
                    else:
                        # All failed tasks are continue_job; group considered complete.
                        persist_group_status(
                            group,
                            TaskGroupStatus.complete,
                            JobEventType.group_completed,
                            "Group completed with continue_job task failures",
                        )
                        made_progress = True

                # Exit once no work remains.
                if not future_map and all(
                    group.status in _TERMINAL_GROUP_STATUSES for group in job.task_groups
                ):
                    break

                # If nothing moved and nothing running, mark job failed to prevent deadlock.
                if not future_map and not made_progress:
                    persist_job_status(JobStatus.failed, "Scheduler made no progress; marking job failed")
                    for group in job.task_groups:
                        if group.status not in _TERMINAL_GROUP_STATUSES:
                            persist_group_status(
                                group,
                                TaskGroupStatus.blocked,
                                JobEventType.group_failed,
                                "Group blocked due to scheduler deadlock",
                            )
                    break

        if job.status is JobStatus.in_progress:
            if all(group.status is TaskGroupStatus.complete for group in job.task_groups):
                persist_job_status(JobStatus.complete, "All groups completed")
            else:
                persist_job_status(JobStatus.failed, "Job ended without full completion")

        save_job(paths, job)
        return job

