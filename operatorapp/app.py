"""Main application runner for Operator."""

from __future__ import annotations

from .codex_executor import CodexTaskExecutor
from .config import OperatorConfig
from .logging_utils import append_run_log
from .planner import PlannerService
from .schemas import JobRecord, JobStatus, TaskKind, TaskStatus, utc_now_z
from .shell_executor import ShellTaskExecutor
from .storage import create_run_paths, save_job, save_task


class OperatorApp:
    def __init__(
        self,
        planner: PlannerService,
        shell_executor: ShellTaskExecutor,
        codex_executor: CodexTaskExecutor,
        config: OperatorConfig,
    ) -> None:
        self._planner = planner
        self._shell_executor = shell_executor
        self._codex_executor = codex_executor
        self._config = config

    def run(self, user_prompt: str) -> JobRecord:
        """Plan a job and execute tasks sequentially until completion or failure."""
        job = self._planner.create_job(user_prompt)
        paths = create_run_paths(self._config.operator_home, job.id)
        save_job(paths, job)
        append_run_log(paths, "INFO", f"Created job with {len(job.tasks)} task(s)")

        ordered_positions = [
            position
            for position, _task in sorted(
                enumerate(job.tasks), key=lambda pair: pair[1].task_index
            )
        ]

        try:
            for position in ordered_positions:
                task = job.tasks[position]
                task.status = TaskStatus.in_progress
                job.updated_at = utc_now_z()
                save_task(paths, task)
                save_job(paths, job)
                append_run_log(
                    paths,
                    "INFO",
                    f"Starting task {task.task_index}: {task.title}",
                    task_id=task.id,
                )

                if task.kind is TaskKind.shell:
                    updated_task = self._shell_executor.execute(paths, task)
                elif task.kind is TaskKind.codex:
                    updated_task = self._codex_executor.execute(paths, task)
                else:  # pragma: no cover - defensive branch for future enum growth
                    raise ValueError(f"Unsupported task kind: {task.kind}")

                job.tasks[position] = updated_task
                job.updated_at = utc_now_z()
                save_job(paths, job)

                if updated_task.status is TaskStatus.failed:
                    job.status = JobStatus.failed
                    job.updated_at = utc_now_z()
                    append_run_log(paths, "ERROR", "Job failed due to task failure")
                    save_job(paths, job)
                    return job

            job.status = JobStatus.complete
            job.updated_at = utc_now_z()
            append_run_log(paths, "INFO", "Job completed successfully")
            save_job(paths, job)
            return job
        except KeyboardInterrupt:
            job.status = JobStatus.interrupted
            job.updated_at = utc_now_z()
            append_run_log(paths, "WARNING", "Job interrupted by user")
            save_job(paths, job)
            raise

