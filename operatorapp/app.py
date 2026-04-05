"""Main application runner for Operator."""

from __future__ import annotations

from .archive_executor import ArchiveTaskExecutor
from .codex_executor import CodexTaskExecutor
from .config import OperatorConfig
from .download_executor import DownloadTaskExecutor
from .events import JobEvent, JobEventType
from .executor_registry import ExecutorRegistry
from .executor_types import TaskExecutor
from .file_io_executor import FileIOTaskExecutor
from .filesystem_executor import FilesystemTaskExecutor
from .git_executor import GitTaskExecutor
from .logging_utils import append_run_log
from .planner import PlannerService
from .scheduler import JobScheduler
from .schemas import JobRecord, JobStatus, TaskKind, TaskRecord, utc_now_z
from .shell_executor import ShellTaskExecutor
from .storage import append_event, create_run_paths, save_group, save_job, save_task
from .wait_executor import WaitTaskExecutor


class _ShellExecutorAdapter:
    kind = TaskKind.shell_command

    def __init__(self, executor: ShellTaskExecutor) -> None:
        self._executor = executor

    def execute(self, paths, job, group, task):  # type: ignore[no-untyped-def]
        return self._executor.execute(paths, task)


class _CodexExecutorAdapter:
    def __init__(self, executor: CodexTaskExecutor, kind: TaskKind) -> None:
        self._executor = executor
        self.kind = kind

    def execute(self, paths, job, group, task):  # type: ignore[no-untyped-def]
        return self._executor.execute(paths, task)


class _KindAdapter:
    def __init__(self, executor: TaskExecutor, kind: TaskKind) -> None:
        self._executor = executor
        self.kind = kind

    def execute(self, paths, job, group, task):  # type: ignore[no-untyped-def]
        return self._executor.execute(paths, job, group, task)


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

    def _build_registry(self) -> ExecutorRegistry:
        download_executor = DownloadTaskExecutor()
        filesystem_executor = FilesystemTaskExecutor()
        file_io_executor = FileIOTaskExecutor()
        archive_executor = ArchiveTaskExecutor()
        git_executor = GitTaskExecutor()
        wait_executor = WaitTaskExecutor()

        executors = [
            _ShellExecutorAdapter(self._shell_executor),
            _CodexExecutorAdapter(self._codex_executor, TaskKind.codex_project),
            _CodexExecutorAdapter(self._codex_executor, TaskKind.codex_modify),
            _KindAdapter(download_executor, TaskKind.download_file),
            _KindAdapter(filesystem_executor, TaskKind.filesystem_create),
            _KindAdapter(filesystem_executor, TaskKind.filesystem_mutate),
            _KindAdapter(file_io_executor, TaskKind.read_file),
            _KindAdapter(file_io_executor, TaskKind.write_file),
            _KindAdapter(file_io_executor, TaskKind.append_file),
            _KindAdapter(file_io_executor, TaskKind.patch_file),
            _KindAdapter(archive_executor, TaskKind.extract_archive),
            _KindAdapter(git_executor, TaskKind.git_repo),
            _KindAdapter(wait_executor, TaskKind.process_wait),
        ]
        return ExecutorRegistry(executors)

    def run(self, user_prompt: str) -> JobRecord:
        """Plan a job and execute it with the concurrent DAG scheduler."""
        job = self._planner.create_job(user_prompt)
        paths = create_run_paths(self._config.operator_home, job.id)

        save_job(paths, job)
        append_event(
            paths,
            JobEvent(
                event_type=JobEventType.job_created,
                job_id=job.id,
                message="Job created",
                data={"group_count": len(job.task_groups)},
            ),
        )
        append_run_log(paths, "INFO", f"Created job with {len(job.task_groups)} group(s)")

        for group in job.task_groups:
            save_group(paths, group)
            for task in group.tasks:
                save_task(paths, task)

        scheduler = JobScheduler(
            registry=self._build_registry(),
            max_concurrent_tasks=self._config.max_concurrent_tasks,
            max_concurrent_codex_tasks=self._config.max_concurrent_codex_tasks,
        )

        try:
            return scheduler.run(paths, job)
        except KeyboardInterrupt:
            job.status = JobStatus.interrupted
            job.updated_at = utc_now_z()
            save_job(paths, job)
            append_event(
                paths,
                JobEvent(
                    event_type=JobEventType.job_status_changed,
                    job_id=job.id,
                    message="Job interrupted by user",
                    data={"status": JobStatus.interrupted.value},
                ),
            )
            append_run_log(paths, "WARNING", "Job interrupted by user")
            raise

