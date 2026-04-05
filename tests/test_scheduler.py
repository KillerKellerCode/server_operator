from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from operatorapp.executor_registry import ExecutorRegistry
from operatorapp.scheduler import JobScheduler, group_is_ready, task_is_ready
from operatorapp.schemas import (
    FailurePolicy,
    JobRecord,
    JobStatus,
    TaskGroupRecord,
    TaskGroupStatus,
    TaskKind,
    TaskRecord,
    TaskStatus,
    make_group_id,
    make_task_id,
)
from operatorapp.storage import create_run_paths, load_group, load_job, load_task, read_events, save_group, save_job, save_task
from operatorapp.task_payloads import CodexModifyPayload, CodexProjectPayload, ShellCommandPayload


@dataclass
class _Recorder:
    lock: threading.Lock = field(default_factory=threading.Lock)
    starts: dict[str, float] = field(default_factory=dict)
    ends: dict[str, float] = field(default_factory=dict)
    active: int = 0
    max_active: int = 0

    def start(self, task_id: str) -> None:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            self.starts[task_id] = time.monotonic()

    def end(self, task_id: str) -> None:
        with self.lock:
            self.ends[task_id] = time.monotonic()
            self.active -= 1


class _FakeExecutor:
    def __init__(
        self,
        kind: TaskKind,
        recorder: _Recorder,
        *,
        delay_seconds: float = 0.0,
        fail_task_ids: set[str] | None = None,
    ) -> None:
        self.kind = kind
        self._recorder = recorder
        self._delay_seconds = delay_seconds
        self._fail_task_ids = fail_task_ids or set()

    def execute(self, paths, job, group, task):  # type: ignore[no-untyped-def]
        self._recorder.start(task.id)
        try:
            if self._delay_seconds > 0:
                time.sleep(self._delay_seconds)
            task.status = TaskStatus.failed if task.id in self._fail_task_ids else TaskStatus.complete
            if task.status is TaskStatus.failed:
                task.error_message = "simulated failure"
            return task
        finally:
            self._recorder.end(task.id)


def _task(job_id: str, group_index: int, task_index: int, kind: TaskKind) -> TaskRecord:
    payload = (
        ShellCommandPayload(command=f"echo {task_index}")
        if kind is TaskKind.shell_command
        else CodexProjectPayload(objective="create", workspace_root="/tmp/project")
        if kind is TaskKind.codex_project
        else CodexModifyPayload(objective="modify", workspace_root="/tmp/project")
    )
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title=f"task-{group_index}-{task_index}",
        kind=kind,
        payload=payload,
    )


def _group(job_id: str, group_index: int, depends_on: list[int], tasks: list[TaskRecord]) -> TaskGroupRecord:
    return TaskGroupRecord(
        id=make_group_id(job_id, group_index),
        parent_job_id=job_id,
        group_index=group_index,
        title=f"group-{group_index}",
        depends_on_group_ids=[make_group_id(job_id, idx) for idx in depends_on],
        tasks=tasks,
    )


def _persist_initial(paths, job: JobRecord) -> None:  # type: ignore[no-untyped-def]
    save_job(paths, job)
    for group in job.task_groups:
        save_group(paths, group)
        for task in group.tasks:
            save_task(paths, task)


def test_group_and_task_ready_helpers() -> None:
    job_id = "jobabcd1"
    group_a = _group(job_id, 0, [], [_task(job_id, 0, 0, TaskKind.shell_command)])
    group_b = _group(job_id, 1, [0], [_task(job_id, 1, 0, TaskKind.shell_command)])
    job = JobRecord(id=job_id, user_prompt="x", task_groups=[group_a, group_b])

    assert group_is_ready(job, group_a) is True
    assert group_is_ready(job, group_b) is False
    group_a.status = TaskGroupStatus.complete
    assert group_is_ready(job, group_b) is True

    group_b.status = TaskGroupStatus.ready
    task = group_b.tasks[0]
    assert task_is_ready(group_b, task) is True
    task.status = TaskStatus.in_progress
    assert task_is_ready(group_b, task) is False


def test_simple_dependency_chain(tmp_path: Path) -> None:
    job_id = "jobabcd1"
    recorder = _Recorder()
    registry = ExecutorRegistry([_FakeExecutor(TaskKind.shell_command, recorder, delay_seconds=0.05)])
    scheduler = JobScheduler(registry, max_concurrent_tasks=2, max_concurrent_codex_tasks=1)

    group0 = _group(job_id, 0, [], [_task(job_id, 0, 0, TaskKind.shell_command)])
    group1 = _group(job_id, 1, [0], [_task(job_id, 1, 0, TaskKind.shell_command)])
    group2 = _group(job_id, 2, [1], [_task(job_id, 2, 0, TaskKind.shell_command)])
    job = JobRecord(id=job_id, user_prompt="chain", task_groups=[group0, group1, group2])
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    _persist_initial(paths, job)

    result = scheduler.run(paths, job)

    assert result.status is JobStatus.complete
    assert recorder.starts[group0.tasks[0].id] < recorder.starts[group1.tasks[0].id]
    assert recorder.starts[group1.tasks[0].id] < recorder.starts[group2.tasks[0].id]


def test_overlapping_independent_groups_without_stage_barrier(tmp_path: Path) -> None:
    job_id = "jobabcd1"
    shell_recorder = _Recorder()
    codex_recorder = _Recorder()
    registry = ExecutorRegistry(
        [
            _FakeExecutor(TaskKind.shell_command, shell_recorder, delay_seconds=0.35),
            _FakeExecutor(TaskKind.codex_modify, codex_recorder, delay_seconds=0.08),
        ]
    )
    scheduler = JobScheduler(registry, max_concurrent_tasks=2, max_concurrent_codex_tasks=2)

    group0 = _group(job_id, 0, [], [_task(job_id, 0, 0, TaskKind.shell_command)])
    group1 = _group(job_id, 1, [], [_task(job_id, 1, 0, TaskKind.codex_modify)])
    group2 = _group(job_id, 2, [1], [_task(job_id, 2, 0, TaskKind.codex_modify)])
    job = JobRecord(id=job_id, user_prompt="overlap", task_groups=[group0, group1, group2])
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    _persist_initial(paths, job)

    result = scheduler.run(paths, job)
    assert result.status is JobStatus.complete
    assert codex_recorder.starts[group2.tasks[0].id] < shell_recorder.ends[group0.tasks[0].id]


def test_independent_tasks_within_group_run_concurrently(tmp_path: Path) -> None:
    job_id = "jobabcd1"
    recorder = _Recorder()
    registry = ExecutorRegistry([_FakeExecutor(TaskKind.shell_command, recorder, delay_seconds=0.1)])
    scheduler = JobScheduler(registry, max_concurrent_tasks=2, max_concurrent_codex_tasks=1)

    group0 = _group(
        job_id,
        0,
        [],
        [
            _task(job_id, 0, 0, TaskKind.shell_command),
            _task(job_id, 0, 1, TaskKind.shell_command),
        ],
    )
    job = JobRecord(id=job_id, user_prompt="parallel", task_groups=[group0])
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    _persist_initial(paths, job)

    result = scheduler.run(paths, job)
    assert result.status is JobStatus.complete
    assert recorder.max_active >= 2


def test_codex_concurrency_limit(tmp_path: Path) -> None:
    job_id = "jobabcd1"
    codex_recorder = _Recorder()
    shell_recorder = _Recorder()
    registry = ExecutorRegistry(
        [
            _FakeExecutor(TaskKind.codex_project, codex_recorder, delay_seconds=0.1),
            _FakeExecutor(TaskKind.codex_modify, codex_recorder, delay_seconds=0.1),
            _FakeExecutor(TaskKind.shell_command, shell_recorder, delay_seconds=0.1),
        ]
    )
    scheduler = JobScheduler(registry, max_concurrent_tasks=3, max_concurrent_codex_tasks=1)

    group0 = _group(
        job_id,
        0,
        [],
        [
            _task(job_id, 0, 0, TaskKind.codex_project),
            _task(job_id, 0, 1, TaskKind.codex_modify),
            _task(job_id, 0, 2, TaskKind.shell_command),
        ],
    )
    job = JobRecord(id=job_id, user_prompt="codex-limit", task_groups=[group0])
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    _persist_initial(paths, job)

    result = scheduler.run(paths, job)
    assert result.status is JobStatus.complete
    assert codex_recorder.max_active == 1


def test_fail_fast_behavior(tmp_path: Path) -> None:
    job_id = "jobabcd1"
    recorder = _Recorder()
    fail_task_id = make_task_id(job_id, 0, 0)
    registry = ExecutorRegistry(
        [_FakeExecutor(TaskKind.shell_command, recorder, fail_task_ids={fail_task_id})]
    )
    scheduler = JobScheduler(registry, max_concurrent_tasks=2, max_concurrent_codex_tasks=1)

    failing_task = _task(job_id, 0, 0, TaskKind.shell_command)
    failing_task.failure_policy = FailurePolicy.fail_fast
    group0 = _group(job_id, 0, [], [failing_task])
    group1 = _group(job_id, 1, [0], [_task(job_id, 1, 0, TaskKind.shell_command)])
    job = JobRecord(id=job_id, user_prompt="fail-fast", task_groups=[group0, group1])
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    _persist_initial(paths, job)

    result = scheduler.run(paths, job)
    assert result.status is JobStatus.failed
    assert result.task_groups[0].status is TaskGroupStatus.failed
    assert result.task_groups[1].status in {TaskGroupStatus.blocked, TaskGroupStatus.pending}


def test_snapshot_and_event_updates_during_scheduling(tmp_path: Path) -> None:
    job_id = "jobabcd1"
    recorder = _Recorder()
    registry = ExecutorRegistry([_FakeExecutor(TaskKind.shell_command, recorder, delay_seconds=0.01)])
    scheduler = JobScheduler(registry, max_concurrent_tasks=1, max_concurrent_codex_tasks=1)

    group0 = _group(job_id, 0, [], [_task(job_id, 0, 0, TaskKind.shell_command)])
    job = JobRecord(id=job_id, user_prompt="snapshots", task_groups=[group0])
    paths = create_run_paths(str(tmp_path / "runs"), job.id)
    _persist_initial(paths, job)

    result = scheduler.run(paths, job)
    assert result.status is JobStatus.complete

    saved_job = load_job(str(Path(paths.job_dir) / "job.json"))
    saved_group = load_group(str(Path(paths.groups_dir) / group0.id / "group.json"))
    saved_task = load_task(str(Path(paths.tasks_dir) / group0.tasks[0].id / "task.json"))
    assert saved_job.status is JobStatus.complete
    assert saved_group.status is TaskGroupStatus.complete
    assert saved_task.status is TaskStatus.complete

    events = read_events(paths.events_jsonl_path)
    event_types = [event.event_type.value for event in events]
    assert "job_status_changed" in event_types
    assert "group_ready" in event_types
    assert "task_ready" in event_types
    assert "task_started" in event_types
    assert "task_completed" in event_types
