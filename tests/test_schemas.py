from __future__ import annotations

import re
from datetime import datetime

import pytest
from pydantic import ValidationError

from operatorapp.schemas import (
    CommandResult,
    FailurePolicy,
    JobRecord,
    JobStatus,
    TaskGroupRecord,
    TaskGroupStatus,
    TaskKind,
    TaskProgress,
    TaskRecord,
    TaskStatus,
    TestPolicy as TaskTestPolicy,
    make_group_id,
    make_job_id,
    make_task_id,
    utc_now_z,
)
from operatorapp.task_payloads import (
    AppendFilePayload,
    CodexModifyPayload,
    CodexProjectPayload,
    DownloadFilePayload,
    ExtractArchivePayload,
    FilesystemCreatePayload,
    FilesystemMutatePayload,
    GitRepoPayload,
    PatchFilePayload,
    ProcessWaitPayload,
    ReadFilePayload,
    ShellCommandPayload,
    WriteFilePayload,
)


def _make_task(*, job_id: str, group_index: int, task_index: int, kind: TaskKind, payload: object) -> TaskRecord:
    return TaskRecord(
        id=make_task_id(job_id, group_index, task_index),
        parent_job_id=job_id,
        parent_group_id=make_group_id(job_id, group_index),
        group_index=group_index,
        task_index=task_index,
        title=f"Task {task_index}",
        kind=kind,
        status=TaskStatus.ready,
        failure_policy=FailurePolicy.fail_fast,
        test_policy=TaskTestPolicy.auto,
        payload=payload,
    )


def test_utc_now_z_format() -> None:
    value = utc_now_z()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value)
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    assert parsed.year >= 2024


def test_job_group_task_id_generation_and_validation() -> None:
    job_id = make_job_id()
    assert re.fullmatch(r"[0-9a-f]{8}", job_id)
    assert make_group_id("abcd1234", 2) == "abcd1234_g002"
    assert make_task_id("abcd1234", 2, 5) == "abcd1234_g002_t005"

    with pytest.raises(ValueError):
        make_group_id("", 0)
    with pytest.raises(ValueError):
        make_group_id("abcd1234", -1)
    with pytest.raises(ValueError):
        make_task_id("abcd1234", -1, 0)
    with pytest.raises(ValueError):
        make_task_id("abcd1234", 0, -1)


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (TaskKind.shell_command, ShellCommandPayload(command="echo hello", cwd="/tmp", timeout_seconds=30)),
        (
            TaskKind.codex_project,
            CodexProjectPayload(
                objective="Scaffold package and tests",
                workspace_root="/tmp/project",
                test_command="pytest -q",
            ),
        ),
        (
            TaskKind.codex_modify,
            CodexModifyPayload(
                objective="Modify parser behavior",
                workspace_root="/tmp/project",
                target_paths=["parser.py"],
            ),
        ),
        (
            TaskKind.download_file,
            DownloadFilePayload(
                url="https://example.com/data.zip",
                destination_path="/tmp/data.zip",
                timeout_seconds=90,
            ),
        ),
        (
            TaskKind.filesystem_create,
            FilesystemCreatePayload(path="/tmp/new-dir", node_type="directory", exist_ok=True),
        ),
        (
            TaskKind.filesystem_mutate,
            FilesystemMutatePayload(
                operation="move",
                source_path="/tmp/a.txt",
                destination_path="/tmp/b.txt",
            ),
        ),
        (TaskKind.read_file, ReadFilePayload(path="/tmp/a.txt", max_bytes=1024)),
        (
            TaskKind.write_file,
            WriteFilePayload(path="/tmp/a.txt", content="hello", create_parents=True),
        ),
        (
            TaskKind.append_file,
            AppendFilePayload(path="/tmp/a.txt", content="\nworld", ensure_trailing_newline=True),
        ),
        (
            TaskKind.patch_file,
            PatchFilePayload(path="/tmp/a.txt", patch="@@ -1 +1 @@\n-a\n+b\n"),
        ),
        (
            TaskKind.extract_archive,
            ExtractArchivePayload(
                archive_path="/tmp/data.tar.gz",
                destination_dir="/tmp/extracted",
                format="tar.gz",
                overwrite=False,
            ),
        ),
        (
            TaskKind.git_repo,
            GitRepoPayload(
                operation="clone",
                repo_url="https://github.com/example/repo.git",
                repo_path="/tmp/repo",
                ref="main",
            ),
        ),
        (
            TaskKind.process_wait,
            ProcessWaitPayload(process_name="build", timeout_seconds=300, poll_interval_seconds=1.0),
        ),
    ],
)
def test_task_payload_variants_construct_and_bind_to_task(kind: TaskKind, payload: object) -> None:
    task = _make_task(job_id="abcd1234", group_index=0, task_index=0, kind=kind, payload=payload)
    assert task.kind is kind
    assert task.payload.kind == kind.value


def test_discriminated_union_validation_rejects_mismatch() -> None:
    with pytest.raises(ValidationError):
        _make_task(
            job_id="abcd1234",
            group_index=0,
            task_index=0,
            kind=TaskKind.shell_command,
            payload=CodexModifyPayload(
                objective="Edit code",
                workspace_root="/tmp/project",
                target_paths=["x.py"],
            ),
        )

    with pytest.raises(ValidationError):
        TaskRecord(
            id="abcd1234_g000_t000",
            parent_job_id="abcd1234",
            parent_group_id="abcd1234_g000",
            group_index=0,
            task_index=0,
            title="Bad payload",
            kind=TaskKind.shell_command,
            payload={"kind": "not_a_kind", "foo": "bar"},
        )


def test_job_record_round_trip_with_task_groups() -> None:
    job_id = "abcd1234"
    group_0_id = make_group_id(job_id, 0)
    group_1_id = make_group_id(job_id, 1)

    task_a = _make_task(
        job_id=job_id,
        group_index=0,
        task_index=0,
        kind=TaskKind.download_file,
        payload=DownloadFilePayload(
            url="https://example.com/app.zip",
            destination_path="/tmp/app.zip",
            timeout_seconds=120,
        ),
    )
    task_b = _make_task(
        job_id=job_id,
        group_index=1,
        task_index=0,
        kind=TaskKind.codex_project,
        payload=CodexProjectPayload(
            objective="Set up and implement parser",
            workspace_root="/tmp/app",
            test_command="pytest -q",
        ),
    )

    group_0 = TaskGroupRecord(
        id=group_0_id,
        parent_job_id=job_id,
        group_index=0,
        title="Fetch source bundle",
        status=TaskGroupStatus.complete,
        tasks=[task_a],
    )
    group_1 = TaskGroupRecord(
        id=group_1_id,
        parent_job_id=job_id,
        group_index=1,
        title="Implement code",
        status=TaskGroupStatus.ready,
        depends_on_group_ids=[group_0_id],
        tasks=[task_b],
    )

    job = JobRecord(
        id=job_id,
        user_prompt="Download package then implement feature",
        summary="Two-group DAG plan.",
        status=JobStatus.in_progress,
        task_groups=[group_0, group_1],
    )

    restored = JobRecord.model_validate_json(job.model_dump_json())
    assert restored == job
    assert restored.task_groups[1].depends_on_group_ids == [group_0_id]
    assert restored.task_groups[1].tasks[0].payload.kind == TaskKind.codex_project.value


def test_progress_and_timeout_fields_validation() -> None:
    progress = TaskProgress(
        state_message="Downloading",
        percent=42.5,
        bytes_completed=425,
        bytes_total=1000,
        current_operation="download_chunk",
    )
    assert progress.percent == 42.5

    started = utc_now_z()
    result = CommandResult(
        command="curl -L https://example.com/file.zip -o file.zip",
        cwd="/tmp",
        stdout="",
        stderr="timed out",
        exit_code=124,
        started_at=started,
        finished_at=started,
        duration_seconds=12.0,
        timed_out=True,
        timeout_seconds=10,
    )
    assert result.timed_out is True
    assert result.timeout_seconds == 10

    with pytest.raises(ValidationError):
        TaskProgress(percent=120.0)
    with pytest.raises(ValidationError):
        TaskProgress(bytes_completed=11, bytes_total=10)
    with pytest.raises(ValidationError):
        CommandResult(
            command="sleep 5",
            exit_code=124,
            started_at=started,
            finished_at=started,
            duration_seconds=5.0,
            timed_out=True,
        )
