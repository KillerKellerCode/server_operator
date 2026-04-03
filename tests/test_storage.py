from __future__ import annotations

from pathlib import Path

from operatorapp.schemas import JobRecord, TaskKind, TaskRecord, utc_now_z
from operatorapp.storage import create_run_paths, load_job, load_task, save_job, save_task


def test_create_run_paths_creates_directories_and_returns_absolute(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path / "runs"), "jobabcd1")

    assert Path(paths.base_dir).is_absolute()
    assert Path(paths.job_dir).is_absolute()
    assert Path(paths.tasks_dir).is_absolute()
    assert Path(paths.job_dir).is_dir()
    assert Path(paths.tasks_dir).is_dir()


def test_save_and_load_job_round_trip(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    now = utc_now_z()
    job = JobRecord(
        id="jobabcd1",
        user_prompt="run this",
        created_at=now,
        updated_at=now,
    )

    job_json_path = save_job(paths, job)
    loaded = load_job(job_json_path)
    file_text = Path(job_json_path).read_text(encoding="utf-8")

    assert loaded.model_dump(mode="json") == job.model_dump(mode="json")
    assert job_json_path == str(Path(paths.job_dir) / "job.json")
    assert '\n  "id": "jobabcd1"' in file_text


def test_save_and_load_task_round_trip(tmp_path: Path) -> None:
    paths = create_run_paths(str(tmp_path), "jobabcd1")
    task = TaskRecord(
        id="jobabcd1_t000",
        parent_job_id="jobabcd1",
        task_index=0,
        title="Create directories",
        kind=TaskKind.shell,
        instructions="mkdir -p data",
    )

    task_json_path = save_task(paths, task)
    loaded = load_task(task_json_path)
    task_dir = Path(paths.tasks_dir) / task.id
    file_text = Path(task_json_path).read_text(encoding="utf-8")

    assert task_dir.is_dir()
    assert loaded.model_dump(mode="json") == task.model_dump(mode="json")
    assert task_json_path == str(task_dir / "task.json")
    assert '\n  "id": "jobabcd1_t000"' in file_text

