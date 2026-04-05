"""Shared executor protocol types for V2 task dispatch."""

from __future__ import annotations

from typing import Protocol

from .schemas import JobRecord, TaskGroupRecord, TaskKind, TaskRecord
from .storage import RunPaths


class TaskExecutor(Protocol):
    kind: TaskKind

    def execute(
        self, paths: RunPaths, job: JobRecord, group: TaskGroupRecord, task: TaskRecord
    ) -> TaskRecord:
        ...

