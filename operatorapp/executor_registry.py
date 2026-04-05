"""Deterministic task-executor registry for V2 dispatch."""

from __future__ import annotations

from .executor_types import TaskExecutor
from .schemas import TaskKind


class ExecutorRegistry:
    def __init__(self, executors: list[TaskExecutor]) -> None:
        by_kind: dict[TaskKind, TaskExecutor] = {}
        for executor in executors:
            if executor.kind in by_kind:
                raise ValueError(f"duplicate executor for task kind: {executor.kind.value}")
            by_kind[executor.kind] = executor
        self._by_kind = by_kind

    def get(self, kind: TaskKind) -> TaskExecutor:
        executor = self._by_kind.get(kind)
        if executor is None:
            supported = ", ".join(task_kind.value for task_kind in self.kinds())
            raise ValueError(
                f"unsupported task kind: {kind.value}; supported kinds: [{supported}]"
            )
        return executor

    def kinds(self) -> list[TaskKind]:
        return list(self._by_kind.keys())

