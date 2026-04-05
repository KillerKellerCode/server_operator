from __future__ import annotations

import pytest

from operatorapp.executor_registry import ExecutorRegistry
from operatorapp.schemas import TaskKind


class _FakeExecutor:
    def __init__(self, kind: TaskKind) -> None:
        self.kind = kind

    def execute(self, paths, job, group, task):  # pragma: no cover - not used in registry tests
        return task


def test_registry_lookup_returns_registered_executor() -> None:
    shell_executor = _FakeExecutor(TaskKind.shell_command)
    codex_executor = _FakeExecutor(TaskKind.codex_modify)
    registry = ExecutorRegistry([shell_executor, codex_executor])

    assert registry.get(TaskKind.shell_command) is shell_executor
    assert registry.get(TaskKind.codex_modify) is codex_executor
    assert registry.kinds() == [TaskKind.shell_command, TaskKind.codex_modify]


def test_registry_get_raises_clear_error_for_unsupported_kind() -> None:
    registry = ExecutorRegistry([_FakeExecutor(TaskKind.shell_command)])

    with pytest.raises(ValueError, match=r"unsupported task kind: codex_project"):
        registry.get(TaskKind.codex_project)


def test_registry_rejects_duplicate_kind_registration() -> None:
    with pytest.raises(ValueError, match=r"duplicate executor for task kind: shell_command"):
        ExecutorRegistry(
            [
                _FakeExecutor(TaskKind.shell_command),
                _FakeExecutor(TaskKind.shell_command),
            ]
        )

