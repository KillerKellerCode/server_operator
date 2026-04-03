"""Types for JSON-capable LLM interactions."""

from __future__ import annotations

from typing import Protocol, TypeVar

T = TypeVar("T")


class JSONLLMClient(Protocol):
    def complete_json(
        self, *, system_prompt: str, user_prompt: str, response_model: type[T]
    ) -> T:
        """Return a model-conformant parsed response."""
        ...

