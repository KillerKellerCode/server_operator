"""OpenAI-backed JSON client implementation."""

from __future__ import annotations

from openai import OpenAI

from .llm_types import JSONLLMClient, T


class OpenAIJSONClient(JSONLLMClient):
    def __init__(self, model: str, api_key: str | None = None) -> None:
        if not model or not model.strip():
            raise ValueError("model must be a non-empty string")
        self.model = model
        self._client = OpenAI(api_key=api_key) if api_key is not None else OpenAI()

    def complete_json(
        self, *, system_prompt: str, user_prompt: str, response_model: type[T]
    ) -> T:
        response = self._client.responses.parse(
            model=self.model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            text_format=response_model,
        )
        parsed = getattr(response, "output_parsed", None)
        if parsed is None:
            raise ValueError("OpenAI JSON response was empty or invalid for the requested model.")
        if isinstance(parsed, response_model):
            return parsed
        try:
            return response_model.model_validate(parsed)  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - defensive branch
            raise ValueError(
                "OpenAI JSON response could not be parsed into the requested model."
            ) from exc
