from __future__ import annotations

import pytest

from operatorapp.openai_client import OpenAIJSONClient
from operatorapp.planner_models import JobPlanResponse


class _FakeParsedResponse:
    def __init__(self, output_parsed) -> None:
        self.output_parsed = output_parsed


class _FakeResponsesAPI:
    def __init__(self, parsed_response: _FakeParsedResponse) -> None:
        self._parsed_response = parsed_response
        self.calls: list[dict[str, object]] = []

    def parse(self, **kwargs):
        self.calls.append(kwargs)
        return self._parsed_response


class _FakeOpenAI:
    def __init__(self, parsed_response: _FakeParsedResponse) -> None:
        self.responses = _FakeResponsesAPI(parsed_response)


def test_constructor_stores_model(monkeypatch) -> None:
    fake = _FakeOpenAI(_FakeParsedResponse(output_parsed={}))
    monkeypatch.setattr(
        "operatorapp.openai_client.OpenAI",
        lambda: fake,
    )

    client = OpenAIJSONClient(model="gpt-test")
    assert client.model == "gpt-test"


def test_complete_json_returns_requested_model_type(monkeypatch) -> None:
    parsed = JobPlanResponse(summary="Simple plan", groups=[])
    fake = _FakeOpenAI(_FakeParsedResponse(output_parsed=parsed))
    monkeypatch.setattr(
        "operatorapp.openai_client.OpenAI",
        lambda: fake,
    )
    client = OpenAIJSONClient(model="gpt-test")

    result = client.complete_json(
        system_prompt="system prompt",
        user_prompt="user prompt",
        response_model=JobPlanResponse,
    )

    assert isinstance(result, JobPlanResponse)
    assert result.summary == "Simple plan"
    assert len(fake.responses.calls) == 1
    call = fake.responses.calls[0]
    assert call["model"] == "gpt-test"
    assert call["input"] == [
        {"role": "system", "content": "system prompt"},
        {"role": "user", "content": "user prompt"},
    ]
    assert call["text_format"] is JobPlanResponse


def test_complete_json_raises_on_empty_or_invalid_output(monkeypatch) -> None:
    fake = _FakeOpenAI(_FakeParsedResponse(output_parsed=None))
    monkeypatch.setattr(
        "operatorapp.openai_client.OpenAI",
        lambda: fake,
    )
    client = OpenAIJSONClient(model="gpt-test")

    with pytest.raises(ValueError, match="empty or invalid"):
        client.complete_json(
            system_prompt="system prompt",
            user_prompt="user prompt",
            response_model=JobPlanResponse,
        )


def test_constructor_passes_api_key(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_openai(*, api_key=None):
        captured["api_key"] = api_key
        return _FakeOpenAI(_FakeParsedResponse(output_parsed={}))

    monkeypatch.setattr("operatorapp.openai_client.OpenAI", fake_openai)

    OpenAIJSONClient(model="gpt-test", api_key="sk-test")
    assert captured["api_key"] == "sk-test"
