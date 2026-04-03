from __future__ import annotations

import json
from pathlib import Path

import pytest

from operatorapp.config import load_config


def test_load_config_from_project_json(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "operator_home": "~/custom-operator",
                "openai_api_key": "sk-test",
                "planner_model": "planner-x",
                "shell_model": "shell-x",
                "codex_command": "codex --quiet",
                "codex_bypass_approvals": True,
                "command_timeout_seconds": 42,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: config_path)

    config = load_config()

    assert config.operator_home == str(Path("~/custom-operator").expanduser())
    assert config.openai_api_key == "sk-test"
    assert config.planner_model == "planner-x"
    assert config.shell_model == "shell-x"
    assert config.codex_command == "codex --quiet"
    assert config.codex_bypass_approvals is True
    assert config.command_timeout_seconds == 42


def test_load_config_ignores_environment_vars(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "operator_home": "~/.operator/runs",
                "planner_model": "planner-model",
                "shell_model": "shell-model",
                "codex_command": "codex",
                "codex_bypass_approvals": False,
                "command_timeout_seconds": 600,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: config_path)
    monkeypatch.setenv("OPERATOR_PLANNER_MODEL", "should-not-be-used")

    config = load_config()

    assert config.planner_model == "planner-model"
    assert config.openai_api_key == ""


def test_load_config_requires_file(monkeypatch, tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.json"
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: missing_path)

    with pytest.raises(FileNotFoundError):
        load_config()
