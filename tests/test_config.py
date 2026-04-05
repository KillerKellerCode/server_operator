from __future__ import annotations

import json
from pathlib import Path

import pytest

from operatorapp.config import load_config


def test_load_config_defaults_from_project_json(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "planner_model": "planner-x",
                "shell_model": "shell-x",
                "codex_command": "codex",
                "codex_bypass_approvals": False,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: config_path)

    config = load_config()

    assert config.operator_home == str(Path("~/.operator/runs").expanduser())
    assert config.openai_api_key == ""
    assert config.default_command_timeout_seconds == 600
    assert config.max_command_timeout_seconds == 7200
    assert config.default_codex_timeout_seconds == 7200
    assert config.max_concurrent_tasks == 4
    assert config.max_concurrent_codex_tasks == 1


def test_load_config_overrides_fields(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "operator_home": "~/custom-operator",
                "openai_api_key": "sk-test",
                "planner_model": "planner-y",
                "shell_model": "shell-y",
                "codex_command": "codex --quiet",
                "codex_bypass_approvals": True,
                "default_command_timeout_seconds": 1200,
                "max_command_timeout_seconds": 2400,
                "default_codex_timeout_seconds": 3600,
                "max_concurrent_tasks": 8,
                "max_concurrent_codex_tasks": 2,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: config_path)

    config = load_config()
    assert config.operator_home == str(Path("~/custom-operator").expanduser())
    assert config.openai_api_key == "sk-test"
    assert config.planner_model == "planner-y"
    assert config.shell_model == "shell-y"
    assert config.codex_command == "codex --quiet"
    assert config.codex_bypass_approvals is True
    assert config.default_command_timeout_seconds == 1200
    assert config.max_command_timeout_seconds == 2400
    assert config.default_codex_timeout_seconds == 3600
    assert config.max_concurrent_tasks == 8
    assert config.max_concurrent_codex_tasks == 2


def test_load_config_migrates_legacy_command_timeout_key(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "planner_model": "planner-z",
                "shell_model": "shell-z",
                "codex_command": "codex",
                "codex_bypass_approvals": False,
                "command_timeout_seconds": 77,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: config_path)

    config = load_config()
    assert config.default_command_timeout_seconds == 77


def test_load_config_requires_file(monkeypatch, tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.json"
    monkeypatch.setattr("operatorapp.config._project_config_path", lambda: missing_path)

    with pytest.raises(FileNotFoundError):
        load_config()

