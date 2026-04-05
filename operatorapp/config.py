"""Configuration loading for Operator."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class OperatorConfig(BaseModel):
    operator_home: str = Field(min_length=1)
    openai_api_key: str = ""
    planner_model: str = Field(min_length=1)
    shell_model: str = Field(min_length=1)
    codex_command: str = Field(min_length=1)
    codex_bypass_approvals: bool
    default_command_timeout_seconds: int = Field(ge=1)
    max_command_timeout_seconds: int = Field(ge=1)
    default_codex_timeout_seconds: int = Field(ge=1)
    max_concurrent_tasks: int = Field(ge=1)
    max_concurrent_codex_tasks: int = Field(ge=1)

    model_config = ConfigDict(extra="forbid")


def _project_config_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config.json"


def load_config() -> OperatorConfig:
    """Load OperatorConfig from project-root config.json."""
    config_path = _project_config_path()
    if not config_path.exists():
        raise FileNotFoundError(
            f"Missing configuration file: {config_path}. Create config.json in the project root."
        )

    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("config.json must contain a JSON object at the top level")

    defaults = {
        "operator_home": "~/.operator/runs",
        "openai_api_key": "",
        "planner_model": "gpt-4.1-mini",
        "shell_model": "gpt-4.1-mini",
        "codex_command": "codex",
        "codex_bypass_approvals": False,
        "default_command_timeout_seconds": 600,
        "max_command_timeout_seconds": 7200,
        "default_codex_timeout_seconds": 7200,
        "max_concurrent_tasks": 4,
        "max_concurrent_codex_tasks": 1,
    }

    # Backward-compatible migration for older config key.
    if "command_timeout_seconds" in raw and "default_command_timeout_seconds" not in raw:
        raw["default_command_timeout_seconds"] = raw["command_timeout_seconds"]
    raw.pop("command_timeout_seconds", None)

    merged = {**defaults, **raw}

    merged["operator_home"] = str(Path(str(merged["operator_home"])).expanduser())

    return OperatorConfig.model_validate(merged)
