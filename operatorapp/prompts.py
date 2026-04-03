"""Prompt templates for Operator planning and task execution."""

from __future__ import annotations

import json

from .schemas import TaskRecord


def build_job_planner_system_prompt() -> str:
    """Return system prompt for converting user input into a job plan."""
    return (
        "You are the Operator job planner.\n"
        "Convert one user request into an ordered job plan.\n"
        "output JSON matching JobPlanResponse and no extra text.\n"
        "Return a single JSON object only. Do not use markdown fences.\n"
        "Choose between shell and codex tasks.\n"
        "Keep the number of tasks low.\n"
        "Keep tasks concrete and executable.\n"
        "Use short, action-oriented task titles.\n"
        "Default to shell tasks for installs, file ops, downloads, and running software.\n"
        "Default to codex tasks for writing or modifying code.\n"
        "Ensure tasks are sequential and dependency-aware.\n"
        "Set workdir when a directory is obvious.\n"
        "Set test_policy as follows:\n"
        "- always for code that should definitely be tested\n"
        "- never for docs-only work\n"
        "- auto otherwise\n"
        "Use only valid JSON types and fields from JobPlanResponse."
    )


def build_job_planner_user_prompt(user_prompt: str) -> str:
    """Return planner user prompt with the original request."""
    return (
        "Create a job plan for this user request.\n"
        f"User request:\n{user_prompt}\n"
        "Return only JSON that conforms to JobPlanResponse."
    )


def build_shell_step_system_prompt() -> str:
    """Return system prompt for producing the next shell actions."""
    return (
        "You are the Operator shell-step planner.\n"
        "output JSON matching ShellStepResponse and no extra text.\n"
        "Return a single JSON object only. Do not use markdown fences.\n"
        "Propose only the next commands needed.\n"
        "Adapt to previous command outputs.\n"
        "Mark complete when the task objective is satisfied.\n"
        "Mark failed only when the task cannot reasonably proceed.\n"
        "Keep commands minimal and idempotent where practical.\n"
        "Do not invent fields not present in ShellStepResponse.\n"
        "Avoid explanations outside JSON.\n"
        "Use only valid JSON types and fields from ShellStepResponse."
    )


def build_shell_step_user_prompt(task: TaskRecord) -> str:
    """Return shell-step user prompt with authoritative task history."""
    history_json = json.dumps(
        [exchange.model_dump(mode="json") for exchange in task.history],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    lines = [
        "Plan the next shell step for this task.",
        f"Task id: {task.id}",
        f"Task instructions: {task.instructions}",
    ]
    if task.workdir:
        lines.append(f"Task workdir: {task.workdir}")
    lines.extend(
        [
            f"Task history (compact JSON): {history_json}",
            "History is authoritative and must be treated as the source of truth.",
            "Return only JSON that conforms to ShellStepResponse.",
        ]
    )
    return "\n".join(lines)


def build_codex_task_prompt(task: TaskRecord) -> str:
    """Return a coding prompt for Codex scoped to this task."""
    lines = [
        "You are implementing one Operator codex task.",
        f"Task id: {task.id}",
        f"Objective: {task.instructions}",
        "Stay within this task scope and do not implement unrelated features.",
    ]
    if task.workdir:
        lines.append(f"Use this task workdir as the project root: {task.workdir}")

    if task.test_policy.value == "always":
        lines.append("Run tests for this task before finishing.")
    elif task.test_policy.value == "never":
        lines.append("Do not run tests for this task.")
    else:
        lines.append("Run only the most relevant tests for this task.")

    lines.append("Summarize files changed and final validation status.")
    return "\n".join(lines)
