"""Prompt templates for Operator V2 planning and task execution."""

from __future__ import annotations

import json

from .schemas import TaskKind, TaskRecord


def build_job_planner_system_prompt() -> str:
    """Return system prompt for converting user input into a V2 group/DAG plan."""
    return (
        "You are the Operator V2 job planner.\n"
        "Convert one user request into ordered task groups with dependency graph edges.\n"
        "Output JSON matching JobPlanResponse and no extra text.\n"
        "Return a single JSON object only. Do not use markdown fences.\n"
        "Plan low-count, concrete, executable groups and tasks.\n"
        "Prefer native typed task kinds for common operations instead of shell commands:\n"
        "- download_file, filesystem_create, filesystem_mutate, read_file, write_file, append_file, patch_file, extract_archive, git_repo, process_wait\n"
        "Use shell_command only when no better native typed task applies.\n"
        "Use codex_project when creating a coding project/workspace from scratch.\n"
        "Use codex_modify for targeted code changes in an existing codebase.\n"
        "Each task payload must be explicit and minimal for its task kind.\n"
        "Set payload.kind and ensure it exactly matches the task kind.\n"
        "Required payload fields by kind:\n"
        "- shell_command: command\n"
        "- codex_project: objective, workspace_root\n"
        "- codex_modify: objective, workspace_root\n"
        "- download_file: url, destination_path\n"
        "- filesystem_create: path, node_type\n"
        "- filesystem_mutate: operation, source_path\n"
        "- read_file: path\n"
        "- write_file: path, content\n"
        "- append_file: path, content\n"
        "- patch_file: path, patch\n"
        "- extract_archive: archive_path, destination_dir, format\n"
        "- git_repo: operation, repo_path\n"
        "- process_wait: process_name, timeout_seconds\n"
        "Set workdir when obvious.\n"
        "Default shell timeout is 600 seconds unless overridden.\n"
        "For likely long-running actions, either choose a better native task kind or request a larger timeout_seconds.\n"
        "Use test_policy:\n"
        "- always for code that should definitely be tested\n"
        "- never for docs-only changes\n"
        "- auto otherwise\n"
        "Use failure_policy:\n"
        "- fail_fast by default\n"
        "- continue_group or continue_job only when explicitly useful\n"
        "Use depends_on_group_indexes to define group dependencies by index.\n"
        "Do not invent fields not present in JobPlanResponse."
    )


def build_job_planner_user_prompt(user_prompt: str) -> str:
    """Return planner user prompt with the original request."""
    return (
        "Create a JobPlanResponse for this user request.\n"
        f"User request:\n{user_prompt}\n"
        "Return only JSON that conforms to JobPlanResponse."
    )


def build_shell_step_system_prompt(
    default_timeout_seconds: int, max_timeout_seconds: int
) -> str:
    """Return system prompt for producing the next shell_command actions."""
    return (
        "You are the Operator shell-step planner for shell_command tasks only.\n"
        "Output JSON matching ShellStepResponse and no extra text.\n"
        "Return a single JSON object only. Do not use markdown fences.\n"
        "Propose only the next commands needed, with fewer concrete commands.\n"
        "Adapt to previous command outputs from task history.\n"
        "Default per-command timeout is "
        f"{default_timeout_seconds} seconds.\n"
        "You may set action.timeout_seconds when justified.\n"
        "Never exceed "
        f"{max_timeout_seconds} seconds for action.timeout_seconds.\n"
        "Mark complete when objective is satisfied.\n"
        "Mark failed only when task cannot reasonably proceed.\n"
        "Keep commands idempotent where practical.\n"
        "Avoid explanations outside JSON.\n"
        "Use only fields from ShellStepResponse."
    )


def build_shell_step_user_prompt(task: TaskRecord) -> str:
    """Return shell-step user prompt with authoritative task history."""
    history_raw = getattr(task, "history", [])
    history_serializable: list[object] = []
    for item in history_raw:
        if hasattr(item, "model_dump"):
            history_serializable.append(item.model_dump(mode="json"))
        else:
            history_serializable.append(item)
    history_json = json.dumps(
        history_serializable, ensure_ascii=False, separators=(",", ":")
    )
    payload_json = json.dumps(task.payload.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":"))

    lines = [
        "Plan the next shell command step for this task.",
        f"Task id: {task.id}",
        f"Task title: {task.title}",
        f"Task kind: {task.kind.value}",
        f"Task payload (compact JSON): {payload_json}",
    ]
    if task.workdir:
        lines.append(f"Task workdir: {task.workdir}")
    if task.timeout_seconds is not None:
        lines.append(f"Task timeout_seconds override: {task.timeout_seconds}")
    lines.extend(
        [
            f"Task history (compact JSON): {history_json}",
            "History is authoritative and must be treated as the source of truth.",
            "Return only JSON that conforms to ShellStepResponse.",
        ]
    )
    return "\n".join(lines)


def build_codex_task_prompt(task: TaskRecord) -> str:
    """Return a coding prompt for Codex scoped to a codex task payload."""
    if task.kind not in (TaskKind.codex_project, TaskKind.codex_modify):
        raise ValueError("build_codex_task_prompt requires codex_project or codex_modify task kind")

    payload_json = json.dumps(task.payload.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True)
    lines = [
        "You are implementing one Operator codex task.",
        f"Task id: {task.id}",
        f"Task kind: {task.kind.value}",
        f"Task title: {task.title}",
        "Keep scope tight to the task payload.",
        "Task payload:",
        payload_json,
    ]

    if task.workdir:
        lines.append(f"Use this task workdir as the project root: {task.workdir}")

    if task.kind is TaskKind.codex_project:
        lines.extend(
            [
                "You own coding-project workspace setup end-to-end for this task.",
                "You own scaffold creation, implementation, dependency install, and validation within scope.",
            ]
        )
    else:
        lines.extend(
            [
                "Modify the existing project in place.",
                "Do not re-scaffold or restructure unrelated areas.",
            ]
        )

    lines.append("Acceptance criteria:")
    lines.append("- Complete exactly the objective implied by the payload without unrelated changes.")
    lines.append("- Summarize files changed and final validation status.")

    if task.test_policy.value == "always":
        lines.append("Test behavior: run tests for this task before finishing.")
    elif task.test_policy.value == "never":
        lines.append("Test behavior: do not run tests for this task.")
    else:
        lines.append("Test behavior: run only the most relevant tests for this task.")

    return "\n".join(lines)
