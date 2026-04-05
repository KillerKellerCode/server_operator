# Operator

Operator is a local Python CLI agent that plans and executes work as a dependency-aware task graph.

## CLI Usage

```bash
operator "your prompt"
python -m operatorapp.cli "your prompt"
```

## V2 Runtime Model

V2 runs jobs as ordered task groups with explicit dependencies:

- A job has `task_groups`.
- A group becomes runnable when all `depends_on_group_ids` are complete.
- Tasks inside a ready group can run concurrently.
- Groups can overlap in time; there are no stage barriers.
- Fail-fast task failures can terminate the group/job early.

This replaces the old V1 flat `for task in ordered_tasks` execution model.

## Task Kinds

Current task kinds:

- `shell_command`
- `codex_project`
- `codex_modify`
- `download_file`
- `filesystem_create`
- `filesystem_mutate`
- `read_file`
- `write_file`
- `append_file`
- `patch_file`
- `extract_archive`
- `git_repo`
- `process_wait`

## Codex vs Native vs Shell

- `codex_project`: Codex owns workspace setup, scaffold, implementation, dependency install, tests/validation within task scope.
- `codex_modify`: Codex performs targeted in-place modifications to an existing codebase.
- Native task kinds (`download_file`, filesystem/file I/O/archive/git/wait) are explicit Python executors for deterministic operations.
- `shell_command` is a narrower fallback for true shell needs, not a catch-all for common local operations.

## Timeout Semantics

Shell command timeout resolution (highest priority first):

1. Planner action override (`ShellStepAction.timeout_seconds`)
2. `ShellCommandPayload.timeout_seconds`
3. `TaskRecord.timeout_seconds`
4. `OperatorConfig.default_command_timeout_seconds`

The final value is capped by `OperatorConfig.max_command_timeout_seconds`.

Codex task timeout:

- `TaskRecord.timeout_seconds` if set
- otherwise `OperatorConfig.default_codex_timeout_seconds`

## Run Directory Layout

Each run is persisted under:

```
<operator_home>/<job_id>/
  job.json
  events.jsonl
  run.log
  groups/
    <group_id>/
      group.json
  tasks/
    <task_id>/
      task.json
      task.log
      llm_prompts.jsonl
      llm_responses.jsonl
```

- `events.jsonl` is append-only structured event history for future UI/daemon consumers.
- `run.log` and `task.log` remain human-readable operational logs.

## Configuration (`config.json`)

Operator reads config from project-root `config.json`.

```json
{
  "operator_home": "~/.operator/runs",
  "openai_api_key": "",
  "planner_model": "gpt-4.1-mini",
  "shell_model": "gpt-4.1-mini",
  "codex_command": "codex",
  "codex_bypass_approvals": false,
  "default_command_timeout_seconds": 600,
  "max_command_timeout_seconds": 7200,
  "default_codex_timeout_seconds": 7200,
  "max_concurrent_tasks": 4,
  "max_concurrent_codex_tasks": 1
}
```

## V3 Direction (Not Implemented Yet)

V2 prepares for V3 by persisting structured events and keeping scheduler/executor boundaries clear.

Not implemented in V2:

- Web UI
- Daemon/service mode
- Distributed worker system
- Database backend

## Running Tests

```bash
./venv/bin/python -m pytest
```
