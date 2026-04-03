# Operator

Operator is a sequential local server agent CLI.
You run `operator "prompt"` and Operator plans a job, executes tasks in order, and persists logs and state locally.

## CLI Usage

```bash
operator "hello world"
```

Direct module form:

```bash
python -m operatorapp.cli "hello world"
```

## Architecture

- `PlannerService` turns a user prompt into an ordered `JobRecord` + `TaskRecord` list.
- `OperatorApp` executes tasks strictly by ascending `task_index`.
- `ShellTaskExecutor` handles `TaskKind.shell` by iterating with LLM-guided shell steps and command-history feedback.
- `CodexTaskExecutor` handles `TaskKind.codex` by delegating to `codex exec` as a black-box worker.
- `storage` and `logging_utils` persist all job/task state and human-readable logs under one run directory.

## On-Disk Run Layout

```
<operator_home>/<job_id>/
  job.json
  run.log
  tasks/
    <task_id>/
      task.json
      task.log
      llm_prompts.jsonl
      llm_responses.jsonl
```

## Example Prompt

`operator "Set up a Python scraper project, then implement a parser module with tests."`

## Example Planned Tasks

```text
1. shell  - Create project directory and virtualenv
2. shell  - Install required dependencies
3. codex  - Implement parser module and tests
```

## Example Shell Task History Object

```json
{
  "timestamp": "2026-04-02T18:22:10Z",
  "sequential": true,
  "actions": [
    {
      "action_type": "shell_command",
      "shell_command": "mkdir -p src",
      "output": "",
      "stderr": "",
      "exit_code": 0
    }
  ]
}
```

## Example Codex Task Description

```text
Title: Implement parser
Kind: codex
Instructions: Add parser module, wire CLI integration, and update tests.
Workdir: /home/user/project
Test policy: always
```

## Configuration

Operator loads runtime settings from project-root `config.json`.

Default `config.json`:

```json
{
  "operator_home": "~/.operator/runs",
  "openai_api_key": "",
  "planner_model": "gpt-4.1-mini",
  "shell_model": "gpt-4.1-mini",
  "codex_command": "codex",
  "codex_bypass_approvals": false,
  "command_timeout_seconds": 600
}
```

Set `openai_api_key` in `config.json` to enable live OpenAI-backed planning/execution.

## Task Types

- `shell` tasks: local shell-command work (filesystem, installs, downloads, software invocation).
- `codex` tasks: delegated coding work executed through Codex CLI.

## v1 Limitations

- Sequential execution only (no concurrency).
- No retries framework.
- No background daemon mode.
- No web UI or database.
- LLM-dependent planning/execution requires live API access.
- Shell task adaptation is single-threaded and step-wise only.
- Codex execution is treated as opaque process output (no fine-grained event parsing).

## Run Tests

```bash
./venv/bin/python -m pytest
```
