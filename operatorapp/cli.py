"""Command-line interface for Operator."""

from __future__ import annotations

import argparse

from openai import OpenAIError

from .app import OperatorApp
from .codex_executor import CodexTaskExecutor
from .config import load_config
from .openai_client import OpenAIJSONClient
from .planner import PlannerService
from .schemas import JobStatus
from .shell_executor import ShellTaskExecutor


def main(argv: list[str] | None = None) -> int:
    """Run the Operator CLI."""
    parser = argparse.ArgumentParser(prog="operator")
    parser.add_argument("prompt")
    args = parser.parse_args(argv)

    print(f'Operator: received prompt "{args.prompt}"')
    print("Operator: loading configuration...")
    try:
        config = load_config()
    except Exception as exc:
        print(f"Operator: failed to load config ({type(exc).__name__}: {exc}).")
        return 2

    print("Operator: initializing planner and executors...")
    if not config.openai_api_key.strip():
        print("Operator: openai_api_key is missing in config.json.")
        print("Operator: set openai_api_key and retry.")
        return 2

    try:
        planner_llm = OpenAIJSONClient(
            model=config.planner_model, api_key=config.openai_api_key
        )
        shell_llm = OpenAIJSONClient(
            model=config.shell_model, api_key=config.openai_api_key
        )
    except Exception as exc:
        print(f"Operator: unable to initialize OpenAI clients ({type(exc).__name__}: {exc}).")
        return 2

    planner = PlannerService(planner_llm)
    shell_executor = ShellTaskExecutor(shell_llm, config)
    codex_executor = CodexTaskExecutor(config)
    app = OperatorApp(planner, shell_executor, codex_executor, config)

    print("Operator: running job...")
    try:
        job = app.run(args.prompt)
    except KeyboardInterrupt:
        print("Operator interrupted.")
        return 130
    except OpenAIError as exc:
        print(f"Operator: OpenAI request failed during execution ({exc}).")
        return 1
    except Exception as exc:
        print(f"Operator: execution failed ({type(exc).__name__}: {exc}).")
        return 1

    print(
        f"Operator: job {job.id} finished with status {job.status.value} "
        f"across {len(job.task_groups)} group(s)."
    )
    if job.status is JobStatus.failed:
        return 1
    if job.status is JobStatus.complete:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
