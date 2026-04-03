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
    config = load_config()

    print("Operator: initializing planner and executors...")
    if not config.openai_api_key.strip():
        print("Operator: openai_api_key is missing in config.json.")
        print("Operator: set openai_api_key and retry.")
        return 0

    try:
        planner_llm = OpenAIJSONClient(
            model=config.planner_model, api_key=config.openai_api_key
        )
        shell_llm = OpenAIJSONClient(
            model=config.shell_model, api_key=config.openai_api_key
        )
    except OpenAIError as exc:
        print(f"Operator: unable to initialize OpenAI clients ({exc}).")
        print("Operator: skipping execution because no live LLM client is available.")
        return 0

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
        print("Operator: skipping execution because live LLM requests are unavailable.")
        return 0

    print(f"Operator: job {job.id} finished with status {job.status.value}.")
    if job.status is JobStatus.failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
