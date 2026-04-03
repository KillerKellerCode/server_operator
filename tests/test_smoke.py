from __future__ import annotations

import subprocess
import sys

from operatorapp.cli import main
from operatorapp.version import __version__


def test_version_is_non_empty_string() -> None:
    assert isinstance(__version__, str)
    assert __version__


def test_main_prints_placeholder_and_returns_zero(capsys) -> None:
    exit_code = main(["hello"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "hello" in captured.out


def test_module_execution_works() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "operatorapp.cli", "hello"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "hello" in result.stdout

