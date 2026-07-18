"""Scaffold smoke tests — the package imports and the CLI parser builds."""

from __future__ import annotations

import pytest

import tickflow
from tickflow.cli import build_parser, main


def test_version_exposed() -> None:
    assert tickflow.__version__ == "0.1.0.dev0"


def test_parser_builds() -> None:
    parser = build_parser()
    assert parser.prog == "tickflow"


def test_version_flag_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert "tickflow" in capsys.readouterr().out


def test_no_command_prints_help() -> None:
    assert main([]) == 1
