"""Scaffold smoke tests — the package imports and the CLI parser builds."""

from __future__ import annotations

import pathlib

import pytest

import tickflow
from tickflow.cli import build_parser, main


def test_version_exposed_and_matches_packaging() -> None:
    """The package version and pyproject must agree.

    Asserted as consistency rather than against a hardcoded literal: a pinned literal only ever
    catches "someone forgot to edit this test", while the real failure worth catching is the two
    sources of truth drifting apart at release time.
    """
    import tomllib

    assert tickflow.__version__
    pyproject = tomllib.loads(
        (pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml").read_text()
    )
    assert tickflow.__version__ == pyproject["project"]["version"]


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
