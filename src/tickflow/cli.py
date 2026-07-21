"""tickflow command-line entry point.

Subcommands are registered here and delegate to focused modules. Day A wires the ingester
(`tickflow ingest`); Day B adds the contract, the quality gate (`tickflow gate`), and the
synthetic fixture (`tickflow fixture`); Day C adds the bar builder (`tickflow bars`) and the
fault-injection grading (`tickflow metrics`); replay and export land in later phases per the
frozen build order (docs/architecture.md §11).
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from tickflow import __version__, bars, capture, contracts, fixture, gate, ingest, metrics, sanity


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tickflow",
        description="Quality-gate and contract-enforcement layer for Kafka-compatible streams.",
    )
    parser.add_argument("--version", action="version", version=f"tickflow {__version__}")
    subparsers = parser.add_subparsers(dest="command")
    ingest.register(subparsers)
    capture.register(subparsers)
    sanity.register(subparsers)
    contracts.register(subparsers)
    gate.register(subparsers)
    fixture.register(subparsers)
    bars.register(subparsers)
    bars.register_slo(subparsers)
    metrics.register(subparsers)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1
    result = handler(args)
    return int(result or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
