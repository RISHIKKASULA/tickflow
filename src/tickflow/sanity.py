"""tickflow sanity — the feed-sanity report for the blocking manual gate (frozen design §0).

Read a capture written by ``tickflow capture``, verify its checksum, and print — for the
four streams (2 exchanges x 2 symbols) — exactly the evidence §0 calls for so Rishik can make
the gate call:

- **message counts** per stream;
- **field-mapping samples** (up to 5 raw→normalized pairs per stream), so the normalization is
  reviewable by eye;
- **timestamp sanity**: event-time within ±60 s of wall clock (``ts_ingest``), and how
  monotone-ish the event-time series is;
- **trade_id** presence and uniqueness stats.

It also prints a *provisional* pass/fail against the frozen thresholds — both feeds present and
each of the 4 streams ≥ 50 messages — but the gate decision (field mapping correct → proceed)
is Rishik's manual call, recorded as ADR-001. This module is reporting glue and is
coverage-excluded (§9); its pure stat helpers are unit-tested.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tickflow.capture import DEFAULT_CAPTURE_ROOT, MANIFEST_FILE, STREAM_FILE, sha256_file
from tickflow.ingest import CANONICAL_SYMBOLS

EXPECTED_EXCHANGES: tuple[str, ...] = ("coinbase", "kraken")
EXPECTED_SYMBOLS: tuple[str, ...] = CANONICAL_SYMBOLS
DEFAULT_MIN_MESSAGES = 50  # frozen gate threshold (§0)
SKEW_TOLERANCE_MS = 60_000  # event-time within ±60 s of wall clock (§0)

StreamKey = tuple[str, str]


@dataclass
class StreamStats:
    """Per-stream (exchange, symbol) sanity figures over a capture."""

    exchange: str
    symbol: str
    count: int = 0
    unique_trade_ids: int = 0
    duplicate_trade_ids: int = 0
    missing_trade_ids: int = 0
    min_ts_event: int | None = None
    max_ts_event: int | None = None
    max_abs_skew_ms: int = 0
    within_60s: int = 0
    out_of_order: int = 0
    samples: list[tuple[dict[str, Any], dict[str, Any]]] = field(default_factory=list)

    @property
    def key(self) -> StreamKey:
        return (self.exchange, self.symbol)


def find_latest_capture(root: Path) -> Path | None:
    """The most-recently-modified capture directory under ``root`` (one with a manifest)."""
    if not root.is_dir():
        return None
    candidates = [d for d in root.iterdir() if d.is_dir() and (d / MANIFEST_FILE).is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda d: (d / MANIFEST_FILE).stat().st_mtime)


def load_manifest(capture_dir: Path) -> dict[str, Any]:
    manifest: dict[str, Any] = json.loads((capture_dir / MANIFEST_FILE).read_text())
    return manifest


def verify_checksum(capture_dir: Path, manifest: dict[str, Any]) -> tuple[bool, str, str]:
    """Return (ok, expected, actual) for the pinned SHA-256 over the stream file."""
    expected = str(manifest.get("sha256", ""))
    actual = sha256_file(capture_dir / manifest.get("stream_file", STREAM_FILE))
    return expected == actual, expected, actual


def read_records(capture_dir: Path, stream_file: str = STREAM_FILE) -> list[dict[str, Any]]:
    """Load the capture's JSONL records (each ``{"raw": ..., "norm": ...}``)."""
    records: list[dict[str, Any]] = []
    with (capture_dir / stream_file).open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def compute_stats(records: list[dict[str, Any]], samples: int = 5) -> dict[StreamKey, StreamStats]:
    """Fold capture records into per-stream stats, in capture (arrival) order.

    Duplicates and out-of-order are counted in arrival order so the figures reflect the feed's
    actual delivery behavior, not a re-sorted view of it.
    """
    stats: dict[StreamKey, StreamStats] = {}
    seen_ids: dict[StreamKey, set[str]] = defaultdict(set)
    running_max_event: dict[StreamKey, int] = {}

    for record in records:
        norm = record["norm"]
        key: StreamKey = (norm["exchange"], norm["symbol"])
        st = stats.get(key)
        if st is None:
            st = stats[key] = StreamStats(exchange=norm["exchange"], symbol=norm["symbol"])

        st.count += 1

        trade_id = str(norm.get("trade_id", ""))
        if not trade_id:
            st.missing_trade_ids += 1
        elif trade_id in seen_ids[key]:
            st.duplicate_trade_ids += 1
        else:
            seen_ids[key].add(trade_id)

        ts_event = int(norm["ts_event"])
        ts_ingest = int(norm["ts_ingest"])
        st.min_ts_event = ts_event if st.min_ts_event is None else min(st.min_ts_event, ts_event)
        st.max_ts_event = ts_event if st.max_ts_event is None else max(st.max_ts_event, ts_event)

        skew = abs(ts_event - ts_ingest)
        st.max_abs_skew_ms = max(st.max_abs_skew_ms, skew)
        if skew <= SKEW_TOLERANCE_MS:
            st.within_60s += 1

        prev = running_max_event.get(key)
        if prev is not None and ts_event < prev:
            st.out_of_order += 1
        running_max_event[key] = ts_event if prev is None else max(prev, ts_event)

        if len(st.samples) < samples:
            st.samples.append((record["raw"], norm))

    for st in stats.values():
        st.unique_trade_ids = len(seen_ids[st.key])
    return stats


@dataclass(frozen=True)
class GateResult:
    """Provisional gate outcome — the objective checks; the field-mapping call is manual."""

    checksum_ok: bool
    per_stream: dict[StreamKey, bool]
    provisional_pass: bool


def evaluate_gate(
    stats: dict[StreamKey, StreamStats],
    checksum_ok: bool,
    min_messages: int = DEFAULT_MIN_MESSAGES,
) -> GateResult:
    per_stream: dict[StreamKey, bool] = {}
    for exchange in EXPECTED_EXCHANGES:
        for symbol in EXPECTED_SYMBOLS:
            key = (exchange, symbol)
            count = stats[key].count if key in stats else 0
            per_stream[key] = count >= min_messages
    provisional = checksum_ok and all(per_stream.values())
    return GateResult(checksum_ok=checksum_ok, per_stream=per_stream, provisional_pass=provisional)


def _p(line: str = "") -> None:
    print(line)


def _format_sample(raw: dict[str, Any], norm: dict[str, Any]) -> list[str]:
    raw_txt = json.dumps(raw, separators=(",", ": "), sort_keys=True)
    norm_txt = json.dumps(norm, separators=(",", ": "), sort_keys=True)
    return [f"    raw : {raw_txt}", f"    norm: {norm_txt}"]


def render_report(
    capture_dir: Path,
    manifest: dict[str, Any],
    stats: dict[StreamKey, StreamStats],
    gate: GateResult,
    samples: int,
    min_messages: int,
) -> None:
    _p("=" * 78)
    _p("tickflow feed-sanity report (§0)")
    _p("=" * 78)
    _p(f"capture      : {capture_dir}")
    _p(f"created_utc  : {manifest.get('created_utc', '?')}")
    _p(f"captured_secs: {manifest.get('captured_seconds', '?')}")
    _p(f"records      : {manifest.get('record_count', '?')}")
    checksum_state = "OK" if gate.checksum_ok else "MISMATCH — capture integrity FAILED"
    _p(f"sha256       : {manifest.get('sha256', '?')[:16]}… [{checksum_state}]")
    _p("")

    _p(f"Per-stream (threshold: ≥ {min_messages} messages):")
    header = (
        f"  {'stream':<20}{'count':>7}{'uniq_id':>9}{'dup_id':>7}{'miss_id':>8}"
        f"{'maxskew_s':>11}{'<=60s':>7}{'ooo':>6}  verdict"
    )
    _p(header)
    _p("  " + "-" * (len(header) - 2))
    for exchange in EXPECTED_EXCHANGES:
        for symbol in EXPECTED_SYMBOLS:
            key = (exchange, symbol)
            label = f"{exchange}:{symbol}"
            verdict = "PASS" if gate.per_stream[key] else "FAIL"
            if key in stats:
                st = stats[key]
                _p(
                    f"  {label:<20}{st.count:>7}{st.unique_trade_ids:>9}"
                    f"{st.duplicate_trade_ids:>7}{st.missing_trade_ids:>8}"
                    f"{st.max_abs_skew_ms / 1000:>11.3f}{st.within_60s:>7}"
                    f"{st.out_of_order:>6}  {verdict}"
                )
            else:
                dashes = f"{'0':>7}{'-':>9}{'-':>7}{'-':>8}{'-':>11}{'-':>7}{'-':>6}"
                _p(f"  {label:<20}{dashes}  {verdict}")
    _p("")

    _p(f"Field-mapping samples (up to {samples} raw->normalized pairs per stream):")
    for exchange in EXPECTED_EXCHANGES:
        for symbol in EXPECTED_SYMBOLS:
            key = (exchange, symbol)
            _p(f"  [{exchange}:{symbol}]")
            st_opt = stats.get(key)
            if not st_opt or not st_opt.samples:
                _p("    (no messages)")
                continue
            for raw, norm in st_opt.samples:
                for row in _format_sample(raw, norm):
                    _p(row)
            _p("")

    _p("-" * 78)
    verdict = "PASS" if gate.provisional_pass else "FAIL"
    _p(f"PROVISIONAL GATE: {verdict}  (checksum + presence + ≥{min_messages}/stream)")
    _p(
        "This is the objective portion only. Field-mapping correctness and the final gate\n"
        "decision are a manual review (§0), recorded as ADR-001 in docs/decisions.md."
    )
    _p("-" * 78)


def _handle(args: argparse.Namespace) -> int:
    if args.capture is not None:
        capture_dir = Path(args.capture)
    else:
        latest = find_latest_capture(Path(args.root))
        if latest is None:
            print(
                f"[sanity] no capture found under {args.root}; run `tickflow capture` first.",
                file=sys.stderr,
            )
            return 2
        capture_dir = latest

    if not (capture_dir / MANIFEST_FILE).is_file():
        print(f"[sanity] {capture_dir} has no {MANIFEST_FILE}.", file=sys.stderr)
        return 2

    manifest = load_manifest(capture_dir)
    checksum_ok, _expected, _actual = verify_checksum(capture_dir, manifest)
    records = read_records(capture_dir, manifest.get("stream_file", STREAM_FILE))
    stats = compute_stats(records, samples=args.samples)
    gate = evaluate_gate(stats, checksum_ok, min_messages=args.min_messages)
    render_report(capture_dir, manifest, stats, gate, args.samples, args.min_messages)
    # Exit non-zero on a failed provisional gate so scripts/CI can branch; the human call stands.
    return 0 if gate.provisional_pass else 1


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "sanity",
        help="Print the feed-sanity report over a capture for the blocking manual gate (§0).",
    )
    parser.add_argument(
        "--capture",
        default=None,
        help="Capture directory to review (default: latest under --root).",
    )
    parser.add_argument(
        "--root",
        default=str(DEFAULT_CAPTURE_ROOT),
        help="Capture root to search when --capture is omitted (default: data/captures).",
    )
    parser.add_argument(
        "--min-messages",
        type=int,
        default=DEFAULT_MIN_MESSAGES,
        dest="min_messages",
        help="Per-stream message threshold for the provisional gate (frozen: 50).",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Raw→normalized field-mapping pairs to print per stream (frozen: 5).",
    )
    parser.set_defaults(handler=_handle)
