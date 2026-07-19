"""tickflow capture — the feed-sanity capture command (frozen design §0).

Connect to both live venue websockets, normalize to trades.v1, and write a local, checksummed
capture under ``data/captures/<name>/`` for the blocking manual feed-sanity gate. Then
``tickflow sanity`` reads that capture and prints the per-stream evidence Rishik reviews before
ADR-001 is recorded (§0).

Capture is intentionally **broker-independent**: it reads the websockets directly and writes a
file, so feed sanity can be judged without standing up the Kafka path. Two provenance decisions
are baked in:

- **Nothing here is ever committed.** Captured market data cannot be redistributed under the
  Coinbase/Kraken ToS (§1); ``data/`` is gitignored. The capture is local evidence only.
- **The stream is checksummed.** The manifest pins a SHA-256 over the exact ``stream.jsonl``
  bytes, so ``tickflow sanity`` verifies it is reviewing the bytes that were captured, and the
  reviewed capture has a stable identity to cite in ADR-001.

Each record keeps both the originating venue sub-message and its normalized form, so the gate
can show raw→normalized field-mapping pairs. Like the ingester, this module is feed I/O glue and
is coverage-excluded (§9); its pure helpers are unit-tested.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import hashlib
import json
import signal
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from tickflow.ingest import (
    CANONICAL_SYMBOLS,
    Backoff,
    CoinbaseFeed,
    Feed,
    KrakenFeed,
    NormalizedTrade,
)

CAPTURE_SCHEMA = "tickflow.capture.v1"
DEFAULT_CAPTURE_ROOT = Path("data/captures")
STREAM_FILE = "stream.jsonl"
MANIFEST_FILE = "manifest.json"


@dataclass(frozen=True, slots=True)
class CaptureManifest:
    """Provenance sidecar for a capture, written once the stream is closed."""

    schema: str
    created_utc: str
    captured_seconds: float
    symbols: list[str]
    feeds: list[str]
    record_count: int
    counts_by_feed: dict[str, int]
    stream_file: str
    sha256: str


def _now_millis() -> int:
    return int(time.time() * 1000)


def _log(message: str) -> None:
    print(f"[capture] {message}", file=sys.stderr, flush=True)


def sha256_file(path: Path) -> str:
    """SHA-256 of a file's bytes, streamed so captures never load fully into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _StreamSink:
    """Append-only JSONL writer for one capture, one record per normalized trade.

    A record is ``{"raw": <venue sub-message>, "norm": <trades.v1 record>}``. Keeping the raw
    sub-message is what lets ``tickflow sanity`` show field-mapping pairs (§0).
    """

    def __init__(self, path: Path) -> None:
        self._handle = path.open("w", encoding="utf-8")
        self.count = 0
        self.counts_by_feed: dict[str, int] = {}

    def write(self, raw: dict[str, Any], trade: NormalizedTrade) -> None:
        line = json.dumps({"raw": raw, "norm": asdict(trade)}, separators=(",", ":"))
        self._handle.write(line + "\n")
        self.count += 1
        self.counts_by_feed[trade.exchange] = self.counts_by_feed.get(trade.exchange, 0) + 1

    def flush(self) -> None:
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()


async def _run_capture_feed(
    feed: Feed,
    symbols: Sequence[str],
    sink: _StreamSink,
    stop: asyncio.Event,
) -> None:
    keep = set(symbols)
    backoff = Backoff()
    while not stop.is_set():
        try:
            async with connect(
                feed.url, ping_interval=20, ping_timeout=20, max_queue=1024, open_timeout=15
            ) as ws:
                await ws.send(feed.subscribe(symbols))
                backoff.reset()
                _log(f"{feed.name}: connected, subscribed to {list(symbols)}")
                async for raw in ws:
                    if stop.is_set():
                        break
                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(message, dict):
                        continue
                    ts_ingest = _now_millis()
                    for raw_trade, trade in feed.iter_pairs(message, ts_ingest):
                        # Guard against symbol creep: only record what we subscribed to (§1).
                        if trade.symbol in keep:
                            sink.write(raw_trade, trade)
                sink.flush()
        except asyncio.CancelledError:
            raise
        except (WebSocketException, OSError) as exc:
            delay = backoff.next_delay()
            _log(f"{feed.name}: disconnected ({exc!r}); reconnecting in {delay:.1f}s")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
    _log(f"{feed.name}: stopped (captured {sink.counts_by_feed.get(feed.name, 0)})")


def build_manifest(
    sink: _StreamSink, symbols: Sequence[str], seconds: float, sha256: str
) -> CaptureManifest:
    return CaptureManifest(
        schema=CAPTURE_SCHEMA,
        created_utc=datetime.now(UTC).isoformat(timespec="seconds"),
        captured_seconds=round(seconds, 3),
        symbols=list(symbols),
        feeds=sorted(sink.counts_by_feed),
        record_count=sink.count,
        counts_by_feed=dict(sorted(sink.counts_by_feed.items())),
        stream_file=STREAM_FILE,
        sha256=sha256,
    )


async def _capture(
    feeds: Sequence[Feed],
    symbols: Sequence[str],
    capture_dir: Path,
    seconds: float,
) -> CaptureManifest:
    capture_dir.mkdir(parents=True, exist_ok=True)
    stream_path = capture_dir / STREAM_FILE
    sink = _StreamSink(stream_path)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    started = time.monotonic()
    tasks = [asyncio.create_task(_run_capture_feed(feed, symbols, sink, stop)) for feed in feeds]
    if seconds > 0:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        stop.set()
    else:
        await stop.wait()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.monotonic() - started
    sink.close()

    manifest = build_manifest(sink, symbols, elapsed, sha256_file(stream_path))
    (capture_dir / MANIFEST_FILE).write_text(
        json.dumps(dataclasses.asdict(manifest), indent=2) + "\n", encoding="utf-8"
    )
    _log(f"wrote {manifest.record_count} records to {stream_path} (sha256 {manifest.sha256[:12]}…)")
    _log(f"counts_by_feed={manifest.counts_by_feed}")
    return manifest


def _select_feeds(choice: str) -> list[Feed]:
    available: dict[str, Feed] = {"coinbase": CoinbaseFeed(), "kraken": KrakenFeed()}
    if choice == "both":
        return [available["coinbase"], available["kraken"]]
    return [available[choice]]


def _default_name() -> str:
    return "capture-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _handle(args: argparse.Namespace) -> int:
    feeds = _select_feeds(args.exchange)
    symbols = tuple(s.strip() for s in args.symbols.split(",") if s.strip())
    capture_dir = Path(args.out) / (args.name or _default_name())
    seconds = args.minutes * 60.0
    _log(f"capturing {list(symbols)} from {[f.name for f in feeds]} for {args.minutes} min")
    try:
        asyncio.run(_capture(feeds, symbols, capture_dir, seconds))
    except KeyboardInterrupt:  # pragma: no cover
        return 130
    print(capture_dir)
    return 0


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "capture",
        help="Capture normalized live trades to a local checksummed stream for the sanity gate.",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=5.0,
        help="Minutes to capture before stopping (frozen gate window: 5).",
    )
    parser.add_argument(
        "--exchange",
        choices=["coinbase", "kraken", "both"],
        default="both",
        help="Which venue(s) to capture (default: both — required for the gate).",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(CANONICAL_SYMBOLS),
        help="Comma-separated canonical symbols (default: BTC-USD,ETH-USD).",
    )
    parser.add_argument(
        "--out",
        default=str(DEFAULT_CAPTURE_ROOT),
        help="Capture root directory (gitignored; default: data/captures).",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Capture subdirectory name (default: timestamped capture-<UTC>).",
    )
    parser.set_defaults(handler=_handle)
