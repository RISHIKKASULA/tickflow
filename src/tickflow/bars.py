"""The downstream consumer: a 1-minute OHLCV bar builder with an SLO checker and a DuckDB sink
(frozen design §4 — the "gates earn their keep" demo).

`barbuilder` is what sits *behind* the gate. It trusts its input: it consumes `trades.valid` and
builds 1-minute OHLCV bars per (exchange, symbol), appended to a local DuckDB. It has no quality
logic of its own — that is the whole point. If the gate were deleted, every fault the gate
quarantines would flow straight into these bars and corrupt them, and the SLO checker would light
up. The gates-ON/OFF experiment (§4, `demo.py`... no — see `slo_experiment` below) measures exactly
that: quality gates judged by what they prevent, not by assertion.

Three coverage-gated pieces live here (§9):

- **`BarBuilder` — the pure aggregator.** Bars are keyed off the **event-time watermark**, never
  wall clock: a trade's bar is `ts_event` floored to the minute. Aggregation is fully
  order-independent — `open`/`close` are picked by `(ts_event, trade_id)`, `high`/`low` by
  max/min, and **volume is summed in integer micro-units** so the byte content of a bar does not
  depend on the order its trades arrived. That is what lets replay be bit-identical: the same set
  of valid trades produces the same bars regardless of delivery order.

- **`check_slo` — the invariant checker.** Every bar must satisfy the frozen SLO: `high >= low`,
  `low <= open <= high`, `low <= close <= high`, `volume > 0`, positive prices, monotone bar
  timestamps per stream, and — the load-bearing one — **no bar built from a message the gate would
  quarantine** (each trade is fed with a `tainted` flag; a bar with any tainted constituent
  violates the SLO). With gates ON nothing tainted ever reaches the builder, so the count is zero;
  with gates OFF the faults land in the bars and the count is `K > 0`.

- **`DuckDbSink` — the append-only store.** Single writer process, INSERT-only (the supported
  DuckDB concurrency pattern per its docs). Bars are stored **locally**; their market values never
  leave the environment (§1 ToS) — the telemetry dashboard publishes counts and rates, never a
  price, and that separation is enforced downstream at export time.

The Kafka consumer that reads `trades.valid` and feeds the builder is network glue, exercised in
the integration lane and marked no-cover, mirroring `gate.run_gate` / `contracts.SchemaRegistry`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BAR_MS = 60_000  # 1-minute bars (frozen §4)
_VOL_SCALE = 1_000_000  # integer micro-units for order-independent, bit-stable volume sums

Record = dict[str, Any]
BarKey = tuple[str, str, int]  # (exchange, symbol, bar_start_ms)


# --------------------------------------------------------------------------------------------
# The bar and the aggregator.
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Bar:
    """One 1-minute OHLCV bar for a (exchange, symbol) stream.

    `tainted` counts constituent trades the gate would have quarantined — always 0 downstream of a
    live gate, non-zero only in the gates-OFF experiment. It is SLO state, not market data, and is
    excluded from the bar's market identity (`digest` / any telemetry).
    """

    exchange: str
    symbol: str
    bar_start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    count: int
    tainted: int = 0

    @property
    def key(self) -> BarKey:
        return (self.exchange, self.symbol, self.bar_start_ms)


@dataclass(slots=True)
class _Acc:
    """Mutable per-bucket accumulator; volume in integer micro-units for a deterministic sum."""

    open_order: tuple[int, str]
    open_price: float
    close_order: tuple[int, str]
    close_price: float
    high: float
    low: float
    volume_micros: int
    count: int
    tainted: int


def bar_start(ts_event_ms: int) -> int:
    """The 1-minute bucket a trade's event-time falls in (floored to the minute)."""
    return ts_event_ms - (ts_event_ms % BAR_MS)


class BarBuilder:
    """Accumulates `trades.valid` records into 1-minute OHLCV bars, order-independently.

    `add` is commutative and associative over a stream's trades: feeding the same set of records in
    any order yields byte-identical bars (open/close keyed by `(ts_event, trade_id)`, high/low by
    max/min, volume summed as integer micro-units). This is what makes replay bit-deterministic.
    """

    def __init__(self) -> None:
        self._acc: dict[BarKey, _Acc] = {}

    def add(self, record: Record, *, tainted: bool = False) -> None:
        """Fold one trade into its minute bucket. `tainted` marks a would-be-quarantined message."""
        exchange = str(record["exchange"])
        symbol = str(record["symbol"])
        ts_event = int(record["ts_event"])
        price = float(record["price"])
        size_micros = round(float(record["size"]) * _VOL_SCALE)
        order = (ts_event, str(record["trade_id"]))
        key: BarKey = (exchange, symbol, bar_start(ts_event))

        acc = self._acc.get(key)
        if acc is None:
            self._acc[key] = _Acc(
                open_order=order,
                open_price=price,
                close_order=order,
                close_price=price,
                high=price,
                low=price,
                volume_micros=size_micros,
                count=1,
                tainted=1 if tainted else 0,
            )
            return
        if order < acc.open_order:
            acc.open_order, acc.open_price = order, price
        if order > acc.close_order:
            acc.close_order, acc.close_price = order, price
        if price > acc.high:
            acc.high = price
        if price < acc.low:
            acc.low = price
        acc.volume_micros += size_micros
        acc.count += 1
        if tainted:
            acc.tainted += 1

    def bars(self) -> list[Bar]:
        """The finished bars, sorted by (exchange, symbol, bar_start_ms) — canonical order."""
        out = [
            Bar(
                exchange=exchange,
                symbol=symbol,
                bar_start_ms=bucket,
                open=acc.open_price,
                high=acc.high,
                low=acc.low,
                close=acc.close_price,
                volume=round(acc.volume_micros / _VOL_SCALE, 6),
                count=acc.count,
                tainted=acc.tainted,
            )
            for (exchange, symbol, bucket), acc in self._acc.items()
        ]
        out.sort(key=lambda b: (b.exchange, b.symbol, b.bar_start_ms))
        return out


def build_bars(records: Iterable[Record], tainted: Iterable[bool] | None = None) -> list[Bar]:
    """Convenience: fold an iterable of records into bars, with an optional parallel taint mask."""
    builder = BarBuilder()
    if tainted is None:
        for record in records:
            builder.add(record)
    else:
        for record, is_tainted in zip(records, tainted, strict=True):
            builder.add(record, tainted=is_tainted)
    return builder.bars()


def _canonical_bar(bar: Bar) -> list[Any]:
    # Market identity of a bar: OHLCV + count, in canonical order. `tainted` is SLO state, not
    # market data, and is deliberately excluded so gates-ON and reference bars compare equal.
    return [
        bar.exchange,
        bar.symbol,
        bar.bar_start_ms,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
        bar.count,
    ]


def bars_digest(bars: Sequence[Bar]) -> str:
    """A SHA-256 over the canonical bar stream — equal digests mean bit-identical bars.

    This is a hash, not market data (it discloses no price), so it is safe to publish; it is how
    the gates-ON bit-identity claim (§4) is checked and reported.
    """
    blob = json.dumps([_canonical_bar(b) for b in bars], separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


# --------------------------------------------------------------------------------------------
# SLO checker (frozen §4).
# --------------------------------------------------------------------------------------------
# Invariant labels — stable identifiers for the SLO report / telemetry.
SLO_HIGH_GE_LOW = "high_ge_low"
SLO_OPEN_IN_RANGE = "open_in_range"
SLO_CLOSE_IN_RANGE = "close_in_range"
SLO_VOLUME_POSITIVE = "volume_positive"
SLO_PRICE_POSITIVE = "price_positive"
SLO_MONOTONE_TIME = "monotone_time"
SLO_NO_QUARANTINABLE = "no_quarantinable"

SLO_INVARIANTS: tuple[str, ...] = (
    SLO_HIGH_GE_LOW,
    SLO_OPEN_IN_RANGE,
    SLO_CLOSE_IN_RANGE,
    SLO_VOLUME_POSITIVE,
    SLO_PRICE_POSITIVE,
    SLO_MONOTONE_TIME,
    SLO_NO_QUARANTINABLE,
)


@dataclass(frozen=True, slots=True)
class SloViolation:
    """One violated invariant on one bar."""

    bar_key: BarKey
    invariant: str
    detail: str


@dataclass
class SloReport:
    """The SLO verdict over a set of bars: the violations and their rollups (counts only)."""

    n_bars: int
    violations: list[SloViolation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    @property
    def n_violated_bars(self) -> int:
        return len({v.bar_key for v in self.violations})

    def counts_by_invariant(self) -> dict[str, int]:
        counts = {name: 0 for name in SLO_INVARIANTS}
        for violation in self.violations:
            counts[violation.invariant] += 1
        return counts

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_bars": self.n_bars,
            "n_violations": len(self.violations),
            "n_violated_bars": self.n_violated_bars,
            "counts_by_invariant": self.counts_by_invariant(),
        }


def check_slo(bars: Sequence[Bar]) -> SloReport:
    """Check every frozen SLO invariant on `bars`; return the report (counts, not values).

    The structural invariants (high >= low, open/close within [low, high]) can only fail if the
    builder is wrong, so they double as a builder self-check; `price_positive` and
    `no_quarantinable` are what the gates-OFF experiment trips. `monotone_time` is checked per
    stream in the order given — `BarBuilder.bars()` already emits canonical order, so a violation
    here means a caller assembled bars out of order.
    """
    report = SloReport(n_bars=len(bars))
    last_start: dict[tuple[str, str], int] = {}

    for bar in bars:
        key = bar.key
        if bar.high < bar.low:
            report.violations.append(
                SloViolation(key, SLO_HIGH_GE_LOW, f"high {bar.high} < low {bar.low}")
            )
        if not (bar.low <= bar.open <= bar.high):
            report.violations.append(
                SloViolation(
                    key, SLO_OPEN_IN_RANGE, f"open {bar.open} outside [{bar.low}, {bar.high}]"
                )
            )
        if not (bar.low <= bar.close <= bar.high):
            report.violations.append(
                SloViolation(
                    key, SLO_CLOSE_IN_RANGE, f"close {bar.close} outside [{bar.low}, {bar.high}]"
                )
            )
        if bar.volume <= 0:
            report.violations.append(
                SloViolation(key, SLO_VOLUME_POSITIVE, f"volume {bar.volume} <= 0")
            )
        if bar.low <= 0:
            report.violations.append(
                SloViolation(key, SLO_PRICE_POSITIVE, f"low price {bar.low} <= 0")
            )
        stream = (bar.exchange, bar.symbol)
        prev = last_start.get(stream)
        if prev is not None and bar.bar_start_ms <= prev:
            report.violations.append(
                SloViolation(
                    key, SLO_MONOTONE_TIME, f"bar_start {bar.bar_start_ms} <= previous {prev}"
                )
            )
        last_start[stream] = bar.bar_start_ms
        if bar.tainted > 0:
            report.violations.append(
                SloViolation(
                    key,
                    SLO_NO_QUARANTINABLE,
                    f"{bar.tainted} constituent trade(s) the gate would quarantine",
                )
            )
    return report


# --------------------------------------------------------------------------------------------
# DuckDB sink (frozen §4) — single writer, append-only. Local storage; values never published.
# --------------------------------------------------------------------------------------------
class DuckDbSink:
    """An append-only DuckDB sink for bars (single writer process — the supported pattern).

    Column names avoid SQL keywords (`bar_open`/`bar_close`, `trade_count`). The connection is
    opened lazily; `append` is INSERT-only (no UPDATE/DELETE), so a bar, once written, is immutable.
    """

    TABLE = "bars"
    _DDL = (
        "CREATE TABLE IF NOT EXISTS bars ("
        "exchange VARCHAR NOT NULL, symbol VARCHAR NOT NULL, bar_start_ms BIGINT NOT NULL, "
        "bar_open DOUBLE NOT NULL, high DOUBLE NOT NULL, low DOUBLE NOT NULL, "
        "bar_close DOUBLE NOT NULL, volume DOUBLE NOT NULL, trade_count BIGINT NOT NULL, "
        "tainted BIGINT NOT NULL DEFAULT 0)"
    )

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._con: Any = None

    def open(self) -> DuckDbSink:
        import duckdb

        self._con = duckdb.connect(self.path)
        self._con.execute(self._DDL)
        return self

    def _require(self) -> Any:
        if self._con is None:
            raise RuntimeError("DuckDbSink is not open(); call open() or use as a context manager.")
        return self._con

    def append(self, bars: Iterable[Bar]) -> int:
        """Append bars (INSERT-only). Returns the number of rows written."""
        con = self._require()
        rows = [
            (
                b.exchange,
                b.symbol,
                b.bar_start_ms,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.count,
                b.tainted,
            )
            for b in bars
        ]
        if rows:
            con.executemany(
                "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        return len(rows)

    def read_bars(self) -> list[Bar]:
        """Read all stored bars back in canonical order (test/inspection helper)."""
        con = self._require()
        result = con.execute(
            "SELECT exchange, symbol, bar_start_ms, bar_open, high, low, bar_close, volume, "
            "trade_count, tainted FROM bars ORDER BY exchange, symbol, bar_start_ms"
        ).fetchall()
        return [
            Bar(
                exchange=str(row[0]),
                symbol=str(row[1]),
                bar_start_ms=int(row[2]),
                open=float(row[3]),
                high=float(row[4]),
                low=float(row[5]),
                close=float(row[6]),
                volume=float(row[7]),
                count=int(row[8]),
                tainted=int(row[9]),
            )
            for row in result
        ]

    def count(self) -> int:
        con = self._require()
        return int(con.execute("SELECT COUNT(*) FROM bars").fetchone()[0])

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> DuckDbSink:
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()


# --------------------------------------------------------------------------------------------
# Kafka consumer glue — reads trades.valid, builds bars, appends to DuckDB (integration lane).
# --------------------------------------------------------------------------------------------
def run_barbuilder(  # pragma: no cover - network I/O, integration lane
    db_path: str,
    bootstrap: str = "localhost:19092",
    group_id: str = "tickflow-barbuilder",
    max_messages: int = 0,
) -> int:
    """Consume `trades.valid`, build bars, and append them to DuckDB (at-least-once, manual commit).

    Downstream of the gate, so it trusts every record: it decodes and folds, and periodically
    flushes finished bars to the sink. Bars stay local (§1). Network I/O — integration lane only.
    """
    import sys

    from confluent_kafka import Consumer

    from tickflow import contracts
    from tickflow.gate import TRADES_VALID

    schema = contracts.load_schema()
    builder = BarBuilder()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TRADES_VALID])
    processed = 0
    try:
        while max_messages == 0 or processed < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                print(f"[barbuilder] consume error: {message.error()}", file=sys.stderr)
                continue
            raw = message.value()
            if raw is None:
                consumer.commit(message=message, asynchronous=False)
                continue
            _schema_id, record = contracts.decode(raw, schema)
            builder.add(record)
            consumer.commit(message=message, asynchronous=False)
            processed += 1
    finally:
        consumer.close()

    bars = builder.bars()
    with DuckDbSink(db_path) as sink:
        written = sink.append(bars)
    report = check_slo(bars)
    print(f"[barbuilder] wrote {written} bars; slo_ok={report.ok}", file=sys.stderr)
    return written


def _handle(args: argparse.Namespace) -> int:  # pragma: no cover - CLI over network I/O
    written = run_barbuilder(
        db_path=args.db,
        bootstrap=args.bootstrap,
        group_id=args.group_id,
        max_messages=args.max_messages,
    )
    print(json.dumps({"bars_written": written, "db": args.db}, indent=2))
    return 0


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "bars",
        help="Consume trades.valid and build 1-minute OHLCV bars into a local DuckDB (§4).",
    )
    parser.add_argument("--db", default="tickflow-bars.duckdb", help="DuckDB file path.")
    parser.add_argument("--bootstrap", default="localhost:19092", help="Kafka bootstrap servers.")
    parser.add_argument("--group-id", default="tickflow-barbuilder", dest="group_id")
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        dest="max_messages",
        help="Stop after N messages (0 = run until interrupted).",
    )
    parser.set_defaults(handler=_handle)
