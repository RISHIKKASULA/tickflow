"""Bar builder + SLO checker + DuckDB sink tests (frozen §4/§10).

What is proven here:

- **Known bars from hand-built ticks** — a small tick sequence yields exactly the OHLCV a human
  can compute, with the minute bucketing keyed off event-time.
- **Order-independence / bit-identity** — the same trades shuffled produce byte-identical bars
  (open/close by (ts_event, trade_id), high/low by max/min, volume summed in integer micro-units).
  This is the property the gates-ON replay experiment (§4) relies on.
- **SLO invariants** — each frozen invariant fires on a bar constructed to violate exactly it, and
  a clean bar set passes; the tainted-constituent invariant is the load-bearing "no bar built from
  a message the gate would quarantine" check.
- **DuckDB sink** — append-only round-trip and the single-writer append pattern.
"""

from __future__ import annotations

from typing import Any

import pytest

from tickflow.bars import (
    BAR_MS,
    SLO_CLOSE_IN_RANGE,
    SLO_HIGH_GE_LOW,
    SLO_MONOTONE_TIME,
    SLO_NO_QUARANTINABLE,
    SLO_OPEN_IN_RANGE,
    SLO_PRICE_POSITIVE,
    SLO_VOLUME_POSITIVE,
    Bar,
    BarBuilder,
    DuckDbSink,
    bar_start,
    bars_digest,
    build_bars,
    check_slo,
)

BASE = 1_735_689_600_000  # 2025-01-01T00:00:00Z, minute-aligned


def tick(
    ts_event: int,
    price: float,
    size: float,
    trade_id: str,
    exchange: str = "coinbase",
    symbol: str = "BTC-USD",
) -> dict[str, Any]:
    return {
        "exchange": exchange,
        "symbol": symbol,
        "trade_id": trade_id,
        "price": price,
        "size": size,
        "side": "buy",
        "ts_event": ts_event,
        "ts_ingest": ts_event + 1,
    }


# --------------------------------------------------------------------------------------------
# Bucketing + known bars.
# --------------------------------------------------------------------------------------------
def test_bar_start_floors_to_the_minute() -> None:
    assert bar_start(BASE) == BASE
    assert bar_start(BASE + 59_999) == BASE
    assert bar_start(BASE + 60_000) == BASE + BAR_MS
    assert bar_start(BASE + 61_500) == BASE + BAR_MS


def test_hand_built_ticks_make_the_expected_bar() -> None:
    ticks = [
        tick(BASE + 1_000, 100.0, 1.0, "1"),
        tick(BASE + 2_000, 110.0, 2.0, "2"),  # the high
        tick(BASE + 3_000, 90.0, 0.5, "3"),  # the low
        tick(BASE + 4_000, 105.0, 1.5, "4"),  # the close
    ]
    bars = build_bars(ticks)
    assert len(bars) == 1
    bar = bars[0]
    assert (bar.exchange, bar.symbol, bar.bar_start_ms) == ("coinbase", "BTC-USD", BASE)
    assert bar.open == 100.0
    assert bar.high == 110.0
    assert bar.low == 90.0
    assert bar.close == 105.0
    assert bar.volume == 5.0
    assert bar.count == 4
    assert bar.tainted == 0


def test_ticks_split_across_minute_buckets_and_streams() -> None:
    ticks = [
        tick(BASE + 1_000, 100.0, 1.0, "1"),
        tick(BASE + 61_000, 101.0, 1.0, "2"),  # next minute
        tick(BASE + 2_000, 200.0, 1.0, "3", symbol="ETH-USD"),  # different stream
    ]
    bars = build_bars(ticks)
    assert len(bars) == 3
    # Canonical order: (exchange, symbol, bar_start).
    keys = [(b.exchange, b.symbol, b.bar_start_ms) for b in bars]
    assert keys == [
        ("coinbase", "BTC-USD", BASE),
        ("coinbase", "BTC-USD", BASE + BAR_MS),
        ("coinbase", "ETH-USD", BASE),
    ]


def test_open_and_close_track_event_time_not_arrival_order() -> None:
    # Deliver latest-first: open must still be the earliest event-time, close the latest.
    ticks = [
        tick(BASE + 4_000, 105.0, 1.0, "4"),
        tick(BASE + 1_000, 100.0, 1.0, "1"),
        tick(BASE + 2_000, 110.0, 1.0, "2"),
    ]
    bar = build_bars(ticks)[0]
    assert bar.open == 100.0
    assert bar.close == 105.0


# --------------------------------------------------------------------------------------------
# Order-independence / bit-identity.
# --------------------------------------------------------------------------------------------
def test_shuffled_delivery_yields_bit_identical_bars() -> None:
    ticks = [tick(BASE + i * 500, 100.0 + (i % 7), 0.1 * (i % 5 + 1), str(i)) for i in range(200)]
    forward = build_bars(ticks)
    reversed_bars = build_bars(list(reversed(ticks)))
    # A deterministic non-trivial permutation.
    shuffled = build_bars(ticks[100:] + ticks[:100])
    assert bars_digest(forward) == bars_digest(reversed_bars)
    assert bars_digest(forward) == bars_digest(shuffled)


def test_volume_sum_is_order_independent_bytewise() -> None:
    # Fractional sizes whose float sum is order-sensitive; integer micro-units make it stable.
    ticks = [tick(BASE + i, 100.0, 0.1, str(i)) for i in range(10)]
    forward = build_bars(ticks)[0]
    backward = build_bars(list(reversed(ticks)))[0]
    assert forward.volume == backward.volume == 1.0


def test_digest_excludes_tainted_flag() -> None:
    clean = build_bars([tick(BASE, 100.0, 1.0, "1")])
    builder = BarBuilder()
    builder.add(tick(BASE, 100.0, 1.0, "1"), tainted=True)
    tainted = builder.bars()
    assert tainted[0].tainted == 1
    # Same market values → same digest even though one bar is flagged tainted.
    assert bars_digest(clean) == bars_digest(tainted)


def test_tainted_constituents_accumulate_within_a_bucket() -> None:
    builder = BarBuilder()
    builder.add(tick(BASE, 100.0, 1.0, "1"), tainted=True)
    builder.add(tick(BASE + 1_000, 101.0, 1.0, "2"), tainted=True)  # same bucket, existing acc
    builder.add(tick(BASE + 2_000, 102.0, 1.0, "3"), tainted=False)
    bar = builder.bars()[0]
    assert bar.count == 3
    assert bar.tainted == 2


def test_build_bars_taint_mask_length_must_match() -> None:
    with pytest.raises(ValueError):
        build_bars([tick(BASE, 100.0, 1.0, "1")], tainted=[True, False])


# --------------------------------------------------------------------------------------------
# SLO invariants — each fires on a bar built to violate exactly it.
# --------------------------------------------------------------------------------------------
def _bar(**overrides: Any) -> Bar:
    base = {
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "bar_start_ms": BASE,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "volume": 5.0,
        "count": 4,
        "tainted": 0,
    }
    base.update(overrides)
    return Bar(**base)  # type: ignore[arg-type]


def test_clean_bars_pass_the_slo() -> None:
    bars = build_bars([tick(BASE + i * 1_000, 100.0 + i, 1.0, str(i)) for i in range(5)])
    report = check_slo(bars)
    assert report.ok
    assert report.n_violated_bars == 0
    assert report.as_dict()["n_violations"] == 0


def test_slo_high_ge_low() -> None:
    report = check_slo([_bar(high=80.0, low=90.0)])
    assert {v.invariant for v in report.violations} >= {SLO_HIGH_GE_LOW}


def test_slo_open_and_close_in_range() -> None:
    report = check_slo([_bar(open=200.0, close=5.0)])
    invariants = {v.invariant for v in report.violations}
    assert SLO_OPEN_IN_RANGE in invariants
    assert SLO_CLOSE_IN_RANGE in invariants


def test_slo_volume_positive() -> None:
    report = check_slo([_bar(volume=0.0)])
    assert {v.invariant for v in report.violations} == {SLO_VOLUME_POSITIVE}


def test_slo_price_positive_catches_nonpositive_low() -> None:
    report = check_slo([_bar(low=-1.0, open=-1.0)])
    invariants = {v.invariant for v in report.violations}
    assert SLO_PRICE_POSITIVE in invariants


def test_slo_no_quarantinable_is_the_load_bearing_invariant() -> None:
    report = check_slo([_bar(tainted=3)])
    assert [v.invariant for v in report.violations] == [SLO_NO_QUARANTINABLE]
    assert "3 constituent" in report.violations[0].detail


def test_slo_monotone_time_fires_on_out_of_order_bars() -> None:
    # Two bars for the same stream, presented newest-first (a caller assembly bug).
    later = _bar(bar_start_ms=BASE + BAR_MS)
    earlier = _bar(bar_start_ms=BASE)
    report = check_slo([later, earlier])
    assert {v.invariant for v in report.violations} == {SLO_MONOTONE_TIME}


def test_slo_report_rollups() -> None:
    report = check_slo([_bar(volume=0.0, tainted=1), _bar(bar_start_ms=BASE + BAR_MS, low=-1.0)])
    counts = report.counts_by_invariant()
    assert counts[SLO_VOLUME_POSITIVE] == 1
    assert counts[SLO_NO_QUARANTINABLE] == 1
    assert counts[SLO_PRICE_POSITIVE] == 1
    assert report.n_violated_bars == 2
    assert not report.ok


# --------------------------------------------------------------------------------------------
# DuckDB sink — append-only round-trip + single-writer pattern.
# --------------------------------------------------------------------------------------------
def test_duckdb_sink_round_trips_bars(tmp_path: Any) -> None:
    bars = build_bars([tick(BASE + i * 1_000, 100.0 + i, 1.0, str(i)) for i in range(5)])
    db = tmp_path / "bars.duckdb"
    with DuckDbSink(db) as sink:
        written = sink.append(bars)
        assert written == len(bars)
        assert sink.count() == len(bars)
        read_back = sink.read_bars()
    assert bars_digest(read_back) == bars_digest(bars)


def test_duckdb_sink_append_only_accumulates(tmp_path: Any) -> None:
    db = tmp_path / "bars.duckdb"
    first = build_bars([tick(BASE, 100.0, 1.0, "1")])
    second = build_bars([tick(BASE + BAR_MS, 101.0, 1.0, "2")])
    with DuckDbSink(db) as sink:
        sink.append(first)
        sink.append(second)
        assert sink.count() == 2
    # Re-open the same file: earlier appends persist (append-only, single writer at a time).
    with DuckDbSink(db) as sink:
        assert sink.count() == 2
        sink.append(build_bars([tick(BASE + 2 * BAR_MS, 102.0, 1.0, "3")]))
        assert sink.count() == 3


def test_duckdb_sink_append_empty_is_a_noop(tmp_path: Any) -> None:
    with DuckDbSink(tmp_path / "bars.duckdb") as sink:
        assert sink.append([]) == 0
        assert sink.count() == 0


def test_duckdb_sink_requires_open() -> None:
    sink = DuckDbSink(":memory:")
    with pytest.raises(RuntimeError):
        sink.count()
