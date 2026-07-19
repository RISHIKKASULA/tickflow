"""Feed-sanity report unit tests — the pure stat helpers behind the manual gate.

Report printing is coverage-excluded glue (§9), but the numbers Rishik reads to make the gate
call must be right: counts, duplicate/uniqueness accounting, wall-clock skew, out-of-order
counting in arrival order, sample capping, and the provisional gate threshold logic.
"""

from __future__ import annotations

from typing import Any

from tickflow.sanity import (
    DEFAULT_THRESHOLDS,
    compute_stats,
    evaluate_gate,
)


def _rec(
    exchange: str,
    symbol: str,
    trade_id: str,
    ts_event: int,
    ts_ingest: int,
) -> dict[str, Any]:
    return {
        "raw": {"trade_id": trade_id},
        "norm": {
            "exchange": exchange,
            "symbol": symbol,
            "trade_id": trade_id,
            "price": 63000.0,
            "size": 0.01,
            "side": "buy",
            "ts_event": ts_event,
            "ts_ingest": ts_ingest,
        },
    }


def test_counts_and_uniqueness_in_arrival_order() -> None:
    base = 1_700_000_000_000
    records = [
        _rec("coinbase", "BTC-USD", "1", base, base),
        _rec("coinbase", "BTC-USD", "2", base + 10, base + 10),
        _rec("coinbase", "BTC-USD", "1", base + 20, base + 20),  # duplicate id
        _rec("coinbase", "BTC-USD", "", base + 30, base + 30),  # missing id
    ]
    stats = compute_stats(records)
    st = stats[("coinbase", "BTC-USD")]
    assert st.count == 4
    assert st.unique_trade_ids == 2  # "1", "2"
    assert st.duplicate_trade_ids == 1  # second "1"
    assert st.missing_trade_ids == 1  # ""


def test_out_of_order_counted_against_running_watermark() -> None:
    base = 1_700_000_000_000
    records = [
        _rec("kraken", "ETH-USD", "1", base + 100, base + 100),
        _rec("kraken", "ETH-USD", "2", base + 50, base + 50),  # regresses -> ooo
        _rec("kraken", "ETH-USD", "3", base + 120, base + 120),  # advances, ok
        _rec("kraken", "ETH-USD", "4", base + 119, base + 119),  # < running max -> ooo
    ]
    st = compute_stats(records)[("kraken", "ETH-USD")]
    assert st.out_of_order == 2
    assert st.min_ts_event == base + 50
    assert st.max_ts_event == base + 120


def test_wall_clock_skew_tolerance() -> None:
    base = 1_700_000_000_000
    records = [
        _rec("coinbase", "ETH-USD", "1", base, base),  # 0 skew
        _rec("coinbase", "ETH-USD", "2", base, base + 59_000),  # 59 s -> within
        _rec("coinbase", "ETH-USD", "3", base, base + 61_000),  # 61 s -> outside
    ]
    st = compute_stats(records)[("coinbase", "ETH-USD")]
    assert st.within_60s == 2
    assert st.max_abs_skew_ms == 61_000


def test_samples_capped_at_limit() -> None:
    base = 1_700_000_000_000
    records = [_rec("coinbase", "BTC-USD", str(i), base + i, base + i) for i in range(10)]
    st = compute_stats(records, samples=3)[("coinbase", "BTC-USD")]
    assert len(st.samples) == 3
    raw, norm = st.samples[0]
    assert raw == {"trade_id": "0"}
    assert norm["exchange"] == "coinbase"


def _stream(count: int, exchange: str, symbol: str) -> list[dict[str, Any]]:
    base = 1_700_000_000_000
    return [_rec(exchange, symbol, str(i), base + i, base + i) for i in range(count)]


def _all_streams(coinbase_n: int, kraken_n: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for symbol in ("BTC-USD", "ETH-USD"):
        records += _stream(coinbase_n, "coinbase", symbol)
        records += _stream(kraken_n, "kraken", symbol)
    return records


def test_gate_passes_when_each_venue_meets_its_own_threshold() -> None:
    # Coinbase at its floor, Kraken at its (much lower) floor -> pass.
    records = _all_streams(DEFAULT_THRESHOLDS["coinbase"], DEFAULT_THRESHOLDS["kraken"])
    gate = evaluate_gate(compute_stats(records), checksum_ok=True)
    assert gate.provisional_pass is True
    assert all(gate.per_stream.values())
    assert gate.thresholds == DEFAULT_THRESHOLDS


def test_recalibration_accepts_sparse_kraken_that_flat_50_would_reject() -> None:
    # The ADR-001 case: Kraken 29/14 clears its per-venue floor (5) but not a flat 50.
    records = _all_streams(coinbase_n=200, kraken_n=14)
    gate = evaluate_gate(compute_stats(records), checksum_ok=True)
    assert gate.provisional_pass is True
    assert gate.per_stream[("kraken", "ETH-USD")] is True
    # A single cross-venue 50 would have failed both Kraken streams.
    flat = evaluate_gate(compute_stats(records), checksum_ok=True, thresholds={"kraken": 50})
    assert flat.per_stream[("kraken", "ETH-USD")] is False


def test_gate_still_fails_a_stalled_kraken_below_its_floor() -> None:
    # Liveness floor, not a lowered bar: a near-dead Kraken stream (3 < 5) still fails,
    # while a healthy sparse Kraken stream (14 >= 5) passes.
    records = _stream(200, "coinbase", "BTC-USD")
    records += _stream(200, "coinbase", "ETH-USD")
    records += _stream(14, "kraken", "BTC-USD")
    records += _stream(3, "kraken", "ETH-USD")
    gate = evaluate_gate(compute_stats(records), checksum_ok=True)
    assert gate.provisional_pass is False
    assert gate.per_stream[("kraken", "ETH-USD")] is False
    assert gate.per_stream[("kraken", "BTC-USD")] is True


def test_gate_fails_on_missing_feed_entirely() -> None:
    records: list[dict[str, Any]] = []
    for symbol in ("BTC-USD", "ETH-USD"):
        records += _stream(200, "coinbase", symbol)  # kraken absent
    gate = evaluate_gate(compute_stats(records), checksum_ok=True)
    assert gate.provisional_pass is False
    assert gate.per_stream[("kraken", "BTC-USD")] is False
    assert gate.per_stream[("kraken", "ETH-USD")] is False


def test_gate_fails_on_checksum_mismatch_even_if_counts_pass() -> None:
    records = _all_streams(DEFAULT_THRESHOLDS["coinbase"], DEFAULT_THRESHOLDS["kraken"])
    gate = evaluate_gate(compute_stats(records), checksum_ok=False)
    assert gate.provisional_pass is False
    assert all(gate.per_stream.values())  # counts fine, but integrity failed
