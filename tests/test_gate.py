"""Gate rules-engine tests — table-driven verdicts for every rule, the boundary literals the
frozen spec calls out (§10), quarantine routing/envelopes, alert-only semantics, and watermark
determinism (same frames in → bit-identical verdicts out).

The engine is pure and deterministic (no wall clock, no randomness), so these tests build raw
`trades.raw` frames with `contracts.encode`, feed them through a fresh `RulesEngine`, and assert
on the verdict stream. The Kafka consumer glue (`run_gate`) is network I/O in the integration
lane and is not unit-tested here.
"""

from __future__ import annotations

from typing import Any

import pytest

from tickflow import contracts
from tickflow.gate import (
    DISPOSITION_QUARANTINE,
    DISPOSITION_VALID,
    QuarantineEnvelope,
    RulesConfig,
    RulesEngine,
    _LruWindow,
    build_envelope,
    evaluate_all,
    load_rules_config,
    verdicts_digest,
)

SCHEMA = contracts.load_schema()
CONFIG = load_rules_config()

BASE_TS = 1_784_443_000_000  # a fixed epoch-millis anchor; nothing here reads the wall clock.


def make_record(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "exchange": "coinbase",
        "symbol": "BTC-USD",
        "trade_id": "1",
        "price": 64_000.0,
        "size": 0.5,
        "side": "buy",
        "ts_event": BASE_TS,
        "ts_ingest": BASE_TS + 1,
    }
    record.update(overrides)
    return record


def frame(**overrides: Any) -> bytes:
    return contracts.encode(make_record(**overrides), SCHEMA, schema_id=7)


def engine() -> RulesEngine:
    return RulesEngine(CONFIG, SCHEMA)


# --------------------------------------------------------------------------------------------
# Config loading + the frozen alert-only invariant.
# --------------------------------------------------------------------------------------------
def test_config_reflects_frozen_defaults() -> None:
    assert CONFIG.symbol_bounds["BTC-USD"] == (1_000.0, 10_000_000.0)
    assert CONFIG.symbol_bounds["ETH-USD"] == (50.0, 1_000_000.0)
    assert CONFIG.lru_size == 10_000
    assert CONFIG.out_of_order_tolerance_ms == 5_000
    assert CONFIG.gap_threshold_ms == 60_000
    assert CONFIG.divergence_pct == 0.5
    assert CONFIG.divergence_sustained_ms == 10_000
    assert CONFIG.divergence_staleness_ms == 30_000  # ADR-001
    assert CONFIG.dispositions == {
        "R1": "quarantine",
        "R2": "quarantine",
        "R3": "quarantine",
        "R4": "quarantine",
        "R5": "alert",
        "R6": "alert",
    }


def test_load_rejects_missing_rule(tmp_path: Any) -> None:
    path = tmp_path / "rules.yaml"
    path.write_text("version: 1\nrules:\n  - id: R1\n    on_violation: quarantine\n    reason: x\n")
    with pytest.raises(ValueError, match="missing required rules"):
        load_rules_config(path)


def test_load_rejects_quarantining_an_alert_only_rule(tmp_path: Any) -> None:
    # The frozen decision: gap/divergence never quarantine true data. Config cannot override it.
    doc = _minimal_rules_doc()
    doc = doc.replace(
        "R6\n    name: divergence\n    kind: divergence\n    on_violation: alert",
        "R6\n    name: divergence\n    kind: divergence\n    on_violation: quarantine",
    )
    path = tmp_path / "rules.yaml"
    path.write_text(doc)
    with pytest.raises(ValueError, match="R6 is alert-only"):
        load_rules_config(path)


def test_load_rejects_alerting_a_quarantine_rule(tmp_path: Any) -> None:
    doc = _minimal_rules_doc().replace(
        "R2\n    name: range\n    kind: range\n    on_violation: quarantine",
        "R2\n    name: range\n    kind: range\n    on_violation: alert",
    )
    path = tmp_path / "rules.yaml"
    path.write_text(doc)
    with pytest.raises(ValueError, match="R2 must quarantine"):
        load_rules_config(path)


# --------------------------------------------------------------------------------------------
# R1 schema — quarantine `malformed`.
# --------------------------------------------------------------------------------------------
def test_r1_pass_valid_frame_routes_valid() -> None:
    verdict = engine().evaluate(frame())
    assert verdict.disposition == DISPOSITION_VALID
    assert verdict.rule_id is None
    assert verdict.record is not None and verdict.record["trade_id"] == "1"
    assert verdict.is_valid and not verdict.is_quarantine


def test_verdict_predicates_agree_with_disposition() -> None:
    quarantined = engine().evaluate(b"garbage")
    assert quarantined.is_quarantine and not quarantined.is_valid


@pytest.mark.parametrize(
    "bad",
    [
        b"",  # empty
        b"\x01\x00\x00\x00\x07",  # wrong magic byte
        b"\x00\x00\x00",  # truncated header
        contracts.encode(make_record(), SCHEMA, 7)[:6],  # truncated body
        contracts.encode(make_record(), SCHEMA, 7)[:5]
        + b"\xff\xff\xff\xff\xff\xff",  # garbage body
    ],
)
def test_r1_fail_malformed_frames_quarantine(bad: bytes) -> None:
    verdict = engine().evaluate(bad)
    assert verdict.disposition == DISPOSITION_QUARANTINE
    assert verdict.rule_id == "R1"
    assert verdict.reason == "malformed"
    assert verdict.record is None  # could not decode


# --------------------------------------------------------------------------------------------
# R2 range — quarantine `out-of-range`, with the boundary controls from §5.
# --------------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("overrides", "valid"),
    [
        ({"price": 64_000.0}, True),  # normal
        ({"price": 1_000.0}, True),  # BTC lower bound, inclusive → pass
        ({"price": 10_000_000.0}, True),  # BTC upper bound, inclusive → pass
        ({"price": 999.99}, False),  # just below BTC lower bound
        ({"price": 10_000_000.01}, False),  # just above BTC upper bound
        ({"price": 0.0}, False),  # price > 0 rule
        ({"price": -5.0}, False),  # negative
        ({"size": 0.0}, False),  # size > 0 rule
        ({"size": -0.1}, False),  # negative size
    ],
)
def test_r2_range_boundaries(overrides: dict[str, Any], valid: bool) -> None:
    verdict = engine().evaluate(frame(**overrides))
    if valid:
        assert verdict.disposition == DISPOSITION_VALID
    else:
        assert verdict.disposition == DISPOSITION_QUARANTINE
        assert verdict.rule_id == "R2"
        assert verdict.reason == "out-of-range"


def test_r2_uses_per_symbol_bounds() -> None:
    # 60 is inside ETH's [50, 1e6] but far below BTC's [1e3, 1e7]; the bound is symbol-scoped.
    eth = engine().evaluate(frame(symbol="ETH-USD", price=60.0))
    assert eth.disposition == DISPOSITION_VALID
    btc = engine().evaluate(frame(symbol="BTC-USD", price=60.0))
    assert btc.disposition == DISPOSITION_QUARANTINE
    assert btc.rule_id == "R2"


# --------------------------------------------------------------------------------------------
# R3 duplicate — quarantine `duplicate`, LRU window of exactly 10,000.
# --------------------------------------------------------------------------------------------
def test_r3_immediate_duplicate_quarantines() -> None:
    eng = engine()
    first = eng.evaluate(frame(trade_id="42"))
    second = eng.evaluate(frame(trade_id="42", ts_ingest=BASE_TS + 2))
    assert first.disposition == DISPOSITION_VALID
    assert second.disposition == DISPOSITION_QUARANTINE
    assert second.rule_id == "R3"
    assert second.reason == "duplicate"


def test_r3_duplicate_is_per_stream() -> None:
    eng = engine()
    eng.evaluate(frame(exchange="coinbase", trade_id="7"))
    # Same trade_id on a different stream (kraken) is not a duplicate.
    other = eng.evaluate(frame(exchange="kraken", symbol="BTC-USD", trade_id="7", price=64_000.0))
    assert other.disposition == DISPOSITION_VALID


def test_r3_lru_window_boundary_at_exactly_10000() -> None:
    window = _LruWindow(10_000)
    for i in range(10_000):
        window.add(str(i))
    assert len(window) == 10_000
    assert "0" in window  # the oldest key is still in the window at exactly capacity
    window.add("10000")  # the 10,001st distinct key evicts the oldest
    assert len(window) == 10_000
    assert "0" not in window  # designed miss: a re-injection beyond the window is not caught
    assert "1" in window and "10000" in window


def test_r3_duplicate_within_window_caught_but_beyond_window_missed() -> None:
    eng = engine()
    # trade_id "orig" enters, then 10,000 more distinct ids push it out of the 10k window.
    assert eng.evaluate(frame(trade_id="orig")).disposition == DISPOSITION_VALID
    for i in range(10_000):
        eng.evaluate(frame(trade_id=f"d{i}"))
    # "orig" has been evicted → its re-injection is a designed miss (routes valid).
    missed = eng.evaluate(frame(trade_id="orig", ts_ingest=BASE_TS + 99))
    assert missed.disposition == DISPOSITION_VALID


# --------------------------------------------------------------------------------------------
# R4 out-of-order — quarantine `out-of-order`, boundary at 5.0s vs 5.001s (frozen §10).
# --------------------------------------------------------------------------------------------
def test_r4_first_message_establishes_watermark() -> None:
    eng = engine()
    verdict = eng.evaluate(frame(trade_id="a", ts_event=BASE_TS))
    assert verdict.disposition == DISPOSITION_VALID
    assert eng.watermark(("coinbase", "BTC-USD")) == BASE_TS


@pytest.mark.parametrize(
    ("lateness_ms", "valid"),
    [
        (0, True),  # at the watermark
        (5_000, True),  # exactly the 5.0s tolerance — inside, passes
        (5_001, False),  # 5.001s — just outside, quarantined
        (60_000, False),  # far late
    ],
)
def test_r4_out_of_order_boundary(lateness_ms: int, valid: bool) -> None:
    eng = engine()
    # Advance the watermark to BASE_TS + 100_000 with a first in-order message.
    eng.evaluate(frame(trade_id="hi", ts_event=BASE_TS + 100_000))
    watermark = BASE_TS + 100_000
    verdict = eng.evaluate(
        frame(trade_id="late", ts_event=watermark - lateness_ms, ts_ingest=BASE_TS + 200_000)
    )
    if valid:
        assert verdict.disposition == DISPOSITION_VALID
    else:
        assert verdict.disposition == DISPOSITION_QUARANTINE
        assert verdict.rule_id == "R4"
        assert verdict.reason == "out-of-order"


def test_r4_late_but_valid_does_not_advance_watermark() -> None:
    eng = engine()
    eng.evaluate(frame(trade_id="hi", ts_event=BASE_TS + 100_000))
    eng.evaluate(frame(trade_id="late", ts_event=BASE_TS + 97_000, ts_ingest=BASE_TS + 1))
    assert eng.watermark(("coinbase", "BTC-USD")) == BASE_TS + 100_000  # unchanged


# --------------------------------------------------------------------------------------------
# R5 gap — ALERT ONLY. Never quarantines; raises an alert and increments telemetry.
# --------------------------------------------------------------------------------------------
def test_r5_gap_is_alert_only_and_message_still_valid() -> None:
    eng = engine()
    eng.evaluate(frame(trade_id="a", ts_event=BASE_TS))
    verdict = eng.evaluate(frame(trade_id="b", ts_event=BASE_TS + 61_000))  # 61s jump > 60s
    assert verdict.disposition == DISPOSITION_VALID  # not quarantined
    assert [a.rule_id for a in verdict.alerts] == ["R5"]
    assert verdict.alerts[0].reason == "gap"
    assert eng.telemetry.gap_alerts == 1


@pytest.mark.parametrize(
    ("jump_ms", "expect_alert"),
    [
        (60_000, False),  # exactly the threshold — not "> threshold", no alert
        (60_001, True),  # just over — alert
    ],
)
def test_r5_gap_boundary(jump_ms: int, expect_alert: bool) -> None:
    eng = engine()
    eng.evaluate(frame(trade_id="a", ts_event=BASE_TS))
    verdict = eng.evaluate(frame(trade_id="b", ts_event=BASE_TS + jump_ms))
    assert verdict.disposition == DISPOSITION_VALID
    assert bool(verdict.alerts) == expect_alert


def test_r5_no_gap_on_first_message() -> None:
    verdict = engine().evaluate(frame(trade_id="a", ts_event=BASE_TS))
    assert verdict.alerts == ()


# --------------------------------------------------------------------------------------------
# R6 divergence — ALERT ONLY, cross-venue, sustained-10s, with ADR-001 staleness.
# --------------------------------------------------------------------------------------------
def _btc(exchange: str, price: float, ts: int, tid: str) -> bytes:
    return frame(
        exchange=exchange, symbol="BTC-USD", price=price, ts_event=ts, ts_ingest=ts, trade_id=tid
    )


def test_r6_no_alert_before_sustained_window() -> None:
    eng = engine()
    # Both venues print; deviation is ~1.5% (> 0.5%) but only just began — not yet sustained 10s.
    eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS, "c0"))
    verdict = eng.evaluate(_btc("kraken", 65_000.0, BASE_TS, "k0"))
    assert verdict.alerts == ()
    assert eng.telemetry.divergence_alerts == 0


def test_r6_alerts_once_divergence_sustained_10s() -> None:
    eng = engine()
    eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS, "c0"))
    eng.evaluate(_btc("kraken", 65_000.0, BASE_TS, "k0"))  # divergence begins at BASE_TS
    # 10s later, still diverging → one alert.
    v = eng.evaluate(_btc("kraken", 65_000.0, BASE_TS + 10_000, "k1"))
    assert [a.rule_id for a in v.alerts] == ["R6"]
    assert v.alerts[0].reason == "divergence"
    assert v.disposition == DISPOSITION_VALID  # alert-only: the trade still flows to valid
    assert eng.telemetry.divergence_alerts == 1
    # It does not re-alert every subsequent tick within the same episode.
    v2 = eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS + 11_000, "c1"))
    assert v2.alerts == ()
    assert eng.telemetry.divergence_alerts == 1


@pytest.mark.parametrize(
    ("sustained_ms", "expect_alert"),
    [
        (9_999, False),  # just under 10s — no alert yet
        (10_000, True),  # exactly 10s — alert
    ],
)
def test_r6_sustained_boundary(sustained_ms: int, expect_alert: bool) -> None:
    eng = engine()
    eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS, "c0"))
    eng.evaluate(_btc("kraken", 65_000.0, BASE_TS, "k0"))
    v = eng.evaluate(_btc("kraken", 65_000.0, BASE_TS + sustained_ms, "k1"))
    assert bool(v.alerts) == expect_alert


@pytest.mark.parametrize(
    ("kraken_price", "expect_alert"),
    [
        (64_320.0, False),  # exactly 0.5% deviation ((64320-64000)/mid=0.499%..) — not "> 0.5%"
        (64_400.0, True),  # ~0.62% — over threshold, and sustained
    ],
)
def test_r6_deviation_threshold_boundary(kraken_price: float, expect_alert: bool) -> None:
    eng = engine()
    eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS, "c0"))
    eng.evaluate(_btc("kraken", kraken_price, BASE_TS, "k0"))
    v = eng.evaluate(_btc("kraken", kraken_price, BASE_TS + 10_000, "k1"))
    assert bool(v.alerts) == expect_alert


def test_r6_deviation_exactly_half_percent_is_not_over() -> None:
    # Construct an exact 0.5% deviation: a=63840, b=64160 → mid 64000, |a-b|/mid = 0.5% exactly.
    eng = engine()
    eng.evaluate(_btc("coinbase", 63_840.0, BASE_TS, "c0"))
    eng.evaluate(_btc("kraken", 64_160.0, BASE_TS, "k0"))
    v = eng.evaluate(_btc("kraken", 64_160.0, BASE_TS + 20_000, "k1"))
    assert v.alerts == ()  # strictly greater-than, so exactly 0.5% does not alert


def test_r6_stale_venue_emits_no_verdict_and_increments_telemetry() -> None:
    eng = engine()
    # Coinbase prints once, then goes quiet. Kraken keeps printing well past the 30s window.
    eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS, "c0"))
    eng.evaluate(_btc("kraken", 66_000.0, BASE_TS, "k0"))  # both fresh here
    # 31s later only Kraken prints — Coinbase's last print is now stale (> 30s), so NO divergence
    # verdict despite a large price gap; telemetry records the unavailability instead (ADR-001).
    v = eng.evaluate(_btc("kraken", 66_000.0, BASE_TS + 31_000, "k1"))
    assert v.alerts == ()
    assert eng.telemetry.divergence_alerts == 0
    assert eng.telemetry.divergence_unavailable["stale_coinbase"] >= 1


def test_r6_never_alerts_with_only_one_venue() -> None:
    eng = engine()
    for i in range(5):
        v = eng.evaluate(_btc("coinbase", 64_000.0 + i * 5_000, BASE_TS + i * 20_000, f"c{i}"))
        assert v.alerts == ()  # a single venue can never diverge cross-venue
    assert eng.telemetry.divergence_alerts == 0
    assert eng.telemetry.divergence_unavailable["stale_kraken"] >= 1


def test_r6_divergence_resets_when_venues_reconverge() -> None:
    eng = engine()
    eng.evaluate(_btc("coinbase", 64_000.0, BASE_TS, "c0"))
    eng.evaluate(_btc("kraken", 65_000.0, BASE_TS, "k0"))  # diverging begins
    # Reconverge before the 10s window elapses → sustain timer resets, no alert.
    eng.evaluate(_btc("kraken", 64_010.0, BASE_TS + 3_000, "k1"))
    v = eng.evaluate(_btc("kraken", 65_000.0, BASE_TS + 12_000, "k2"))  # diverge again, fresh start
    assert v.alerts == ()  # only 0ms sustained since the restart
    assert eng.telemetry.divergence_alerts == 0


# --------------------------------------------------------------------------------------------
# Quarantine routing + envelope (§3).
# --------------------------------------------------------------------------------------------
def test_envelope_carries_rule_offset_ts_and_raw() -> None:
    raw = frame(price=-1.0)  # R2 out-of-range
    verdict = engine().evaluate(raw)
    envelope = build_envelope(verdict, raw, offset=99, partition=2)
    assert isinstance(envelope, QuarantineEnvelope)
    assert envelope.rule_id == "R2"
    assert envelope.reason == "out-of-range"
    assert envelope.offset == 99
    assert envelope.partition == 2
    assert envelope.ts_ms == BASE_TS + 1  # the record's ts_ingest
    assert envelope.raw == raw


def test_envelope_key_is_offset_derived_and_stable() -> None:
    raw = frame(price=-1.0)
    verdict = engine().evaluate(raw)
    key1 = build_envelope(verdict, raw, offset=5, partition=1).key
    key2 = build_envelope(verdict, raw, offset=5, partition=1).key
    assert key1 == key2 == b"1:5"  # idempotent quarantine write key (§3)


def test_envelope_key_falls_back_to_content_hash_without_offset() -> None:
    raw = b"\x00garbage"  # R1 malformed, no source offset (e.g. direct replay)
    verdict = engine().evaluate(raw)
    envelope = build_envelope(verdict, raw, offset=None)
    assert envelope.ts_ms is None  # could not decode → no ts
    assert envelope.key.startswith(b"sha256:")


def test_envelope_round_trips_through_json() -> None:
    import base64
    import json

    raw = frame(trade_id="dup")
    eng = engine()
    eng.evaluate(raw)
    dup_verdict = eng.evaluate(frame(trade_id="dup", ts_ingest=BASE_TS + 9))
    envelope = build_envelope(dup_verdict, raw, offset=3, partition=0)
    decoded = json.loads(envelope.to_json())
    assert decoded["rule_id"] == "R3"
    assert base64.b64decode(decoded["raw_b64"]) == raw


def test_build_envelope_rejects_valid_verdict() -> None:
    raw = frame()
    verdict = engine().evaluate(raw)
    with pytest.raises(ValueError, match="requires a quarantine verdict"):
        build_envelope(verdict, raw)


# --------------------------------------------------------------------------------------------
# Telemetry rollup.
# --------------------------------------------------------------------------------------------
def test_telemetry_counts_by_rule() -> None:
    eng = engine()
    eng.evaluate(frame(trade_id="ok"))  # valid
    eng.evaluate(b"garbage")  # R1
    eng.evaluate(frame(trade_id="bad", price=-1.0))  # R2
    eng.evaluate(frame(trade_id="ok", ts_ingest=BASE_TS + 5))  # R3 (dup of "ok")
    snapshot = eng.telemetry.as_dict()
    assert snapshot["valid"] == 1
    assert snapshot["quarantined"] == {"R1": 1, "R2": 1, "R3": 1}


# --------------------------------------------------------------------------------------------
# Determinism — the property that makes fixture replay meaningful (frozen §2/§10).
# --------------------------------------------------------------------------------------------
def _mixed_stream() -> list[bytes]:
    """A stream touching every rule path: valid, malformed, out-of-range, dup, out-of-order,
    a gap, and a sustained cross-venue divergence."""
    frames = [
        _btc("coinbase", 64_000.0, BASE_TS, "c0"),
        _btc("kraken", 65_000.0, BASE_TS, "k0"),  # divergence begins
        b"\x00\x01\x02malformed",  # R1
        frame(trade_id="x", price=2.0),  # R2 (below BTC bound)
        _btc("coinbase", 64_000.0, BASE_TS, "c0"),  # R3 duplicate of c0
        _btc("coinbase", 64_000.0, BASE_TS - 30_000, "old"),  # R4 out-of-order
        _btc("kraken", 65_000.0, BASE_TS + 10_000, "k1"),  # R6 sustained alert
        _btc("coinbase", 64_000.0, BASE_TS + 90_000, "c1"),  # R5 gap (90s jump)
    ]
    return frames


def test_replay_is_bit_identical_across_two_fresh_engines() -> None:
    frames = _mixed_stream()
    first = evaluate_all(CONFIG, SCHEMA, frames)
    second = evaluate_all(CONFIG, SCHEMA, frames)
    assert verdicts_digest(first) == verdicts_digest(second)


def test_replay_verdicts_match_expected_routing() -> None:
    verdicts = evaluate_all(CONFIG, SCHEMA, _mixed_stream())
    routing = [(v.disposition, v.rule_id) for v in verdicts]
    assert routing == [
        (DISPOSITION_VALID, None),  # c0
        (DISPOSITION_VALID, None),  # k0 (divergence begins, no alert yet)
        (DISPOSITION_QUARANTINE, "R1"),
        (DISPOSITION_QUARANTINE, "R2"),
        (DISPOSITION_QUARANTINE, "R3"),
        (DISPOSITION_QUARANTINE, "R4"),
        (DISPOSITION_VALID, None),  # k1 — valid, but carries the R6 alert
        (DISPOSITION_VALID, None),  # c1 — valid, but carries the R5 gap alert
    ]
    # The alert-only rules attach alerts without changing routing.
    assert [a.rule_id for a in verdicts[6].alerts] == ["R6"]
    assert [a.rule_id for a in verdicts[7].alerts] == ["R5"]


def test_different_config_can_change_verdicts_but_stays_deterministic() -> None:
    # A stricter out-of-order tolerance flips a borderline message; determinism holds regardless.
    strict = RulesConfig(
        price_min=CONFIG.price_min,
        size_min=CONFIG.size_min,
        symbol_bounds=CONFIG.symbol_bounds,
        lru_size=CONFIG.lru_size,
        out_of_order_tolerance_ms=1_000,
        gap_threshold_ms=CONFIG.gap_threshold_ms,
        divergence_pct=CONFIG.divergence_pct,
        divergence_sustained_ms=CONFIG.divergence_sustained_ms,
        divergence_staleness_ms=CONFIG.divergence_staleness_ms,
        dispositions=CONFIG.dispositions,
        reasons=CONFIG.reasons,
    )
    frames = [
        frame(trade_id="hi", ts_event=BASE_TS + 100_000),
        frame(trade_id="late", ts_event=BASE_TS + 97_000, ts_ingest=BASE_TS + 1),  # 3s late
    ]
    default = evaluate_all(CONFIG, SCHEMA, frames)
    stricter = evaluate_all(strict, SCHEMA, frames)
    assert default[1].disposition == DISPOSITION_VALID  # 3s < 5s default tolerance
    assert stricter[1].disposition == DISPOSITION_QUARANTINE  # 3s > 1s strict tolerance
    assert verdicts_digest(stricter) == verdicts_digest(evaluate_all(strict, SCHEMA, frames))


# --------------------------------------------------------------------------------------------
# A minimal valid rules.yaml used by the config-rejection tests above.
# --------------------------------------------------------------------------------------------
_MINIMAL_RULES_YAML = """\
version: 1
rules:
  - id: R1
    name: schema
    kind: schema
    on_violation: quarantine
    reason: malformed
  - id: R2
    name: range
    kind: range
    on_violation: quarantine
    reason: out-of-range
    params:
      price_min: 0.0
      size_min: 0.0
      symbol_bounds:
        BTC-USD: [1000.0, 10000000.0]
        ETH-USD: [50.0, 1000000.0]
  - id: R3
    name: duplicate
    kind: duplicate
    on_violation: quarantine
    reason: duplicate
    params:
      lru_size: 10000
  - id: R4
    name: out_of_order
    kind: out_of_order
    on_violation: quarantine
    reason: out-of-order
    params:
      tolerance_ms: 5000
  - id: R5
    name: gap
    kind: gap
    on_violation: alert
    reason: gap
    params:
      threshold_ms: 60000
  - id: R6
    name: divergence
    kind: divergence
    on_violation: alert
    reason: divergence
    params:
      deviation_pct: 0.5
      sustained_ms: 10000
      staleness_ms: 30000
"""


def _minimal_rules_doc() -> str:
    return _MINIMAL_RULES_YAML


def test_minimal_rules_doc_loads() -> None:
    # Sanity-check the fixture the rejection tests mutate: unmutated, it must load cleanly.
    import pathlib
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        path = pathlib.Path(d) / "rules.yaml"
        path.write_text(_minimal_rules_doc())
        cfg = load_rules_config(path)
        assert cfg.lru_size == 10_000
