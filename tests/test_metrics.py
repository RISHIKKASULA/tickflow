"""Fault-injection grading + bootstrap CI tests (frozen §6/§10).

Three layers are proven:

- **Bootstrap CIs** — seeded determinism, the CI brackets the point estimate, the degenerate
  all-hit / all-miss cases pin to a point, a rigged two-value case has hand-checkable percentiles,
  and `n == 0` is reported as undefined rather than a fake 0.
- **Grading arithmetic** — a rigged mini-manifest with a hand-computed recall/precision/false-
  quarantine table graded exactly, including a mislabelled quarantine and a designed miss.
- **The CI replay** — the committed fixture injected + replayed through the real gate grades to the
  honest numbers: R1/R2/R4 recall ~100% (deterministic rules), R3 recall < 100% (designed misses
  drag it down, exactly as intended), precision ~100%, zero false quarantine, complete accounting.
"""

from __future__ import annotations

from typing import Any

import pytest

from tickflow import fixture, metrics
from tickflow.fixture import (
    DUPLICATE,
    MALFORMED,
    OUT_OF_ORDER,
    OUT_OF_RANGE,
    FaultEntry,
    InjectionManifest,
)
from tickflow.gate import (
    DISPOSITION_QUARANTINE,
    DISPOSITION_VALID,
    Verdict,
)
from tickflow.metrics import Estimate, bootstrap_proportion_ci, grade


# --------------------------------------------------------------------------------------------
# Bootstrap CIs.
# --------------------------------------------------------------------------------------------
def test_bootstrap_is_seeded_deterministic() -> None:
    a = bootstrap_proportion_ci(92, 100)
    b = bootstrap_proportion_ci(92, 100)
    assert a == b


def test_bootstrap_ci_brackets_the_point() -> None:
    est = bootstrap_proportion_ci(92, 100)
    assert est.point == 0.92
    assert est.ci_low <= est.point <= est.ci_high
    assert est.ci_low < est.ci_high  # a non-degenerate proportion has a real interval


def test_bootstrap_degenerate_all_hits_and_all_misses_pin_to_a_point() -> None:
    all_hit = bootstrap_proportion_ci(50, 50)
    assert (all_hit.point, all_hit.ci_low, all_hit.ci_high) == (1.0, 1.0, 1.0)
    none = bootstrap_proportion_ci(0, 50)
    assert (none.point, none.ci_low, none.ci_high) == (0.0, 0.0, 0.0)


def test_bootstrap_rigged_two_value_percentiles() -> None:
    # Resampling a two-item 0/1 sample: the resample mean is in {0, 0.5, 1.0} with weights
    # 1/4, 1/2, 1/4, so the 2.5th percentile is 0.0 and the 97.5th is 1.0. Hand-checkable.
    est = bootstrap_proportion_ci(1, 2)
    assert est.point == 0.5
    assert est.ci_low == 0.0
    assert est.ci_high == 1.0


def test_bootstrap_n_zero_is_undefined() -> None:
    est = bootstrap_proportion_ci(0, 0)
    assert not est.defined
    assert est.format() == "n/a (n=0)"
    assert est.as_dict() == {"point": None, "ci_low": None, "ci_high": None, "n": 0}


def test_bootstrap_rejects_successes_out_of_range() -> None:
    with pytest.raises(ValueError):
        bootstrap_proportion_ci(5, 3)


def test_estimate_format_and_as_dict_when_defined() -> None:
    est = Estimate(0.84, 0.80, 0.87, 100)
    assert est.format() == "0.8400 [0.8000, 0.8700]"
    assert est.as_dict() == {"point": 0.84, "ci_low": 0.8, "ci_high": 0.87, "n": 100}


# --------------------------------------------------------------------------------------------
# Rigged mini-manifest with a hand-computed table.
# --------------------------------------------------------------------------------------------
def _entry(
    index: int, fault_class: str, is_fault: bool, detectable: bool, rule: str | None
) -> FaultEntry:
    disposition = DISPOSITION_QUARANTINE if (is_fault and detectable) else DISPOSITION_VALID
    return FaultEntry(
        index=index,
        stream="coinbase:BTC-USD",
        fault_class=fault_class,
        is_fault=is_fault,
        detectable=detectable,
        expected_disposition=disposition,
        expected_rule=rule,
        boundary=None,
    )


def _q(rule: str) -> Verdict:
    return Verdict(DISPOSITION_QUARANTINE, rule, "reason", "", {"trade_id": "x"})


def _v() -> Verdict:
    return Verdict(DISPOSITION_VALID, None, None, "", {"trade_id": "x"})


def test_rigged_manifest_grades_to_the_hand_computed_table() -> None:
    # 8 frames. Entries at 0-5; frames 6,7 are untouched clean (no entry).
    entries = [
        _entry(0, MALFORMED, is_fault=True, detectable=True, rule="R1"),  # caught
        _entry(1, MALFORMED, is_fault=True, detectable=True, rule="R1"),  # MISSED (routed valid)
        _entry(2, OUT_OF_RANGE, is_fault=True, detectable=True, rule="R2"),  # caught
        _entry(3, OUT_OF_RANGE, is_fault=False, detectable=True, rule=None),  # control, wrongly q'd
        _entry(4, DUPLICATE, is_fault=True, detectable=True, rule="R3"),  # caught
        _entry(5, DUPLICATE, is_fault=True, detectable=False, rule=None),  # designed miss
    ]
    manifest = InjectionManifest(seed=42, n_total=8, lru_size=100, entries=entries)
    verdicts = [
        _q("R1"),  # 0 caught
        _v(),  # 1 missed
        _q("R2"),  # 2 caught
        _q("R2"),  # 3 control wrongly quarantined as R2 (false quarantine + precision miss)
        _q("R3"),  # 4 caught
        _v(),  # 5 designed miss routes valid
        _v(),  # 6 clean
        _v(),  # 7 clean
    ]
    report = grade(verdicts, manifest, b=2000)

    mal = report.per_class[MALFORMED]
    assert (mal.n_faults, mal.n_detected, mal.n_designed_miss) == (2, 1, 0)
    assert mal.recall.point == 0.5

    rng = report.per_class[OUT_OF_RANGE]
    assert (rng.n_faults, rng.n_detected) == (1, 1)
    assert rng.recall.point == 1.0
    # R2 quarantined twice (idx 2 correct, idx 3 a control) → precision 1/2.
    assert rng.n_quarantined_as_rule == 2
    assert rng.precision.point == 0.5

    dup = report.per_class[DUPLICATE]
    assert (dup.n_faults, dup.n_detected, dup.n_designed_miss) == (2, 1, 1)
    assert dup.recall.point == 0.5  # the designed miss drags it down
    assert dup.precision.point == 1.0

    ooo = report.per_class[OUT_OF_ORDER]
    assert ooo.n_faults == 0
    assert not ooo.recall.defined  # no out-of-order faults injected → n/a

    # False quarantine over controls (idx 3) + untouched clean (idx 6, 7) = 1/3.
    assert report.n_controls == 3
    assert report.false_quarantine.point == pytest.approx(1 / 3)

    assert report.completeness.ok
    assert report.completeness.n_valid == 4
    assert report.completeness.n_quarantine == 4


def _all_keys(obj: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            keys.add(key)
            keys |= _all_keys(value)
    elif isinstance(obj, list):
        for item in obj:
            keys |= _all_keys(item)
    return keys


def test_grade_report_serializes_to_telemetry_only() -> None:
    manifest = InjectionManifest(
        seed=42, n_total=2, lru_size=100, entries=[_entry(0, MALFORMED, True, True, "R1")]
    )
    report = grade([_q("R1"), _v()], manifest, b=500)
    payload = report.as_dict()
    # No market-data-derived field may appear as a key anywhere in the report (§8 ToS). Checked as
    # exact field names — "ci_low"/"ci_high" are CI bounds, not the market "low"/"high".
    forbidden = {"price", "open", "high", "low", "close", "volume", "vwap", "mid", "bid", "ask"}
    assert _all_keys(payload) & forbidden == set()
    assert payload["per_class"][MALFORMED]["recall"]["point"] == 1.0


# --------------------------------------------------------------------------------------------
# The CI replay over the committed fixture — the honest end-to-end numbers.
# --------------------------------------------------------------------------------------------
def test_committed_fixture_grades_to_the_honest_numbers() -> None:
    report = metrics.grade_committed_fixture(b=2000)
    assert report.completeness.ok
    # 100,000 clean frames + the extra duplicate frames the injector inserts.
    assert report.completeness.n_total > 100_000

    # Deterministic rules → perfect recall/precision on the non-boundary classes.
    for cls in (MALFORMED, OUT_OF_RANGE, OUT_OF_ORDER):
        m = report.per_class[cls]
        assert m.recall.point == 1.0
        assert m.precision.point == 1.0
        assert m.n_designed_miss == 0

    # R3 duplicate: the designed misses (dups past the LRU window) drag recall below 100% — the
    # frozen point of the whole exercise. Precision stays perfect.
    dup = report.per_class[DUPLICATE]
    assert dup.n_designed_miss > 0
    assert dup.n_detected < dup.n_faults
    assert dup.recall.point < 1.0
    assert dup.recall.ci_low <= dup.recall.point <= dup.recall.ci_high
    assert dup.precision.point == 1.0

    # No gate over-rejection: zero false quarantine over the boundary controls + all clean.
    assert report.false_quarantine.point == 0.0
    assert report.n_controls > 90_000


def test_committed_fixture_grade_requires_a_valid_fixture(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        fixture, "verify_fixture", lambda *a, **k: (False, "parquet_sha256 mismatch")
    )
    with pytest.raises(ValueError, match="failed verification"):
        metrics.grade_committed_fixture(b=100)
