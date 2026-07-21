"""The gates-ON/OFF SLO experiment — the thesis made visible (frozen §4).

This is the project's signature result, checked end to end against the real gate and the real
fault injector:

- **Gates ON hold the SLO.** Replaying the fault-injected fixture with the gate on yields bars with
  zero SLO violations — every quarantine-worthy frame was removed before it could reach a bar.
- **Gates OFF break the SLO.** The same fixture with everything routed valid drops the faults into
  the bars; the SLO checker counts `K > 0` violated bars, dominated by the load-bearing
  `no_quarantinable` invariant (plus corrupted extremes from out-of-range faults).
- **Bit-identity on the catchable subset.** With the designed-miss duplicates filtered out, the
  gate's gates-ON bars are bit-identical to the manifest's ground-truth valid projection.
- **Designed misses are reported, not hidden.** The dups past the LRU window (`detectable=False`)
  are surfaced as the R3 recall gap — the frozen point that keeps the numbers honest.

Also the clean-input anchor: feeding the *clean* fixture through the gate routes everything valid
with zero SLO violations and reconstructs the clean bars exactly.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from tickflow import bars as bars_mod
from tickflow import contracts, fixture
from tickflow.bars import (
    SLO_NO_QUARANTINABLE,
    SLO_PRICE_POSITIVE,
    bars_digest,
    build_bars,
    check_slo,
    expected_valid_projection_bars,
    fixture_label,
    gates_off_bars,
    gates_on_bars,
    run_slo_experiment,
)
from tickflow.gate import evaluate_all, load_rules_config

SCHEMA = contracts.load_schema()
CONFIG = load_rules_config()
# A small LRU so the designed-miss dup boundary (past the window) is exercised cheaply, with
# streams long enough to host the exact dedup-window edge pair.
SMALL_CONFIG = dataclasses.replace(CONFIG, lru_size=100)
N = 300


def _injected() -> fixture.InjectionResult:
    clean = fixture.generate_clean(n_per_stream=N)
    return fixture.inject_faults(clean, SMALL_CONFIG, SCHEMA)


# --------------------------------------------------------------------------------------------
# Clean-input anchor.
# --------------------------------------------------------------------------------------------
def test_clean_fixture_passes_the_gate_with_no_slo_violations() -> None:
    clean = fixture.generate_clean(n_per_stream=N)
    clean_flat = fixture.flatten(clean)
    frames = [contracts.encode(r, SCHEMA, fixture.FIXTURE_SCHEMA_ID) for r in clean_flat]
    verdicts = evaluate_all(SMALL_CONFIG, SCHEMA, frames)
    assert all(v.is_valid for v in verdicts)  # zero false quarantine on clean
    on_bars = gates_on_bars(verdicts)
    assert check_slo(on_bars).ok
    # Gate output reconstructs the clean bars exactly when the input is clean.
    assert bars_digest(on_bars) == bars_digest(build_bars(clean_flat))


# --------------------------------------------------------------------------------------------
# The signature experiment.
# --------------------------------------------------------------------------------------------
def test_gates_on_hold_the_slo_gates_off_break_it() -> None:
    result = _injected()
    comparison = run_slo_experiment(SMALL_CONFIG, SCHEMA, result.frames, result.manifest)

    # Gates ON: the SLO holds.
    assert comparison.gates_on.ok
    assert comparison.gates_on.n_violated_bars == 0

    # Gates OFF: the SLO visibly breaks.
    assert not comparison.gates_off.ok
    assert comparison.gates_off.n_violated_bars > 0

    # Bit-identity on the catchable subset, and the designed misses are surfaced.
    assert comparison.bit_identical
    assert comparison.designed_miss_dups > 0
    assert comparison.thesis_holds


def test_gates_off_violations_are_dominated_by_quarantinable_constituents() -> None:
    result = _injected()
    comparison = run_slo_experiment(SMALL_CONFIG, SCHEMA, result.frames, result.manifest)
    counts = comparison.gates_off.counts_by_invariant()
    # Every bar containing a quarantine-worthy frame trips the load-bearing invariant.
    assert counts[SLO_NO_QUARANTINABLE] > 0
    # Out-of-range faults (negative/zero prices) also corrupt bar extremes.
    assert counts[SLO_PRICE_POSITIVE] > 0


def test_experiment_is_deterministic() -> None:
    a = run_slo_experiment(SMALL_CONFIG, SCHEMA, *_frames_manifest())
    b = run_slo_experiment(SMALL_CONFIG, SCHEMA, *_frames_manifest())
    assert a.as_dict() == b.as_dict()
    assert a.on_digest == b.on_digest
    assert a.reference_digest == b.reference_digest


def _frames_manifest() -> tuple[list[bytes], fixture.InjectionManifest]:
    result = _injected()
    return result.frames, result.manifest


# --------------------------------------------------------------------------------------------
# The component helpers.
# --------------------------------------------------------------------------------------------
def test_gates_off_bars_drop_undecodable_malformed_frames() -> None:
    result = _injected()
    verdicts = evaluate_all(SMALL_CONFIG, SCHEMA, result.frames)
    # Some frames are malformed (R1) and cannot decode into a bar even with gates off.
    n_malformed = sum(1 for v in verdicts if v.rule_id == "R1")
    assert n_malformed > 0
    n_decodable = sum(1 for v in verdicts if v.record is not None)
    off = gates_off_bars(verdicts)
    assert sum(b.count for b in off) == n_decodable  # every decodable frame, once


def test_gates_on_bars_have_no_tainted_constituents() -> None:
    result = _injected()
    verdicts = evaluate_all(SMALL_CONFIG, SCHEMA, result.frames)
    on = gates_on_bars(verdicts)
    assert all(b.tainted == 0 for b in on)


def test_gates_on_bars_equal_projection_on_catchable_subset() -> None:
    result = _injected()
    from tickflow.bars import _is_designed_miss

    by_index = result.manifest.by_index()
    catchable = [f for i, f in enumerate(result.frames) if not _is_designed_miss(by_index.get(i))]
    verdicts = evaluate_all(SMALL_CONFIG, SCHEMA, catchable)
    on = gates_on_bars(verdicts)
    # The projection reads the manifest by absolute index, so it runs over the FULL frames.
    projection = expected_valid_projection_bars(result.frames, result.manifest, SCHEMA)
    assert bars_digest(on) == bars_digest(projection)


def test_full_stream_gates_on_bars_are_not_clean_bars() -> None:
    # The honest negative: over the FULL stream the designed-miss dups and the value-altering
    # controls mean gates-ON bars are NOT the raw clean-fixture bars — which is why the reference
    # is the ground-truth valid projection (ADR-002), not the clean fixture.
    result = _injected()
    verdicts = evaluate_all(SMALL_CONFIG, SCHEMA, result.frames)
    on = gates_on_bars(verdicts)
    clean_bars = build_bars(fixture.flatten(fixture.generate_clean(n_per_stream=N)))
    assert bars_digest(on) != bars_digest(clean_bars)


# --------------------------------------------------------------------------------------------
# The committed-fixture runner — the CLI surface the experiment was missing (Day D).
# --------------------------------------------------------------------------------------------
def test_fixture_label_states_scale_seed_and_window() -> None:
    """No SLO number may travel without the fixture that produced it (small != committed)."""
    result = _injected()
    label = fixture_label("small", result.manifest, SMALL_CONFIG)
    assert "small" in label
    assert f"{result.manifest.n_total:,}" in label
    assert "seed 42" in label
    assert "LRU 100" in label  # the small config's window, not the committed 10,000


def test_committed_fixture_experiment_reproduces_the_thesis_at_fixture_scale() -> None:
    """The §4 experiment over the committed 100k fixture, via the path `tickflow slo` calls.

    Deliberately not asserting the exact violated-bar count: that is a measured number the CLI
    regenerates and the docs quote, and pinning it here would turn a measurement into a
    self-referential constant. What is asserted is the shape of the result — the thesis.
    """
    comparison = bars_mod.run_committed_fixture_experiment()

    assert comparison.n_frames == 100_477  # manifest accounting: 100,000 clean + 477 added dups
    assert "trades.v1.clean.parquet" in comparison.fixture
    assert "LRU 10,000" in comparison.fixture

    assert comparison.gates_on.ok
    assert comparison.gates_on.n_violated_bars == 0
    assert not comparison.gates_off.ok
    assert comparison.gates_off.n_violated_bars > 0
    assert comparison.gates_off.n_bars == comparison.gates_on.n_bars

    off_counts = comparison.gates_off.counts_by_invariant()
    assert off_counts[SLO_NO_QUARANTINABLE] > 0  # the load-bearing invariant
    assert off_counts[SLO_PRICE_POSITIVE] > 0  # corrupted extremes from R2 faults
    # One bar can trip several invariants, so the per-invariant total may exceed the bar count.
    assert sum(off_counts.values()) >= comparison.gates_off.n_violated_bars

    assert comparison.bit_identical
    assert comparison.designed_miss_dups > 0
    assert comparison.thesis_holds


def test_committed_fixture_experiment_refuses_an_unpinned_fixture(monkeypatch: Any) -> None:
    """The checksum is the system of record: a fixture that fails its pins produces no numbers."""
    monkeypatch.setattr(fixture, "verify_fixture", lambda *a, **k: (False, "content_digest drift"))
    with pytest.raises(ValueError, match="failed verification"):
        bars_mod.run_committed_fixture_experiment()
