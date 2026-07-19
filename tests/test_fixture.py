"""Fixture generator + fault injector tests (frozen §5/§10).

Three things are proven here:

- **Generator determinism** — same seed produces a bit-identical clean stream, and the freshly
  generated content digest equals the pin committed in `fixtures.yaml` (guards against silent
  generator drift), plus the committed parquet artifact verifies against its pins.
- **Manifest correctness** — the injection manifest's counts, rates, and per-entry expectations
  match what was actually injected, and re-injecting with the same seed is byte-identical.
- **Detectability** — every injected fault is actually caught by the rule it targets when the
  real `RulesEngine` runs the faulted stream, every boundary control routes valid, and every
  designed-miss (dup past the LRU window) routes valid. Because the injector declares the expected
  verdict and the gate is the real engine, this cross-checks both at once — and the boundary cases
  are what keep detection genuinely imperfect (so Day C's numbers are informative, not rigged).

Most injector tests use a small config (tiny LRU, short streams) so the exact dedup-window edge is
exercised cheaply; one test runs the full 100,000-message fixture through the real gate.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest

from tickflow import contracts, fixture
from tickflow.fixture import (
    DUPLICATE,
    MALFORMED,
    OUT_OF_ORDER,
    OUT_OF_RANGE,
    FaultEntry,
    InjectionManifest,
    _corrupt_frame,
    _duplicate,
    _in_range_control,
    _out_of_range,
    _sample_disjoint,
    _skewed,
    content_digest,
    flatten,
    generate_clean,
    generate_stream,
    inject_faults,
)
from tickflow.gate import (
    DISPOSITION_QUARANTINE,
    DISPOSITION_VALID,
    RulesEngine,
    load_rules_config,
)

SCHEMA = contracts.load_schema()
CONFIG = load_rules_config()

# A small, fast config: a 100-key LRU with 250-message streams still exercises the exact
# dedup-window edge (a dup lru-1 keys back vs lru keys back) without the full 100k fixture.
SMALL_CONFIG = dataclasses.replace(CONFIG, lru_size=100)
SMALL_N = 250


def small_clean() -> dict[tuple[str, str], list[dict[str, Any]]]:
    return generate_clean(n_per_stream=SMALL_N)


# --------------------------------------------------------------------------------------------
# Clean generator: determinism, and cleanliness against the gate.
# --------------------------------------------------------------------------------------------
def test_generator_is_bit_identical_for_the_same_seed() -> None:
    first = flatten(generate_clean(n_per_stream=500))
    second = flatten(generate_clean(n_per_stream=500))
    assert first == second
    assert content_digest(first) == content_digest(second)


def test_generator_differs_by_stream() -> None:
    # Distinct per-stream seeds → the four streams are not copies of each other.
    streams = generate_clean(n_per_stream=200)
    digests = {key: content_digest(recs) for key, recs in streams.items()}
    assert len(set(digests.values())) == 4


def test_generated_stream_shape_and_invariants() -> None:
    recs = generate_stream("coinbase", "BTC-USD", 1_000, stream_index=0)
    assert len(recs) == 1_000
    lo, hi = CONFIG.symbol_bounds["BTC-USD"]
    prev_ts = None
    ids = set()
    for r in recs:
        assert set(r) == {
            "exchange",
            "symbol",
            "trade_id",
            "price",
            "size",
            "side",
            "ts_event",
            "ts_ingest",
        }
        assert lo <= r["price"] <= hi  # stays inside the R2 band → never a false out-of-range
        assert r["price"] > 0 and r["size"] > 0
        assert r["side"] in ("buy", "sell")
        if prev_ts is not None:
            assert r["ts_event"] > prev_ts  # strictly increasing → never a false out-of-order
        prev_ts = r["ts_event"]
        ids.add(r["trade_id"])
    assert len(ids) == 1_000  # unique trade_ids → never a false duplicate


def test_clean_fixture_produces_zero_quarantines() -> None:
    # Clean-control preservation: an untouched clean stream must route entirely to valid.
    frames = [
        contracts.encode(r, SCHEMA, fixture.FIXTURE_SCHEMA_ID)
        for r in flatten(generate_clean(n_per_stream=400))
    ]
    engine = RulesEngine(CONFIG, SCHEMA)
    verdicts = [engine.evaluate(f) for f in frames]
    assert all(v.disposition == DISPOSITION_VALID for v in verdicts)
    assert engine.telemetry.quarantined == {}


# --------------------------------------------------------------------------------------------
# Committed fixture: the pin matches the generator, and the artifact verifies.
# --------------------------------------------------------------------------------------------
def test_committed_pin_matches_freshly_generated_content() -> None:
    manifest = fixture.load_fixtures_manifest()
    fresh = content_digest(flatten(generate_clean()))
    assert manifest["content_sha256"] == fresh
    assert manifest["n_total"] == 100_000
    assert manifest["seed"] == 42


def test_committed_fixture_verifies_against_its_pins() -> None:
    ok, detail = fixture.verify_fixture()
    assert ok, detail


def test_verify_fixture_reports_mismatched_pins(tmp_path: Path) -> None:
    import yaml

    records = flatten(generate_clean(n_per_stream=100))
    parquet = tmp_path / "fx.parquet"
    fixture.write_parquet(records, parquet)
    bad_yaml = tmp_path / "fixtures.yaml"
    bad_yaml.write_text(
        yaml.safe_dump({"parquet_sha256": "deadbeef", "content_sha256": "deadbeef"})
    )
    ok, detail = fixture.verify_fixture(bad_yaml, parquet)
    assert not ok
    assert "parquet_sha256 mismatch" in detail and "content_sha256 mismatch" in detail


def test_parquet_round_trips_preserving_content(tmp_path: Path) -> None:
    records = flatten(generate_clean(n_per_stream=300))
    path = tmp_path / "fx.parquet"
    fixture.write_parquet(records, path)
    read_back = fixture.read_parquet(path)
    assert read_back == records
    assert content_digest(read_back) == content_digest(records)
    # sha256_file + build_fixtures_manifest cover the pinning path.
    built = fixture.build_fixtures_manifest(records, path)
    assert built["content_sha256"] == content_digest(records)
    assert built["parquet_sha256"] == fixture.sha256_file(path)


# --------------------------------------------------------------------------------------------
# Injector helpers — precise boundary behavior against the gate.
# --------------------------------------------------------------------------------------------
def _record(**overrides: Any) -> dict[str, Any]:
    base = generate_stream("coinbase", "BTC-USD", 1, stream_index=0)[0]
    base.update(overrides)
    return base


def _verdict(record: dict[str, Any]) -> Any:
    return RulesEngine(CONFIG, SCHEMA).evaluate(
        contracts.encode(record, SCHEMA, fixture.FIXTURE_SCHEMA_ID)
    )


@pytest.mark.parametrize("kind", [0, 1, 2])
def test_corrupt_frame_is_undecodable(kind: int) -> None:
    import random

    good = contracts.encode(_record(), SCHEMA, fixture.FIXTURE_SCHEMA_ID)
    # Force each corruption branch deterministically by seeding to the desired first randint.
    rng = random.Random()
    rng.randint = lambda a, b: kind if (a, b) == (0, 2) else 0  # type: ignore[method-assign]
    bad = _corrupt_frame(good, rng)
    assert _verdict_bytes(bad).rule_id == "R1"


def _verdict_bytes(raw: bytes) -> Any:
    return RulesEngine(CONFIG, SCHEMA).evaluate(raw)


@pytest.mark.parametrize("kind", [0, 1, 2, 3, 4])
def test_out_of_range_faults_quarantine_r2(kind: int) -> None:
    bad, label = _out_of_range(_record(), kind)
    v = _verdict(bad)
    assert v.disposition == DISPOSITION_QUARANTINE and v.rule_id == "R2"
    assert label


@pytest.mark.parametrize("kind", [0, 1])
def test_in_range_controls_stay_valid(kind: int) -> None:
    bounds = CONFIG.symbol_bounds["BTC-USD"]
    ok, label = _in_range_control(_record(), bounds, kind)
    assert ok["price"] in bounds  # exactly on the inclusive bound
    assert _verdict(ok).disposition == DISPOSITION_VALID
    assert label


def test_skew_boundary_49_vs_51_seconds() -> None:
    # A watermark-anchored skew: 4.9s is inside the 5s tolerance (valid), 5.1s is outside (R4).
    engine = RulesEngine(CONFIG, SCHEMA)
    base = _record(ts_event=fixture.BASE_TS_MS + 1_000_000)
    engine.evaluate(contracts.encode(base, SCHEMA, fixture.FIXTURE_SCHEMA_ID))
    wm = base["ts_event"]
    control = _skewed(
        _record(trade_id="c", ts_event=wm), wm, CONFIG.out_of_order_tolerance_ms - 100
    )
    fault = _skewed(_record(trade_id="f", ts_event=wm), wm, CONFIG.out_of_order_tolerance_ms + 100)
    assert (
        engine.evaluate(contracts.encode(control, SCHEMA, fixture.FIXTURE_SCHEMA_ID)).disposition
        == DISPOSITION_VALID
    )
    fv = engine.evaluate(contracts.encode(fault, SCHEMA, fixture.FIXTURE_SCHEMA_ID))
    assert fv.disposition == DISPOSITION_QUARANTINE and fv.rule_id == "R4"


def test_duplicate_helper_stamps_fresh_event_time() -> None:
    original = _record(ts_event=1000, ts_ingest=1001)
    dup = _duplicate(original, watermark=9_000)
    assert dup["trade_id"] == original["trade_id"]
    assert dup["price"] == original["price"]
    assert dup["ts_event"] == 9_000  # fresh → isolates R3 (not R4)


def test_sample_disjoint_partitions_without_overlap() -> None:
    import random

    groups = _sample_disjoint(list(range(100)), [10, 20, 5], random.Random(1))
    flat = [i for g in groups for i in g]
    assert [len(g) for g in groups] == [10, 20, 5]
    assert len(set(flat)) == len(flat)  # disjoint
    assert set(flat) <= set(range(100))


# --------------------------------------------------------------------------------------------
# Injection manifest: determinism + correctness.
# --------------------------------------------------------------------------------------------
def test_injection_is_byte_deterministic() -> None:
    clean = small_clean()
    a = inject_faults(clean, SMALL_CONFIG, SCHEMA)
    b = inject_faults(clean, SMALL_CONFIG, SCHEMA)
    assert a.frames == b.frames
    assert a.manifest.to_json() == b.manifest.to_json()
    assert a.manifest.digest() == b.manifest.digest()


def test_manifest_counts_and_rate_are_consistent() -> None:
    result = inject_faults(small_clean(), SMALL_CONFIG, SCHEMA)
    manifest = result.manifest
    # n_total accounts for the extra emitted duplicate frames.
    assert manifest.n_total == len(result.frames)
    assert manifest.n_clean_controls == manifest.n_total - len(manifest.entries)
    # Every quarantine-expecting entry names its rule; every valid-expecting entry names none.
    for e in manifest.entries:
        if e.expected_disposition == DISPOSITION_QUARANTINE:
            assert e.expected_rule in {"R1", "R2", "R3", "R4"} and e.is_fault
        else:
            assert e.expected_rule is None
    # All four fault classes are present, and the fault rate is in the intended ~2% ballpark.
    assert {e.fault_class for e in manifest.entries} == {
        MALFORMED,
        OUT_OF_RANGE,
        DUPLICATE,
        OUT_OF_ORDER,
    }
    assert 0.01 <= manifest.fault_rate() <= 0.03


def test_manifest_includes_boundary_and_designed_miss_entries() -> None:
    manifest = inject_faults(small_clean(), SMALL_CONFIG, SCHEMA).manifest
    boundaries = {e.boundary for e in manifest.entries}
    # The exact dedup-window edge pair, the out-of-order edges, and the range controls are present.
    assert {"window_edge_inside", "window_edge_beyond"} <= boundaries
    assert {"5.1s", "4.9s", "5.0s_exact"} <= boundaries
    assert {"at_lower_bound", "at_upper_bound"} <= boundaries
    # Designed misses exist: dups the gate genuinely cannot catch → recall < 100% by design.
    misses = [e for e in manifest.entries if e.is_fault and not e.detectable]
    assert misses and all(e.fault_class == DUPLICATE for e in misses)
    assert all(e.expected_disposition == DISPOSITION_VALID for e in misses)


def test_manifest_to_dict_is_serializable_and_rollups_match() -> None:
    manifest = inject_faults(small_clean(), SMALL_CONFIG, SCHEMA).manifest
    payload = manifest.to_dict()
    assert payload["seed"] == 42
    assert payload["lru_size"] == 100
    counts = payload["counts_by_class"]
    # Duplicate rollup: detectable + designed_miss == faults; controls tracked separately.
    dup = counts[DUPLICATE]
    assert dup["detectable"] + dup["designed_miss"] == dup["faults"]
    assert counts[OUT_OF_RANGE]["controls"] >= 2  # at least both inclusive-bound controls


# --------------------------------------------------------------------------------------------
# The crux: every injected fault is detectable by the rule it targets.
# --------------------------------------------------------------------------------------------
def _crosscheck(
    clean: dict[tuple[str, str], list[dict[str, Any]]], config: Any
) -> InjectionManifest:
    result = inject_faults(clean, config, SCHEMA)
    engine = RulesEngine(config, SCHEMA)
    verdicts = [engine.evaluate(f) for f in result.frames]
    faulted_indices = set(result.manifest.by_index())

    for e in result.manifest.entries:
        v = verdicts[e.index]
        assert v.disposition == e.expected_disposition, (e.fault_class, e.boundary, e.index)
        if e.expected_disposition == DISPOSITION_QUARANTINE:
            assert v.rule_id == e.expected_rule, (e.fault_class, e.boundary, v.rule_id)

    # No pure-clean frame is ever quarantined (zero false-quarantine on the controls).
    false_q = [
        i
        for i, v in enumerate(verdicts)
        if i not in faulted_indices and v.disposition != DISPOSITION_VALID
    ]
    assert false_q == []
    return result.manifest


def test_every_fault_class_is_detectable_small() -> None:
    manifest = _crosscheck(small_clean(), SMALL_CONFIG)
    # Sanity: detectable faults for each quarantine class actually exist to have been checked.
    detectable = {e.fault_class for e in manifest.entries if e.is_fault and e.detectable}
    assert detectable == {MALFORMED, OUT_OF_RANGE, DUPLICATE, OUT_OF_ORDER}


def test_full_fixture_injection_grades_exactly_against_the_gate() -> None:
    # The real 100k fixture, the real 10,000-key LRU, the real gate — the Day C grading substrate.
    manifest = _crosscheck(generate_clean(), CONFIG)
    assert manifest.n_total >= 100_000
    designed_misses = [e for e in manifest.entries if e.is_fault and not e.detectable]
    assert len(designed_misses) >= 1  # the beyond-window dups drag R3 recall below 100%


# --------------------------------------------------------------------------------------------
# FaultEntry / manifest small-surface coverage.
# --------------------------------------------------------------------------------------------
def test_empty_manifest_rollups() -> None:
    manifest = InjectionManifest(seed=42, n_total=0, lru_size=100, entries=[])
    assert manifest.fault_rate() == 0.0
    assert manifest.n_clean_controls == 0
    assert manifest.counts_by_class() == {}
    assert manifest.by_index() == {}


def test_fault_entry_is_frozen() -> None:
    entry = FaultEntry(
        0, "coinbase:BTC-USD", MALFORMED, True, True, DISPOSITION_QUARANTINE, "R1", "x"
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        entry.index = 1  # type: ignore[misc]
