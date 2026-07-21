"""Telemetry export + dashboard rendering — the release-blocking ToS boundary (frozen §7/§8).

The load-bearing test in this file is not that the page renders. It is that a market-data field
**cannot** reach a published artifact: `assert_telemetry_only` is what stands between a refactor
that starts serializing bars and a public page that violates the exchange ToS. So it is tested
from both sides — that it catches every forbidden name at any depth, and that it does not catch
the SLO invariant labels and CI bounds whose names merely *contain* market words.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tickflow import export, fixture


# --------------------------------------------------------------------------------------------
# The ToS boundary: no market data in a published artifact.
# --------------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "payload",
    [
        {"price": 42.0},
        {"nested": {"open": 1.0}},
        {"bars": [{"exchange": "coinbase", "high": 2.0}]},
        {"a": {"b": [{"c": {"volume": 3.0}}]}},
        {"vwap": 1},
        {"bar_open": 1},
        {"size": 0.5},
    ],
)
def test_market_fields_are_rejected_at_any_depth(payload: dict[str, Any]) -> None:
    with pytest.raises(export.MarketDataLeak):
        export.assert_telemetry_only(payload)


def test_slo_labels_and_ci_bounds_are_not_market_fields() -> None:
    """The exact-name rule must not fire on names that merely contain a market word.

    `price_positive` / `high_ge_low` are SLO invariant labels and `ci_low` / `ci_high` are
    confidence-interval bounds. A substring rule would reject all of these, and the natural
    response would be to weaken the check -- which is how the real constraint gets lost.
    """
    export.assert_telemetry_only(
        {
            "counts_by_invariant": {
                "price_positive": 3,
                "high_ge_low": 0,
                "open_in_range": 0,
                "close_in_range": 0,
                "volume_positive": 0,
            },
            "recall": {"point": 1.0, "ci_low": 0.9, "ci_high": 1.0, "n": 10},
            "n_violated_bars": 12,
            "bar_start_ms": 1,
        }
    )


def test_leak_message_names_every_offending_path() -> None:
    with pytest.raises(export.MarketDataLeak) as excinfo:
        export.assert_telemetry_only({"a": {"price": 1}, "b": [{"close": 2}]})
    message = str(excinfo.value)
    assert "$.a.price" in message
    assert "$.b[0].close" in message


def test_find_market_fields_returns_empty_for_clean_payloads() -> None:
    assert export.find_market_fields({"n": 1, "rate": 0.5, "tags": ["ok"]}) == []


# --------------------------------------------------------------------------------------------
# Provenance.
# --------------------------------------------------------------------------------------------
def test_provenance_carries_the_fixture_pins_and_runner() -> None:
    prov = export.provenance("bench", generated_at="2026-07-20T00:00:00Z")
    assert prov["generated_at"] == "2026-07-20T00:00:00Z"
    assert prov["profile"] == "bench"
    assert len(prov["fixture_content_sha256"]) == 64
    assert len(prov["fixture_parquet_sha256"]) == 64
    assert prov["runner"]
    assert prov["tickflow_version"]


def test_provenance_refuses_to_emit_a_blank_pin(monkeypatch: Any) -> None:
    """A missing pin must raise, never default to "".

    An empty provenance field reads as cosmetic but silently unlinks every published number from
    the fixture that makes it reproducible -- the one property this project claims.
    """
    monkeypatch.setattr(fixture, "load_fixtures_manifest", lambda *a, **k: {"parquet_sha256": "x"})
    with pytest.raises(ValueError, match="missing required pin"):
        export.provenance("dev")


# --------------------------------------------------------------------------------------------
# Rendering.
# --------------------------------------------------------------------------------------------
def _payload() -> dict[str, Any]:
    est = {"point": 1.0, "ci_low": 1.0, "ci_high": 1.0, "n": 5}
    return {
        "provenance": {
            "generated_at": "2026-07-20T12:00:00Z",
            "tickflow_version": "0.9",
            "commit": "abc123def456789",
            "runner": "Darwin arm64",
            "profile": "in-process (no broker)",
            "fixture_parquet_sha256": "a" * 64,
            "fixture_content_sha256": "b" * 64,
        },
        "grade": {
            "fixture": "committed fixture: 10 frames",
            "per_class": {
                "malformed": {
                    "rule": "R1",
                    "n_faults": 5,
                    "n_designed_miss": 0,
                    "recall": est,
                    "precision": est,
                }
            },
            "false_quarantine_rate": {
                "all_controls": {"point": 0.0, "ci_low": 0.0, "ci_high": 0.0, "n": 98800},
                "near_boundary_controls": {"point": None, "ci_low": None, "ci_high": None, "n": 0},
            },
            "completeness": {
                "n_total": 10,
                "n_valid": 5,
                "n_quarantine": 5,
                "loss": 0,
                "duplicate": 0,
                "ok": True,
            },
        },
        "slo": {
            "fixture": "committed fixture: 10 frames",
            "gates_on": {"n_bars": 3, "n_violated_bars": 0, "n_violations": 0},
            "gates_off": {
                "n_bars": 3,
                "n_violated_bars": 2,
                "n_violations": 3,
                "counts_by_invariant": {
                    "no_quarantinable": 2,
                    "price_positive": 1,
                    "high_ge_low": 0,
                },
            },
            "bit_identical": True,
            "designed_miss_dups": 4,
        },
    }


def test_page_is_self_contained_static_html() -> None:
    page = export.render_html(_payload())
    assert page.startswith("<!doctype html>")
    assert "<script" not in page  # no JS at all -- values are baked in at render time
    assert "http://" not in page and "https://" not in page  # no external assets
    assert "{" not in page.split("</style>")[1]  # no unrendered f-string braces in the body


def test_page_states_a_refresh_TIME_and_never_a_cadence() -> None:
    """The Actions cron is best-effort, so the page may not promise a cadence."""
    page = export.render_html(_payload())
    assert "Last refreshed" in page
    assert "2026-07-20T12:00:00Z" in page
    for cadence in ("updated daily", "Updated daily", "every day", "hourly", "nightly"):
        assert cadence not in page


def test_page_shows_both_false_quarantine_denominators() -> None:
    page = export.render_html(_payload())
    assert "All controls" in page
    assert "Near-boundary controls" in page
    assert "98,800" in page
    assert "n/a (n=0)" in page  # an undefined estimate degrades honestly, not to "0.0000"


def test_page_shows_the_gates_on_off_comparison_and_invariant_breakdown() -> None:
    page = export.render_html(_payload())
    assert "Gates <strong>ON</strong>" in page
    assert "Gates <strong>OFF</strong>" in page
    assert "VIOLATED" in page
    assert "no_quarantinable" in page
    assert "high_ge_low" not in page.split("Which invariants break")[1].split("</table>")[0]


def test_rendering_a_payload_with_market_data_is_refused() -> None:
    payload = _payload()
    payload["grade"]["price"] = 100.0
    with pytest.raises(export.MarketDataLeak):
        export.render_html(payload)


def test_write_site_emits_both_artifacts(tmp_path: Path) -> None:
    json_path, html_path = export.write_site(_payload(), tmp_path)
    assert json.loads(json_path.read_text())["provenance"]["tickflow_version"] == "0.9"
    assert html_path.read_text().startswith("<!doctype html>")


# --------------------------------------------------------------------------------------------
# End to end over the committed fixture.
# --------------------------------------------------------------------------------------------
def test_build_telemetry_is_telemetry_only_end_to_end() -> None:
    payload = export.build_telemetry(b=200)
    export.assert_telemetry_only(payload)  # already asserted inside; re-checked as the contract
    assert set(payload) == {"provenance", "grade", "slo"}
    assert payload["grade"]["n_total"] == 100_477
    assert payload["slo"]["thesis_holds"]
    # Both denominators survive the round trip into the published artifact.
    fq = payload["grade"]["false_quarantine_rate"]
    assert fq["all_controls"]["n"] == 98_800
    assert fq["near_boundary_controls"]["n"] == 460


def test_no_performance_figure_is_published() -> None:
    """No throughput/latency number in the artifact or on the page (deliberate removal).

    An earlier version published in-process rule-engine throughput. It varied more than 3x
    between a laptop and a CI runner and 1.6x between two runs on the same CI runner class, so it
    described the machine rather than the gate, and it was removed rather than caveated. Asserted
    here so it cannot drift back in unnoticed -- and so the next person to add one has to delete
    a test that says why, rather than merely filling an empty-looking field.
    """
    payload = export.build_telemetry(b=200)
    assert set(payload) == {"provenance", "grade", "slo"}

    perf_keys = {
        "throughput",
        "msgs_per_s",
        "elapsed_s",
        "latency",
        "latency_ms",
        "p50",
        "p95",
        "p99",
    }
    assert export.find_market_fields(payload) == []  # the ToS rule still holds independently
    found = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in perf_keys:
                    found.add(key)
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    assert found == set(), f"performance figure(s) back in the artifact: {sorted(found)}"

    page = export.render_html(_payload())
    assert "msg/s" not in page
    assert "throughput" not in page.lower().split("</style>")[1] or "no throughput" in page.lower()
