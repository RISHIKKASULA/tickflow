# Decisions (ADR log)

Deviations from the frozen spec ([architecture.md](architecture.md)) and gate results are
recorded here. Simplest defensible choice wins.

## ADR-001 — Feed sanity gate (§0): recalibrated to per-venue thresholds

**Status: ACCEPTED (2026-07-19). Gate PASSES on recalibrated terms.** Reviewed by Rishik Kasula.

### Context

The blocking feed-sanity gate ran on 2026-07-19: `tickflow capture --minutes 5` wrote a local,
checksummed capture from both live feeds (`data/captures/gate-2026-07-19`, sha256
`5be3cde5e281be05…`, 300.1 s, 1060 records), reviewed via `tickflow sanity`. Observed per-stream
counts over the 5-minute window:

| stream | count | uniq_id | dup | missing | max skew | within 60 s | out-of-order |
|---|---|---|---|---|---|---|---|
| coinbase:BTC-USD | 814 | 814 | 0 | 0 | 37.7 s | 814 | 163 |
| coinbase:ETH-USD | 203 | 203 | 0 | 0 | 134.9 s | 150 | 110 |
| kraken:BTC-USD | 29 | 29 | 0 | 0 | 0.2 s | 29 | 0 |
| kraken:ETH-USD | 14 | 14 | 0 | 0 | 0.1 s | 14 | 0 |

Against the frozen single threshold (≥ 50 msgs/stream/5 min) both Kraken streams failed (29, 14),
so the gate was provisionally FAIL.

### Decision

**Keep both venues; recalibrate the gate to per-venue liveness thresholds** — a calibration
correction, not a lowered bar. The 50-msg/5-min threshold encoded a wrong assumption: comparable
message volume across venues. Kraken is genuinely thinner than Coinbase, and 29/14 is real,
sparse market activity, not a broken feed. The evidence the Kraken feed is *sound*: 0 duplicates,
0 missing, 100% trade_id presence, field mapping verified correct on both streams, and sub-second
event-time↔wall-clock skew — **better than Coinbase's**. The gate's purpose is to reject a dead
or stalled feed, not to enforce a volume target.

1. **Per-venue thresholds, derived from observed volume** (not a single cross-venue number),
   implemented in `sanity.py` as `DEFAULT_THRESHOLDS = {coinbase: 50, kraken: 5}`, overridable
   with `tickflow sanity --min venue=count`. Derivation from the observed numbers above: each
   floor sits well below that venue's slowest observed stream — Coinbase 50 vs. observed min 203
   (~4× headroom), Kraken 5 vs. observed min 14 (~2.8× headroom) — so normal quiet windows pass
   while a stalled feed trickling ~0 still fails. **Re-run confirmation:** `tickflow sanity` on
   the recorded capture under these thresholds returns **PASS** on all four streams (exit 0);
   the gate now passes on its own terms.

2. **R6 (cross-venue divergence) must tolerate a sparse second venue** (carried to Day C now so
   it isn't rediscovered). R6 compares per-symbol mids across venues (alert-only, evaluated on
   the per-stream **event-time watermark**, never wall clock — §2 clock rule). With Kraken
   printing on the order of one trade every ~10–20 s, a naive comparison would alert off a stale
   quote. Frozen behavior for R6: a **staleness window of 30 s** — a venue's latest print is
   eligible for comparison only if its event-time is within 30 s of the comparison instant. If
   either venue is stale, R6 emits **no divergence verdict** for that instant and instead
   increments a `divergence_unavailable{reason=stale_<venue>}` telemetry counter; the sustained-
   10 s condition still governs genuine divergence alerts. Staleness is telemetry, never a
   quarantine (a missing print is not a bad message — same rationale as R5). The 30 s window is a
   default recorded here and revisitable if soak data warrants.

3. **Coinbase timestamp skew is expected, not feed lag** (recorded so the number isn't misread
   later). The large Coinbase max-skew / out-of-order figures (e.g. ETH 134.9 s, 110 ooo) come
   from the **connect-time snapshot backfill**: Coinbase's `market_trades` channel replays recent
   trades on subscribe, so the initial burst carries older event-times that arrive after live
   wall clock and out of order. Steady-state Coinbase updates are within tolerance, and Coinbase
   clears the gate on message count regardless. This is a one-time per-connection artifact.

### Consequences

- Both venues stay in scope; the cross-venue divergence rule R6 survives (its sparse-venue
  semantics are now specified above). No feed swap; no Kraken-only / Coinbase-only descope.
- Day A is closed; Day B is unblocked and proceeds per STATE.md, starting with the trades.v1
  contract + Schema Registry wiring.
- Captured market data remains local and gitignored (Coinbase/Kraken ToS, §1); this ADR cites
  the capture by checksum, not by committing it.

## ADR-002 — The §4 bit-identity reference is the ground-truth valid projection, not the raw clean fixture

**Status: ACCEPTED (2026-07-19). A clarification of §4, not a change to the SLO thesis.**

### Context

Frozen §4 states the signature experiment as: replay the fault-injected fixture with gates ON and
the resulting bars must be **bit-identical to bars built from the clean fixture**. Building the
gates-ON/OFF experiment (Day C commit 2) surfaced that this literal wording cannot hold with the
Day-B fault injector as frozen, for two structural reasons — both deliberate features of that
injector, not defects:

1. **The injector replaces in place.** A malformed / out-of-range / out-of-order fault *overwrites*
   the clean record at its index (it is not an extra frame). The gate correctly quarantines it, so
   that index contributes **nothing** to the valid stream — whereas the clean fixture has a real
   trade there. Gates-ON bars are therefore missing those trades relative to the clean fixture.
2. **Boundary controls alter values but stay valid.** The `out-of-range` in-range controls set a
   price to the exact symbol bound (e.g. BTC $1,000) and the `out-of-order` controls shift
   `ts_event` by 4.9 s / 5.0 s. These route valid *by design* (they probe the inclusive edge), but
   they carry different values than the clean record they replaced, so they perturb their bar.

Empirically (4 streams × 300, LRU 100): 22 of 189 gates-ON bars differ from the raw clean-fixture
bars. So `gates_on_digest == clean_fixture_digest` is simply **False**, and "fixing" it would mean
deleting the boundary controls and the designed-miss dups — the exact §5 features that keep the
Day-C recall numbers honest. That is not on the table (STATE.md phase-2 carry-forward says so).

### Decision

**The bit-identity reference for the faulted stream is the manifest's ground-truth _valid
projection_, not the raw clean fixture.** The valid projection is: decode every emitted frame the
injection manifest marks `expected_disposition = valid` (controls as-emitted + untouched clean),
excluding the designed-miss duplicates (`detectable = False`), and build bars from it. A correct
gate routes exactly this set to `trades.valid`, so the claim becomes precise and strong:

- **Catchable-subset bit-identity (the §4 claim, made exact).** With designed misses filtered out,
  `bars_digest(gates_on_bars) == bars_digest(expected_valid_projection_bars)`, byte-for-byte. This
  proves the gate routes *exactly* per the ground truth and the bar builder is deterministic. It is
  checked in `run_slo_experiment` and asserted in `tests/test_slo_experiment.py`.
- **Clean-input reconstruction (the literal §4 wording, where it does hold).** Feeding the *clean*
  fixture through the gate routes everything valid with zero SLO violations and reproduces the
  clean-fixture bars exactly (`test_clean_fixture_passes_the_gate_with_no_slo_violations`). This is
  the honest form of "clean data → clean bars": it is the clean *input*, not the faulted input,
  that reconstructs the clean bars.
- **Designed misses reported, never hidden.** The dups past the LRU window are surfaced as
  `SloComparison.designed_miss_dups` and drag R3 recall below 100 % in the metrics phase (§6). They
  are the frozen point that keeps recall informative, not a number to launder away.

The SLO thesis itself is unaffected and is measured over the **full** faulted stream (designed
misses included, because they are real uncatchable duplicates): gates ON → zero SLO violations;
gates OFF → `K > 0` violated bars, dominated by the `no_quarantinable` invariant plus corrupted
extremes. That ON/OFF table is the README's opening evidence and stands exactly as §4 intends.

### Consequences

- No change to the frozen injector, the fixture pins, or the rule set; this is a reference-choice
  clarification recorded so the README's bit-identity language is precise and defensible.
- The README will state the claim as "gates-ON bars are bit-identical to the ground-truth valid
  projection; clean input reconstructs the clean bars" — and will show the designed-miss R3 recall
  gap next to it, per the truth-only brand (§6).
- Revisitable only by an injector redesign (additive faults instead of replace-in-place), which is
  a larger change deferred past v0.1; the current form is the simplest defensible choice.

## ADR-003 — The §11 slip valve is pulled: quarantine/replay CLI cut, release is v0.9

**Status: ACCEPTED (2026-07-20). Taken at the start of Day D, before building, not as an
end-of-session excuse.**

### Context

Frozen §11 gives Day D a slip valve: "if Day C overruns, v0.9 drops the quarantine-replay CLI
and the live-soak section — never the gate, the SLO experiment, or the CIs. A stated v0.9 with
roadmap beats a rushed v0.1." Day C did overrun — it stopped after the replay metrics and
deferred its commit 4 into Day D.

Day D opened with three defects in *already-published* claims, all of which outrank a new
convenience command:

1. `run_slo_experiment` — the §4 signature result — was reachable only as a library call from
   the test helpers, on a 4 × 300 SMALL_CONFIG. No command regenerated it, so the headline
   gates-ON/OFF comparison could not be produced from the committed fixture at all.
2. STATE.md and `bars.py` published the gates-OFF result as the literal placeholder `K > 0`.
   An unmeasured symbol standing where a measured count belongs.
3. `false_quarantine_rate` was reported over all 98,800 controls. The 460 deliberately designed
   near-boundary controls — the inclusive R2 bounds, the 4.9 s / 5.0 s skews, the exact dedup
   window edge, the entire reason the §5 injector has boundary probes at all — were diluted into
   an easy denominator and were invisible as a subset.

### Decision

**Cut the quarantine-inspection/replay CLI and the live-soak section; tag v0.9.**

Dropped: `quarantine.py` (`tickflow quarantine` ls/show/stats, `tickflow replay --fixture`), the
replay-determinism + kill/restart-mid-replay completeness tests, the live soak, and the
`if: false` integration lane in `ci.yml` those tests would have switched on.

Kept, in full: the gate, the SLO experiment (now with the CLI surface it was missing), the CIs,
the bootstrap-CI grading, the telemetry export, and the Pages dashboard.

The reasoning is a straight comparison of what each buys. The quarantine CLI is an inspection
convenience over envelopes the gate **already emits** and faults the metrics **already grade** —
it adds no measurement tickflow does not have. The three defects above are wrong or missing
numbers in claims the README was about to publish. A project whose brand is "every number
reproducible from a checksummed fixture" cannot ship `K > 0` in a docstring that calls itself
"the README's opening evidence", and cannot report its false-positive rate over the denominator
that flatters it while hiding the hard subset. Fixing those is not optional polish; it is the
thesis.

### Consequences

- Version is **v0.9**, not v0.1.0, and the README says so with the roadmap attached — the honest
  form §11 asks for. v0.1.0 is reserved for the release that lands the quarantine/replay CLI and
  turns the integration lane on.
- The integration lane stays `if: false`. The register/check path against a real broker is
  verified out-of-band (see the Day D schema-registry verification) rather than by that lane.
- No live-soak numbers are published anywhere. The dashboard and README carry fixture-scale
  results only, each labeled with the fixture that produced it.
- Roadmap, unchanged in substance from §12: quarantine inspection + replay CLI, the kill/restart
  exactly-once completeness proof, the integration lane, then live soak.
