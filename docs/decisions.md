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
misses included, because they are real uncatchable duplicates). Measured on the committed fixture
(100,477 frames, seed 42, LRU 10,000; `tickflow slo`): gates ON → **0 of 15,061 bars violated**;
gates OFF → **1,076 of 15,061**, dominated by the `no_quarantinable` invariant (1,076) plus
corrupted extremes (`price_positive`, 144). On the 4 × 300 small config with LRU 100 — the scale
the "22 of 189" figure above comes from — the same experiment gives 0 vs **12 of 189**
(`no_quarantinable` 12, `price_positive` 3). The two scales are not comparable, so every figure
is labeled with the fixture that produced it. Per-invariant counts sum past the violated-bar count
because one bar can trip several invariants at once (1,076 + 144 = 1,220 violations spread over
1,076 distinct bars); the bar count is the headline, the invariant counts are the diagnosis.
That ON/OFF table is the README's opening evidence and stands exactly as §4 intends.

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

## ADR-004 — Throughput removed, latency never shipped (§6, §8, §14)

**Status: ACCEPTED (2026-07-21).** Records a deviation from three frozen sections that the
CHANGELOG, README, and STATE describe at length but no ADR had captured. §"Deviations require
an ADR in `docs/decisions.md`" makes this entry mandatory, not optional.

### Context

Frozen §6 requires "**Throughput** (msgs/s sustained through the gate) and **e2e latency**
p50/p95/p99 (ts_ingest → gate emit), `bench` profile only", §6 also requires "every proportion
and every latency percentile carries a **bootstrap 95% CI**", §8 puts "throughput/latency (with
environment disclosure line)" on the dashboard, and §14 makes "every P/R, completeness, and
latency number carries a 95% CI + full provenance" an acceptance criterion.

Neither ships. Latency was never implemented — no measurement of `ts_ingest → gate emit` exists
in `src/` or `tests/` at any commit. Throughput was implemented, published in v0.9 and v0.9.1,
and then removed on `main`.

### Decision

Publish no performance figure at all.

Throughput measured ~230,000 msg/s on Apple silicon, 124,819 on one CI runner, and 76,071 on
another: more than 3x across machines, and 1.6x between two runs on the same CI runner class.
A number whose spread across runners exceeds any difference it could reveal about the gate
describes the runner, not the gate. Caveating it would have kept a figure readers would quote
without the caveat.

Latency is worse, not better. §6 scopes it to the `bench` profile — a Redpanda broker in
Docker — and ADR-003 cut the integration lane that would have exercised it. In-process,
`ts_ingest → gate emit` measures function-call overhead on the machine that happens to be
running, with the same runner-dependence throughput showed and no broker in the path to make
it meaningful. A p99 from that setup would be precision without accuracy.

### Consequences

- §6, §8, and §14's performance clauses are not met and will not be met at this scope. §14's
  "every latency number carries a 95% CI" is now vacuous rather than satisfied: there are no
  latency numbers.
- The telemetry artifact is fully deterministic apart from its provenance stamps, which CI
  asserts. That determinism is a direct consequence of having no wall-clock value in it.
- `measure_throughput`, the `throughput` key, the dashboard row and caption, the README row,
  and the asserting tests are gone from `main`. **Tags `v0.9` and `v0.9.1` still ship them**;
  the removal is unreleased.
- If a performance claim is ever wanted, it needs the `bench` profile actually running in a
  pinned environment, not an in-process proxy — which is roadmap work behind the same lane
  ADR-003 cut.

## ADR-005 — Published metrics come from an in-process replay, not a broker (§7, §10)

**Status: ACCEPTED (2026-07-21).** Records that the §7 system-of-record pipeline was replaced
wholesale. Like ADR-004, this is a deviation the docs describe and no ADR captured.

### Context

Frozen §7 specifies: "**System of record = GitHub Actions.** The metrics workflow: verify
fixture checksum → start Redpanda (`bench`) in Docker on `ubuntu-latest` → replay faulted
fixture → grade against manifest → bootstrap CIs → export telemetry JSON → deploy dashboard to
Pages." §"`bench`" is named "the only profile CI publishes metrics from".

What runs is `ci.yml`'s `metrics` job and `pages.yml`, both of which state plainly that
"everything here runs in-process against the committed, checksum-pinned fixture. No broker is
started." The `redpanda-bench` service in `docker-compose.yaml` is referenced by no workflow,
no script, and no source file.

### Decision

Keep the in-process replay as the system of record; do not stand up a broker in CI.

The published numbers are detection precision/recall, false-quarantine rates, completeness,
and the gates-ON/OFF SLO comparison. Every one is a property of the rules engine evaluating
frames against a pinned fixture. None depends on delivery semantics, partitioning, consumer
groups, or commit behaviour — the things a broker would add. Replaying the same committed
parquet through the same `RulesEngine` yields identical verdicts with or without Redpanda in
front of it, which is why the drift check can assert byte-identity on the artifact at all.

A broker in the metrics path would add a Docker service, a readiness wait, and a class of
flake (topic creation, rebalance timing) to a job whose output is required to be
deterministic — in exchange for no change in any published figure.

### Consequences

- §7's pipeline description is superseded by this entry. `bench` is not the profile CI
  publishes from; `in-process (no broker)` is, and every artifact records that in its
  `profile` stamp.
- §10's kill/restart and duplicate-delivery properties remain unproven, as ADR-003 already
  recorded when it cut the integration lane. `metrics.Completeness` therefore reports
  `duplicate=0` as an assigned constant, not a measurement — the one place where an
  in-process system of record is weaker than the frozen design, and it should be read that
  way until the lane exists.
- `docker-compose.yaml`'s `bench` profile stays for local use and for the roadmap work; it is
  currently exercised by nothing.
- §9's workflow layout names `metrics.yml`; the function lives in a `metrics` job inside
  `ci.yml` plus `pages.yml`. Same work, different files.

## ADR-006 — §8 shipped as a blocklist; the declared-field allowlist now exists

**Status: ACCEPTED (2026-07-21).** Records that the frozen §8 enforcement was implemented
inverted from Day D until this entry, what that let through, and what the fix does and does
not prove.

### What §8 required, and what shipped

Frozen §8: "the exporter writes only fields declared in `telemetry_schema.json` ... A
release-blocking test fails the build if an exported artifact contains **any undeclared
field**." That is an allowlist. It is fail-closed: a field nobody anticipated is a leak.

What shipped was `MARKET_FIELD_NAMES`, a fifteen-name blocklist in `export.py`, and
`telemetry_schema.json` never existed — the filename appears nowhere in the repo except §8
itself. A blocklist is fail-open: it rejects the names it knows and passes everything else.

The two are not variations on a theme. They are opposite defaults, and the difference is the
whole point of the clause.

### What it let through

Confirmed by probe against the committed artifact, before the fix:

- `mid_px` at the top level: **passed**
- `mid_px` nested under `grade`: **passed**
- `last_trade` under `grade.per_class.duplicate`: **passed**
- `price`, `volume` at either level: blocked (they are in the fifteen)

So the boundary held for the exact names someone thought of in advance and was open for every
name they did not. Traversal was never the weakness — `find_market_fields` walks dicts and
lists to any depth correctly. The set it walks with was.

Nothing exploited this. No published artifact ever contained an undeclared field, which is
why the gap survived: the gate produced the right answer for the wrong reason on every run,
and a gate that has never failed looks identical to a gate that cannot fail.

`scripts/release_gate.sh` and `pages.yml`'s grep are independent scans over the built artifact
and were subject to the same blind spot; they scan for the same fifteen names.

### The change

`contracts/telemetry_schema.json` declares every field path of a telemetry artifact — 138
paths, enumerated from the committed one and approved field by field, with the repeated
`Estimate` shape defined once and referenced at all six sites. `assert_telemetry_only` runs
two independent passes:

- **Pass 1 (primary, fail-closed):** every field path must be declared; types are enforced
  (`int`/`float`/`str`/`bool`/`object`, with `bool` checked before `int` since Python's `bool`
  is an `int` subclass); declared-but-missing fields and undeclared nulls are failures; an
  unreadable or absent schema raises rather than degrading to allow-everything.
- **Pass 2 (secondary, fail-open):** `MARKET_FIELD_NAMES`, unchanged.

Both run on every export. Pass 2 is deliberately **not** folded into pass 1. They have
different blind spots: pass 1 catches names nobody anticipated but cannot see a market value
smuggled into a declared free-text string; pass 2 catches a known market name even if a future
schema edit wrongly declares it. Collapsing them into one check would leave one of those
uncovered.

The schema location is `contracts/`, beside `rules.yaml` and `trades.v1.avsc`. §8 gives a bare
filename and §9's layout does not list the file at all; this is the coherent placement, not a
second deviation.

### The enums are the mechanism, not friction

`grade.per_class` declares exactly four keys (`malformed`, `out-of-range`, `out-of-order`,
`duplicate`) and `counts_by_invariant` exactly seven. These are enums by choice.

Adding a fifth fault class will therefore fail the gate until someone edits this schema. That
is the intended behaviour and the entire value of the change. **The failure mode this ADR
exists to prevent is a future maintainer hitting that rejection while adding a legitimate
fault class, reading it as an obstacle, and "fixing" it by widening the enum to a wildcard.**
A wildcard under `per_class` re-admits `per_class.mid_px`. That is precisely the hole
described above, reopened, and it would again produce correct results on every run until the
day it did not.

Adding a field to a published artifact means editing this schema, deliberately, as part of the
change that adds it. That edit is the review point. Removing the need for the edit removes the
review.

### What pass 1 does not prove

Stated plainly so a reader does not infer more coverage than exists.

**It checks field names, not values.** `grade.fixture`, the digests, and the provenance strings
are declared free text. A market value placed inside one of those strings passes pass 1 (the
path is declared) and passes pass 2 (the blocklist matches keys, not values). Pass 1 closes the
undeclared-field fail-open case, which is what §8 requires and what the probes above exercised.
It does not make the export boundary total. Value-scanning free text was considered and
rejected: it false-positives on 64-character digests, and a check that cries wolf gets
weakened, which is the failure mode this whole entry is about.

**Declaring the provenance fields validates their presence and type, never their truth.** The
committed artifact stamps `commit: 773b226a1aaa...`, a commit whose tree still defines
`measure_throughput` and therefore cannot have produced it. That artifact passes this schema
cleanly, and should — a schema is not a provenance check. Different defect, different fix; it
is not addressed here and is not made less true by anything in this entry.
