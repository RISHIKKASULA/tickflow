# STATE

Build order and current status for tickflow v0.1. The frozen design lives in
[docs/architecture.md](docs/architecture.md); decisions and deviations in
[docs/decisions.md](docs/decisions.md).

## Build order → commit arc (from the frozen spec, §11)

**Day A (Fri Jul 17):** `feat: scaffold package, tooling, and CI skeleton` · `feat: add
compose stack with dev and bench profiles` · `feat: add exchange ingesters with
normalization` · `feat: add capture and sanity commands` → **stop at the feed-sanity gate:
Rishik reviews feed sanity, ADR-001 records pass/fail.** ⛔ Also the git checkpoint: repo
name `tickflow`, public, full file list presented for approval before first push.

**Day B (Sat Jul 18):** `feat: add trades.v1 contract with schema registry wiring` · `feat:
add declarative rules engine with quarantine routing` (+ gate unit tests) · `feat: add
synthetic fixture generator and fault injector` (+ determinism tests).

**Day C (Sun Jul 19):** `feat: add barbuilder with SLO checker and DuckDB sink` · `feat: add
gates-off demo mode with SLO comparison` · `feat: add fault-injection grading with bootstrap
CIs` · `feat: add quarantine inspection and replay CLI`.

**Day D (Mon Jul 20):** `feat: add telemetry export with schema enforcement` · `feat: add
metrics workflow and Pages dashboard` · live soak · `test: complete coverage to gate` ·
`docs: write README with measured results` · release grep gate · `chore: release v0.1.0`.

## Status

- [x] Day A: scaffold package + tooling + CI skeleton — ruff/mypy/pytest green, coverage gate
  wired (core-only), CI skeleton (lint→type→test) + disabled integration lane.
- [x] Day A: compose stack (dev + bench) — single-node Redpanda + Schema Registry, brought up
  and health-verified locally on the `dev` profile.
- [x] Day A: exchange ingesters + normalization — Coinbase (primary) + Kraken normalized to
  trades.v1, idempotent producer to `trades.raw`, reconnect/backoff. **Raw ticks verified
  flowing end to end**: a 25s live run produced 322 records (316 Coinbase + 6 Kraken), counts
  matching exactly with zero loss.
- [x] Day A: capture + sanity commands — `tickflow capture --minutes 5` writes a local
  (gitignored) checksummed capture from both live feeds; `tickflow sanity` reports per-stream
  counts, raw→normalized field-mapping pairs, timestamp/skew sanity, and trade_id uniqueness,
  with a provisional gate verdict. Pure helpers unit-tested; feed I/O is coverage-excluded (§9).
- [x] §0 feed-sanity gate — **RUN 2026-07-19; PASSES on recalibrated per-venue thresholds.**
  Reviewed by Rishik; recorded as **ADR-001 (ACCEPTED)**. Day A closed; Day B unblocked. See
  the gate result below.
- [x] Day B commit 1: trades.v1 contract + Schema Registry wiring — `contracts/trades.v1.avsc`,
  a hand-rolled Confluent Avro wire codec (magic + schema id + fastavro body, timestamps as
  `timestamp-millis`, exact round-trip), a `SchemaRegistry` REST client + `tickflow contract`
  (register/check/show), the ingester switched from JSON to Avro, and a **BACKWARD-compat CI
  job**. Codec unit-tested (contracts.py 100%); registry/E2E is integration-lane. **Register/check
  verified LOCALLY against a real broker on 2026-07-20** (Day D) — see the verification below;
  the earlier "CI-verified only, `docker` unavailable on the M4" caveat is now retired.
- [x] Day B commit 2: declarative rules engine with quarantine routing — `contracts/rules.yaml`
  (the 6 frozen rules, declarative: every threshold/bound/disposition read from YAML) +
  `src/tickflow/gate.py` (`RulesEngine` + quarantine envelope + `tickflow gate` consumer). R1–R4
  quarantine to `trades.quarantine`; R5 gap and R6 divergence are **alert-only** and structurally
  cannot quarantine (the config loader rejects any YAML that tries). Everything evaluates on the
  **per-stream event-time watermark**, never wall clock; R6 honors the ADR-001 **30 s staleness
  window** (no verdict + `divergence_unavailable{reason=stale_<venue>}` telemetry on a stale
  venue). Table-driven verdict tests cover each rule's pass/fail, the boundary literals (5.0 s vs
  5.001 s skew, LRU eviction at exactly 10,000, inclusive range edges, 0.5% deviation, 10 s
  sustain), quarantine routing/envelopes, and replay-twice bit-identity. **gate.py 100% covered;
  ruff/mypy/pytest green (89 gate tests).** The Kafka consumer glue is integration-lane (no broker
  on the M4 this session).
- [x] Day B commit 3: synthetic fixture generator and fault injector — `src/tickflow/fixture.py`,
  `fixtures.yaml`, and the committed `fixtures/trades.v1.clean.parquet` (2.9 MB). Seeded (42),
  integer-driven, cross-platform-deterministic clean generator: 4 streams × 25,000 = 100,000
  `trades.v1` messages, pinned by a platform-independent **content digest** (+ the parquet byte
  sha) that `tickflow fixture verify` checks — CI's system of record. Seeded fault injector
  rewrites ~2% into the four quarantine-rule classes (`malformed`→R1, `out-of-range`→R2,
  `duplicate`→R3, `out-of-order`→R4) and emits an **injection manifest** (per-entry index, class,
  expected verdict, boundary label, detectable flag). **Boundary faults are deliberate**: inclusive
  range-bound controls, 4.9 s / 5.0 s / 5.1 s skews, and the exact dedup-window edge (lru−1 keys
  back → caught vs lru back → designed miss). Tests cross-check the manifest against the **real**
  `RulesEngine`: every fault caught by its rule, every control valid, every designed-miss valid,
  zero false-quarantine on clean; generator + injection determinism proven. **fixture.py 100%
  covered; ruff/mypy/pytest green (118 tests total).**
- **Day B COMPLETE.** Next work is Day C commit 1 (barbuilder + SLO + DuckDB).
- [x] Day C commit 1: barbuilder + SLO checker + DuckDB sink — `src/tickflow/bars.py`. `BarBuilder`
  folds `trades.valid` into 1-minute OHLCV bars per (exchange, symbol) keyed off the **event-time
  watermark**, fully order-independent (open/close by `(ts_event, trade_id)`, high/low by max/min,
  volume summed in **integer micro-units**) so replayed bars are bit-identical regardless of
  delivery order. `check_slo` enforces the frozen invariants (high ≥ low, open/close within
  [low, high], volume > 0, positive prices, monotone bar timestamps, and the load-bearing **no bar
  built from a message the gate would quarantine**). `DuckDbSink` is an append-only single-writer
  local store; bar market values never leave the environment (§1 ToS). Kafka consumer glue is
  integration-lane. **bars.py 100% covered.**
- [x] Day C commit 2: gates-off demo mode + SLO comparison — `run_slo_experiment` in `bars.py` +
  the first-class `--gates-off` flag on `tickflow gate`. Replaying the fault-injected fixture twice:
  **gates ON → 0 of 15,061 bars violated; gates OFF → 1,076 of 15,061** on the committed fixture
  (100,477 frames, seed 42, LRU 10,000; `tickflow slo`), dominated by `no_quarantinable` (1,076)
  plus corrupted extremes (`price_positive`, 144) — the thesis made visible. On the 4 × 300
  small config with LRU 100 the same experiment gives 0 vs **12 of 189** (`no_quarantinable` 12,
  `price_positive` 3); the two scales are not comparable and are always labeled. Per-invariant
  counts can exceed the violated-bar count — one bar can trip several invariants at once — so
  1,076 + 144 = 1,220 violations across 1,076 distinct bars is consistent, not contradictory.
  Bit-identity is checked on the **catchable**
  fault subset (designed-miss dups filtered out) against the manifest's ground-truth **valid
  projection**; designed misses are surfaced as `designed_miss_dups` (R3 recall gap), not hidden.
  **ADR-002** records why the reference is the valid projection, not the raw clean fixture (the
  injector replaces in place and its boundary controls alter values by design; the raw clean fixture
  is reconstructed only from clean *input*, which is also asserted).
- [x] Day C commit 3: fault-injection grading with bootstrap CIs — `src/tickflow/metrics.py` +
  `tickflow metrics` + a new **`metrics` CI job**. Grades the gate's verdict stream against the
  injection manifest: recall per fault class, precision per rule, false-quarantine over controls +
  all clean, completeness (every input accounted for exactly once). Every proportion carries a 95%
  bootstrap CI (B=10,000, seed 42, percentile; the exact `Binomial(n, k/n)/n` identity). Committed
  fixture grades to: R1/R2/R4 recall **1.0000**, R3 recall **0.8407 [0.8071, 0.8721]** (76 designed
  misses, reported honestly), all precision **1.0000**, false-quarantine **0.0000** over both
  denominators — 0/98,800 all controls **and** 0/460 designed near-boundary controls (the hard
  subset, split out in Day D; see the verified manifest accounting below) — completeness ok.
  In-process/no-broker, so it runs in CI's fast lane. **metrics.py 100%
  covered; whole toolchain green (ruff/mypy/pytest, 158 tests, core coverage 100%).**
- **Day C COMPLETE (commits 1–3).** Stopped after the replay metrics per the run's mandate; the
  quarantine-inspection + replay CLI (the original Day-C commit 4) is deferred to Day D via the §11
  slip valve — never the gate, the SLO experiment, or the CIs.

## Feed-sanity gate result — RUN 2026-07-19 (PASS on recalibrated terms; ADR-001)

Capture: `data/captures/gate-2026-07-19` (local, gitignored; ToS §1), sha256
`5be3cde5e281be05…`, 300.1 s, 1060 records. Command: `tickflow sanity --capture <dir>`. Verdict
under per-venue liveness thresholds (ADR-001: coinbase ≥ 50, kraken ≥ 5):

| stream | count | min | verdict | uniq_id | dup | missing | max skew | within 60s | ooo |
|---|---|---|---|---|---|---|---|---|---|
| coinbase:BTC-USD | 814 | 50 | **PASS** | 814 | 0 | 0 | 37.7 s | 814 | 163 |
| coinbase:ETH-USD | 203 | 50 | **PASS** | 203 | 0 | 0 | 134.9 s | 150 | 110 |
| kraken:BTC-USD | 29 | 5 | **PASS** | 29 | 0 | 0 | 0.2 s | 29 | 0 |
| kraken:ETH-USD | 14 | 5 | **PASS** | 14 | 0 | 0 | 0.1 s | 14 | 0 |

Both feeds connect keylessly. Field mapping verified correct on all 4 streams (Kraken
`qty`→`size`, `BTC/USD`→`BTC-USD`, int `trade_id`→str, ISO `timestamp`→ms; Coinbase
`product_id`→`symbol`, `BUY`→`buy`, `time`→ms). trade_id 100% present, 0 duplicates, 0 missing
on every stream; Kraken event-time within ~0.2 s of wall clock.

**Recalibration (ADR-001, path b).** The original single 50-msg threshold wrongly assumed
comparable volume across venues. Kraken is genuinely thinner (29/14 is real sparse activity, not
a broken feed — uniqueness/mapping/skew all clean), so the gate now uses per-venue liveness
floors derived from observed volume, and it PASSES on its own terms. ADR-001 also settles two
carry-forwards: R6 divergence gets a **30 s staleness window** (skip + telemetry when a venue has
no recent print, never quarantine), and the large **Coinbase skew/ooo is the connect-time
snapshot backfill**, not feed lag. Full reasoning in [docs/decisions.md](docs/decisions.md#adr-001).

## Day B — COMPLETE

All three Day B commits are done and the whole toolchain is green (ruff/mypy/pytest, 118 tests,
core coverage 100%):

1. **Commit 1 — DONE.** `contracts/trades.v1.avsc` + `contracts.py` (Avro wire codec,
   `SchemaRegistry`, `ensure_registered`), `tickflow contract` (register/check/show), ingester
   switched to Avro, BACKWARD-compat CI job.
2. **Commit 2 — DONE.** `contracts/rules.yaml` + `src/tickflow/gate.py`: declarative
   `RulesConfig`/`RulesEngine` (all 6 rules, thresholds read from YAML), `QuarantineEnvelope`
   (rule_id, detail, offset, ts, raw bytes; idempotent key), `verdicts_digest` bit-identity check,
   `evaluate_all`, and the `tickflow gate` at-least-once/manual-commit consumer (integration-lane).
3. **Commit 3 — DONE.** `src/tickflow/fixture.py` + `fixtures.yaml` + committed
   `fixtures/trades.v1.clean.parquet`: seeded deterministic 100,000-message clean fixture (content
   digest pinned), and a seeded fault injector + injection manifest whose expected verdicts are
   cross-checked against the real `RulesEngine`. `tickflow fixture generate/verify`.

**The Day C substrate now exists**: `fixture.generate_clean()` → clean records; `fixture.flatten`
+ `contracts.encode` → clean frames; `fixture.inject_faults(clean, config, schema)` →
`InjectionResult(frames, manifest)`. The manifest's `entries` (index, class, `is_fault`,
`detectable`, `expected_disposition`, `expected_rule`, `boundary`) are the ground truth for
grading; `by_index()`, `counts_by_class()`, `fault_rate()`, `to_json()`/`digest()` are ready for
the metrics code. `fixture.verify_fixture()` is the CI checksum gate.

## Day C — COMPLETE (commits 1–3)

Day C turned the gate into measured evidence (frozen §4/§6). The whole toolchain is green
(ruff/mypy/pytest, **158 tests, core coverage 100%** — bars.py + metrics.py added at 100%):

1. **Commit 1 — DONE.** `src/tickflow/bars.py`: `BarBuilder` (1-min OHLCV per stream, event-time
   watermark, order-independent so replay is bit-identical), `check_slo` (the frozen invariants,
   including "no bar built from a quarantine-worthy message"), `DuckDbSink` (append-only, single
   writer, local-only bar values).
2. **Commit 2 — DONE.** `run_slo_experiment` + `--gates-off`: on the committed fixture (100,477
   frames, seed 42, LRU 10,000) gates ON → **0 of 15,061 bars violated**, gates OFF → **1,076 of
   15,061** (`no_quarantinable` 1,076, `price_positive` 144 — 1,220 violations over 1,076 bars,
   because one bar can trip several invariants). Regenerate with `tickflow slo`.
   Bit-identity checked on the catchable subset vs the ground-truth
   **valid projection**; designed misses surfaced as the R3 recall gap. **ADR-002** records the
   reference clarification (valid projection, not the raw clean fixture — the injector replaces in
   place and its boundary controls alter values by design).
3. **Commit 3 — DONE.** `src/tickflow/metrics.py` + `tickflow metrics` + a `metrics` CI job:
   recall per class, precision per rule, false-quarantine, completeness, each with a 95% bootstrap
   CI. Committed-fixture numbers: R1/R2/R4 recall 1.0000, **R3 recall 0.8407 [0.8071, 0.8721]**
   (76 designed misses, honest), precision 1.0000, false-quarantine 0.0000 over both denominators
   (0/98,800 all controls, 0/460 near-boundary), complete.

**Not done (deferred to Day D by the §11 slip valve):** the original Day-C commit 4 —
`quarantine.py` (`tickflow quarantine` ls/show/stats + `tickflow replay --fixture`) and the
replay-determinism + kill/restart completeness tests. This run stopped after the replay metrics
landed, per its mandate. The gate, the SLO experiment, and the CIs — the parts the slip valve
protects — are all shipped.

## Schema registry — VERIFIED LOCALLY 2026-07-20 (previously CI-only)

`tickflow contract register` / `check` had only ever executed inside CI. Day A recorded `docker`
as unavailable on this machine, so the whole registry path was untested outside GitHub's runners.
Docker Desktop was brought up and the `dev` profile started (`docker compose --profile dev up -d`,
Redpanda **v24.2.7**, healthy on the second poll). All four checks pass:

| check | result |
|---|---|
| `tickflow contract register` | **PASS** — `registered trades.raw-value (BACKWARD) as schema id 1`, exit 0 |
| registry state after register | subject `trades.raw-value`, `compatibilityLevel: BACKWARD`, versions `[1]` |
| `tickflow contract check` | **PASS** — `local contract is compatible with the registered latest (BACKWARD)`, exit 0 |
| re-register (idempotency) | **PASS** — still schema id 1, still versions `[1]`; no version churn |
| `check` vs an incompatible contract | **PASS (correctly fails)** — added a required `int` field with no default → `INCOMPATIBLE`, **exit 1** |

The last row is the one that matters: a compatibility gate that only ever returns "compatible" is
indistinguishable from a no-op. Feeding it a genuinely BACKWARD-breaking change (a required field
with no default, so old readers cannot read new data) makes it fail with a non-zero exit, which is
what the CI job depends on. The mutation was made to a scratch copy of `trades.v1.avsc` and
reverted; `git status` on `contracts/` is clean.

Caveat kept in the open: this was the **`dev`** profile, which bypasses fsync. It is the right
profile for verifying *functional* correctness of register/check, and it is why **no latency or
throughput number from this run is published anywhere**.

## Injection manifest accounting — VERIFIED 2026-07-20 (not assumed)

Re-derived from the injector itself, not carried forward from notes. Regenerate with
`tickflow metrics` (the `grade` block's `n_total` / `n_controls` / `false_quarantine_rate`).

| quantity | count | how it arises |
|---|---|---|
| clean input rows | 100,000 | 4 streams × 25,000, the committed parquet |
| malformed faults | 400 | replaced **in place** (100/stream) → R1 |
| out-of-range faults | 400 | replaced in place → R2 |
| out-of-order faults | 400 | replaced in place, 5.1 s skew → R4 |
| duplicate faults | 477 | **added** frames: 400 immediate + 75 beyond-window + 2 window-edge → R3 |
| **frames emitted** | **100,477** | 100,000 + 477 added dups (replacements add nothing) |
| total faults | 1,677 | 400 + 400 + 400 + 477 |
| **all controls** | **98,800** | 100,477 − 1,677 |
| ├ untouched clean | 98,340 | no manifest entry at all |
| └ **near-boundary controls** | **460** | designed, `is_fault=False`, replaced in place |
|   ├ at inclusive R2 bound | 200 | 111 lower + 89 upper (split is seeded, not fixed) |
|   ├ 4.9 s skew | 200 | just inside the 5.0 s R4 tolerance |
|   └ 5.0 s skew exactly | 60 | the inclusive edge itself |

The 460 are the hard subset: every one sits one representable step from a quarantine decision, so
they are where an off-by-one in a comparison operator would surface. The other 98,340 are ordinary
clean traffic no rule comes near. Reporting only the pooled rate would hide that distinction, so
**both denominators are published side by side, each with its own n and CI.**

## Day D scope decision — the §11 slip valve is PULLED (taken 2026-07-20, before building)

**Decided up front, not at the end: the quarantine-inspection/replay CLI (the original Day-C
commit 4, deferred into Day D) is CUT. tickflow releases as v0.9, not v0.1.0.** Recorded as
**ADR-003**.

What that cut covers, exactly: `quarantine.py` (`tickflow quarantine` ls/show/stats over the
envelopes the gate already emits, `tickflow replay --fixture`), the replay-determinism +
kill/restart completeness tests, the live-soak section, and the still-`if: false` integration
lane those tests would have enabled. What it does **not** touch — the parts §11 says the valve
must never reach — the gate, the SLO experiment, and the CIs, all of which ship.

Day D's own work is what it buys: the SLO experiment currently has no CLI surface and nothing
regenerates it, the `K > 0` placeholder is still unmeasured, and `false_quarantine_rate` is
reported only over the easy 98,800-control denominator. Those are evidence defects in shipped
claims; the quarantine CLI is a convenience over data the gate already emits and the metrics
already grade. Publishing a wrong number is worse than shipping one fewer command, so the
measurement work wins the session and the CLI slips to the roadmap.

## What Day D does  ← **IN PROGRESS**

Day D publishes the evidence and ships v0.9 (frozen §7/§8/§11/§14). In order:

1. **`feat: add telemetry export with schema enforcement`** — `export.py`: write only fields
   declared in `telemetry_schema.json` (counts, rates, latencies, verdicts, timestamps). The
   grade/SLO/telemetry payloads already added in Day C are telemetry-only by construction (a test
   already asserts no market-field keys, allowing `ci_low`/`ci_high`); Day D makes the allowlist a
   **release-blocking** enforcement — a smuggled `price`/`open`/`high`/`low`/`close`/`volume`/`vwap`
   field fails the build (§8, ToS §1). The provenance stamp (commit SHA, fixture + manifest
   checksums, compose profile, runner spec, timestamps) is added to every metrics artifact (§6).
2. **`feat: add metrics workflow and Pages dashboard`** — `metrics.yml` (broker-based, `bench`
   profile, cron+manual): verify fixture checksum → start Redpanda → replay → grade → bootstrap →
   export telemetry JSON → deploy `site/` to Pages. **Dashboard is PIPELINE TELEMETRY ONLY, never
   price charts** (Coinbase ToS, release-blocking): the gate P/R table with CIs per fault class
   (surfacing the R3 designed-miss recall gap next to the perfect classes), completeness,
   throughput/latency with the environment-disclosure line, quarantine rate by rule, the
   gates-ON/OFF SLO comparison, the live-soak section (labeled), and a "last successful refresh"
   timestamp (never a cadence claim). Plain HTML/CSS + one committed JSON — no JS frameworks, no
   price/open/high/low/close/volume/vwap anywhere.
3. **`feat: add quarantine inspection and replay CLI`** (deferred from Day C) — `quarantine.py`:
   `tickflow quarantine` ls/show/stats over the envelopes the gate emits + `tickflow replay
   --fixture`. Then replay-determinism + completeness (**kill/restart the gate consumer mid-replay
   → exactly-once accounting**) tests green; this is what enables the currently-`if: false`
   integration lane in `ci.yml` (mini-fixture → replay → gate → bars → grade → export, E2E < 5 min
   against a real Redpanda). Slip-valve droppable for a v0.9 if Day D overruns.
4. **Live soak on the M4** (overnight-capable, real Coinbase+Kraken): reconnects, gaps, real
   divergence alerts, uptime/completeness telemetry — labeled "measured on Apple M4 … not
   independently reproducible" (§7). Soak telemetry may publish; soak market data may not.
5. **`test: complete coverage to gate`** — close any remaining core-coverage gaps to the ≥ 85 %
   frozen bar (currently 100 % on the built core).
6. **`docs: write README with measured results`** — numbers + CIs from the real metrics artifact;
   the three verified gap citations (Confluent clause, Grab, DEW) load-bearing; incident references
   hedged as motivation only; full limitations section (synthetic fixture, `bench`/fsync caveat,
   deterministic-rules recall scope, single-node, at-least-once, `double` not decimal, telemetry-
   only dashboard, best-effort cron refresh). State the bit-identity claim per ADR-002.
7. **Release grep gate** — release-blocking scan over README, docs/, src/, site/, and the git log:
   zero employer/internship/private-work references **and** zero market-data-derived export.
8. **`chore: release v0.1.0`** — tag once §14 acceptance criteria all pass. (Verify the checkpoint
   rule at first push; never change a GitHub setting to work around a problem — surface it.)

## Local environment notes

- Redpanda `dev` profile may still be running from Day A verification:
  `docker compose --profile dev up -d` / `... down -v`. Kafka on `localhost:19092`, Schema
  Registry on `localhost:18081`.
- `trades.raw` was created with 4 partitions during verification; Day B formalizes topic
  provisioning alongside the contract.
