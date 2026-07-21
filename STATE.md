# STATE

Build order and current status for tickflow. **v0.9 is SHIPPED AND CLOSED (2026-07-20)** — see
[v0.9 — SHIPPED AND CLOSED](#v09--shipped-and-closed-2026-07-20) for what shipped, what was cut,
and the verified numbers. ADR-003 pulled the §11 slip valve; v0.1.0 is reserved for the release
that lands the quarantine/replay CLI. The frozen design lives in
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

## Day D — COMPLETE. Released v0.9.

Day D published the evidence and shipped (frozen §7/§8/§11/§14). Toolchain green throughout:
ruff, mypy strict, **181 tests, core coverage 100%** (export.py added at 100%).

1. **`feat: add a CLI surface for the gates-ON/OFF SLO experiment` — DONE.** The §4 signature
   result had no command that produced it: `run_slo_experiment` was reachable only as a library
   call from the test helpers, on a 4 × 300 small config with a 100-key LRU. Added `tickflow slo`
   (verify pins → inject → compare → print/write JSON, **exit non-zero if the thesis fails**, so
   it is a check and not a report), and `tickflow metrics` now emits one artifact with both
   blocks: `{"grade": …, "slo": …}`. `fixture_label` stamps every result with frame count, seed,
   LRU size, and tolerance, derived from the manifest and live config, so a small-config number
   can never be mistaken for a committed-fixture number.
2. **`docs: replace the K > 0 placeholder with the measured counts` — DONE.** Measured at
   fixture scale and written into all four sites (STATE, `bars.py` ×2, ADR-002, plus the
   changelog and the SLO test docstring). Recorded that per-invariant counts can exceed the
   violated-bar count, so 1,220 violations over 1,076 bars does not read as inconsistent. The
   tests still assert **shape, not magnitude** — pinning a measured count in a test turns a
   measurement into a self-referential constant.
3. **`feat: split false-quarantine into all-controls and near-boundary` — DONE.** Both
   denominators, each with its own n and CI, in the telemetry JSON and the printed table:
   **0/98,800** all controls and **0/460** near-boundary. Manifest accounting re-derived from the
   injector rather than trusted (table above; every line checked out). Also documented the
   degenerate-interval caveat, because it cuts against us: a percentile bootstrap over a
   zero-event sample returns `[0, 0]`, an artifact of the method at the boundary, not proof the
   rate is zero — the honest ceiling is the rule of three, ≈3/n, which is ~200× looser on 460
   controls than on 98,800.
4. **Schema registry verified locally — DONE.** First time outside CI. All four checks pass,
   including the load-bearing one: `check` correctly **fails with exit 1** on a BACKWARD-breaking
   contract. Full result in the section above.
5. **`feat: add telemetry export with schema enforcement and Pages dashboard` — DONE.**
   `export.py` + `tickflow export` + `.github/workflows/pages.yml`. Telemetry-only is enforced
   **mechanically** (`assert_telemetry_only` walks the payload and raises on any market-data field
   name at any depth; both `build_telemetry` and `render_html` call it; CI re-checks the built
   artifact). Exact-name matching, deliberately — `price_positive` / `high_ge_low` are SLO labels
   and `ci_low` / `ci_high` are CI bounds, and a substring rule would reject them and train the
   next person to weaken the check. Page is static HTML, **no JavaScript, no external assets**,
   rendered from the committed `site/telemetry.json`. Says "last refreshed <timestamp>", never a
   cadence; CI fails the build if a cadence claim appears. Caught and fixed a silent provenance
   bug on the way: the fixture pin was read as `content_digest` while `fixtures.yaml` spells it
   `content_sha256`, so it defaulted to `""` and shipped blank — missing pins now raise.
6. **`docs: write README with measured results` — DONE.** Every number traced to
   `site/telemetry.json` with its JSON path, and cross-checked against the artifact
   programmatically (0 mismatches). Leads with gates ON/OFF, reads its own perfect-recall rows
   skeptically, and publishes **no performance figure at all** (see below). Limitations state all four required items — dev-mode bypasses fsync so **no latency
   claim is made anywhere**; fixtures are synthetic and no captured real data is ever committed;
   designed-miss dups pull R3 recall below 100% by construction; SLO numbers are fixture-scale,
   not live-traffic — plus single-node, at-least-once, `double` not decimal, and best-effort
   refresh. Added an explicit "what v0.9 does not ship" section. All internal links verified.
7. **`feat: add the release-blocking scan` + `chore: release v0.9` — DONE.** The scan is now
   `scripts/release_gate.sh` and a CI job (fetch-depth 0, since a history scan on a shallow clone
   is theatre) rather than an instruction to run greps by hand. **Verified by planting each
   violation class** — a reference in a tracked file, a market field in `site/telemetry.json`, a
   cadence claim on the page — and confirming all three block. That exercise found a real bug in
   the gate itself: `git grep -E` does not implement `\b` (it needs `-P`), so the pattern matched
   nothing and the scan could only ever report PASS. Replaced with `git ls-files` piped to real
   grep. The two files that legitimately contain the prohibited words because they *define* the
   prohibition are exempt by path, and exempt hits are **printed, never skipped**, so an exemption
   cannot become a hiding place. Gate PASSES. Tagged **v0.9**.

### Not shipped in v0.9 (roadmap, in order)

Cut by the ADR-003 slip valve. Nothing here is blocked; it is deferred work with a clear path:

1. `quarantine.py` — `tickflow quarantine` ls/show/stats over the envelopes the gate already
   emits, and `tickflow replay --fixture`.
2. Replay-determinism + **kill/restart-mid-replay exactly-once** completeness tests against a
   real broker. Completeness is currently asserted in-process only.
3. The end-to-end integration lane in `ci.yml` (still `if: false`) — mini-fixture → replay → gate
   → bars → grade → export against a real Redpanda, which (2) is what enables.
4. Live soak on real Coinbase + Kraken feeds. **No live-soak number is published anywhere today.**

v0.1.0 is reserved for the release that lands 1–3.

### Day D verification summary

| claim | how it was checked |
|---|---|
| manifest accounting (100,477 / 1,677 / 460 / 98,800) | re-derived from the injector, not trusted |
| gates ON/OFF counts | `tickflow slo` on the committed fixture |
| small-config reference (0 vs 12 of 189) | re-run and confirmed before quoting |
| schema registry register/check | real Redpanda v24.2.7, **incl. a deliberate incompatible contract** |
| release gate | **planted violations of all three classes**; all blocked |
| README numbers | cross-checked against `site/telemetry.json` programmatically, 0 mismatches |
| dashboard | parsed, no unclosed tags, no JS, no external assets, no cadence claim |
| telemetry-only | `assert_telemetry_only` + independent grep, on the built artifact |

## v0.9 — SHIPPED AND CLOSED (2026-07-20)

**tickflow v0.9 is released and pushed.** Tags `v0.9`
(`6fd31b6`) and `v0.9.1` (`e556a19`, release-gate interpreter fix) are pushed and unmoved.
Post-tag work has since landed on `main` and is unreleased — see [Unreleased] in the
CHANGELOG; the throughput removal below is part of it and is **not** in either tag.
`main` is in sync with origin; `ci` and `pages` are green on the same commit; the dashboard is
live at <https://rishikkasula.github.io/tickflow/> (HTTP 200). Local broker torn down, no
containers/volumes/networks left.

### What shipped

The declarative gate (6 rules, R1–R4 quarantine, R5–R6 structurally alert-only), the Avro
contract with a BACKWARD-compat registry check verified against a real broker, the seeded fixture
generator + fault injector with its ground-truth manifest, the bar builder with the SLO checker,
the gates-ON/OFF SLO experiment **with a CLI that regenerates it**, bootstrap-CI grading over
both false-quarantine denominators, telemetry export with mechanically enforced ToS compliance,
the static Pages dashboard, and the release-blocking scan as a script plus a CI job.

### What was deliberately cut, and why

Cut via the §11 slip valve (**ADR-003**, decided before building, not as an end-of-session
excuse): the **quarantine inspection / replay CLI** (`tickflow quarantine` ls/show/stats,
`tickflow replay --fixture`), its kill/restart-mid-replay exactly-once tests, the integration
lane those enable, and the **live soak**.

The reason: Day D opened with three defects in *already-published* claims — the signature SLO
experiment had no command that produced it and had never run at fixture scale, the gates-OFF
result was the literal placeholder `K > 0`, and the false-quarantine rate hid its 460 designed
near-boundary controls inside a 98,800 denominator. The quarantine CLI is an inspection
convenience over envelopes the gate already emits and faults the metrics already grade; it adds
no measurement. Publishing a wrong number is worse than shipping one fewer command, so the
measurement work took the session. §11 permits exactly this trade and prefers a stated v0.9 with
a roadmap to a rushed v0.1.0.

Also removed rather than caveated: the in-process gate throughput figure. It varied more than 3×
across machines and 1.6× between two runs on the same CI runner class, so it described the runner
rather than the gate. Nothing replaced it; tickflow makes no performance claim. This removal
landed **after** v0.9.1 and is unreleased — both tags still ship the figure. Recorded as ADR-004.

### Verified headline numbers

All from the **committed fixture: `trades.v1.clean.parquet`, 100,477 frames, seed 42,
LRU 10,000, out-of-order tolerance 5,000 ms**. Fixture-scale, not live-traffic. Regenerate every
figure below with `tickflow export` (or `tickflow slo` / `tickflow metrics` individually); the
artifact is `site/telemetry.json` and CI fails if the committed copy drifts from it.

| measure | value | source |
|---|---|---|
| gates ON | 0 of 15,061 bars violated | `slo.gates_on` |
| gates OFF | 1,076 of 15,061 bars violated | `slo.gates_off` |
| gates-OFF breakdown | `no_quarantinable` 1,076, `price_positive` 144 | `slo.gates_off.counts_by_invariant` |
| bit-identity (catchable subset) | holds; 76 designed-miss dups reported | `slo.bit_identical` |
| recall R1 / R2 / R4 | 1.0000 [1.0000, 1.0000], n = 400 each | `grade.per_class` |
| recall R3 (duplicate) | **0.8407 [0.8071, 0.8721]**, n = 477 | `grade.per_class` |
| precision, all rules | 1.0000 [1.0000, 1.0000] | `grade.per_class` |
| false quarantine, all controls | 0.0000, n = 98,800 | `grade.false_quarantine_rate.all_controls` |
| false quarantine, near-boundary | 0.0000, n = 460 | `grade.false_quarantine_rate.near_boundary_controls` |
| completeness | 100,477 accounted for exactly once (98,876 valid / 1,601 quarantined, 0 loss) | `grade.completeness` |

Read with their caveats, which are stated in the README and not softened here: R1/R2/R4 recall is
~100% *by construction* because the rules are deterministic; R3 is below 100% *by construction*
because the injector plants duplicates no finite-memory deduplicator can catch; and the
zero-rate CIs are degenerate — a percentile bootstrap over a zero-event sample cannot see events
it never observed, so the honest ceiling is the rule of three, ≈3/n.

### If work resumes

Nothing below is started, and none of it is blocking.

1. **Node 20 action deprecations.** `actions/checkout@v4`, `actions/upload-artifact@v4`,
   `astral-sh/setup-uv@v5`, and `actions/deploy-pages@v4` are being force-run on Node 24 and warn
   on every run. Warnings only today; bump before it becomes an error.
2. **The deferred quarantine / replay CLI** (`quarantine.py`), then the kill/restart-mid-replay
   exactly-once completeness proof, then switching on the `if: false` integration lane in
   `ci.yml`. That sequence is what v0.1.0 is reserved for.
3. **Live soak** on real Coinbase + Kraken feeds. Soak telemetry may publish; soak market data
   may not. No live-soak number is published anywhere today.

## Local environment notes

- Redpanda: `docker compose --profile dev up -d --wait` / `docker compose --profile dev down -v`.
  Kafka on `localhost:19092`, Schema Registry on `localhost:18081`. Docker Desktop must be
  running first (`open -a Docker`); Day A recorded it as unavailable, but it works on this
  machine as of Day D.
- **No published number requires a broker.** Everything on the dashboard and in the README is
  produced in-process from the committed fixture, so `tickflow export` works with Docker stopped.
  The broker is needed only for `tickflow contract register` / `check`.
- The `dev` profile bypasses fsync. It is fine for functional verification and its timings are
  never quoted; `bench` is the only profile from which timings could ever be published, and v0.9
  publishes none.
