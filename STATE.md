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
  job**. Codec unit-tested (contracts.py 100%); registry/E2E is integration-lane (no broker on
  the M4 this session — `docker` unavailable, so the live register/check is CI-verified).
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

## What Day C does next  ← **NEXT**

Day C turns the gate into measured evidence (frozen §4/§6, commit arc §11). In order:

1. **`feat: add barbuilder with SLO checker and DuckDB sink`** — `bars.py` consumes
   `trades.valid`, builds 1-minute OHLCV bars per (exchange, symbol), appends to DuckDB
   (single-writer, append-only). SLO invariants: high ≥ low, high ≥ open/close ≥ low, volume > 0,
   monotone bar timestamps, no bar built from a message the gate would quarantine. Bar values
   never leave the local environment (§1 ToS).
2. **`feat: add gates-off demo mode with SLO comparison`** — a first-class `--gates-off` flag
   routing everything to valid. The signature experiment: replay the fault-injected fixture (from
   commit 3) twice. Gates ON → bars **bit-identical** to clean-fixture bars, zero SLO violations;
   gates OFF → the SLO checker counts K violated bars. The ON/OFF table is the README's opening
   evidence. **⚠ Carry-forward the injector forces you to resolve here:** the `duplicate` class
   includes *designed misses* (dups past the LRU window, manifest `detectable=False`) that the
   gate legitimately cannot catch, so they route valid **even with gates ON** — meaning raw
   gates-ON bars will NOT be bit-identical to clean-fixture bars. Resolve deliberately, e.g. run
   the §4 bit-identity experiment on the *catchable* fault subset (filter the injected stream to
   `is_fault → detectable`, keeping all controls), and report the designed-miss dups separately as
   the R3 recall gap (§6). The manifest's `detectable`/`expected_disposition` fields exist for
   exactly this split. Do NOT "fix" it by deleting the designed misses — they are the frozen §5
   point that keeps recall honest.
3. **`feat: add fault-injection grading with bootstrap CIs`** — `metrics.py` grades gate output
   against the injection manifest: for each entry compare the gate verdict at `entry.index` to
   `expected_disposition`/`expected_rule`; detection recall/precision **per fault class** (recall
   denominator = `is_fault` entries, so designed misses correctly drag R3 recall below 100%),
   false-quarantine rate on `is_fault=False` controls + untouched clean (`n_clean_controls`),
   completeness (every input accounted for exactly once across valid+quarantine). `manifest.by_index()`
   and `counts_by_class()` are the grading handles. Every proportion carries a bootstrap 95% CI
   (B=10,000, seed 42, percentile intervals); report format `point [lo, hi]`.
4. **`feat: add quarantine inspection and replay CLI`** — `quarantine.py`: `tickflow quarantine`
   ls/show/stats over the envelopes this commit's gate emits, plus `tickflow replay --fixture`.
   Then replay-determinism + completeness (kill/restart mid-replay → exactly-once accounting)
   tests green. **Slip valve (§11):** if Day C overruns, v0.9 drops this quarantine-replay CLI and
   the live-soak section — never the gate, the SLO experiment, or the CIs.

Then Day D: telemetry export with schema enforcement, metrics workflow + Pages dashboard, live
soak, coverage-to-gate, README with measured numbers, release grep gate, `chore: release v0.1.0`.

## Local environment notes

- Redpanda `dev` profile may still be running from Day A verification:
  `docker compose --profile dev up -d` / `... down -v`. Kafka on `localhost:19092`, Schema
  Registry on `localhost:18081`.
- `trades.raw` was created with 4 partitions during verification; Day B formalizes topic
  provisioning alongside the contract.
