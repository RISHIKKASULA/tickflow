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
- [ ] Day B commit 2: declarative rules engine with quarantine routing  ← **NEXT**
- [ ] Day B commit 3: synthetic fixture generator and fault injector

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

## What Day B does next

Day A is closed and the gate is PASS (ADR-001). Commit 1 (contract + registry wiring) is done;
**NEXT = commit 2** below. Carry-forward from ADR-001 that commit 2 must honor: the rules engine
implements R6's **30 s staleness window** and stale→telemetry (not quarantine) behavior for the
sparse second venue. The gate consumer decodes with `contracts.decode` (a raised `ValueError`
is R1 `malformed`), and evaluates against the per-stream event-time watermark, never wall clock.

1. **Day B commit 1 — DONE.** `contracts/trades.v1.avsc` + `contracts.py` (Avro wire codec,
   `SchemaRegistry`, `ensure_registered`), `tickflow contract` (register/check/show), ingester
   switched to Avro, BACKWARD-compat CI job. The live register/check runs in CI (needs a broker).
2. **Day B commit 2 — `feat: add declarative rules engine with quarantine routing`** (+ gate  ← **NEXT**
   unit tests). Implement the 6 frozen rules from `contracts/rules.yaml` (R1 schema, R2 range,
   R3 duplicate/LRU-10k, R4 out-of-order vs per-stream watermark, R5 gap alert-only, R6
   divergence alert-only), the gate consumer (at-least-once, manual commits) routing to
   `trades.valid` / `trades.quarantine` (envelope: rule_id, detail, offset, ts, raw bytes),
   evaluating against the per-stream **event-time watermark**, never wall clock. Table-driven
   verdict tests including boundary literals (5.0 s vs 5.001 s skew, LRU eviction at exactly
   10,000, range edges) and watermark determinism.
3. **Day B commit 3 — `feat: add synthetic fixture generator and fault injector`** (+
   determinism tests). `fixture.py`: seeded (42) 4 streams × 25,000 = 100,000 clean messages
   as zstd parquet, SHA-256 pinned in `fixtures.yaml`; seeded fault injector producing the
   faulted stream + injection manifest (exact counts/ids per fault class, with boundary
   controls). Seeded-determinism tests (same seed → same checksum; manifest counts match).

Then continue with Day C per the commit arc above.

## Local environment notes

- Redpanda `dev` profile may still be running from Day A verification:
  `docker compose --profile dev up -d` / `... down -v`. Kafka on `localhost:19092`, Schema
  Registry on `localhost:18081`.
- `trades.raw` was created with 4 partitions during verification; Day B formalizes topic
  provisioning alongside the contract.
