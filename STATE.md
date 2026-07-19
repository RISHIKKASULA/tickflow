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
- [ ] §0 feed-sanity gate — **RUN 2026-07-19; provisional FAIL, awaiting manual review →
  ADR-001** (BLOCKING before any gate logic). See the gate result below.

## Feed-sanity gate result — RUN 2026-07-19 (provisional FAIL, awaiting review)

Capture: `data/captures/gate-2026-07-19` (local, gitignored; ToS §1), sha256
`5be3cde5e281be05…`, 300.1 s, 1060 records. Command: `tickflow sanity --capture <dir>`.

| stream | count | ≥50? | uniq_id | dup | missing | max skew | within 60s | ooo |
|---|---|---|---|---|---|---|---|---|
| coinbase:BTC-USD | 814 | **PASS** | 814 | 0 | 0 | 37.7 s | 814 | 163 |
| coinbase:ETH-USD | 203 | **PASS** | 203 | 0 | 0 | 134.9 s | 150 | 110 |
| kraken:BTC-USD | 29 | **FAIL** | 29 | 0 | 0 | 0.2 s | 29 | 0 |
| kraken:ETH-USD | 14 | **FAIL** | 14 | 0 | 0 | 0.1 s | 14 | 0 |

**What passes:** both feeds connect keylessly, no keys/account. Field mapping verified correct
by eye on all 4 streams (Kraken `qty`→`size`, `BTC/USD`→`BTC-USD`, int `trade_id`→str, ISO
`timestamp`→ms; Coinbase `product_id`→`symbol`, `BUY`→`buy`, `time`→ms). trade_id 100% present,
0 duplicates, 0 missing on every stream. Kraken event-time sits within ~0.2 s of wall clock.

**What fails:** both **Kraken** streams fall below the frozen 50-msg/5-min threshold (29 and
14). Coinbase is abundant. The large Coinbase skew/ooo is **not** feed lag — it is the
connect-time **snapshot backfill** (Coinbase replays recent trades on subscribe); steady-state
Coinbase updates are within tolerance, and it clears the gate regardless.

**Verdict:** provisional **FAIL** on Kraken throughput alone. Everything else (connectivity,
field mapping, uniqueness) passes. Per §0 this means **STOP and re-scope by ADR before any gate
logic** — do not build the contract layer on a feed that can't sustain the window.

**Awaiting Rishik's manual review + re-scope decision → recorded as ADR-001 (still PENDING).**
Candidate re-scope paths for that decision (not yet chosen): (a) Coinbase-only for v0.1 — drops
the cross-venue divergence rule R6, which needs two venues; (b) keep Kraken but recalibrate the
gate for its genuinely lower volume (longer window and/or a per-venue threshold — 29+14 is real,
sparse data, not a broken feed); (c) swap Kraken for a checked higher-volume alternate to
preserve the two-venue / R6 story. ADR-001 records the choice and its scope consequences.

## What Day B does next

**Day B is BLOCKED** until the gate above is confirmed PASS or re-scoped in ADR-001. No
contract/rules/fixture work begins until then (frozen §0: the gate is binding). Once ADR-001
records the decision, resume from the first unchecked item below; a re-scope may amend it (e.g.
Coinbase-only drops R6 from the Day B rules engine and the divergence path throughout).

1. **Day B commit 1 — `feat: add trades.v1 contract with schema registry wiring`.** Add
   `contracts/trades.v1.avsc`, register subject `trades.raw-value` in Redpanda's Schema
   Registry with **BACKWARD** compatibility (CI check), and switch the ingester's on-wire
   encoding from JSON to Avro (record shape unchanged).
2. **Day B commit 2 — `feat: add declarative rules engine with quarantine routing`** (+ gate
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
