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
- [ ] Day A: capture + sanity commands  ← **NEXT**
- [ ] §0 feed-sanity gate reviewed → ADR-001 (BLOCKING before any gate logic)

## What Day B does next

Day A is not fully closed: the remaining `feat: add capture and sanity commands` and the
blocking §0 feed-sanity gate come first, then Day B proper. Precisely, in order:

1. **Finish Day A — capture + sanity commands.** `tickflow capture --minutes 5` writes a
   local (gitignored) capture from both live feeds; `tickflow sanity` prints, for the
   2 exchanges × 2 symbols: message counts, 5 raw→normalized field-mapping pairs per feed,
   timestamp sanity (event-time within ±60 s of wall clock, monotone-ish), and trade_id
   presence/uniqueness stats.
2. **§0 feed-sanity gate (blocking, manual).** Rishik reviews the sanity output. Gate: both
   feeds connect keylessly and all 4 streams produce ≥ 50 messages in 5 min with field mapping
   verified correct → proceed. Any feed failing → STOP and re-scope by ADR first. Record the
   result as **ADR-001** in `docs/decisions.md` (currently PENDING).
3. **Day B commit 1 — `feat: add trades.v1 contract with schema registry wiring`.** Add
   `contracts/trades.v1.avsc`, register subject `trades.raw-value` in Redpanda's Schema
   Registry with **BACKWARD** compatibility (CI check), and switch the ingester's on-wire
   encoding from JSON to Avro (record shape unchanged).
4. **Day B commit 2 — `feat: add declarative rules engine with quarantine routing`** (+ gate
   unit tests). Implement the 6 frozen rules from `contracts/rules.yaml` (R1 schema, R2 range,
   R3 duplicate/LRU-10k, R4 out-of-order vs per-stream watermark, R5 gap alert-only, R6
   divergence alert-only), the gate consumer (at-least-once, manual commits) routing to
   `trades.valid` / `trades.quarantine` (envelope: rule_id, detail, offset, ts, raw bytes),
   evaluating against the per-stream **event-time watermark**, never wall clock. Table-driven
   verdict tests including boundary literals (5.0 s vs 5.001 s skew, LRU eviction at exactly
   10,000, range edges) and watermark determinism.
5. **Day B commit 3 — `feat: add synthetic fixture generator and fault injector`** (+
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
