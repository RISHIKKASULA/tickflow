# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Package scaffold, tooling (uv, ruff, mypy, pytest + coverage, pre-commit), and CI skeleton.
- docker-compose Redpanda stack with `dev` and `bench` profiles.
- Exchange ingesters (Coinbase primary, Kraken secondary) normalizing both venues to the
  common `trades.v1` schema and producing raw ticks to `trades.raw`.
- Feed-sanity gate commands: `tickflow capture` writes a local, checksummed capture from both
  live feeds; `tickflow sanity` reports per-stream counts, field mapping, timestamp/skew, and
  trade_id uniqueness with a provisional pass/fail against per-venue liveness thresholds.
- `trades.v1` Avro contract (`contracts/trades.v1.avsc`) with Schema Registry wiring: subject
  `trades.raw-value` registered with BACKWARD compatibility (CI check), a `tickflow contract`
  command (register/check/show), and the ingester switched to the Confluent Avro wire format.
- Declarative rules engine with quarantine routing (`contracts/rules.yaml`, `src/tickflow/gate.py`):
  the six frozen rules (R1 schema, R2 range, R3 duplicate/LRU-10k, R4 out-of-order, R5 gap,
  R6 cross-venue divergence) evaluated against the per-stream event-time watermark, never wall
  clock. R1–R4 quarantine to `trades.quarantine` with a self-describing envelope (rule_id,
  detail, offset, ts, raw bytes; idempotent write key); R5–R6 are alert-only. R6 honors the
  ADR-001 30 s staleness window (no verdict + `divergence_unavailable` telemetry on a stale
  venue). `tickflow gate` runs the at-least-once consumer with manual commits.
- Synthetic fixture generator and fault injector (`src/tickflow/fixture.py`, `fixtures.yaml`,
  committed `fixtures/trades.v1.clean.parquet`): a seeded (42), integer-driven, deterministic
  clean fixture of 4 streams × 25,000 = 100,000 `trades.v1` messages as zstd parquet, pinned by
  a platform-independent content digest (CI's system of record). A seeded fault injector rewrites
  ~2% of messages into the four quarantine-rule fault classes (`malformed`→R1, `out-of-range`→R2,
  `duplicate`→R3, `out-of-order`→R4) and emits an injection manifest — the ground truth Day C
  grades detection precision/recall against. Boundary faults are deliberate: values exactly on
  the inclusive range bound, 4.9 s vs 5.0 s vs 5.1 s skews, and duplicates at the exact LRU-window
  edge (lru−1 keys back → caught vs lru back → designed miss). `tickflow fixture generate/verify`.
- Downstream bar builder with SLO checker and DuckDB sink (`src/tickflow/bars.py`): the §4
  "gates earn their keep" consumer. `BarBuilder` folds `trades.valid` into 1-minute OHLCV bars
  per (exchange, symbol) keyed off the event-time watermark — fully order-independent (open/close
  by (ts_event, trade_id), high/low by max/min, volume summed in integer micro-units) so replayed
  bars are bit-identical regardless of delivery order. `check_slo` enforces the frozen invariants
  (high ≥ low, open/close within [low, high], volume > 0, positive prices, monotone bar timestamps,
  and — load-bearing — no bar built from a message the gate would quarantine). `DuckDbSink` is an
  append-only single-writer local store; bar market values never leave the environment (§1 ToS).
  `tickflow bars` runs the consumer (integration lane).
- Gates-off demo mode with the signature SLO comparison (`run_slo_experiment` in `bars.py`, the
  first-class `--gates-off` flag on `tickflow gate`): replay the fault-injected fixture through the
  real gate twice. Gates ON → zero SLO violations; gates OFF → K > 0 violated bars (dominated by
  the `no_quarantinable` invariant plus corrupted extremes) — the thesis made visible. Bit-identity
  is checked on the catchable fault subset (designed-miss dups filtered out) against the manifest's
  ground-truth valid projection; the designed misses are surfaced as the R3 recall gap, not hidden.
  ADR-002 records why the reference is the valid projection, not the raw clean fixture (the injector
  replaces in place and its boundary controls alter values by design).
- Fault-injection grading with bootstrap CIs (`src/tickflow/metrics.py`, `tickflow metrics`): grade
  the gate's verdict stream against the injection manifest — detection recall per fault class,
  precision per rule, false-quarantine rate over the boundary controls + all untouched clean, and
  completeness (every input accounted for exactly once). Every proportion carries a 95% bootstrap CI
  (B=10,000, seed 42, percentile interval; the exact `Binomial(n, k/n)/n` identity for a proportion).
  Reported honestly as `point [lo, hi]`: R1/R2/R4 recall 1.0000, R3 recall 0.8407 [0.8071, 0.8721]
  — the designed-miss duplicates drag it below 100% by design (§5), not a failure. A new `metrics`
  CI job replays the committed fixture in-process (no broker), grades it, and archives the telemetry
  JSON; the broker-based publishing workflow with full provenance is Day D.
