# tickflow — Design Doc & Spec (FROZEN v1.0, 2026-07-09)

Status: frozen for implementation on Opus, Days 10–13 (Jul 17–20). Deviations require an ADR
in `docs/decisions.md`; simplest defensible choice wins. This file becomes
`docs/architecture.md` (commit-arc/scaffold sections move to STATE.md). Same governance as
fraudscore and filinglens: no `CLAUDE.md`, no `.claude/` in the repo; sole author
`Rishik Kasula <contact.rishikkasula@gmail.com>` verified with `git log --format=full` on the
first commit; the standing git checkpoint rule applies at repo creation and first push.

**One-liner:** an open-source quality-gate and contract-enforcement layer for Kafka-compatible
streams — declarative per-topic contracts (Avro schema + semantic rules) enforced inline,
violations routed to a quarantine topic, protecting a downstream consumer with a measurable
SLO — with every quality claim measured by seeded fault-injection replay in CI, with
confidence intervals. Demonstrated on live cross-exchange crypto market data.

**Gap anchor (verified, the three load-bearing citations):** ① Confluent paywalls inline
stream quality rules — its docs state verbatim: "Schema rules are only available on Confluent
Enterprise (not on the Community edition)" / Stream Governance "Advanced" on Cloud; Flink SQL
and ksqlDB don't execute rules at all. ② Grab built exactly this capability in-house
(engineering.grab.com, Nov 2025): field-level semantic rules on 100+ Kafka topics, violations
routed to a dedicated topic, because stream DQ validation "lacked an effective solution."
③ Data Engineering Weekly (Jan 2026): data contracts stayed "descriptive artifacts… rather
than interfaces with failure semantics" — with the term's originator agreeing in the comments.
The pipeline is a solved problem (2,856 GitHub lookalikes); the enforced gate is not.
**Incident numbers (Unity, Knight Capital, Monte Carlo survey) are MOTIVATION ONLY — hedged
wording, no precise load-bearing claims, no dollar figure presented as established fact.**

**Honest framing (frozen README language):** market data is the demo domain, not the product —
it is free, 24/7, and naturally hostile (duplicates, gaps, out-of-order delivery, venue
divergence). The product is the gate. If the gates were deleted, the remaining pipeline would
be worthless by design — the README says this. **Boundary rule (frozen): no reference to any
employer, internship, or private prior work anywhere in this repo — README, docs, code
comments, or commit messages. Release-blocking grep before v0.1.0.**

---

## 0. DAY-1 FEED SANITY GATE (blocking, manual, before any gate logic)

`tickflow capture --minutes 5` — connect to both live websockets, normalize to the common
schema, write a local capture. Then `tickflow sanity`: for 2 exchanges × 2 symbols, print
message counts, field-mapping samples (5 raw→normalized pairs per feed), timestamp sanity
(event-time within ±60 s of wall clock, monotone-ish), and trade_id presence/uniqueness stats.
**Rishik manually reviews.** Gate: both feeds connect keylessly, all 4 streams produce ≥ 50
messages in 5 min, field mapping verified correct → proceed. Any feed failing → **STOP**;
re-scope (Kraken-only, or swap in a checked alternate) by ADR first. Result committed to
`docs/decisions.md` as ADR-001.

## 1. Feeds (frozen; verified against official docs 2026-07-09)

- **Coinbase** (primary): `wss://advanced-trade-ws.coinbase.com`, `market_trades` channel —
  keyless, no account, no card; limits 8 conn/s/IP, 8 unauth msgs/s/IP.
- **Kraken** (secondary, cross-source checks): `wss://ws.kraken.com/v2`, `trade` channel —
  keyless; ~150 (re)connects per rolling 10 min/IP; ping to survive 1-min idle timeout.
- **Symbols (frozen):** `BTC-USD`, `ETH-USD` on both venues. 2 × 2 = 4 streams. No symbol creep.
- **ToS resolution (frozen, release-blocking):** Coinbase's Market Data Terms prohibit public
  redistribution of the data "or any Derived Works." Therefore: **(a) no captured market data
  is ever committed to the repo; (b) the public dashboard shows pipeline telemetry only —
  never prices, bars, volumes, or anything derived from market data values.** Enforced
  mechanically (§8) and by release grep. Live captures stay local and gitignored.
- Reconnect policy: exponential backoff (1s → 60s cap, jitter), resubscribe on connect;
  disconnect/reconnect events are telemetry.

## 2. The contract (`contracts/trades.v1.avsc` + `contracts/rules.yaml`, committed)

**Avro schema `trades.v1`** (registered in Redpanda's built-in Schema Registry, subject
`trades.raw-value`, BACKWARD compatibility enforced as a CI check):

```
{ exchange: string("coinbase"|"kraken"), symbol: string, trade_id: string,
  price: double, size: double, side: string("buy"|"sell"|"unknown"),
  ts_event: timestamp-millis, ts_ingest: timestamp-millis }
```

`double` for price/size is a documented v0.1 limitation (not decimal); fine at these
magnitudes, stated honestly. Ingesters normalize both venues' raw messages to this schema —
normalization is the producer's job; the gate never sees venue-specific formats.

**Rules (`rules.yaml`, frozen rule set — declarative, versioned, per-topic):**

| ID | Rule | Params (frozen defaults) | On violation |
|---|---|---|---|
| R1 | schema | Avro decode must succeed | quarantine `malformed` |
| R2 | range | static per-symbol bounds: BTC price ∈ [1e3, 1e7], ETH ∈ [50, 1e6]; price > 0, size > 0 | quarantine `out-of-range` |
| R3 | duplicate | (exchange, symbol, trade_id) seen in LRU window of 10,000 per stream | quarantine `duplicate` |
| R4 | out-of-order | ts_event < per-stream watermark − 5,000 ms | quarantine `out-of-order` |
| R5 | gap | watermark jump > 60 s on an active stream | **alert only** (can't quarantine a missing message) |
| R6 | divergence | cross-venue mid deviation > 0.5% sustained 10 s (per symbol) | **alert only** |

**Frozen rationale for alert-only rules (README wording):** a genuine market move diverges
across venues before it converges — quarantining messages for R5/R6 would punish true data
for reality's behavior. Detection ≠ rejection; the gate distinguishes them. **Clock rule
(frozen, what makes replay deterministic):** R3–R6 evaluate against the per-stream event-time
watermark (max ts_event seen), never wall clock. Same messages in → same verdicts out, live
or replay. Wall-clock freshness exists only as live-mode telemetry, excluded from CI metrics.

## 3. Topology (frozen — minimal by design)

```
coinbase-ingester ─┐                        ┌─→ trades.valid ──→ barbuilder → DuckDB (+SLO)
                   ├─→ trades.raw ─→ GATE ──┤
kraken-ingester ───┘                        └─→ trades.quarantine (envelope: rule_id, detail,
                                                 offset, ts, raw bytes)
```

Single-node Redpanda in Docker (native ARM64). Two compose profiles (frozen):
- **`dev`** — `--mode dev-container --smp 1 --memory 1G`: functional work only. Redpanda docs
  state this mode bypasses fsync, "which results in unrealistically fast clusters."
  **Numbers from `dev` are never published. Anywhere.**
- **`bench`** — fsync-safe (dev-container off, write caching disabled): the only profile CI
  publishes metrics from; profile name recorded in every metrics artifact.

Delivery semantics (frozen): idempotent producers; at-least-once consumption with manual
commits after processing; quarantine writes idempotent via envelope key. Exactly-once
transactions are deepening, and the README limitations say so. Consumer: hand-rolled
`confluent-kafka-python` (deliberate — fundamentals signal; Quix Streams noted as deepening).

## 4. Downstream consumer & SLO (`barbuilder`) — the "gates earn their keep" demo

Consumes `trades.valid`, builds 1-minute OHLCV bars per (exchange, symbol), appends to DuckDB
(single writer process, append-only — the supported concurrency pattern per DuckDB docs).

**SLO (frozen):** every bar must satisfy validity invariants — high ≥ low, high ≥ open/close ≥
low, volume > 0, monotone bar timestamps, no bar built from a message the contract would
quarantine. **The signature experiment (deterministic, in CI):** replay the fault-injected
fixture twice — gates ON and `--gates-off` (a first-class demo flag that routes everything to
valid). Gates ON: bars must be **bit-identical** to bars built from the clean fixture, zero SLO
violations. Gates OFF: SLO checker counts K violated bars (dup-inflated volume, corrupted
extremes, time regressions). The ON/OFF comparison table is the README's opening evidence and
the dashboard's centerpiece — quality gates measured by what they prevent, not by assertion.
Bars are evidence for telemetry counts only; **bar values never leave the local environment** (§1).

## 5. Fixtures & fault injection (`src/tickflow/fixture.py` — the ground-truth machine)

- **Committed fixture = synthetic, generated, seeded** (fraudscore discipline; also the ToS
  decision — captured real data cannot be committed, §1). `make_fixture.py`: seeded (42)
  per-stream random-walk prices with realistic arrival gaps, venue-consistent trade_ids,
  4 streams × 25,000 = **100,000 clean messages**, written as zstd parquet;
  **SHA-256 pinned in `fixtures.yaml`** — CI verifies the checksum before every metrics run.
- **Fault injector** (seeded 42, applied to the clean fixture at test time; never committed
  separately): produces the faulted stream plus an **injection manifest** (exact counts and
  message ids per class) — the ground truth the gate is graded against.
- **Fault classes (frozen)** — each maps to a rule, and each includes boundary cases so
  detection is genuinely imperfect and the numbers are informative, not rigged:
  `malformed` (corrupt Avro bytes) · `out-of-range` (neg/zero/absurd price or size; values
  just inside bounds as controls) · `duplicate` (immediate dups AND dups re-injected beyond
  the 10,000-message LRU window — designed misses) · `out-of-order` (skews straddling the 5 s
  tolerance: 4.9 s controls, 5.1 s faults) · plus untouched clean messages as false-positive
  controls. ~2% of messages faulted; exact counts in the manifest.
- Replay: `tickflow replay --fixture <path>` produces the fixture through the real broker at
  max rate; the same command is the CI benchmark, the demo, and the determinism test.

## 6. Metrics & uncertainty (frozen — the TRUTH-ONLY core)

Graded against the injection manifest, per fault class:
- **Detection recall** — faulted messages quarantined with the correct rule label.
- **Detection precision** — quarantined messages that were actually faulted (mislabels and
  quarantined clean controls count against).
- **False-quarantine rate** on clean controls.
- **Completeness** — every input message accounted for exactly once across valid + quarantine
  (loss = 0 and dup-delivery = 0 asserted, not assumed).
- **Throughput** (msgs/s sustained through the gate) and **e2e latency** p50/p95/p99
  (ts_ingest → gate emit), `bench` profile only.
- **SLO table** — gates-ON vs gates-OFF violated-bar counts (§4).

**Uncertainty (frozen, same discipline as fraudscore/filinglens):** every proportion and every
latency percentile carries a **bootstrap 95% CI** — resample items (per class for P/R; message
latencies for percentiles) with replacement, B = 10,000, seed 42, percentile intervals;
B reduced via config in CI's fast lane, full B in the metrics job. Report format everywhere,
README included: `point [CI_low, CI_high]`. Small-n classes will have wide CIs; the report
says so plainly. Because rules are deterministic, recall on non-boundary faults should be
~100% — **the README says that too**, and points at the boundary classes and the
completeness-under-load numbers as the non-trivial results. No number is impressive-by-
construction and undisclosed; that's the brand.

Every metrics artifact records: commit SHA, fixture checksum, injection-manifest checksum,
compose profile, runner spec (`ubuntu-latest`, 4 vCPU/16 GB), and timestamps.

## 7. Reproducibility & the two-tier truth rule (frozen)

1. **System of record = GitHub Actions.** The metrics workflow: verify fixture checksum → start
   Redpanda (`bench`) in Docker on `ubuntu-latest` → replay faulted fixture → grade against
   manifest → bootstrap CIs → export telemetry JSON → deploy dashboard to Pages. Every
   published number is fork-and-rerun reproducible. Actions is free/unlimited on public repos;
   Docker Hub pulls from Actions are not rate-limited (verified).
2. **Live soak runs on the M4** (hours-long, real Coinbase+Kraken): reconnects, gaps, real
   divergence alerts, uptime/completeness telemetry. Published in a clearly separated
   dashboard/README section labeled **"measured on Apple M4 against live feeds — not
   independently reproducible."** Soak telemetry may be published; soak market data may not.

**Refresh honesty (frozen):** GitHub `schedule` is best-effort ("queued jobs may be dropped")
and auto-disables after 60 days of repo inactivity. The dashboard displays
**"last successful refresh: <timestamp>"** and never claims a guaranteed cadence.

## 8. Dashboard (GitHub Pages, static — telemetry only, release-blocking)

Single-page static site, committed by the metrics workflow: gate P/R table with CIs per fault
class, completeness, throughput/latency (with environment disclosure line), quarantine rate by
rule, gates-ON/OFF SLO comparison, live-soak section (labeled), last-refreshed timestamp.
**Mechanical enforcement (frozen):** the exporter writes only fields declared in
`telemetry_schema.json` (counts, rates, latencies, verdicts, timestamps — no price, open,
high, low, close, volume, vwap, or any market-data-derived value). A release-blocking test
fails the build if an exported artifact contains any undeclared field. This is a ToS
requirement (§1), not a style choice — README says why.

*Implementation (ADR-006):* the allowlist lives at `contracts/telemetry_schema.json`, beside
the other two contracts; §9's layout predates it. `export.assert_telemetry_only` runs two
independent passes and both raise `MarketDataLeak`. **Pass 1 (primary, fail-closed)** rejects
any field path the schema does not declare, plus type drift, missing declared fields, and
undeclared nulls; a missing or unreadable schema is fatal rather than permissive. **Pass 2
(secondary, fail-open)** keeps the `MARKET_FIELD_NAMES` exact-name blocklist, which still
catches a known market name if a future schema edit wrongly declares one. They are kept
separate because they fail differently; neither is derived from the other. The `per_class`
and `counts_by_invariant` key sets are enums, so a fifth fault class or an eighth invariant
requires a deliberate schema edit. ADR-006 records that §8 shipped as the blocklist alone from
Day D until 2026-07-21, what that let through, and the limits of what pass 1 does and does not
prove. No JS frameworks; plain HTML/CSS +
one committed JSON. (DuckDB-WASM is a deepening option, not v0.1.)

## 9. Repo scaffold & quality tooling (frozen)

```
tickflow/
├── src/tickflow/        ingest.py (both venues + normalize)  gate.py (rules engine + router)
│                        contracts.py (registry client, schema load)  quarantine.py (envelope,
│                        CLI ls/show/stats, replay)  bars.py (barbuilder + SLO checker)
│                        fixture.py (generator + fault injector + manifest)  replay.py
│                        metrics.py (grading + bootstrap CIs)  export.py (telemetry schema
│                        enforcement)  cli.py
├── contracts/           trades.v1.avsc  rules.yaml
├── fixtures.yaml        fixture + manifest checksums, seeds, N
├── tests/               unit + integration mirroring src; conftest builds mini-fixture
├── site/                dashboard template (HTML/CSS)
├── docker-compose.yaml  profiles: dev | bench
├── .github/workflows/   ci.yml (lint→type→test)  metrics.yml (replay→grade→publish, cron+manual)
├── docs/                architecture.md  decisions.md
├── pyproject.toml       uv; deps: confluent-kafka fastavro websockets duckdb pandas pyarrow
│                        pyyaml scipy; dev: pytest pytest-cov ruff mypy pre-commit
├── README.md CHANGELOG.md LICENSE(MIT) STATE.md .gitignore   (data/captures/ gitignored)
```

**ruff** (lint+format) · **mypy** on `src/` · **pytest + coverage ≥ 85%** on core logic (gate,
fixture/injector, metrics, export; excludes CLI/ingester glue) · **pre-commit** · CI on 3.12
with uv cache; Redpanda service container for integration lane; badge after first green.
Explicitly NOT in repo: `CLAUDE.md`, `.claude/`, assistant config. README leads with the
pipeline diagram and dashboard link (per plan §5 — structures must differ across the three repos).

## 10. Test plan

- **gate:** table-driven verdicts for every rule incl. boundary literals (5.0 s vs 5.001 s
  skew; LRU eviction at exactly 10,000; bounds edges); watermark clock determinism (same
  message sequence → same verdicts, twice); mislabel impossibility checks.
- **fixture/injector:** seeded determinism (same seed → same checksum); manifest counts match
  injected reality; clean-control preservation.
- **metrics:** rigged mini-manifest with hand-computed P/R; bootstrap seeded determinism, CI
  contains point estimate, rigged two-value percentile case (fraudscore tests reused as pattern).
- **completeness:** kill/restart the gate consumer mid-replay → all messages still accounted
  for exactly once (at-least-once + idempotent quarantine proven, not claimed).
- **barbuilder/SLO:** hand-built tick sequences → known bars; gates-ON bit-identity vs clean
  fixture; gates-OFF detects seeded corruption; DuckDB single-writer append pattern.
- **export:** telemetry-schema enforcement test — an artifact with a smuggled `price` field
  must fail the build (this test is the release gate of §8).
- **integration (CI):** mini-fixture (2,000 msgs) → replay → gate → bars → grade → export,
  end-to-end < 5 min against a real Redpanda container.
- **contract:** schema BACKWARD-compatibility check in CI (registry compatibility API).

## 11. Build order → commit arc (3 focused hrs/day; Opus executes, judgment frozen here)

**Day A (Fri Jul 17):** `feat: scaffold package, tooling, and CI skeleton` · `feat: add
compose stack with dev and bench profiles` · `feat: add exchange ingesters with normalization`
· `feat: add capture and sanity commands` → **stop at the gate: Rishik reviews feed sanity,
ADR-001 records pass/fail.** ⛔ Also the git checkpoint: repo name `tickflow`, public, full
file list presented for approval before first push.
**Day B (Sat Jul 18):** `feat: add trades.v1 contract with schema registry wiring` · `feat:
add declarative rules engine with quarantine routing` (+ gate unit tests) · `feat: add
synthetic fixture generator and fault injector` (+ determinism tests). ← signature commits
**Day C (Sun Jul 19):** `feat: add barbuilder with SLO checker and DuckDB sink` · `feat: add
gates-off demo mode with SLO comparison` · `feat: add fault-injection grading with bootstrap
CIs` · `feat: add quarantine inspection and replay CLI` · replay-determinism + completeness
tests green.
**Day D (Mon Jul 20):** `feat: add telemetry export with schema enforcement` · `feat: add
metrics workflow and Pages dashboard` · overnight-capable live soak run started early ·
`test: complete coverage to gate` · `docs: write README with measured results` (numbers + CIs
from the real metrics artifact) · **release grep gate** (employer refs + price-export scan
over README/docs/src/git log) · `chore: release v0.1.0`.
**Slip valve (per plan Day-9 decision):** if Day C overruns, v0.9 drops the quarantine-replay
CLI and the live-soak section — never the gate, the SLO experiment, or the CIs. A stated v0.9
with roadmap beats a rushed v0.1.

## 12. v0.1 scope vs deepening roadmap

**v0.1 ships:** 2 venues × 2 symbols, one contract (Avro + 6 frozen rules), inline gate with
quarantine envelope + inspect/replay CLI, barbuilder with SLO + gates-ON/OFF experiment,
seeded fault-injection grading with bootstrap CIs in CI (system of record), telemetry-only
Pages dashboard, live-soak telemetry (labeled). Honest v0.1 framing: "one contract, six rules,
every number reproducible from a checksummed fixture."
**Deepening (post-sprint, ordered):** process-level chaos (broker kill mid-replay, Toxiproxy
latency/partition — Jepsen's Redpanda analysis as the fault menu) → exactly-once transactions
→ datacontract-cli interop (contracts exportable to the open format) → Quix Streams processor
comparison → Iceberg sink → Alpaca IEX equities as a third source (market-hours/halt
semantics) → DuckDB-WASM dashboard → statistical/ML anomaly gate, benchmarked *against* the
declarative baseline (v0.3+ at the earliest; the ML story in v0.1 is the evaluation
methodology, deliberately not a model).

## 13. Limitations (frozen README section — all stated, none hidden)

(a) CI metrics are measured on a **synthetic fixture** — realistic in structure, not real
market data (a deliberate ToS + reproducibility decision, §1/§5); live-feed soak numbers are
real but labeled not independently reproducible. (b) Published latency comes from the `bench`
profile on a shared CI runner — indicative, not production-representative; `dev` profile
bypasses fsync per Redpanda's own docs and is never quoted. (c) Rules are deterministic
checks: recall is measured against the fault classes the rules target — no claim about
unknown-unknown anomalies (that's the v0.3 research question). (d) Single-node broker: no
replication, partition-failover, or multi-node claims. (e) At-least-once + idempotent sink,
not exactly-once transactions. (f) `double` for price/size, not decimal. (g) Dashboard shows
telemetry only — feed ToS prohibits republishing derived market data; this constrains the demo
and the README says so. (h) Dashboard refresh is best-effort (Actions cron semantics);
"last refreshed" timestamp shown instead of a cadence claim.

## 14. Acceptance criteria for v0.1.0

- Feed sanity gate ADR-001 recorded as PASS (or re-scoped by ADR before build — gate binding)
- Metrics artifact committed from the Actions system-of-record run: every P/R, completeness,
  and latency number carries a 95% CI + full provenance (SHA, checksums, profile, runner)
- Gates-ON/OFF SLO comparison table in README and dashboard; gates-ON bars bit-identical to
  clean-fixture bars in CI
- Telemetry-schema enforcement test green; no market-data-derived value in any published artifact
- CI green: ruff + mypy + pytest ≥ 85% core coverage; mini-fixture E2E < 5 min; schema
  BACKWARD-compat check passing
- README: three verified gap citations (Confluent clause, Grab, DEW) load-bearing; incident
  references hedged as motivation; limitations section complete incl. fsync + cron caveats
- `git log --format=full`: sole author, no co-author trailers; no AI-tooling files in repo
- Boundary rule verified: release-blocking grep over README, docs/, src/, site/, and git log —
  zero employer/internship/private-work references
- Checkpoint rule satisfied at repo creation + first push

## 15. Checkpoint reminder (standing rule)

Before the first push: present repo name (`tickflow`), visibility (**public**), and the full
committed file list for approval — confirming no AI-tooling artifacts and no captured market
data. After that one approval, routine commits proceed without re-asking. Never change any
GitHub setting to work around a problem — surface it instead.
