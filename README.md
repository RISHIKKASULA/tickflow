# tickflow

An open-source **quality-gate and contract-enforcement layer for Kafka-compatible streams**.
Declarative per-topic contracts (Avro schema + semantic rules) are enforced **inline**;
violations are routed to a quarantine topic; a downstream consumer is protected with a
measurable SLO. Every quality claim is measured by **seeded fault-injection replay in CI**,
reported with 95% confidence intervals.

```
coinbase-ingester ─┐                        ┌─→ trades.valid ──→ barbuilder → DuckDB (+SLO)
                   ├─→ trades.raw ─→ GATE ──┤
kraken-ingester ───┘                        └─→ trades.quarantine (rule_id, detail, offset, …)
```

> **Status: under construction.** This repository is being built to a frozen design
> ([docs/architecture.md](docs/architecture.md)) over a short sprint. Measured results,
> confidence intervals, and the live dashboard land at the v0.1.0 release. Until then, treat
> unbuilt sections as roadmap. Current progress is tracked in [STATE.md](STATE.md).

## Why this exists

The streaming *pipeline* is a solved problem; the *enforced quality gate* in front of it is
not. Inline stream-quality rules sit behind enterprise tiers (Confluent's docs state schema
rules are "only available on Confluent Enterprise, not on the Community edition"), and SQL
engines like Flink SQL and ksqlDB don't execute such rules at all. Teams that need field-level
semantic enforcement have built it in-house (Grab, Nov 2025: field-level semantic rules across
100+ Kafka topics with violations routed to a dedicated topic). Data contracts, meanwhile, have
largely remained *descriptive artifacts rather than interfaces with failure semantics* (Data
Engineering Weekly, Jan 2026). tickflow is that missing gate, built as a contract with failure
semantics.

## Honest framing

**Market data is the demo domain, not the product.** It is free, 24/7, and naturally hostile —
duplicates, gaps, out-of-order delivery, and cross-venue divergence. The product is the gate.
If the gates were deleted, the remaining pipeline would be worthless by design. Cross-exchange
crypto trades (Coinbase + Kraken) are the demonstration feed precisely because they exercise
every rule.

**Data-handling boundary (a ToS requirement, not a style choice):** the exchanges' market-data
terms prohibit redistributing the data or derived works. Therefore **no captured market data is
ever committed to this repo**, and **the public dashboard shows pipeline telemetry only** —
counts, rates, latencies, and verdicts, never prices, bars, volumes, or anything derived from
market-data values. This is enforced mechanically and by a release-blocking check.

## The contract

One contract, six declarative rules ([contracts/](contracts/), Day B):

| ID | Rule | On violation |
|----|------|--------------|
| R1 | schema (Avro decode) | quarantine `malformed` |
| R2 | range (per-symbol static bounds; price/size > 0) | quarantine `out-of-range` |
| R3 | duplicate (LRU window per stream) | quarantine `duplicate` |
| R4 | out-of-order (event-time watermark tolerance) | quarantine `out-of-order` |
| R5 | gap (watermark jump on an active stream) | **alert only** |
| R6 | divergence (cross-venue mid deviation) | **alert only** |

Rules evaluate against the per-stream **event-time watermark**, never wall clock — so the same
messages in produce the same verdicts out, live or in replay. That determinism is what makes
the CI measurements meaningful.

## Development

```bash
uv sync                 # install (Python 3.12)
uv run ruff check .     # lint
uv run mypy             # type-check (strict, on src/)
uv run pytest           # tests + coverage
docker compose --profile dev up -d    # single-node Redpanda for local work
```

`dev` and `bench` compose profiles are documented in
[docs/architecture.md](docs/architecture.md) §3. **Numbers are only ever published from the
`bench` profile;** the `dev` profile bypasses fsync per Redpanda's own docs and its numbers are
never quoted.

## License

[MIT](LICENSE).
