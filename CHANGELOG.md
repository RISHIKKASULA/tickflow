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
