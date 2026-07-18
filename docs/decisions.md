# Decisions (ADR log)

Deviations from the frozen spec ([architecture.md](architecture.md)) and gate results are
recorded here. Simplest defensible choice wins.

## ADR-001 — Feed sanity gate (§0)

**Status: PENDING.** The blocking manual feed-sanity gate has not run yet. Before any gate
logic, `tickflow capture --minutes 5` connects to both live websockets, normalizes to the
common schema, and writes a local capture; `tickflow sanity` then prints per-stream message
counts, field-mapping samples, timestamp sanity, and trade_id stats for the 2 exchanges ×
2 symbols. Rishik reviews manually. Gate: both feeds connect keylessly, all 4 streams produce
≥ 50 messages in 5 min, field mapping verified correct → proceed to Day B. Any feed failing →
STOP and re-scope (Kraken-only, or a checked alternate) by ADR first. Result recorded here.
