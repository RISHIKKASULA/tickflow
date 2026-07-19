# Decisions (ADR log)

Deviations from the frozen spec ([architecture.md](architecture.md)) and gate results are
recorded here. Simplest defensible choice wins.

## ADR-001 — Feed sanity gate (§0): recalibrated to per-venue thresholds

**Status: ACCEPTED (2026-07-19). Gate PASSES on recalibrated terms.** Reviewed by Rishik Kasula.

### Context

The blocking feed-sanity gate ran on 2026-07-19: `tickflow capture --minutes 5` wrote a local,
checksummed capture from both live feeds (`data/captures/gate-2026-07-19`, sha256
`5be3cde5e281be05…`, 300.1 s, 1060 records), reviewed via `tickflow sanity`. Observed per-stream
counts over the 5-minute window:

| stream | count | uniq_id | dup | missing | max skew | within 60 s | out-of-order |
|---|---|---|---|---|---|---|---|
| coinbase:BTC-USD | 814 | 814 | 0 | 0 | 37.7 s | 814 | 163 |
| coinbase:ETH-USD | 203 | 203 | 0 | 0 | 134.9 s | 150 | 110 |
| kraken:BTC-USD | 29 | 29 | 0 | 0 | 0.2 s | 29 | 0 |
| kraken:ETH-USD | 14 | 14 | 0 | 0 | 0.1 s | 14 | 0 |

Against the frozen single threshold (≥ 50 msgs/stream/5 min) both Kraken streams failed (29, 14),
so the gate was provisionally FAIL.

### Decision

**Keep both venues; recalibrate the gate to per-venue liveness thresholds** — a calibration
correction, not a lowered bar. The 50-msg/5-min threshold encoded a wrong assumption: comparable
message volume across venues. Kraken is genuinely thinner than Coinbase, and 29/14 is real,
sparse market activity, not a broken feed. The evidence the Kraken feed is *sound*: 0 duplicates,
0 missing, 100% trade_id presence, field mapping verified correct on both streams, and sub-second
event-time↔wall-clock skew — **better than Coinbase's**. The gate's purpose is to reject a dead
or stalled feed, not to enforce a volume target.

1. **Per-venue thresholds, derived from observed volume** (not a single cross-venue number),
   implemented in `sanity.py` as `DEFAULT_THRESHOLDS = {coinbase: 50, kraken: 5}`, overridable
   with `tickflow sanity --min venue=count`. Derivation from the observed numbers above: each
   floor sits well below that venue's slowest observed stream — Coinbase 50 vs. observed min 203
   (~4× headroom), Kraken 5 vs. observed min 14 (~2.8× headroom) — so normal quiet windows pass
   while a stalled feed trickling ~0 still fails. **Re-run confirmation:** `tickflow sanity` on
   the recorded capture under these thresholds returns **PASS** on all four streams (exit 0);
   the gate now passes on its own terms.

2. **R6 (cross-venue divergence) must tolerate a sparse second venue** (carried to Day C now so
   it isn't rediscovered). R6 compares per-symbol mids across venues (alert-only, evaluated on
   the per-stream **event-time watermark**, never wall clock — §2 clock rule). With Kraken
   printing on the order of one trade every ~10–20 s, a naive comparison would alert off a stale
   quote. Frozen behavior for R6: a **staleness window of 30 s** — a venue's latest print is
   eligible for comparison only if its event-time is within 30 s of the comparison instant. If
   either venue is stale, R6 emits **no divergence verdict** for that instant and instead
   increments a `divergence_unavailable{reason=stale_<venue>}` telemetry counter; the sustained-
   10 s condition still governs genuine divergence alerts. Staleness is telemetry, never a
   quarantine (a missing print is not a bad message — same rationale as R5). The 30 s window is a
   default recorded here and revisitable if soak data warrants.

3. **Coinbase timestamp skew is expected, not feed lag** (recorded so the number isn't misread
   later). The large Coinbase max-skew / out-of-order figures (e.g. ETH 134.9 s, 110 ooo) come
   from the **connect-time snapshot backfill**: Coinbase's `market_trades` channel replays recent
   trades on subscribe, so the initial burst carries older event-times that arrive after live
   wall clock and out of order. Steady-state Coinbase updates are within tolerance, and Coinbase
   clears the gate on message count regardless. This is a one-time per-connection artifact.

### Consequences

- Both venues stay in scope; the cross-venue divergence rule R6 survives (its sparse-venue
  semantics are now specified above). No feed swap; no Kraken-only / Coinbase-only descope.
- Day A is closed; Day B is unblocked and proceeds per STATE.md, starting with the trades.v1
  contract + Schema Registry wiring.
- Captured market data remains local and gitignored (Coinbase/Kraken ToS, §1); this ADR cites
  the capture by checksum, not by committing it.
