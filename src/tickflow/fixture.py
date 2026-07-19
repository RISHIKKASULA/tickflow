"""The ground-truth machine: a seeded synthetic fixture generator and a fault injector with an
injection manifest (frozen design §5).

Two halves, both coverage-gated core (§9):

- **Clean fixture generator.** Seeded (42), fully deterministic, 4 streams x 25,000 = 100,000
  clean `trades.v1` messages — per-stream mean-reverting random-walk prices with realistic
  arrival gaps and venue-consistent sequential trade_ids. The generator uses only integer
  arithmetic and a seeded `random.Random`, never the wall clock and never a transcendental
  float op, so the byte content is reproducible across machines. The fixture is written as
  zstd parquet and its **content digest is pinned in `fixtures.yaml`** — CI verifies it before
  every metrics run. This is CI's system of record: every published number is reproducible from
  this checksummed fixture.

- **Fault injector.** Seeded (42), applied to the clean fixture at grading time (never committed
  separately). It rewrites ~2% of messages into the four fault classes the quarantine rules
  target and emits an **injection manifest** — the exact index, class, and *expected gate
  behavior* of every fault and control. The manifest is the ground truth Day C grades detection
  precision/recall against.

**Why boundary faults, not just clear-cut ones (the point of this module).** The gate's rules
are deterministic, so recall on obvious faults (a negative price, a corrupt frame) is ~100% by
construction — an uninformative number. The injector therefore plants faults *straddling* every
threshold, paired with controls that sit just on the safe side:

- `out-of-range` — absurd/neg/zero values (faults) **and** values exactly on the inclusive bound
  (controls that must NOT quarantine).
- `out-of-order` — 5.1 s skew (fault, quarantined) vs 4.9 s and exactly 5.0 s skew (controls,
  valid): the tolerance is 5.0 s, evaluated on the per-stream event-time watermark.
- `duplicate` — immediate dups (caught) **and** dups re-injected just past the 10,000-key LRU
  window (designed misses the gate cannot catch), including the exact window edge: a dup at
  `lru_size - 1` distinct keys back (still caught) vs `lru_size` back (evicted, missed).
- untouched clean messages as false-positive controls.

Because the injector declares the *expected* verdict for every entry and the gate is the real
`RulesEngine`, the two are cross-checked in tests: every fault its rule should catch is caught,
every control and every designed-miss routes valid. Detection is genuinely imperfect at the
edges, which is what makes the Day C numbers mean something.

R5 (gap) and R6 (divergence) are alert-only and are not graded for precision/recall (§6), so they
are not fault classes here; the clean generator simply keeps arrival gaps under the 60 s gap
threshold and the venues' prices tightly coupled so the clean fixture raises neither alert.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from tickflow import contracts
from tickflow.gate import (
    DISPOSITION_QUARANTINE,
    DISPOSITION_VALID,
    RulesConfig,
)

SEED = 42
N_PER_STREAM = 25_000
FIXTURE_SCHEMA_ID = 1  # a fixed wire schema id; the gate decodes with the schema, not the id

# The four frozen streams (2 venues x 2 symbols), in canonical sorted order — this order defines
# the fixture's row order and therefore its content digest.
STREAMS: tuple[tuple[str, str], ...] = (
    ("coinbase", "BTC-USD"),
    ("coinbase", "ETH-USD"),
    ("kraken", "BTC-USD"),
    ("kraken", "ETH-USD"),
)

# A fixed epoch-millis anchor (2025-01-01T00:00:00Z). Nothing here reads the wall clock.
BASE_TS_MS = 1_735_689_600_000

# Per-symbol price anchors, in micro-dollars (integer arithmetic → cross-platform determinism).
_ANCHOR_MICROS: dict[str, int] = {"BTC-USD": 64_000_000_000, "ETH-USD": 3_400_000_000}
_PRICE_STEP_MICROS: dict[str, int] = {"BTC-USD": 15_000_000, "ETH-USD": 800_000}  # ~$15 / ~$0.80
_REVERSION_DIVISOR = 4_000  # pull toward the anchor each step — keeps prices tightly in-band

_SIZE_ANCHOR_MICROS = 500_000  # 0.5 units
_SIZE_STEP_MICROS = 50_000
_SIZE_MIN_MICROS = 1

# Per-venue inter-arrival gap bounds in ms. Kraken is genuinely thinner (ADR-001); both stay well
# under the 60 s R5 gap threshold so the clean fixture raises no gap alert.
_GAP_MS: dict[str, tuple[int, int]] = {"coinbase": (200, 4_000), "kraken": (2_000, 30_000)}

FIXTURES_YAML = Path(__file__).resolve().parents[2] / "fixtures.yaml"
FIXTURE_PARQUET = Path(__file__).resolve().parents[2] / "fixtures" / "trades.v1.clean.parquet"

_FIELD_ORDER = (
    "exchange",
    "symbol",
    "trade_id",
    "price",
    "size",
    "side",
    "ts_event",
    "ts_ingest",
)

Record = dict[str, Any]


# --------------------------------------------------------------------------------------------
# Clean fixture generator — seeded, integer-driven, deterministic.
# --------------------------------------------------------------------------------------------
def _stream_seed(seed: int, stream_index: int) -> int:
    # A stable integer per-stream seed (no hashing of tuples — hash() is salted per process).
    return seed * 10_000 + stream_index


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def generate_stream(
    exchange: str, symbol: str, n: int, stream_index: int, seed: int = SEED
) -> list[Record]:
    """One stream of `n` clean trades.v1 records — mean-reverting walk, monotone event-time.

    Deterministic given (seed, stream_index): integer price/size walks, integer arrival gaps, and
    strictly increasing ts_event so the clean stream trips none of R1-R4.
    """
    rng = random.Random(_stream_seed(seed, stream_index))
    anchor = _ANCHOR_MICROS[symbol]
    step = _PRICE_STEP_MICROS[symbol]
    lo = int(anchor * 0.5)  # soft band, comfortably inside the R2 bounds for both symbols
    hi = int(anchor * 1.5)
    price_micros = anchor
    size_micros = _SIZE_ANCHOR_MICROS
    ts_event = BASE_TS_MS
    gap_lo, gap_hi = _GAP_MS[exchange]
    id_base = (stream_index + 1) * 1_000_000_000  # venue-consistent monotone integer ids

    records: list[Record] = []
    for i in range(n):
        # Mean-reverting price walk, then size walk — both integer, both kept strictly positive.
        price_micros += rng.randint(-step, step) + (anchor - price_micros) // _REVERSION_DIVISOR
        price_micros = _clamp(price_micros, lo, hi)
        size_micros = max(
            _SIZE_MIN_MICROS, size_micros + rng.randint(-_SIZE_STEP_MICROS, _SIZE_STEP_MICROS)
        )
        if i > 0:
            ts_event += rng.randint(gap_lo, gap_hi)
        records.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "trade_id": str(id_base + i),
                "price": round(price_micros / 1_000_000, 6),
                "size": round(size_micros / 1_000_000, 6),
                "side": "buy" if rng.random() < 0.5 else "sell",
                "ts_event": ts_event,
                "ts_ingest": ts_event + rng.randint(1, 500),
            }
        )
    return records


def generate_clean(
    n_per_stream: int = N_PER_STREAM, seed: int = SEED
) -> dict[tuple[str, str], list[Record]]:
    """All four streams as clean records, keyed by (exchange, symbol)."""
    return {
        (exchange, symbol): generate_stream(exchange, symbol, n_per_stream, index, seed)
        for index, (exchange, symbol) in enumerate(STREAMS)
    }


def flatten(streams: dict[tuple[str, str], list[Record]]) -> list[Record]:
    """The streams concatenated in canonical STREAMS order — the fixture's row order."""
    flat: list[Record] = []
    for key in STREAMS:
        flat.extend(streams.get(key, []))
    return flat


def content_digest(records: list[Record]) -> str:
    """A platform-independent SHA-256 over the record content (the fixture's true identity).

    Hashes the canonical JSON of each record in row order — not the parquet container bytes, which
    can vary by writer version — so regeneration determinism holds across machines.
    """
    blob = "\n".join(
        json.dumps({k: r[k] for k in _FIELD_ORDER}, separators=(",", ":"), sort_keys=True)
        for r in records
    )
    return hashlib.sha256(blob.encode()).hexdigest()


# --------------------------------------------------------------------------------------------
# Fault injector — the four quarantine-rule fault classes, with boundary controls.
# --------------------------------------------------------------------------------------------
# Fault-class labels (map 1:1 to the quarantine rules R1-R4).
MALFORMED = "malformed"  # -> R1
OUT_OF_RANGE = "out-of-range"  # -> R2
DUPLICATE = "duplicate"  # -> R3
OUT_OF_ORDER = "out-of-order"  # -> R4

_RULE_OF_CLASS = {MALFORMED: "R1", OUT_OF_RANGE: "R2", DUPLICATE: "R3", OUT_OF_ORDER: "R4"}


@dataclass(frozen=True, slots=True)
class FaultEntry:
    """Ground truth for one injected frame: where it is, what it is, and what the gate should do."""

    index: int  # position in the faulted frame stream
    stream: str  # "exchange:symbol"
    fault_class: str
    is_fault: bool  # ground truth: is this message actually bad?
    detectable: bool  # can the gate catch it? (False = designed miss, e.g. dup past the window)
    expected_disposition: str  # what a correct gate does: "valid" | "quarantine"
    expected_rule: str | None  # the rule that should catch it, or None
    boundary: str | None  # boundary label, e.g. "5.1s", "window_edge_beyond", or None
    note: str = ""


@dataclass
class InjectionManifest:
    """The full ground truth for a faulted stream: every fault/control entry plus rollup counts."""

    seed: int
    n_total: int  # length of the faulted frame stream
    lru_size: int  # the dedup window the beyond-window misses were sized against
    entries: list[FaultEntry] = field(default_factory=list)

    @property
    def n_clean_controls(self) -> int:
        """Untouched clean messages — every frame with no manifest entry (§5 FP controls)."""
        return self.n_total - len(self.entries)

    def counts_by_class(self) -> dict[str, dict[str, int]]:
        counts: dict[str, dict[str, int]] = {}
        for entry in self.entries:
            bucket = counts.setdefault(
                entry.fault_class, {"faults": 0, "detectable": 0, "designed_miss": 0, "controls": 0}
            )
            if entry.is_fault:
                bucket["faults"] += 1
                bucket["detectable" if entry.detectable else "designed_miss"] += 1
            else:
                bucket["controls"] += 1
        return counts

    def fault_rate(self) -> float:
        faults = sum(1 for e in self.entries if e.is_fault)
        return faults / self.n_total if self.n_total else 0.0

    def by_index(self) -> dict[int, FaultEntry]:
        return {e.index: e for e in self.entries}

    def to_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "n_total": self.n_total,
            "lru_size": self.lru_size,
            "n_clean_controls": self.n_clean_controls,
            "fault_rate": round(self.fault_rate(), 6),
            "counts_by_class": self.counts_by_class(),
            "entries": [asdict(e) for e in self.entries],
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True).encode()

    def digest(self) -> str:
        return hashlib.sha256(self.to_json()).hexdigest()


@dataclass(frozen=True, slots=True)
class InjectionResult:
    """A faulted frame stream plus its manifest — the injector's whole output."""

    frames: list[bytes]
    manifest: InjectionManifest


def _corrupt_frame(good: bytes, rng: random.Random) -> bytes:
    """Turn a valid wire frame into a malformed one the codec cannot decode (R1)."""
    kind = rng.randint(0, 2)
    if kind == 0:  # flip the Confluent magic byte
        return bytes([0x01]) + good[1:]
    if kind == 1:  # truncate into the header/body
        return good[: rng.randint(1, 4)]
    # garble the body after the 5-byte header
    return good[:5] + bytes(rng.randint(0, 255) for _ in range(6))


def _out_of_range(record: Record, kind: int) -> tuple[Record, str]:
    """A record whose price/size violates R2. Returns (record, boundary-label)."""
    bad = dict(record)
    if kind == 0:
        bad["price"] = -abs(record["price"])
        return bad, "negative_price"
    if kind == 1:
        bad["price"] = 0.0
        return bad, "zero_price"
    if kind == 2:
        bad["size"] = 0.0
        return bad, "zero_size"
    if kind == 3:
        bad["price"] = record["price"] * 1_000_000.0  # absurdly high, above the symbol ceiling
        return bad, "absurd_high_price"
    bad["price"] = record["price"] / 1_000_000.0  # absurdly low, below the symbol floor
    return bad, "absurd_low_price"


def _in_range_control(record: Record, bounds: tuple[float, float], kind: int) -> tuple[Record, str]:
    """A record sitting exactly on / just inside the inclusive R2 bound — must stay valid."""
    lo, hi = bounds
    ok = dict(record)
    if kind == 0:
        ok["price"] = lo  # exactly the inclusive lower bound
        return ok, "at_lower_bound"
    ok["price"] = hi  # exactly the inclusive upper bound
    return ok, "at_upper_bound"


def _skewed(record: Record, watermark: int, offset_ms: int) -> Record:
    """A record whose ts_event is `offset_ms` below the current stream watermark (R4 probe)."""
    skewed = dict(record)
    skewed["ts_event"] = watermark - offset_ms
    return skewed


def _duplicate(original: Record, watermark: int) -> Record:
    """A re-delivery of `original`'s trade_id carrying a fresh (current) event-time.

    A duplicate is graded against R3, so it must isolate R3: it reuses the original's (in-range)
    price and trade_id but stamps the current watermark as ts_event, so it is neither out-of-range
    (R2) nor out-of-order (R4). Whether it is caught then depends only on whether the key is still
    resident in the dedup window — which is exactly what the duplicate boundary probes. (Carrying
    the original's old ts_event would let R4 quarantine a beyond-window dup, masking the designed
    miss the spec calls for.)
    """
    dup = dict(original)
    dup["ts_event"] = watermark
    dup["ts_ingest"] = watermark + 1
    return dup


def _sample_disjoint(pool: list[int], counts: list[int], rng: random.Random) -> list[list[int]]:
    """Deal disjoint index groups of the given sizes from a shuffled copy of `pool`."""
    shuffled = list(pool)
    rng.shuffle(shuffled)
    groups: list[list[int]] = []
    start = 0
    for count in counts:
        groups.append(sorted(shuffled[start : start + count]))
        start += count
    return groups


def inject_faults(
    clean: dict[tuple[str, str], list[Record]],
    config: RulesConfig,
    schema: Any,
    seed: int = SEED,
    fault_rate: float = 0.02,
) -> InjectionResult:
    """Rewrite ~`fault_rate` of the clean fixture into the four fault classes + boundary controls.

    Deterministic given (seed, config, clean input). Returns the faulted frame stream (Avro wire
    bytes, some deliberately corrupted) and the injection manifest declaring the expected gate
    verdict for every fault and control. Streams are processed in canonical order and concatenated;
    R1-R4 are per-stream, so this concatenation yields exactly the verdicts the gate produces.
    """
    lru = config.lru_size
    frames: list[bytes] = []
    entries: list[FaultEntry] = []

    # The first stream long enough hosts the exact dedup-window-edge pair; others host the
    # "clearly beyond" designed-miss dups. Every stream gets malformed/range/order/immediate-dup.
    designated = next((i for i, k in enumerate(STREAMS) if len(clean.get(k, [])) > lru + 3), None)

    for stream_index, key in enumerate(STREAMS):
        records = clean.get(key, [])
        rng = random.Random(_stream_seed(seed, stream_index) ^ 0x5EED)
        _inject_stream(
            records=records,
            stream_key=key,
            is_designated=(stream_index == designated),
            config=config,
            schema=schema,
            rng=rng,
            fault_rate=fault_rate,
            frames=frames,
            entries=entries,
        )

    manifest = InjectionManifest(seed=seed, n_total=len(frames), lru_size=lru, entries=entries)
    return InjectionResult(frames=frames, manifest=manifest)


def _inject_stream(
    records: list[Record],
    stream_key: tuple[str, str],
    is_designated: bool,
    config: RulesConfig,
    schema: Any,
    rng: random.Random,
    fault_rate: float,
    frames: list[bytes],
    entries: list[FaultEntry],
) -> None:
    """Emit one stream's faulted frames (left-to-right) and append its manifest entries.

    The single left-to-right pass tracks the event-time watermark exactly as the gate will, so
    out-of-order skews are computed against the same clock, and the dedup-window arithmetic (which
    key is still resident when a duplicate arrives) is counted precisely.
    """
    exchange, symbol = stream_key
    stream_label = f"{exchange}:{symbol}"
    length = len(records)
    bounds = config.symbol_bounds[symbol]
    lru = config.lru_size

    def encode(record: Record) -> bytes:
        return contracts.encode(record, schema, FIXTURE_SCHEMA_ID)

    # ---- Reserve the exact dedup-window edge zone on the designated stream ------------------
    # A at z0 gets a duplicate re-injected `lru-1` distinct keys later (still resident -> caught);
    # B at z0+1 gets one `lru` distinct keys later (just evicted -> designed miss). The whole zone
    # is kept clean so the distinct-key count between original and duplicate is exact.
    zone: set[int] = set()
    z0 = 1
    caught_after = missed_after = -1
    if is_designated and length > lru + 3:
        zone = set(range(z0, z0 + lru + 3))
        caught_after = z0 + (lru - 1)  # A's dup emitted right after this clean index
        missed_after = (z0 + 1) + lru  # B's dup emitted right after this clean index

    # ---- Choose "clearly beyond" designed-miss dup anchors (non-designated streams) ---------
    beyond_anchors: dict[int, int] = {}  # emission index -> original index
    if not is_designated:
        margin = max(2, lru // 5)
        n_beyond = max(1, round(fault_rate * 0.05 * length))
        anchor_lo = lru + margin + 1
        if anchor_lo < length:
            candidates = [i for i in range(anchor_lo, length) if i not in zone]
            rng.shuffle(candidates)
            for a in sorted(candidates[:n_beyond]):
                beyond_anchors[a] = a - lru - margin

    beyond_originals = set(beyond_anchors.values())

    # ---- Deal disjoint index sets for the replace-faults and immediate-dup anchors ----------
    reserved = zone | set(beyond_anchors) | beyond_originals | {0}
    pool = [i for i in range(1, length) if i not in reserved]
    n = length

    def budget(fraction: float) -> int:
        return min(len(pool), max(1, round(fault_rate * fraction * n)))

    counts = [
        budget(0.20),  # malformed
        budget(0.20),  # out-of-range faults
        budget(0.10),  # out-of-range controls (on the inclusive bound)
        budget(0.20),  # out-of-order faults (5.1 s)
        budget(0.10),  # out-of-order controls (4.9 s)
        budget(0.03),  # out-of-order controls (exactly 5.0 s)
        budget(0.20),  # immediate duplicates
    ]
    groups = _sample_disjoint(pool, counts, rng)
    mal_idx, oor_idx, oorc_idx, ooo_idx, oooc_idx, oooe_idx, dup_idx = (set(g) for g in groups)

    tolerance = config.out_of_order_tolerance_ms
    watermark = 0

    for i in range(length):
        clean_record = records[i]
        # Snapshot the clean, valid version for any duplicate re-injection of this index.
        emit_index = len(frames)

        if i in mal_idx:
            frames.append(_corrupt_frame(encode(clean_record), rng))
            entries.append(
                FaultEntry(
                    emit_index,
                    stream_label,
                    MALFORMED,
                    True,
                    True,
                    DISPOSITION_QUARANTINE,
                    "R1",
                    "corrupt_avro",
                )
            )
        elif i in oor_idx:
            bad, label = _out_of_range(clean_record, rng.randint(0, 4))
            frames.append(encode(bad))
            entries.append(
                FaultEntry(
                    emit_index,
                    stream_label,
                    OUT_OF_RANGE,
                    True,
                    True,
                    DISPOSITION_QUARANTINE,
                    "R2",
                    label,
                )
            )
        elif i in oorc_idx:
            ok, label = _in_range_control(clean_record, bounds, rng.randint(0, 1))
            frames.append(encode(ok))
            entries.append(
                FaultEntry(
                    emit_index,
                    stream_label,
                    OUT_OF_RANGE,
                    False,
                    True,
                    DISPOSITION_VALID,
                    None,
                    label,
                    "just-inside control",
                )
            )
            watermark = max(watermark, int(ok["ts_event"]))
        elif i in ooo_idx:
            bad = _skewed(clean_record, watermark, tolerance + 100)  # 5.1 s late
            frames.append(encode(bad))
            entries.append(
                FaultEntry(
                    emit_index,
                    stream_label,
                    OUT_OF_ORDER,
                    True,
                    True,
                    DISPOSITION_QUARANTINE,
                    "R4",
                    "5.1s",
                )
            )
        elif i in oooc_idx:
            ok = _skewed(clean_record, watermark, tolerance - 100)  # 4.9 s late — inside tolerance
            frames.append(encode(ok))
            entries.append(
                FaultEntry(
                    emit_index,
                    stream_label,
                    OUT_OF_ORDER,
                    False,
                    True,
                    DISPOSITION_VALID,
                    None,
                    "4.9s",
                    "just-inside control",
                )
            )
        elif i in oooe_idx:
            ok = _skewed(clean_record, watermark, tolerance)  # exactly 5.0 s — the inclusive edge
            frames.append(encode(ok))
            entries.append(
                FaultEntry(
                    emit_index,
                    stream_label,
                    OUT_OF_ORDER,
                    False,
                    True,
                    DISPOSITION_VALID,
                    None,
                    "5.0s_exact",
                    "boundary control",
                )
            )
        else:
            frames.append(encode(clean_record))
            watermark = max(watermark, int(clean_record["ts_event"]))

        # Immediate duplicate: re-emit this key right after it (still resident -> caught by R3).
        if i in dup_idx:
            dup_index = len(frames)
            frames.append(encode(_duplicate(clean_record, watermark)))
            entries.append(
                FaultEntry(
                    dup_index,
                    stream_label,
                    DUPLICATE,
                    True,
                    True,
                    DISPOSITION_QUARANTINE,
                    "R3",
                    "immediate",
                )
            )

        # Clearly-beyond designed miss: re-deliver a long-evicted key (routes valid).
        if i in beyond_anchors:
            original = records[beyond_anchors[i]]
            miss_index = len(frames)
            frames.append(encode(_duplicate(original, watermark)))
            entries.append(
                FaultEntry(
                    miss_index,
                    stream_label,
                    DUPLICATE,
                    True,
                    False,
                    DISPOSITION_VALID,
                    None,
                    "beyond_window",
                    "designed miss",
                )
            )

        # Exact dedup-window edge pair on the designated stream.
        if is_designated and i == caught_after:
            edge_index = len(frames)
            frames.append(encode(_duplicate(records[z0], watermark)))  # lru-1 back -> resident
            entries.append(
                FaultEntry(
                    edge_index,
                    stream_label,
                    DUPLICATE,
                    True,
                    True,
                    DISPOSITION_QUARANTINE,
                    "R3",
                    "window_edge_inside",
                    "dup at lru-1 distinct keys back",
                )
            )
        if is_designated and i == missed_after:
            edge_index = len(frames)
            frames.append(encode(_duplicate(records[z0 + 1], watermark)))  # lru back -> evicted
            entries.append(
                FaultEntry(
                    edge_index,
                    stream_label,
                    DUPLICATE,
                    True,
                    False,
                    DISPOSITION_VALID,
                    None,
                    "window_edge_beyond",
                    "dup at lru distinct keys back — just evicted",
                )
            )


# --------------------------------------------------------------------------------------------
# Parquet + fixtures.yaml I/O (storage; the content digest above is the authority).
# --------------------------------------------------------------------------------------------
def write_parquet(records: list[Record], path: Path) -> None:
    """Write the flat record list as a single zstd parquet file (deterministic row order)."""
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records, columns=list(_FIELD_ORDER))
    frame = frame.astype({"trade_id": "string", "ts_event": "int64", "ts_ingest": "int64"})
    frame.to_parquet(path, engine="pyarrow", compression="zstd", index=False)


def read_parquet(path: Path) -> list[Record]:
    """Read a fixture parquet back to a flat record list (row order preserved)."""
    import pandas as pd

    frame = pd.read_parquet(path)  # engine="auto" resolves to pyarrow (the installed backend)
    records: list[Record] = []
    for row in frame.to_dict(orient="records"):
        records.append(
            {
                "exchange": str(row["exchange"]),
                "symbol": str(row["symbol"]),
                "trade_id": str(row["trade_id"]),
                "price": float(row["price"]),
                "size": float(row["size"]),
                "side": str(row["side"]),
                "ts_event": int(row["ts_event"]),
                "ts_ingest": int(row["ts_ingest"]),
            }
        )
    return records


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_fixtures_manifest(records: list[Record], parquet_path: Path) -> dict[str, Any]:
    """The `fixtures.yaml` contents: seeds, N, and the pins CI verifies before a metrics run."""
    root = FIXTURES_YAML.parent
    parquet_ref = (
        str(parquet_path.relative_to(root))
        if parquet_path.is_relative_to(root)
        else parquet_path.name
    )
    return {
        "version": 1,
        "seed": SEED,
        "n_per_stream": N_PER_STREAM,
        "n_total": len(records),
        "streams": [f"{ex}:{sym}" for ex, sym in STREAMS],
        "parquet": parquet_ref,
        "content_sha256": content_digest(records),
        "parquet_sha256": sha256_file(parquet_path),
    }


def load_fixtures_manifest(path: Path = FIXTURES_YAML) -> dict[str, Any]:
    import yaml

    parsed: dict[str, Any] = yaml.safe_load(path.read_text())
    return parsed


def verify_fixture(
    fixtures_yaml: Path = FIXTURES_YAML, parquet_path: Path = FIXTURE_PARQUET
) -> tuple[bool, str]:
    """Check the committed fixture against its pins: parquet bytes and decoded content digest."""
    manifest = load_fixtures_manifest(fixtures_yaml)
    file_ok = sha256_file(parquet_path) == manifest.get("parquet_sha256")
    content_ok = content_digest(read_parquet(parquet_path)) == manifest.get("content_sha256")
    if file_ok and content_ok:
        return True, "fixture matches pinned parquet_sha256 and content_sha256"
    parts = []
    if not file_ok:
        parts.append("parquet_sha256 mismatch")
    if not content_ok:
        parts.append("content_sha256 mismatch")
    return False, "; ".join(parts)


# --------------------------------------------------------------------------------------------
# CLI — generate/verify the committed fixture (glue over file I/O).
# --------------------------------------------------------------------------------------------
def _handle(args: argparse.Namespace) -> int:  # pragma: no cover - CLI over file I/O
    import yaml

    if args.action == "verify":
        ok, detail = verify_fixture()
        print(detail)
        return 0 if ok else 1

    records = flatten(generate_clean())
    write_parquet(records, FIXTURE_PARQUET)
    manifest = build_fixtures_manifest(records, FIXTURE_PARQUET)
    FIXTURES_YAML.write_text(
        "# Pinned identity of the committed synthetic fixture (frozen §5). CI verifies these\n"
        "# before every metrics run: content_sha256 is the platform-independent record digest;\n"
        "# parquet_sha256 is the committed file's byte checksum. Regenerate with `tickflow\n"
        "# fixture generate`; the fault injector runs at grading time and is never committed.\n"
        + yaml.safe_dump(manifest, sort_keys=True)
    )
    print(f"wrote {len(records)} records to {FIXTURE_PARQUET}")
    print(f"content_sha256 {manifest['content_sha256']}")
    print(f"parquet_sha256 {manifest['parquet_sha256']}")
    return 0


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "fixture",
        help="Generate or verify the committed synthetic trades.v1 fixture (CI system of record).",
    )
    parser.add_argument(
        "action",
        choices=["generate", "verify"],
        help="generate (write parquet + fixtures.yaml) or verify (check the committed pins).",
    )
    parser.set_defaults(handler=_handle)
