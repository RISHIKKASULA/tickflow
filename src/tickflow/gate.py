"""The declarative rules engine and the quarantine-routing gate consumer (frozen design §2/§3).

The gate is the product. Everything upstream normalizes; everything downstream trusts. This
module is two things:

- **`RulesEngine` — the pure, deterministic core (coverage-gated, §9).** It reads the six frozen
  rules and their thresholds from `contracts/rules.yaml` (nothing is hard-coded — edit the YAML,
  change the gate) and turns one raw `trades.raw` frame into a `Verdict`: route to `trades.valid`
  or quarantine it with a rule label, plus any alerts raised. R1-R4 quarantine; R5-R6 are
  ALERT-ONLY and structurally cannot quarantine (see the frozen rationale below). Every stateful
  check runs on the **per-stream event-time watermark** (max `ts_event` seen on that stream),
  never wall clock — that is precisely what makes fixture replay bit-identical: same messages in,
  same verdicts out, live or replay.

- **`run_gate` — the Kafka consumer glue (integration lane, no-cover).** At-least-once
  consumption with manual commits after processing, routing to `trades.valid` /
  `trades.quarantine`, quarantine writes idempotent via an offset-derived envelope key. This half
  is network I/O exercised in the integration lane, mirroring how `contracts.py` marks its
  registry client — the deterministic verdict logic it calls is what the unit tests pin down.

**Alert-only, and why it differs from quarantine (frozen §2 rationale).** R5 (gap) and R6
(divergence) detect conditions that are *real data behaving like reality*, not corrupt messages.
A gap is a message that never arrived — there is nothing to route. A cross-venue divergence is
what a genuine market move looks like before the venues re-converge; quarantining those trades
would delete true prices and turn the gate into a false-quarantine machine. So R5/R6 raise an
alert and increment telemetry, and the trade still flows to `trades.valid`. Quarantine is for
messages that are *wrong* (unschema'd, out-of-range, duplicated, or arrived too late to trust);
alerting is for the pipeline *noticing* something without rejecting good data. Detection is not
rejection, and the gate keeps them separate — enforced here by construction: `_ALERT_ONLY` rules
may not be configured to quarantine, and `load_rules_config` rejects any YAML that tries.

**R6 staleness (ADR-001).** Kraken prints roughly one trade every ~10-20 s, so a naive cross-venue
comparison would alert off a stale quote. A venue's last print is eligible for comparison only if
its event-time is within `staleness_ms` (30 s) of the comparison instant. If the other venue is
stale (or has never printed), R6 emits **no divergence verdict** and instead increments
`divergence_unavailable{reason=stale_<venue>}` — staleness is telemetry, never a quarantine, same
rationale as a gap.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from tickflow import contracts

RULES_PATH = Path(__file__).resolve().parents[2] / "contracts" / "rules.yaml"

TRADES_RAW = "trades.raw"
TRADES_VALID = "trades.valid"
TRADES_QUARANTINE = "trades.quarantine"

# The two frozen venues (§1). R6 compares last-trade prices across exactly these, per symbol.
VENUES: tuple[str, str] = ("coinbase", "kraken")

# R5/R6 are alert-only by frozen decision; the config loader refuses to let YAML make them
# quarantine. This is the "gap/divergence never quarantine true data" invariant, made mechanical.
_ALERT_ONLY: frozenset[str] = frozenset({"R5", "R6"})
_QUARANTINE_RULES: frozenset[str] = frozenset({"R1", "R2", "R3", "R4"})

DISPOSITION_VALID = "valid"
DISPOSITION_QUARANTINE = "quarantine"

# Decode failures that mean "malformed frame" → R1. contracts.decode raises ValueError on a bad
# header; a corrupt fastavro body surfaces as EOFError/IndexError (verified against fastavro).
_DECODE_ERRORS: tuple[type[BaseException], ...] = (ValueError, EOFError, IndexError, TypeError)

StreamKey = tuple[str, str]  # (exchange, symbol)


# --------------------------------------------------------------------------------------------
# Declarative config — every threshold comes from contracts/rules.yaml (frozen §2).
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class RulesConfig:
    """The six frozen rules resolved from `rules.yaml` into typed parameters.

    The engine is *declarative* in that all bounds, thresholds, dispositions, and reason labels
    are read from here; the rule *logic* (deterministic checks) is code, because the rules are
    fixed deterministic predicates, not a general DSL.
    """

    price_min: float
    size_min: float
    symbol_bounds: dict[str, tuple[float, float]]
    lru_size: int
    out_of_order_tolerance_ms: int
    gap_threshold_ms: int
    divergence_pct: float
    divergence_sustained_ms: int
    divergence_staleness_ms: int
    dispositions: dict[str, str]  # rule_id -> "quarantine" | "alert"
    reasons: dict[str, str]  # rule_id -> reason label ("malformed", "out-of-range", ...)


def load_rules_config(path: Path = RULES_PATH) -> RulesConfig:
    """Parse `rules.yaml` into a `RulesConfig`, enforcing the frozen alert-only invariant.

    Raises ValueError if a rule is missing, or if the YAML tries to make an alert-only rule
    (R5/R6) quarantine, or a quarantine rule (R1-R4) merely alert — the gate's routing contract
    is not overridable by config.
    """
    doc = yaml.safe_load(path.read_text())
    rules_by_id: dict[str, dict[str, Any]] = {}
    for rule in doc.get("rules", []):
        rules_by_id[str(rule["id"])] = rule

    missing = (_QUARANTINE_RULES | _ALERT_ONLY) - rules_by_id.keys()
    if missing:
        raise ValueError(f"rules.yaml is missing required rules: {sorted(missing)}")

    dispositions: dict[str, str] = {}
    reasons: dict[str, str] = {}
    for rule_id, rule in rules_by_id.items():
        disposition = str(rule["on_violation"])
        dispositions[rule_id] = disposition
        reasons[rule_id] = str(rule["reason"])
        if rule_id in _ALERT_ONLY and disposition != "alert":
            raise ValueError(
                f"{rule_id} is alert-only by frozen decision (gap/divergence never quarantine "
                f"true data); rules.yaml sets on_violation={disposition!r}"
            )
        if rule_id in _QUARANTINE_RULES and disposition != "quarantine":
            raise ValueError(
                f"{rule_id} must quarantine on violation; rules.yaml sets "
                f"on_violation={disposition!r}"
            )

    r2 = rules_by_id["R2"]["params"]
    r6 = rules_by_id["R6"]["params"]
    return RulesConfig(
        price_min=float(r2["price_min"]),
        size_min=float(r2["size_min"]),
        symbol_bounds={
            str(sym): (float(lo), float(hi)) for sym, (lo, hi) in r2["symbol_bounds"].items()
        },
        lru_size=int(rules_by_id["R3"]["params"]["lru_size"]),
        out_of_order_tolerance_ms=int(rules_by_id["R4"]["params"]["tolerance_ms"]),
        gap_threshold_ms=int(rules_by_id["R5"]["params"]["threshold_ms"]),
        divergence_pct=float(r6["deviation_pct"]),
        divergence_sustained_ms=int(r6["sustained_ms"]),
        divergence_staleness_ms=int(r6["staleness_ms"]),
        dispositions=dispositions,
        reasons=reasons,
    )


# --------------------------------------------------------------------------------------------
# Verdicts, alerts, telemetry — the engine's outputs.
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Alert:
    """An alert-only observation (R5 gap or R6 divergence). Never changes routing."""

    rule_id: str
    reason: str
    stream: str  # "exchange:symbol" for a gap; "symbol" for a cross-venue divergence
    detail: str
    at_event_ms: int


@dataclass(frozen=True, slots=True)
class Verdict:
    """The gate's decision for one raw frame: where it routes, why, and any alerts raised."""

    disposition: str  # DISPOSITION_VALID | DISPOSITION_QUARANTINE
    rule_id: str | None  # the quarantining rule (e.g. "R2"), or None when valid
    reason: str | None  # the rule's reason label, or None when valid
    detail: str  # human-readable specifics for the quarantine envelope / logs
    record: dict[str, Any] | None  # the decoded trade, or None if R1 could not decode it
    alerts: tuple[Alert, ...] = ()

    @property
    def is_valid(self) -> bool:
        return self.disposition == DISPOSITION_VALID

    @property
    def is_quarantine(self) -> bool:
        return self.disposition == DISPOSITION_QUARANTINE


@dataclass
class Telemetry:
    """Running counters the engine emits — counts, rates, verdicts only (no market values, §8)."""

    valid: int = 0
    quarantined: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    gap_alerts: int = 0
    divergence_alerts: int = 0
    divergence_unavailable: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def as_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "quarantined": dict(self.quarantined),
            "gap_alerts": self.gap_alerts,
            "divergence_alerts": self.divergence_alerts,
            "divergence_unavailable": dict(self.divergence_unavailable),
        }


# --------------------------------------------------------------------------------------------
# Small deterministic state helpers.
# --------------------------------------------------------------------------------------------
class _LruWindow:
    """A window of the last `capacity` distinct keys, FIFO-evicted by first insertion.

    R3's dedup memory. Membership is O(1); a first-seen key is inserted; a repeat is a hit and
    does not refresh recency, so eviction order is deterministic and independent of hit traffic.
    At exactly `capacity` distinct keys nothing is evicted yet; the (capacity+1)-th distinct key
    evicts the oldest — so a duplicate re-injected beyond the window is a designed miss (§5).
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._keys: OrderedDict[str, None] = OrderedDict()

    def __contains__(self, key: str) -> bool:
        return key in self._keys

    def add(self, key: str) -> None:
        self._keys[key] = None
        if len(self._keys) > self.capacity:
            self._keys.popitem(last=False)  # evict the oldest distinct key

    def __len__(self) -> int:
        return len(self._keys)


@dataclass(frozen=True, slots=True)
class _Print:
    """A venue's last trade for a symbol, on the event-time clock."""

    price: float
    ts_event: int


# --------------------------------------------------------------------------------------------
# The engine.
# --------------------------------------------------------------------------------------------
class RulesEngine:
    """Deterministic per-stream rules engine (frozen §2). One instance per gate process.

    `evaluate` is a pure function of the engine's accumulated state and the input frame — no wall
    clock, no randomness — so replaying the same frames through a fresh engine yields byte-for-byte
    identical verdicts (see `digest`). All state keys off the per-stream event-time watermark.
    """

    def __init__(self, config: RulesConfig, schema: Any | None = None) -> None:
        self.config = config
        self.schema = schema if schema is not None else contracts.load_schema()
        self.telemetry = Telemetry()
        # Per-stream (exchange, symbol) state.
        self._watermark: dict[StreamKey, int] = {}
        self._seen: dict[StreamKey, _LruWindow] = {}
        # Per-symbol cross-venue state for R6.
        self._last_print: dict[str, dict[str, _Print]] = {}
        self._diverging_since: dict[str, int | None] = {}
        self._diverge_alerted: dict[str, bool] = {}

    # -- R1 ---------------------------------------------------------------------------------
    def evaluate(self, raw: bytes) -> Verdict:
        """Apply R1-R6 to one raw `trades.raw` frame and return the routing verdict."""
        try:
            _schema_id, record = contracts.decode(raw, self.schema)
        except _DECODE_ERRORS as exc:
            return self._quarantine("R1", f"avro decode failed: {exc}", record=None)

        stream: StreamKey = (str(record["exchange"]), str(record["symbol"]))

        violation = (
            self._check_range(record)
            or self._check_duplicate(stream, record)
            or self._check_out_of_order(stream, record)
        )
        if violation is not None:
            rule_id, detail = violation
            return self._quarantine(rule_id, detail, record=record)

        alerts = self._accept(stream, record)
        self.telemetry.valid += 1
        return Verdict(DISPOSITION_VALID, None, None, "", record, alerts)

    def _quarantine(self, rule_id: str, detail: str, record: dict[str, Any] | None) -> Verdict:
        self.telemetry.quarantined[rule_id] += 1
        return Verdict(
            DISPOSITION_QUARANTINE, rule_id, self.config.reasons[rule_id], detail, record
        )

    # -- R2 range ---------------------------------------------------------------------------
    def _check_range(self, record: dict[str, Any]) -> tuple[str, str] | None:
        price = float(record["price"])
        size = float(record["size"])
        if price <= self.config.price_min:
            return "R2", f"price {price} <= min {self.config.price_min}"
        if size <= self.config.size_min:
            return "R2", f"size {size} <= min {self.config.size_min}"
        bounds = self.config.symbol_bounds.get(str(record["symbol"]))
        if bounds is not None:
            lo, hi = bounds
            if not (lo <= price <= hi):
                return "R2", f"price {price} outside [{lo}, {hi}] for {record['symbol']}"
        return None

    # -- R3 duplicate -----------------------------------------------------------------------
    def _check_duplicate(self, stream: StreamKey, record: dict[str, Any]) -> tuple[str, str] | None:
        window = self._seen.get(stream)
        if window is None:
            window = _LruWindow(self.config.lru_size)
            self._seen[stream] = window
        key = str(record["trade_id"])
        if key in window:
            return "R3", f"trade_id {key} seen within the last {self.config.lru_size} on {stream}"
        window.add(key)
        return None

    # -- R4 out-of-order --------------------------------------------------------------------
    def _check_out_of_order(
        self, stream: StreamKey, record: dict[str, Any]
    ) -> tuple[str, str] | None:
        watermark = self._watermark.get(stream)
        if watermark is None:
            return None  # first message on the stream establishes the watermark; never late
        ts_event = int(record["ts_event"])
        if ts_event < watermark - self.config.out_of_order_tolerance_ms:
            lateness = watermark - ts_event
            return "R4", f"ts_event {ts_event} is {lateness}ms < watermark {watermark} - tolerance"
        return None

    # -- accept path: advance watermark, then R5 gap + R6 divergence (alert-only) ------------
    def _accept(self, stream: StreamKey, record: dict[str, Any]) -> tuple[Alert, ...]:
        ts_event = int(record["ts_event"])
        prev = self._watermark.get(stream)
        alerts: list[Alert] = []

        # R5 gap — the watermark advancing by more than the threshold on an already-active stream.
        if prev is not None and ts_event - prev > self.config.gap_threshold_ms:
            jump = ts_event - prev
            self.telemetry.gap_alerts += 1
            alerts.append(
                Alert(
                    "R5",
                    self.config.reasons["R5"],
                    f"{stream[0]}:{stream[1]}",
                    f"watermark jumped {jump}ms (> {self.config.gap_threshold_ms}ms)",
                    ts_event,
                )
            )

        if prev is None or ts_event > prev:
            self._watermark[stream] = ts_event

        alerts.extend(self._divergence(record))
        return tuple(alerts)

    # -- R6 divergence (alert-only, ADR-001 staleness) --------------------------------------
    def _divergence(self, record: dict[str, Any]) -> list[Alert]:
        symbol = str(record["symbol"])
        exchange = str(record["exchange"])
        instant = int(record["ts_event"])

        prints = self._last_print.setdefault(symbol, {})
        prints[exchange] = _Print(float(record["price"]), instant)

        # Eligibility: each venue's last print must be within the staleness window of `instant`
        # (event-time). The venue that just printed is always fresh, so at most the *other* venue
        # can be stale — in which case no divergence verdict, only telemetry (ADR-001).
        fresh: dict[str, _Print] = {}
        for venue in VENUES:
            last = prints.get(venue)
            if last is None or instant - last.ts_event > self.config.divergence_staleness_ms:
                self.telemetry.divergence_unavailable[f"stale_{venue}"] += 1
            else:
                fresh[venue] = last

        if len(fresh) < len(VENUES):
            return []  # cannot compare — emit no divergence verdict

        a, b = fresh[VENUES[0]].price, fresh[VENUES[1]].price
        mid = (a + b) / 2.0
        deviation_pct = abs(a - b) / mid * 100.0
        over = deviation_pct > self.config.divergence_pct

        since = self._diverging_since.get(symbol)
        if not over:
            self._diverging_since[symbol] = None
            self._diverge_alerted[symbol] = False
            return []

        if since is None:
            self._diverging_since[symbol] = instant
            self._diverge_alerted[symbol] = False
            return []

        # Over threshold and already tracking — alert once the divergence has been sustained.
        sustained = instant - since
        if sustained >= self.config.divergence_sustained_ms and not self._diverge_alerted.get(
            symbol, False
        ):
            self._diverge_alerted[symbol] = True
            self.telemetry.divergence_alerts += 1
            return [
                Alert(
                    "R6",
                    self.config.reasons["R6"],
                    symbol,
                    f"cross-venue deviation {deviation_pct:.4f}% sustained {sustained}ms "
                    f"(> {self.config.divergence_pct}% for "
                    f"{self.config.divergence_sustained_ms}ms)",
                    instant,
                )
            ]
        return []

    def watermark(self, stream: StreamKey) -> int | None:
        """The current event-time watermark for a stream (max ts_event routed valid)."""
        return self._watermark.get(stream)


# --------------------------------------------------------------------------------------------
# Quarantine envelope (§3) — pure construction + serialization; the Kafka write is glue below.
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class QuarantineEnvelope:
    """What lands on `trades.quarantine`: rule_id, detail, source offset, ts, and the raw bytes.

    Self-describing so Day C's `tickflow quarantine` inspection/replay CLI can read it back. The
    raw frame is base64-encoded in the JSON body; `key` is the idempotency key for the quarantine
    write (frozen §3) — derived from the source coordinates so replaying the same input offset
    produces the same key and never double-writes.
    """

    rule_id: str
    reason: str
    detail: str
    offset: int | None
    partition: int | None
    ts_ms: int | None  # the record's ts_ingest when decodable, else None (R1 malformed)
    raw: bytes

    @property
    def key(self) -> bytes:
        source = f"{self.partition}:{self.offset}"
        if self.offset is None:
            # No source coordinates (e.g. direct replay): fall back to a content hash so identical
            # raw frames still deduplicate deterministically.
            source = "sha256:" + hashlib.sha256(self.raw).hexdigest()
        return source.encode()

    def to_json(self) -> bytes:
        payload = {
            "rule_id": self.rule_id,
            "reason": self.reason,
            "detail": self.detail,
            "offset": self.offset,
            "partition": self.partition,
            "ts_ms": self.ts_ms,
            "raw_b64": base64.b64encode(self.raw).decode("ascii"),
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


def build_envelope(
    verdict: Verdict, raw: bytes, offset: int | None = None, partition: int | None = None
) -> QuarantineEnvelope:
    """Build the quarantine envelope for a quarantined verdict (§3)."""
    if not verdict.is_quarantine or verdict.rule_id is None or verdict.reason is None:
        raise ValueError("build_envelope requires a quarantine verdict")
    ts_ms = int(verdict.record["ts_ingest"]) if verdict.record is not None else None
    return QuarantineEnvelope(
        rule_id=verdict.rule_id,
        reason=verdict.reason,
        detail=verdict.detail,
        offset=offset,
        partition=partition,
        ts_ms=ts_ms,
        raw=raw,
    )


# --------------------------------------------------------------------------------------------
# Determinism digest — proves "same fixture in → same verdicts out" bit-for-bit (frozen §10).
# --------------------------------------------------------------------------------------------
def _canonical(verdict: Verdict) -> dict[str, Any]:
    return {
        "disposition": verdict.disposition,
        "rule_id": verdict.rule_id,
        "reason": verdict.reason,
        "record": verdict.record,
        "alerts": [
            {"rule_id": a.rule_id, "reason": a.reason, "stream": a.stream, "at": a.at_event_ms}
            for a in verdict.alerts
        ],
    }


def verdicts_digest(verdicts: list[Verdict]) -> str:
    """A SHA-256 over the canonical verdict stream — equal digests mean bit-identical routing.

    `detail` strings are excluded (they are human-facing); routing, decoded record, and alert
    identity are what must be deterministic, and they are what this hashes.
    """
    blob = json.dumps([_canonical(v) for v in verdicts], separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


def evaluate_all(config: RulesConfig, schema: Any, frames: list[bytes]) -> list[Verdict]:
    """Run a fresh engine over `frames`, returning the verdict stream (a determinism helper)."""
    engine = RulesEngine(config, schema)
    return [engine.evaluate(frame) for frame in frames]


# --------------------------------------------------------------------------------------------
# Kafka consumer glue — at-least-once, manual commits, idempotent quarantine (frozen §3).
# Network I/O, exercised in the integration lane like contracts.py's registry client.
# --------------------------------------------------------------------------------------------
def run_gate(  # pragma: no cover - network I/O, integration lane
    bootstrap: str = "localhost:19092",
    group_id: str = "tickflow-gate",
    registry_url: str = contracts.DEFAULT_REGISTRY_URL,
    max_messages: int = 0,
    gates_off: bool = False,
) -> Telemetry:
    """Consume `trades.raw`, apply the gate, and route to valid/quarantine with manual commits.

    At-least-once: the source offset is committed only after the message has been produced to its
    destination and the producer flushed, so a crash re-delivers rather than drops. Quarantine
    writes are keyed by the envelope key (source coordinates), so re-delivery is idempotent.

    `gates_off` is the first-class §4 demo flag: the engine still *evaluates* every frame (so the
    telemetry records what WOULD have been quarantined), but every decodable frame is routed to
    `trades.valid` — nothing is quarantined. This is what makes the downstream SLO visibly break;
    it exists to demonstrate the gate's value, never for production use.
    """
    import sys

    from confluent_kafka import Consumer

    from tickflow.ingest import build_producer

    config = load_rules_config()
    schema = contracts.load_schema()
    engine = RulesEngine(config, schema)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,  # manual commits after processing (§3)
            "auto.offset.reset": "earliest",
        }
    )
    producer = build_producer(bootstrap)
    consumer.subscribe([TRADES_RAW])

    def _log(message: str) -> None:
        print(f"[gate] {message}", file=sys.stderr, flush=True)

    processed = 0
    try:
        while max_messages == 0 or processed < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                _log(f"consume error: {message.error()}")
                continue

            raw = message.value()
            if raw is None:  # tombstone/empty payload — nothing to gate
                consumer.commit(message=message, asynchronous=False)
                continue
            verdict = engine.evaluate(raw)
            if verdict.is_quarantine and not gates_off:
                envelope = build_envelope(
                    verdict, raw, offset=message.offset(), partition=message.partition()
                )
                producer.produce(TRADES_QUARANTINE, key=envelope.key, value=envelope.to_json())
            elif verdict.record is not None or not verdict.is_quarantine:
                # Gates on → valid frames only. Gates off → every decodable frame flows to valid
                # (a malformed frame with no decoded record has nothing to route downstream).
                producer.produce(TRADES_VALID, key=message.key(), value=raw)

            producer.flush(10)  # ensure the destination write lands before we commit the source
            consumer.commit(message=message, asynchronous=False)
            processed += 1

        return engine.telemetry
    finally:
        consumer.close()
        producer.flush(10)
        _log(f"stopped; telemetry={engine.telemetry.as_dict()}")


def _handle(args: Any) -> int:  # pragma: no cover - CLI over network I/O
    telemetry = run_gate(
        bootstrap=args.bootstrap,
        group_id=args.group_id,
        registry_url=args.schema_registry,
        max_messages=args.max_messages,
        gates_off=args.gates_off,
    )
    print(json.dumps(telemetry.as_dict(), indent=2))
    return 0


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "gate",
        help="Run the quality gate over trades.raw, routing to trades.valid / trades.quarantine.",
    )
    parser.add_argument("--bootstrap", default="localhost:19092", help="Kafka bootstrap servers.")
    parser.add_argument("--group-id", default="tickflow-gate", dest="group_id")
    parser.add_argument(
        "--schema-registry",
        default=contracts.DEFAULT_REGISTRY_URL,
        dest="schema_registry",
        help="Schema Registry base URL.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        dest="max_messages",
        help="Stop after N messages (0 = run until interrupted).",
    )
    parser.add_argument(
        "--gates-off",
        action="store_true",
        dest="gates_off",
        help="Demo mode (§4): evaluate but route everything to trades.valid so the SLO breaks.",
    )
    parser.set_defaults(handler=_handle)
