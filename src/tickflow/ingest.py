"""Exchange ingesters: connect to venue websockets, normalize both venues to the trades.v1
common schema, and produce raw ticks to the trades.raw topic.

Normalization is the producer's job (frozen design §2): the gate never sees venue-specific
formats. Day A emits JSON-serialized normalized records so the feed→broker path can be
verified end to end and reviewed by eye at the sanity gate; the Avro contract + Schema
Registry wiring lands in Day B, at which point the on-wire encoding switches to Avro without
changing the normalized record shape.

Feeds (frozen §1):
  - Coinbase (primary): wss://advanced-trade-ws.coinbase.com, `market_trades` channel — keyless.
  - Kraken (secondary): wss://ws.kraken.com/v2, `trade` channel — keyless.
Symbols (frozen): BTC-USD, ETH-USD on both venues (canonical form). No symbol creep.
Reconnect: exponential backoff (1s → 60s cap, jitter), resubscribe on connect; disconnect and
reconnect events are telemetry.

This module is ingester glue: it is excluded from the coverage gate (§9). The pure
normalization functions are still unit-tested for field-mapping correctness.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
import signal
import sys
import time
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Protocol

from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

CANONICAL_SYMBOLS: tuple[str, ...] = ("BTC-USD", "ETH-USD")
DEFAULT_BOOTSTRAP = "localhost:19092"
DEFAULT_TOPIC = "trades.raw"

# Trim any sub-microsecond fractional digits so datetime.fromisoformat accepts the timestamp.
_SUBMICRO = re.compile(r"(\.\d{6})\d+")


@dataclass(frozen=True, slots=True)
class NormalizedTrade:
    """A single trade normalized to the trades.v1 schema (§2)."""

    exchange: str  # "coinbase" | "kraken"
    symbol: str  # canonical, e.g. "BTC-USD"
    trade_id: str
    price: float
    size: float
    side: str  # "buy" | "sell" | "unknown"
    ts_event: int  # exchange event time, epoch millis
    ts_ingest: int  # local receipt time, epoch millis


def _now_millis() -> int:
    return int(time.time() * 1000)


def _iso_to_millis(ts: str) -> int:
    text = _SUBMICRO.sub(r"\1", ts.strip())
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return int(datetime.fromisoformat(text).timestamp() * 1000)


def _normalize_side(raw: str) -> str:
    side = raw.strip().lower()
    return side if side in ("buy", "sell") else "unknown"


class Feed(Protocol):
    """A venue feed: how to subscribe, and how to normalize its trade messages."""

    name: str
    url: str

    def subscribe(self, symbols: Sequence[str]) -> str: ...

    def parse(self, message: Any, ts_ingest: int) -> Iterator[NormalizedTrade]: ...


class CoinbaseFeed:
    name = "coinbase"
    url = "wss://advanced-trade-ws.coinbase.com"

    def subscribe(self, symbols: Sequence[str]) -> str:
        return json.dumps(
            {"type": "subscribe", "product_ids": list(symbols), "channel": "market_trades"}
        )

    def parse(self, message: Any, ts_ingest: int) -> Iterator[NormalizedTrade]:
        if message.get("channel") != "market_trades":
            return
        for event in message.get("events", []):
            for trade in event.get("trades", []):
                yield NormalizedTrade(
                    exchange=self.name,
                    symbol=str(trade["product_id"]),
                    trade_id=str(trade["trade_id"]),
                    price=float(trade["price"]),
                    size=float(trade["size"]),
                    side=_normalize_side(str(trade["side"])),
                    ts_event=_iso_to_millis(str(trade["time"])),
                    ts_ingest=ts_ingest,
                )


class KrakenFeed:
    name = "kraken"
    url = "wss://ws.kraken.com/v2"
    _TO_VENUE: ClassVar[dict[str, str]] = {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD"}
    _TO_CANON: ClassVar[dict[str, str]] = {v: k for k, v in _TO_VENUE.items()}

    def subscribe(self, symbols: Sequence[str]) -> str:
        venue = [self._TO_VENUE.get(s, s) for s in symbols]
        return json.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": venue}})

    def parse(self, message: Any, ts_ingest: int) -> Iterator[NormalizedTrade]:
        if message.get("channel") != "trade" or message.get("type") not in ("snapshot", "update"):
            return
        for trade in message.get("data", []):
            yield NormalizedTrade(
                exchange=self.name,
                symbol=self._TO_CANON.get(str(trade["symbol"]), str(trade["symbol"])),
                trade_id=str(trade["trade_id"]),
                price=float(trade["price"]),
                size=float(trade["qty"]),
                side=_normalize_side(str(trade["side"])),
                ts_event=_iso_to_millis(str(trade["timestamp"])),
                ts_ingest=ts_ingest,
            )


@dataclass
class Backoff:
    """Exponential backoff with full jitter, capped (frozen §1: 1s → 60s)."""

    base: float = 1.0
    cap: float = 60.0
    _attempt: int = field(default=0, repr=False)

    def reset(self) -> None:
        self._attempt = 0

    def next_delay(self) -> float:
        ceiling = min(self.cap, self.base * (2.0**self._attempt))
        self._attempt += 1
        # Full jitter in [0.5, 1.0] of the ceiling — spreads reconnects, keeps a floor.
        return ceiling * (0.5 + 0.5 * _jitter())


def _jitter() -> float:
    # Isolated so tests can stay deterministic; the exact value only affects timing.
    import random

    return random.random()


def build_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # idempotent producer (frozen §3)
            "linger.ms": 5,
            "compression.type": "lz4",
            "client.id": "tickflow-ingester",
        }
    )


def _key(trade: NormalizedTrade) -> bytes:
    # Keyed by stream + trade id so producer partitioning is stable and dedup-friendly.
    return f"{trade.exchange}|{trade.symbol}|{trade.trade_id}".encode()


def _encode(trade: NormalizedTrade) -> bytes:
    return json.dumps(asdict(trade)).encode()


def _log(message: str) -> None:
    print(f"[ingest] {message}", file=sys.stderr, flush=True)


def _produce(producer: Producer, topic: str, trade: NormalizedTrade) -> None:
    try:
        producer.produce(topic, key=_key(trade), value=_encode(trade))
    except BufferError:
        # Local queue full: serve delivery callbacks to drain, then retry once.
        producer.poll(0.5)
        producer.produce(topic, key=_key(trade), value=_encode(trade))


async def _run_feed(
    feed: Feed,
    symbols: Sequence[str],
    producer: Producer,
    topic: str,
    counters: dict[str, int],
    stop: asyncio.Event,
) -> None:
    backoff = Backoff()
    while not stop.is_set():
        try:
            async with connect(
                feed.url, ping_interval=20, ping_timeout=20, max_queue=1024, open_timeout=15
            ) as ws:
                await ws.send(feed.subscribe(symbols))
                backoff.reset()
                _log(f"{feed.name}: connected, subscribed to {list(symbols)}")
                async for raw in ws:
                    if stop.is_set():
                        break
                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(message, dict):
                        continue
                    for trade in feed.parse(message, _now_millis()):
                        _produce(producer, topic, trade)
                        counters[feed.name] += 1
                    producer.poll(0)
        except asyncio.CancelledError:
            raise
        except (WebSocketException, OSError) as exc:
            delay = backoff.next_delay()
            _log(f"{feed.name}: disconnected ({exc!r}); reconnecting in {delay:.1f}s")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
    _log(f"{feed.name}: stopped (produced {counters[feed.name]})")


async def _ingest(
    feeds: Sequence[Feed],
    symbols: Sequence[str],
    bootstrap: str,
    topic: str,
    duration: float,
) -> dict[str, int]:
    producer = build_producer(bootstrap)
    stop = asyncio.Event()
    counters: dict[str, int] = {feed.name: 0 for feed in feeds}

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # add_signal_handler is unavailable on some platforms (e.g. Windows).
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(_run_feed(feed, symbols, producer, topic, counters, stop))
        for feed in feeds
    ]
    if duration > 0:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=duration)
        stop.set()
    else:
        await stop.wait()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    pending = producer.flush(10)
    _log(f"done; counts={counters}; {pending} message(s) still queued after flush")
    return counters


def _select_feeds(choice: str) -> list[Feed]:
    available: dict[str, Feed] = {"coinbase": CoinbaseFeed(), "kraken": KrakenFeed()}
    if choice == "both":
        return [available["coinbase"], available["kraken"]]
    return [available[choice]]


def _handle(args: argparse.Namespace) -> int:
    feeds = _select_feeds(args.exchange)
    symbols = tuple(s.strip() for s in args.symbols.split(",") if s.strip())
    try:
        asyncio.run(_ingest(feeds, symbols, args.bootstrap, args.topic, args.duration))
    except KeyboardInterrupt:  # pragma: no cover
        return 130
    return 0


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "ingest",
        help="Stream live trades from exchange websockets, normalize, and produce to trades.raw.",
    )
    parser.add_argument(
        "--exchange",
        choices=["coinbase", "kraken", "both"],
        default="both",
        help="Which venue(s) to ingest (Coinbase is the primary feed; default: both).",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(CANONICAL_SYMBOLS),
        help="Comma-separated canonical symbols (default: BTC-USD,ETH-USD).",
    )
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Destination topic.")
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Seconds to run before stopping (0 = run until interrupted).",
    )
    parser.set_defaults(handler=_handle)
