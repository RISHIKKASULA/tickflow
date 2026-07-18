"""Normalization unit tests — field mapping from each venue's raw message to trades.v1.

The ingester's run loop is I/O glue (coverage-excluded, §9), but the pure normalization is
the producer's contract obligation and is tested directly with representative raw payloads.
"""

from __future__ import annotations

from tickflow.ingest import (
    CoinbaseFeed,
    KrakenFeed,
    _iso_to_millis,
    _normalize_side,
)


def test_iso_to_millis_utc_z() -> None:
    # 2026-07-18T12:00:00.500Z -> epoch millis, fractional preserved.
    assert _iso_to_millis("2026-07-18T12:00:00.500Z") % 1000 == 500


def test_iso_to_millis_trims_sub_microsecond() -> None:
    # Nanosecond precision must not break parsing (trimmed to micros).
    a = _iso_to_millis("2026-07-18T12:00:00.123456789Z")
    b = _iso_to_millis("2026-07-18T12:00:00.123456Z")
    assert a == b


def test_normalize_side() -> None:
    assert _normalize_side("BUY") == "buy"
    assert _normalize_side("sell") == "sell"
    assert _normalize_side("weird") == "unknown"


def test_coinbase_parse_maps_fields() -> None:
    message = {
        "channel": "market_trades",
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "12345",
                        "product_id": "BTC-USD",
                        "price": "63000.42",
                        "size": "0.001",
                        "side": "BUY",
                        "time": "2026-07-18T12:00:00.250Z",
                    }
                ],
            }
        ],
    }
    trades = list(CoinbaseFeed().parse(message, ts_ingest=1_700_000_000_000))
    assert len(trades) == 1
    t = trades[0]
    assert t.exchange == "coinbase"
    assert t.symbol == "BTC-USD"
    assert t.trade_id == "12345"
    assert t.price == 63000.42
    assert t.size == 0.001
    assert t.side == "buy"
    assert t.ts_event % 1000 == 250
    assert t.ts_ingest == 1_700_000_000_000


def test_coinbase_parse_ignores_other_channels() -> None:
    assert list(CoinbaseFeed().parse({"channel": "heartbeats"}, ts_ingest=0)) == []


def test_kraken_parse_maps_fields_and_symbol() -> None:
    message = {
        "channel": "trade",
        "type": "update",
        "data": [
            {
                "symbol": "ETH/USD",
                "side": "sell",
                "price": 3200.5,
                "qty": 1.25,
                "trade_id": 99,
                "timestamp": "2026-07-18T12:00:00.750Z",
            }
        ],
    }
    trades = list(KrakenFeed().parse(message, ts_ingest=42))
    assert len(trades) == 1
    t = trades[0]
    assert t.exchange == "kraken"
    assert t.symbol == "ETH-USD"  # normalized from ETH/USD
    assert t.trade_id == "99"  # coerced to string
    assert t.price == 3200.5
    assert t.size == 1.25
    assert t.side == "sell"
    assert t.ts_event % 1000 == 750


def test_kraken_parse_ignores_non_trade() -> None:
    assert list(KrakenFeed().parse({"channel": "status", "type": "update"}, ts_ingest=0)) == []


def test_kraken_subscribe_maps_canonical_to_venue_symbols() -> None:
    import json

    sub = json.loads(KrakenFeed().subscribe(["BTC-USD", "ETH-USD"]))
    assert sub["params"]["symbol"] == ["BTC/USD", "ETH/USD"]
    assert sub["params"]["channel"] == "trade"


def test_coinbase_subscribe_uses_canonical_symbols() -> None:
    import json

    sub = json.loads(CoinbaseFeed().subscribe(["BTC-USD"]))
    assert sub["product_ids"] == ["BTC-USD"]
    assert sub["channel"] == "market_trades"
