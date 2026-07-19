"""Capture command unit tests — the pure helpers that give a capture its provenance.

The websocket run loop is feed I/O glue (coverage-excluded, §9). The checksum and manifest are
what let the sanity gate trust it is reviewing the exact bytes captured, so they are tested
directly: same bytes → same digest, and the manifest reflects what the sink actually wrote.
"""

from __future__ import annotations

from pathlib import Path

from tickflow.capture import (
    CAPTURE_SCHEMA,
    STREAM_FILE,
    _StreamSink,
    build_manifest,
    sha256_file,
)
from tickflow.ingest import NormalizedTrade


def _trade(exchange: str, symbol: str, trade_id: str) -> NormalizedTrade:
    return NormalizedTrade(
        exchange=exchange,
        symbol=symbol,
        trade_id=trade_id,
        price=63000.0,
        size=0.01,
        side="buy",
        ts_event=1_700_000_000_000,
        ts_ingest=1_700_000_000_010,
    )


def test_sha256_file_is_stable_and_content_addressed(tmp_path: Path) -> None:
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"tickflow\n")
    b.write_bytes(b"tickflow\n")
    assert sha256_file(a) == sha256_file(b)
    b.write_bytes(b"tickflow!\n")
    assert sha256_file(a) != sha256_file(b)


def test_stream_sink_writes_one_line_per_trade_and_counts_by_feed(tmp_path: Path) -> None:
    path = tmp_path / STREAM_FILE
    sink = _StreamSink(path)
    sink.write({"trade_id": "1"}, _trade("coinbase", "BTC-USD", "1"))
    sink.write({"trade_id": "2"}, _trade("coinbase", "ETH-USD", "2"))
    sink.write({"trade_id": "3"}, _trade("kraken", "BTC-USD", "3"))
    sink.close()

    lines = path.read_text().splitlines()
    assert len(lines) == 3
    assert sink.count == 3
    assert sink.counts_by_feed == {"coinbase": 2, "kraken": 1}


def test_stream_sink_record_keeps_raw_and_normalized(tmp_path: Path) -> None:
    import json

    path = tmp_path / STREAM_FILE
    sink = _StreamSink(path)
    sink.write({"trade_id": "42", "px": "63000.0"}, _trade("coinbase", "BTC-USD", "42"))
    sink.close()

    record = json.loads(path.read_text().splitlines()[0])
    assert record["raw"] == {"trade_id": "42", "px": "63000.0"}
    assert record["norm"]["exchange"] == "coinbase"
    assert record["norm"]["symbol"] == "BTC-USD"
    assert record["norm"]["trade_id"] == "42"


def test_build_manifest_reflects_sink_and_checksum(tmp_path: Path) -> None:
    path = tmp_path / STREAM_FILE
    sink = _StreamSink(path)
    sink.write({"trade_id": "1"}, _trade("coinbase", "BTC-USD", "1"))
    sink.write({"trade_id": "2"}, _trade("kraken", "ETH-USD", "2"))
    sink.close()

    digest = sha256_file(path)
    manifest = build_manifest(sink, ["BTC-USD", "ETH-USD"], seconds=300.0, sha256=digest)
    assert manifest.schema == CAPTURE_SCHEMA
    assert manifest.record_count == 2
    assert manifest.counts_by_feed == {"coinbase": 1, "kraken": 1}
    assert manifest.feeds == ["coinbase", "kraken"]
    assert manifest.symbols == ["BTC-USD", "ETH-USD"]
    assert manifest.captured_seconds == 300.0
    assert manifest.sha256 == digest
    assert manifest.stream_file == STREAM_FILE
