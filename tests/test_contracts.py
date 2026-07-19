"""trades.v1 contract tests — schema load and the Confluent Avro wire codec.

The Schema Registry client is network I/O exercised in the integration lane (no broker here).
The codec is pure and load-bearing — the gate's R1 rule is "this decode succeeds" — so it is
tested directly: exact round-trip (including epoch-millis timestamps that travel as Avro
timestamp-millis), the Confluent wire header, and rejection of malformed frames.
"""

from __future__ import annotations

from typing import Any

import pytest

from tickflow.contracts import (
    MAGIC_BYTE,
    SUBJECT,
    SchemaRegistry,
    decode,
    encode,
    load_schema,
    load_schema_dict,
    schema_str,
)

RECORD: dict[str, Any] = {
    "exchange": "kraken",
    "symbol": "BTC-USD",
    "trade_id": "103935244",
    "price": 64687.2,
    "size": 5.457e-05,
    "side": "buy",
    "ts_event": 1784443158756,
    "ts_ingest": 1784443158757,
}


def test_subject_is_frozen() -> None:
    assert SUBJECT == "trades.raw-value"


def test_schema_has_expected_fields_and_timestamp_logical_types() -> None:
    schema = load_schema_dict()
    fields = {f["name"]: f["type"] for f in schema["fields"]}
    assert list(fields) == [
        "exchange",
        "symbol",
        "trade_id",
        "price",
        "size",
        "side",
        "ts_event",
        "ts_ingest",
    ]
    assert fields["price"] == "double"
    assert fields["size"] == "double"
    for ts in ("ts_event", "ts_ingest"):
        assert fields[ts] == {"type": "long", "logicalType": "timestamp-millis"}


def test_schema_str_is_deterministic() -> None:
    assert schema_str() == schema_str()


def test_encode_emits_confluent_wire_header() -> None:
    schema = load_schema()
    wire = encode(RECORD, schema, schema_id=42)
    assert wire[0] == MAGIC_BYTE
    assert int.from_bytes(wire[1:5], "big") == 42


def test_round_trip_preserves_record_and_schema_id() -> None:
    schema = load_schema()
    wire = encode(RECORD, schema, schema_id=7)
    schema_id, record = decode(wire, schema)
    assert schema_id == 7
    assert record == RECORD
    # Millisecond timestamps survive the timestamp-millis logical type exactly.
    assert record["ts_event"] == 1784443158756
    assert isinstance(record["ts_event"], int)


def test_decode_rejects_bad_magic_byte() -> None:
    schema = load_schema()
    wire = bytearray(encode(RECORD, schema, schema_id=1))
    wire[0] = 1  # not the Confluent magic 0
    with pytest.raises(ValueError):
        decode(bytes(wire), schema)


def test_decode_rejects_truncated_frame() -> None:
    schema = load_schema()
    with pytest.raises(ValueError):
        decode(b"\x00\x00\x00", schema)  # shorter than the 5-byte header


def test_registry_trims_trailing_slash() -> None:
    # Pure construction, no network: the base URL is normalized for path joins.
    assert SchemaRegistry("http://localhost:18081/").base_url == "http://localhost:18081"
