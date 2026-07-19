"""The trades.v1 contract: schema loading, the Confluent Avro wire codec, and Schema Registry
wiring (frozen design §2).

Two responsibilities live here:

- **Codec (pure, tested).** Encode/decode a trades.v1 record to the Confluent wire format — a
  1-byte magic 0, a 4-byte big-endian schema id, then the fastavro schemaless body. The encoder
  is hand-rolled on `confluent-kafka` + `fastavro` rather than a framework serializer: the
  fundamentals are the point (§3). Records carry epoch-millis ints in/out (`ts_event`,
  `ts_ingest`), which the gate does integer watermark math on; the on-wire schema keeps them as
  Avro `timestamp-millis`, so the codec converts at the boundary and round-trips exactly.

- **Registry (network, integration-lane).** A minimal Schema Registry REST client over stdlib
  `urllib` — register the subject with BACKWARD compatibility, test a candidate schema, fetch by
  id. These methods talk to Redpanda's built-in registry, so they are exercised in the
  integration lane (and by `tickflow contract`), not in unit tests; they are marked no-cover.

The BACKWARD compatibility choice (§2) means new schema versions must be readable by consumers
on the old schema — so fields may be removed or gain defaults, but required fields cannot be
added without a default. The gate's R1 rule is exactly "Avro decode against this schema
succeeds"; everything downstream trusts the record shape because of it.
"""

from __future__ import annotations

import argparse
import io
import json
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import fastavro

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "trades.v1.avsc"
SUBJECT = "trades.raw-value"
DEFAULT_REGISTRY_URL = "http://localhost:18081"
COMPATIBILITY = "BACKWARD"

MAGIC_BYTE = 0
_HEADER_LEN = 5  # magic byte + 4-byte schema id
# Fields carried in-record as epoch-millis ints but declared timestamp-millis on the wire.
_TS_FIELDS: tuple[str, ...] = ("ts_event", "ts_ingest")


def load_schema_dict(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    """The raw contract JSON as written on disk."""
    parsed: dict[str, Any] = json.loads(path.read_text())
    return parsed


def load_schema(path: Path = CONTRACT_PATH) -> Any:
    """The fastavro-parsed schema used by the codec."""
    return fastavro.parse_schema(load_schema_dict(path))


def schema_str(path: Path = CONTRACT_PATH) -> str:
    """Compact JSON string of the schema, as registered with the Schema Registry."""
    return json.dumps(load_schema_dict(path), separators=(",", ":"), sort_keys=True)


def _to_avro(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    for name in _TS_FIELDS:
        out[name] = datetime.fromtimestamp(int(record[name]) / 1000, tz=UTC)
    return out


def _from_avro(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    for name in _TS_FIELDS:
        value = out[name]
        if isinstance(value, datetime):
            out[name] = int(value.timestamp() * 1000)
    return out


def encode(record: dict[str, Any], schema: Any, schema_id: int) -> bytes:
    """Serialize a trades.v1 record to the Confluent wire format."""
    buffer = io.BytesIO()
    buffer.write(bytes([MAGIC_BYTE]))
    buffer.write(schema_id.to_bytes(4, "big"))
    fastavro.schemaless_writer(buffer, schema, _to_avro(record))
    return buffer.getvalue()


def decode(data: bytes, schema: Any) -> tuple[int, dict[str, Any]]:
    """Deserialize wire bytes to (schema_id, record). Raises ValueError on a bad frame.

    A raised ValueError is what the gate's R1 rule treats as a `malformed` quarantine.
    """
    if len(data) < _HEADER_LEN or data[0] != MAGIC_BYTE:
        raise ValueError("not a Confluent-framed Avro message (bad magic byte or truncated)")
    schema_id = int.from_bytes(data[1:_HEADER_LEN], "big")
    record = cast(
        "dict[str, Any]", fastavro.schemaless_reader(io.BytesIO(data[_HEADER_LEN:]), schema)
    )
    return schema_id, _from_avro(record)


class SchemaRegistry:
    """Minimal Schema Registry REST client (Redpanda's built-in registry).

    Network methods are exercised in the integration lane, not unit tests (no broker here).
    """

    def __init__(self, base_url: str = DEFAULT_REGISTRY_URL) -> None:
        self.base_url = base_url.rstrip("/")

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> Any:  # pragma: no cover - network I/O, integration lane
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read()
        return json.loads(raw) if raw else {}

    def set_compatibility(
        self, subject: str, level: str = COMPATIBILITY
    ) -> None:  # pragma: no cover - network I/O
        self._request("PUT", f"/config/{subject}", {"compatibility": level})

    def register(self, subject: str, schema: str) -> int:  # pragma: no cover - network I/O
        result = self._request(
            "POST", f"/subjects/{subject}/versions", {"schema": schema, "schemaType": "AVRO"}
        )
        return int(result["id"])

    def is_compatible(self, subject: str, schema: str) -> bool:  # pragma: no cover - network I/O
        try:
            result = self._request(
                "POST",
                f"/compatibility/subjects/{subject}/versions/latest",
                {"schema": schema, "schemaType": "AVRO"},
            )
        except urllib.error.HTTPError as exc:
            if exc.code == 404:  # no prior version to check against — trivially compatible
                return True
            raise
        return bool(result.get("is_compatible", False))


def ensure_registered(
    registry: SchemaRegistry, subject: str = SUBJECT, schema: str | None = None
) -> int:  # pragma: no cover - network I/O, integration lane
    """Pin BACKWARD compatibility on the subject, register the contract, return its schema id."""
    payload = schema if schema is not None else schema_str()
    registry.set_compatibility(subject, COMPATIBILITY)
    return registry.register(subject, payload)


def _handle(args: argparse.Namespace) -> int:  # pragma: no cover - CLI over network I/O
    if args.action == "show":
        print(json.dumps(load_schema_dict(), indent=2))
        return 0

    registry = SchemaRegistry(args.registry)
    if args.action == "register":
        schema_id = ensure_registered(registry, args.subject)
        print(f"registered {args.subject} (BACKWARD) as schema id {schema_id}")
        return 0

    # action == "check": CI compatibility gate — the local contract must stay BACKWARD-compatible.
    compatible = registry.is_compatible(args.subject, schema_str())
    state = "compatible" if compatible else "INCOMPATIBLE"
    print(f"{args.subject}: local contract is {state} with the registered latest ({COMPATIBILITY})")
    return 0 if compatible else 1


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "contract",
        help="Register/check the trades.v1 contract in the Schema Registry, or show it.",
    )
    parser.add_argument(
        "action",
        choices=["register", "check", "show"],
        help="register (subject + BACKWARD), check (CI compatibility gate), or show the schema.",
    )
    parser.add_argument(
        "--registry", default=DEFAULT_REGISTRY_URL, help="Schema Registry base URL."
    )
    parser.add_argument("--subject", default=SUBJECT, help="Registry subject.")
    parser.set_defaults(handler=_handle)
