"""tickflow — a quality-gate and contract-enforcement layer for Kafka-compatible streams.

Declarative per-topic contracts (Avro schema + semantic rules) enforced inline, violations
routed to a quarantine topic, protecting a downstream consumer with a measurable SLO. Every
quality claim is measured by seeded fault-injection replay in CI, with confidence intervals.
See docs/architecture.md for the frozen design.
"""

__version__ = "0.9.0"
