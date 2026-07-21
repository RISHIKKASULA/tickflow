"""Fault-injection grading with bootstrap confidence intervals (frozen §6 — the TRUTH-ONLY core).

Every quality claim tickflow makes is a number graded here against the injection manifest, and
**every proportion carries a 95% bootstrap CI**. Because the rules are deterministic, recall on
non-boundary faults is ~100% by construction — an uninformative number the README admits to. What
these metrics exist to surface honestly are the informative parts:

- **Detection recall, per fault class** — the fraction of real faults quarantined with the *correct*
  rule label. The recall denominator is every `is_fault` entry, so the `duplicate` class's
  **designed misses** (dups past the LRU window the gate cannot catch, manifest `detectable=False`)
  correctly drag R3 recall below 100%. That gap is the point, not a defect (§5).
- **Detection precision, per rule** — of the messages quarantined with a rule, the fraction that
  were genuinely that rule's fault. A mislabel or a quarantined clean control counts against it.
- **False-quarantine rate** — over the boundary controls (`is_fault=False`) *and* every untouched
  clean message. This is where a gate that over-rejects would show up; the frozen invariant is that
  it stays at zero.
- **Completeness** — every input frame accounted for exactly once across valid + quarantine (loss
  and duplicate-delivery both asserted zero, not assumed).

**Bootstrap (frozen §6).** Each proportion's CI comes from resampling its Bernoulli outcomes with
replacement, B = 10,000, seed 42, percentile interval. Resampling the mean of a length-`n` 0/1
sample is exactly `Binomial(n, k/n) / n`, so `bootstrap_proportion_ci` uses that identity — it is
the same distribution as item resampling, just O(B) instead of O(B·n), which matters when the
false-quarantine denominator is ~98,000 clean messages. Report format everywhere: `point [lo, hi]`.

The grading here is deterministic and runs **in process** — a seeded fixture replayed through the
real `RulesEngine`, no broker required — so it is exactly what CI's fast lane executes on every
push. The broker-based metrics job that publishes provenance-stamped artifacts (commit SHA, runner
spec, latency percentiles) is the Day-D system of record (§7); this module is its truth core.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from tickflow import bars, contracts, fixture
from tickflow.fixture import DUPLICATE, MALFORMED, OUT_OF_ORDER, OUT_OF_RANGE
from tickflow.gate import RulesConfig, Verdict, evaluate_all, load_rules_config

if TYPE_CHECKING:
    from tickflow.fixture import InjectionManifest, Record

SEED = 42
BOOTSTRAP_B = 10_000
CI_ALPHA = 0.05

# The four graded fault classes and the rule each should be caught by (frozen §2 table, R1-R4).
FAULT_CLASSES: tuple[str, ...] = (MALFORMED, OUT_OF_RANGE, DUPLICATE, OUT_OF_ORDER)
RULE_OF_CLASS: dict[str, str] = {
    MALFORMED: "R1",
    OUT_OF_RANGE: "R2",
    DUPLICATE: "R3",
    OUT_OF_ORDER: "R4",
}
CLASS_OF_RULE: dict[str, str] = {rule: cls for cls, rule in RULE_OF_CLASS.items()}


# --------------------------------------------------------------------------------------------
# Bootstrap CI (frozen §6).
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Estimate:
    """A point proportion with its bootstrap 95% CI over `n` items. `n == 0` → undefined."""

    point: float
    ci_low: float
    ci_high: float
    n: int

    @property
    def defined(self) -> bool:
        return self.n > 0 and not math.isnan(self.point)

    def format(self) -> str:
        if not self.defined:
            return f"n/a (n={self.n})"
        return f"{self.point:.4f} [{self.ci_low:.4f}, {self.ci_high:.4f}]"

    def as_dict(self) -> dict[str, Any]:
        if not self.defined:
            return {"point": None, "ci_low": None, "ci_high": None, "n": self.n}
        return {
            "point": round(self.point, 6),
            "ci_low": round(self.ci_low, 6),
            "ci_high": round(self.ci_high, 6),
            "n": self.n,
        }


def bootstrap_proportion_ci(
    successes: int, n: int, b: int = BOOTSTRAP_B, seed: int = SEED, alpha: float = CI_ALPHA
) -> Estimate:
    """The 95% bootstrap percentile CI for a proportion `successes / n`.

    Exact-equivalent to resampling the length-`n` 0/1 outcome array with replacement B times and
    taking percentiles of the resample means: the count of ones in each resample is
    `Binomial(n, successes/n)`, so we sample that directly (O(B), not O(B·n)). Deterministic given
    `seed`. `n == 0` yields an undefined estimate (reported as `n/a`).
    """
    import numpy as np

    if n <= 0:
        return Estimate(math.nan, math.nan, math.nan, 0)
    if successes < 0 or successes > n:
        raise ValueError(f"successes {successes} out of range for n {n}")
    point = successes / n
    rng = np.random.default_rng(seed)
    resample_props = rng.binomial(n, point, size=b) / n
    lo, hi = np.percentile(resample_props, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return Estimate(point, float(lo), float(hi), n)


# --------------------------------------------------------------------------------------------
# Grade report structures.
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ClassMetrics:
    """Per-fault-class recall + per-rule precision (a class maps 1:1 to a rule)."""

    fault_class: str
    rule: str
    n_faults: int
    n_detected: int
    n_designed_miss: int
    recall: Estimate
    n_quarantined_as_rule: int
    n_precision_hits: int
    precision: Estimate

    def as_dict(self) -> dict[str, Any]:
        return {
            "fault_class": self.fault_class,
            "rule": self.rule,
            "n_faults": self.n_faults,
            "n_detected": self.n_detected,
            "n_designed_miss": self.n_designed_miss,
            "recall": self.recall.as_dict(),
            "n_quarantined_as_rule": self.n_quarantined_as_rule,
            "n_precision_hits": self.n_precision_hits,
            "precision": self.precision.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class Completeness:
    """Every input accounted for exactly once across valid + quarantine (frozen §6)."""

    n_total: int
    n_valid: int
    n_quarantine: int
    loss: int
    duplicate: int

    @property
    def ok(self) -> bool:
        return (
            self.loss == 0
            and self.duplicate == 0
            and self.n_valid + self.n_quarantine == self.n_total
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_total": self.n_total,
            "n_valid": self.n_valid,
            "n_quarantine": self.n_quarantine,
            "loss": self.loss,
            "duplicate": self.duplicate,
            "ok": self.ok,
        }


@dataclass(frozen=True, slots=True)
class GradeReport:
    """The full graded result over one faulted replay — telemetry only, no market values (§8)."""

    n_total: int
    seed: int
    bootstrap_b: int
    manifest_digest: str
    per_class: dict[str, ClassMetrics]
    false_quarantine: Estimate
    n_controls: int
    completeness: Completeness
    fixture: str = "unspecified fixture"

    def as_dict(self) -> dict[str, Any]:
        return {
            "fixture": self.fixture,
            "n_total": self.n_total,
            "seed": self.seed,
            "bootstrap_b": self.bootstrap_b,
            "manifest_digest": self.manifest_digest,
            "per_class": {cls: metrics.as_dict() for cls, metrics in self.per_class.items()},
            "false_quarantine_rate": self.false_quarantine.as_dict(),
            "n_controls": self.n_controls,
            "completeness": self.completeness.as_dict(),
        }


# --------------------------------------------------------------------------------------------
# Grading (frozen §6) — deterministic, pure over (verdicts, manifest).
# --------------------------------------------------------------------------------------------
def grade(
    verdicts: list[Verdict],
    manifest: InjectionManifest,
    b: int = BOOTSTRAP_B,
    seed: int = SEED,
    fixture_label: str = "unspecified fixture",
) -> GradeReport:
    """Grade a verdict stream against the injection manifest (frozen §6).

    `verdicts[i]` is the gate's decision for the i-th injected frame, so it aligns with the
    manifest's absolute indices. Recall is per fault class (denominator = that class's `is_fault`
    entries, so designed misses lower R3 recall); precision is per rule (denominator = messages
    quarantined with that rule); false-quarantine is over controls + untouched clean.
    """
    by_index = manifest.by_index()

    faults_caught: dict[str, list[bool]] = {cls: [] for cls in FAULT_CLASSES}
    designed_miss: dict[str, int] = {cls: 0 for cls in FAULT_CLASSES}
    quarantined_correct: dict[str, list[bool]] = {rule: [] for rule in RULE_OF_CLASS.values()}
    control_quarantined: list[bool] = []
    n_valid = 0
    n_quarantine = 0

    for index, verdict in enumerate(verdicts):
        entry = by_index.get(index)
        if verdict.is_quarantine:
            n_quarantine += 1
            rule = verdict.rule_id
            if rule is not None:
                correct = entry is not None and entry.is_fault and entry.expected_rule == rule
                quarantined_correct.setdefault(rule, []).append(correct)
        else:
            n_valid += 1

        if entry is not None and entry.is_fault:
            caught = verdict.is_quarantine and verdict.rule_id == entry.expected_rule
            faults_caught[entry.fault_class].append(caught)
            if not entry.detectable:
                designed_miss[entry.fault_class] += 1
        else:
            # A boundary control (is_fault=False) or an untouched clean message: it must NOT
            # quarantine. Both together are the false-quarantine denominator (§6).
            control_quarantined.append(verdict.is_quarantine)

    per_class: dict[str, ClassMetrics] = {}
    for cls in FAULT_CLASSES:
        rule = RULE_OF_CLASS[cls]
        caught_flags = faults_caught[cls]
        n_faults = len(caught_flags)
        n_detected = sum(caught_flags)
        correct_flags = quarantined_correct.get(rule, [])
        n_quar = len(correct_flags)
        n_hits = sum(correct_flags)
        per_class[cls] = ClassMetrics(
            fault_class=cls,
            rule=rule,
            n_faults=n_faults,
            n_detected=n_detected,
            n_designed_miss=designed_miss[cls],
            recall=bootstrap_proportion_ci(n_detected, n_faults, b, seed),
            n_quarantined_as_rule=n_quar,
            n_precision_hits=n_hits,
            precision=bootstrap_proportion_ci(n_hits, n_quar, b, seed),
        )

    n_controls = len(control_quarantined)
    n_false_q = sum(control_quarantined)
    false_quarantine = bootstrap_proportion_ci(n_false_q, n_controls, b, seed)

    # In-process replay routes each frame to exactly one verdict, so loss is the only way the
    # accounting can break here (a short verdict stream); duplicate delivery is a broker/at-least-
    # once concern proven separately in the integration lane (kill/restart mid-replay, §10).
    completeness = Completeness(
        n_total=manifest.n_total,
        n_valid=n_valid,
        n_quarantine=n_quarantine,
        loss=manifest.n_total - len(verdicts),
        duplicate=0,
    )

    return GradeReport(
        n_total=manifest.n_total,
        seed=seed,
        bootstrap_b=b,
        manifest_digest=manifest.digest(),
        per_class=per_class,
        false_quarantine=false_quarantine,
        n_controls=n_controls,
        completeness=completeness,
        fixture=fixture_label,
    )


# --------------------------------------------------------------------------------------------
# Replay + grade — the in-process CI pipeline over the committed fixture (frozen §5/§7).
# --------------------------------------------------------------------------------------------
def _regroup(records: list[Record]) -> dict[tuple[str, str], list[Record]]:
    """Group a flat record list back into per-stream lists (row order preserved)."""
    streams: dict[tuple[str, str], list[Record]] = {key: [] for key in fixture.STREAMS}
    for record in records:
        streams.setdefault((str(record["exchange"]), str(record["symbol"])), []).append(record)
    return streams


def replay_and_grade(
    clean: dict[tuple[str, str], list[Record]],
    config: RulesConfig,
    schema: Any,
    b: int = BOOTSTRAP_B,
    seed: int = SEED,
    fixture_name: str = "unspecified fixture",
) -> GradeReport:
    """Inject faults into `clean`, replay through the real gate, and grade (frozen §5/§6)."""
    result = fixture.inject_faults(clean, config, schema, seed=seed)
    verdicts = evaluate_all(config, schema, result.frames)
    label = bars.fixture_label(fixture_name, result.manifest, config)
    return grade(verdicts, result.manifest, b=b, seed=seed, fixture_label=label)


def grade_committed_fixture(
    config: RulesConfig | None = None,
    schema: Any | None = None,
    b: int = BOOTSTRAP_B,
    seed: int = SEED,
    verify: bool = True,
) -> GradeReport:
    """Verify the committed fixture's pins, then replay it through the injector + gate and grade.

    This is the fault-injection replay CI runs: the committed parquet is the system of record, so
    its checksum is checked first (unless `verify=False` for a synthetic in-test fixture), then the
    seeded injector + real gate produce the graded numbers — reproducible from the checksummed
    fixture alone (§7).
    """
    if verify:
        ok, detail = fixture.verify_fixture()
        if not ok:
            raise ValueError(f"committed fixture failed verification: {detail}")
    config = config if config is not None else load_rules_config()
    schema = schema if schema is not None else contracts.load_schema()
    clean = _regroup(fixture.read_parquet(fixture.FIXTURE_PARQUET))
    return replay_and_grade(
        clean,
        config,
        schema,
        b=b,
        seed=seed,
        fixture_name=f"committed {fixture.FIXTURE_PARQUET.name}",
    )


# --------------------------------------------------------------------------------------------
# CLI — run the grade and print the telemetry report (no market values, §8).
# --------------------------------------------------------------------------------------------
def _handle(args: argparse.Namespace) -> int:  # pragma: no cover - CLI over file I/O
    report = grade_committed_fixture(b=args.bootstrap_b, seed=args.seed)
    payload: dict[str, Any] = {"grade": report.as_dict()}
    if not args.no_slo:
        payload["slo"] = bars.run_committed_fixture_experiment(seed=args.seed).as_dict()
    if args.out:
        Path(args.out).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps(payload, indent=2, sort_keys=True))
    print(f"[metrics] fixture {report.fixture}")
    for cls, class_metrics in report.per_class.items():
        print(
            f"[metrics] {cls:<13} recall {class_metrics.recall.format():<26} "
            f"precision {class_metrics.precision.format()}",
        )
    print(f"[metrics] false-quarantine {report.false_quarantine.format()}")
    print(f"[metrics] completeness ok={report.completeness.ok}")
    if "slo" in payload:
        slo = payload["slo"]
        print(
            f"[metrics] slo gates ON {slo['gates_on']['n_violated_bars']} violated bars "
            f"/ gates OFF {slo['gates_off']['n_violated_bars']} of {slo['gates_off']['n_bars']}"
        )
    return 0


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "metrics",
        help="Replay the committed fixture through the injector + gate; grade with bootstrap CIs.",
    )
    parser.add_argument(
        "--bootstrap-b",
        type=int,
        default=BOOTSTRAP_B,
        dest="bootstrap_b",
        help="Bootstrap resamples (frozen default 10000; reduce in a fast lane).",
    )
    parser.add_argument("--seed", type=int, default=SEED, help="Bootstrap + injector seed.")
    parser.add_argument("--out", default="", help="Optional path to write the telemetry JSON.")
    parser.add_argument(
        "--no-slo",
        action="store_true",
        dest="no_slo",
        help="Skip the gates-ON/OFF SLO experiment block (grade only).",
    )
    parser.set_defaults(handler=_handle)
