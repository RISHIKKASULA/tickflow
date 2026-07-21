"""Telemetry export with schema enforcement, and the static dashboard renderer (frozen §7/§8).

This module is the only thing allowed to produce a *published* artifact, and it exists because of
one hard constraint (§1, Coinbase/Kraken ToS, release-blocking): **tickflow may publish how its
pipeline behaved, never what the market did.** Counts, rates, confidence intervals, verdict labels,
digests, and timings may leave the machine. A price, a bar's open/high/low/close, a volume, a
vwap, a mid — may not, in any form, including "derived" or "aggregated" forms.

That rule is enforced here mechanically rather than by review, because review is exactly what
fails at 2am on release day:

- `assert_telemetry_only` walks the entire payload and rejects any **field name** in
  `MARKET_FIELD_NAMES`. A smuggled `price` / `open` / `high` / `low` / `close` / `volume` / `vwap`
  key fails the export, and the export is what the release depends on.
- The match is on **exact field names**, deliberately, not substrings. `price_positive` and
  `high_ge_low` are SLO *invariant labels* and `ci_low` / `ci_high` are confidence-interval
  bounds; none of them carries a market value. A substring rule would reject all of those and,
  worse, would train whoever hit it to weaken the check. An exact-name rule stays sharp.

**What the dashboard renders.** Detection precision/recall per fault class with CIs, the
false-quarantine rate over both denominators, the gates-ON/OFF SLO comparison, completeness, and
gate throughput. No chart of prices. No table of bars. No OHLCV value anywhere, because none ever
enters the artifact this page is rendered from.

**The refresh line.** The page says "last refreshed <timestamp>" and never "updated daily". The
Actions cron that regenerates it is best-effort — GitHub explicitly does not guarantee scheduled
runs fire on time, or at all, on a busy queue. Claiming a cadence the infrastructure does not
promise would be a small, avoidable lie, so the page states only the fact it can prove: when the
data it is showing was actually produced.

The page is rendered server-side into static HTML with every value baked in — no JavaScript, no
framework, no fetch. `telemetry.json` is committed next to it as the machine-readable artifact, so
every number on the page is traceable to a file a reader can diff, and `tickflow export`
regenerates both.
"""

from __future__ import annotations

import argparse
import html
import json
import platform
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tickflow import __version__, bars, contracts, fixture, metrics
from tickflow.gate import evaluate_all, load_rules_config

REPO_ROOT = Path(__file__).resolve().parents[2]
SITE_DIR = REPO_ROOT / "site"
TELEMETRY_JSON = SITE_DIR / "telemetry.json"
INDEX_HTML = SITE_DIR / "index.html"

# Exact field names that carry market data. See the module docstring for why this is an exact-name
# rule and not a substring rule.
MARKET_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "price",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "mid",
        "bid",
        "ask",
        "size",
        "notional",
        "ohlcv",
        "bar_open",
        "bar_close",
    }
)


class MarketDataLeak(Exception):
    """A market-data-derived field reached a published artifact (§1 ToS, release-blocking)."""


def find_market_fields(payload: Any, path: str = "$") -> list[str]:
    """Every path in `payload` whose *key* is an exact market-data field name."""
    found: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            here = f"{path}.{key}"
            if key in MARKET_FIELD_NAMES:
                found.append(here)
            found.extend(find_market_fields(value, here))
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            found.extend(find_market_fields(item, f"{path}[{i}]"))
    return found


def assert_telemetry_only(payload: Any) -> None:
    """Raise `MarketDataLeak` if any market-data field name appears anywhere in `payload`."""
    leaks = find_market_fields(payload)
    if leaks:
        raise MarketDataLeak(
            "market-data field(s) in a published artifact (§1 ToS): " + ", ".join(sorted(leaks))
        )


# --------------------------------------------------------------------------------------------
# Provenance (frozen §6) — every artifact says where it came from.
# --------------------------------------------------------------------------------------------
def _git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover - git always present in CI
        return "unknown"
    return out.stdout.strip() or "unknown"


def runner_spec() -> str:
    """A one-line description of the machine that produced the timings."""
    return f"{platform.system()} {platform.machine()}, Python {platform.python_version()}"


def provenance(profile: str, generated_at: str | None = None) -> dict[str, Any]:
    """The provenance stamp carried by every metrics artifact (§6).

    The fixture pins are read by their exact `fixtures.yaml` key and are **required**: a missing
    key raises rather than defaulting to an empty string. A blank provenance field looks like a
    cosmetic gap on a page but silently unlinks every published number from the fixture that is
    supposed to make it reproducible, which is the one property this project claims.
    """
    manifest = fixture.load_fixtures_manifest()
    missing = [key for key in ("content_sha256", "parquet_sha256") if not manifest.get(key)]
    if missing:
        raise ValueError(f"fixtures.yaml is missing required pin(s): {', '.join(missing)}")
    return {
        "generated_at": generated_at or datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tickflow_version": __version__,
        "commit": _git_commit(),
        "runner": runner_spec(),
        "profile": profile,
        "fixture_parquet_sha256": str(manifest["parquet_sha256"]),
        "fixture_content_sha256": str(manifest["content_sha256"]),
    }


# --------------------------------------------------------------------------------------------
# Gate throughput — a pipeline measurement, not a market one.
# --------------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Throughput:
    """In-process rule-engine throughput over the committed fixture.

    Deliberately narrow: this times `evaluate_all` over already-decoded wire frames in one process.
    It is **not** end-to-end pipeline throughput and carries no broker, network, or fsync cost, so
    it must never be quoted as a system capacity figure. It answers one question — is the rules
    engine fast enough that gating is not the bottleneck — and the dashboard labels it as such.
    """

    n_frames: int
    elapsed_s: float
    msgs_per_s: float
    runner: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_frames": self.n_frames,
            "elapsed_s": round(self.elapsed_s, 4),
            "msgs_per_s": round(self.msgs_per_s, 1),
            "runner": self.runner,
            "scope": "in-process rule engine only; no broker, network, or fsync cost included",
        }


def measure_throughput(frames: list[bytes], config: Any, schema: Any) -> Throughput:
    """Time the real `RulesEngine` over `frames` (single pass, single process)."""
    start = time.perf_counter()
    evaluate_all(config, schema, frames)
    elapsed = time.perf_counter() - start
    rate = len(frames) / elapsed if elapsed > 0 else 0.0
    return Throughput(len(frames), elapsed, rate, runner_spec())


# --------------------------------------------------------------------------------------------
# The artifact.
# --------------------------------------------------------------------------------------------
def build_telemetry(
    b: int = metrics.BOOTSTRAP_B,
    seed: int = metrics.SEED,
    profile: str = "in-process (no broker)",
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Regenerate the whole published artifact from the committed fixture (§6/§7).

    One command, one JSON, every published number inside it. Enforced telemetry-only before it is
    returned, so a leak fails here rather than on the public page.
    """
    config = load_rules_config()
    schema = contracts.load_schema()

    grade_report = metrics.grade_committed_fixture(config=config, schema=schema, b=b, seed=seed)
    comparison = bars.run_committed_fixture_experiment(config=config, schema=schema, seed=seed)

    clean = metrics._regroup(fixture.read_parquet(fixture.FIXTURE_PARQUET))
    injected = fixture.inject_faults(clean, config, schema, seed=seed)
    throughput = measure_throughput(injected.frames, config, schema)

    payload = {
        "provenance": provenance(profile, generated_at),
        "grade": grade_report.as_dict(),
        "slo": comparison.as_dict(),
        "throughput": throughput.as_dict(),
    }
    assert_telemetry_only(payload)
    return payload


# --------------------------------------------------------------------------------------------
# The static dashboard.
# --------------------------------------------------------------------------------------------
_CSS = """
:root { color-scheme: light dark; --fg:#12161c; --muted:#5b6472; --bg:#fbfcfd; --card:#fff;
  --line:#e2e6ec; --ok:#12693f; --bad:#a3231d; --accent:#1c3f94; }
@media (prefers-color-scheme: dark) { :root { --fg:#e8ecf2; --muted:#9aa4b2; --bg:#11141a;
  --card:#181c24; --line:#2a3038; --ok:#4ac98a; --bad:#f0837c; --accent:#8fb0ff; } }
* { box-sizing:border-box; }
body { margin:0; padding:2.5rem 1.25rem 4rem; background:var(--bg); color:var(--fg);
  font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif; }
main { max-width:64rem; margin:0 auto; }
h1 { font-size:1.7rem; margin:0 0 .3rem; letter-spacing:-.02em; }
h2 { font-size:1.1rem; margin:2.4rem 0 .5rem; letter-spacing:-.01em; }
p.sub { color:var(--muted); margin:0 0 .4rem; }
p.note { color:var(--muted); font-size:.87rem; margin:.5rem 0 0; }
section { background:var(--card); border:1px solid var(--line); border-radius:10px;
  padding:1rem 1.15rem; margin-top:.6rem; }
.scroll { overflow-x:auto; }
table { border-collapse:collapse; width:100%; font-variant-numeric:tabular-nums; font-size:.93rem; }
th,td { text-align:left; padding:.45rem .7rem; border-bottom:1px solid var(--line);
  white-space:nowrap; }
th { color:var(--muted); font-weight:600; font-size:.8rem; text-transform:uppercase;
  letter-spacing:.04em; }
tr:last-child td { border-bottom:none; }
td.num { text-align:right; }
.ok { color:var(--ok); font-weight:600; }
.bad { color:var(--bad); font-weight:600; }
.banner { border-left:3px solid var(--accent); padding:.6rem .9rem; background:var(--card);
  border-radius:0 8px 8px 0; margin:1.2rem 0 0; font-size:.92rem; }
code { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:.88em; }
footer { color:var(--muted); font-size:.85rem; margin-top:2.5rem; border-top:1px solid var(--line);
  padding-top:1rem; }
"""


def _est(estimate: dict[str, Any]) -> str:
    if estimate.get("point") is None:
        return f"n/a (n={estimate.get('n', 0)})"
    return (
        f"{estimate['point']:.4f} "
        f"<span class='muted'>[{estimate['ci_low']:.4f}, {estimate['ci_high']:.4f}]</span>"
    )


def render_html(payload: dict[str, Any]) -> str:
    """Render the telemetry payload to a standalone static page (no JS, no external assets)."""
    assert_telemetry_only(payload)  # never render what may not be published
    prov = payload["provenance"]
    grade = payload["grade"]
    slo = payload["slo"]
    tput = payload["throughput"]
    esc = html.escape

    rows = []
    for cls, m in sorted(grade["per_class"].items()):
        rows.append(
            f"<tr><td><code>{esc(cls)}</code></td><td>{esc(m['rule'])}</td>"
            f"<td class='num'>{m['n_faults']}</td>"
            f"<td class='num'>{_est(m['recall'])}</td>"
            f"<td class='num'>{_est(m['precision'])}</td>"
            f"<td class='num'>{m['n_designed_miss']}</td></tr>"
        )

    fq = grade["false_quarantine_rate"]
    fq_rows = "".join(
        f"<tr><td>{label}</td><td class='num'>{est['n']:,}</td>"
        f"<td class='num'>{_est(est)}</td><td>{note}</td></tr>"
        for label, est, note in (
            (
                "All controls",
                fq["all_controls"],
                "boundary controls + every untouched clean message",
            ),
            (
                "Near-boundary controls",
                fq["near_boundary_controls"],
                "the hard subset: one step from a quarantine decision",
            ),
        )
    )

    on, off = slo["gates_on"], slo["gates_off"]
    off_counts = "".join(
        f"<tr><td><code>{esc(k)}</code></td><td class='num'>{v:,}</td></tr>"
        for k, v in sorted(off["counts_by_invariant"].items())
        if v
    )
    comp = grade["completeness"]

    def verdict(ok: bool) -> str:
        return f"<span class='{'ok' if ok else 'bad'}'>{'ok' if ok else 'VIOLATED'}</span>"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>tickflow — pipeline telemetry</title>
<style>{_CSS}</style></head>
<body><main>

<h1>tickflow — pipeline telemetry</h1>
<p class="sub">Quality-gate measurements from a seeded fault-injection replay. Every number below
is regenerated by <code>tickflow export</code> from the committed fixture and is reproducible from
its checksum alone.</p>
<p class="sub">Last refreshed <strong>{esc(prov["generated_at"])}</strong>.</p>

<div class="banner"><strong>Pipeline telemetry only.</strong> This page publishes how the gate
behaved &mdash; counts, rates, confidence intervals, verdict labels, timings. It publishes no
market data: no prices, no derived market values, and no OHLCV bar values, here or in
<code>telemetry.json</code>. That is enforced mechanically at export time, not by review.</div>

<h2>Detection, by fault class</h2>
<section><div class="scroll"><table>
<tr><th>Fault class</th><th>Rule</th><th>n faults</th><th>Recall (95% CI)</th>
<th>Precision (95% CI)</th><th>Designed misses</th></tr>
{"".join(rows)}
</table></div>
<p class="note">Rules are deterministic, so recall on non-boundary faults is ~100% by
construction &mdash; an uninformative number stated plainly rather than presented as an
achievement. The informative row is <code>duplicate</code>: duplicates re-delivered past the
dedup window are uncatchable by design, and they drag R3 recall below 100%. That gap is the
point, not a defect.</p></section>

<h2>False quarantine &mdash; both denominators</h2>
<section><div class="scroll"><table>
<tr><th>Denominator</th><th>n</th><th>Rate (95% CI)</th><th>What it is</th></tr>
{fq_rows}
</table></div>
<p class="note">Reported over two denominators because the pooled one flatters the result. A
percentile bootstrap over a zero-event sample returns [0.0000, 0.0000]; that is an artifact of the
method at the boundary, not proof the true rate is exactly zero. The honest one-sided ceiling is
the rule of three, roughly 3/n &mdash; which is about 200&times; looser on the near-boundary
subset than on all controls. Hence both, side by side.</p></section>

<h2>Do the gates earn their keep? (gates ON vs OFF)</h2>
<section><div class="scroll"><table>
<tr><th>Arm</th><th>Bars</th><th>Violated bars</th><th>SLO</th></tr>
<tr><td>Gates <strong>ON</strong></td><td class="num">{on["n_bars"]:,}</td>
<td class="num">{on["n_violated_bars"]:,}</td><td>{verdict(bool(on["n_violations"] == 0))}</td></tr>
<tr><td>Gates <strong>OFF</strong></td><td class="num">{off["n_bars"]:,}</td>
<td class="num">{off["n_violated_bars"]:,}</td>
<td>{verdict(bool(off["n_violations"] == 0))}</td></tr>
</table></div>
<p class="note">Same fixture, same bar builder, gate removed. Fixture:
<code>{esc(slo["fixture"])}</code>.</p>
</section>

<h2>Which invariants break with the gate off</h2>
<section><div class="scroll"><table>
<tr><th>SLO invariant</th><th>Violations</th></tr>
{off_counts}
</table></div>
<p class="note">Per-invariant counts can exceed the violated-bar count, because a single bar can
trip several invariants at once. Violated <em>bars</em> is the headline; these counts are the
diagnosis. Bit-identity on the catchable subset:
<strong>{"holds" if slo["bit_identical"] else "FAILS"}</strong>
&mdash; gates-ON bars match the ground-truth valid projection byte for byte, with the
{slo["designed_miss_dups"]} designed-miss duplicates excluded and reported instead of
hidden.</p></section>

<h2>Completeness and gate throughput</h2>
<section><div class="scroll"><table>
<tr><th>Measure</th><th>Value</th></tr>
<tr><td>Frames accounted for</td><td class="num">{comp["n_total"]:,}</td></tr>
<tr><td>Routed valid</td><td class="num">{comp["n_valid"]:,}</td></tr>
<tr><td>Quarantined</td><td class="num">{comp["n_quarantine"]:,}</td></tr>
<tr><td>Loss / duplicate delivery</td>
<td class="num">{comp["loss"]} / {comp["duplicate"]}</td></tr>
<tr><td>Accounting</td><td>{verdict(bool(comp["ok"]))}</td></tr>
<tr><td>Gate throughput</td><td class="num">{tput["msgs_per_s"]:,.0f} msg/s</td></tr>
</table></div>
<p class="note">Throughput is <strong>{esc(tput["scope"])}</strong>, measured on
{esc(tput["runner"])} over {tput["n_frames"]:,} frames in {tput["elapsed_s"]:.2f}s. It is not a
system capacity figure and is not independently reproducible. No latency claim is made anywhere:
the local broker profile bypasses fsync, so latency measured against it would be meaningless.</p>
</section>

<footer>
<p>tickflow {esc(prov["tickflow_version"])} &middot; commit <code>{esc(prov["commit"][:12])}</code>
&middot; profile <code>{esc(prov["profile"])}</code> &middot; runner {esc(prov["runner"])}</p>
<p>Fixture content sha256 <code>{esc(prov["fixture_content_sha256"][:16])}</code> &middot;
parquet sha256 <code>{esc(prov["fixture_parquet_sha256"][:16])}</code></p>
<p>Regenerated by a best-effort scheduled job. Scheduled runs are not guaranteed to fire on time,
so this page states when its data was produced and never claims a refresh cadence. Source data:
<a href="telemetry.json">telemetry.json</a>.</p>
</footer>

</main></body></html>
"""


def write_site(payload: dict[str, Any], site_dir: Path = SITE_DIR) -> tuple[Path, Path]:
    """Write `telemetry.json` + the rendered `index.html`. Returns both paths."""
    site_dir.mkdir(parents=True, exist_ok=True)
    json_path = site_dir / "telemetry.json"
    html_path = site_dir / "index.html"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    html_path.write_text(render_html(payload))
    return json_path, html_path


def _handle(args: argparse.Namespace) -> int:  # pragma: no cover - CLI over file I/O
    payload = build_telemetry(b=args.bootstrap_b, seed=args.seed, profile=args.profile)
    json_path, html_path = write_site(payload, Path(args.site))
    print(f"[export] wrote {json_path}")
    print(f"[export] wrote {html_path}")
    print(f"[export] telemetry-only check passed ({len(MARKET_FIELD_NAMES)} field names blocked)")
    print(f"[export] generated_at {payload['provenance']['generated_at']}")
    return 0


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "export",
        help="Regenerate the telemetry JSON + static dashboard (telemetry-only, §7/§8).",
    )
    parser.add_argument(
        "--bootstrap-b",
        type=int,
        default=metrics.BOOTSTRAP_B,
        dest="bootstrap_b",
        help="Bootstrap resamples (frozen default 10000).",
    )
    parser.add_argument("--seed", type=int, default=metrics.SEED, help="Bootstrap + injector seed.")
    parser.add_argument("--site", default=str(SITE_DIR), help="Output directory for the site.")
    parser.add_argument(
        "--profile",
        default="in-process (no broker)",
        help="Compose profile / execution context recorded in the provenance stamp.",
    )
    parser.set_defaults(handler=_handle)
