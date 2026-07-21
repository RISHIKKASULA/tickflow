#!/usr/bin/env bash
# Release-blocking scan (frozen §11/§14). Run before tagging; CI runs it too.
#
# Two independent prohibitions, both of which must hold across README, docs/, src/, site/, the
# workflows, AND the git log (messages and bodies -- a leaked reference in a commit body is
# permanent in a way a file edit is not):
#
#   1. No employer / internship / private-work references anywhere. This repository is public
#      personal work and must not name or imply an employer relationship.
#   2. No market-data-derived export. Field names carrying prices, bars, or volumes must not
#      appear as JSON keys in any published artifact (Coinbase/Kraken ToS §1).
#
# Exits non-zero on the first violation with the offending line, so it is usable as a gate.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

fail=0
note() { printf '  %s\n' "$*"; }

# ---------------------------------------------------------------------------------------------
# 1. Employer / internship / private-work references.
# ---------------------------------------------------------------------------------------------
# Word-boundary anchored so ordinary English survives: "internal state" must not trip
# "intern", and "company-wide" style prose is matched only as a standalone word.
EMPLOYER_RE='\b(intern|interns|internship|internships|employer|employers|my (company|team at)|at work|dayjob|day job|nda|proprietary code|confidential)\b'

# Deliberately NOT `git grep`: its -E engine does not implement \b as a word boundary (that
# needs -P, which is not built into every git). A \b pattern there matches nothing and the check
# passes silently -- a gate that can only ever say PASS. `git ls-files` + real grep gives one
# regex dialect for both this scan and the commit-log scan below, and it was verified to fail on
# a planted reference before being trusted.
# Two files legitimately contain these words because they *define the prohibition*: the frozen
# design doc that states the rule, and this script, which contains the pattern. They are exempt
# by path -- and every exempt hit is still PRINTED rather than silently skipped, so an exemption
# can never become a hiding place. Nothing else is exempt.
POLICY_FILES='^(docs/architecture\.md|scripts/release_gate\.sh):'

echo "[gate] scanning tracked files for employer/internship references..."
all_hits=$(git ls-files -z -- \
      README.md CHANGELOG.md STATE.md docs src site tests .github contracts scripts \
      '*.toml' '*.yaml' '*.yml' \
      | xargs -0 grep -nEi "$EMPLOYER_RE" 2>/dev/null)
policy_hits=$(printf '%s\n' "$all_hits" | grep -E "$POLICY_FILES")
real_hits=$(printf '%s\n' "$all_hits" | grep -vE "$POLICY_FILES" | grep -v '^$')

if [ -n "$real_hits" ]; then
  echo "[gate] FAIL: employer/internship reference in tracked files:"
  note "$real_hits"
  fail=1
else
  echo "[gate]   ok - none outside the policy-defining files"
fi
if [ -n "$policy_hits" ]; then
  # The doc hits are listed in full -- that is a real document whose wording deserves review.
  # This script's own hits are pure self-reference (it contains the search pattern), so they are
  # reported as a count: printing them buries the actual verdict under the pattern definition.
  doc_hits=$(printf '%s\n' "$policy_hits" | grep -v '^scripts/release_gate\.sh:')
  self_count=$(printf '%s\n' "$policy_hits" | grep -c '^scripts/release_gate\.sh:')
  if [ -n "$doc_hits" ]; then
    echo "[gate]   noted (policy text, exempt by path - review, do not ignore):"
    printf '%s\n' "$doc_hits" | sed 's/^/    /'
  fi
  echo "[gate]   noted: $self_count self-references in this script (it contains the pattern)"
fi

echo "[gate] scanning git log (subjects and bodies)..."
if hits=$(git log --format='%H%n%s%n%b' | grep -nEi "$EMPLOYER_RE"); then
  echo "[gate] FAIL: employer/internship reference in commit history:"
  note "$hits"
  fail=1
else
  echo "[gate]   ok - none in commit history"
fi

# ---------------------------------------------------------------------------------------------
# 2. Market-data-derived export.
# ---------------------------------------------------------------------------------------------
# Interpreter resolution matters here. A bare `python` picks up whatever is first on PATH, which
# on a developer machine is usually a system Python with none of this project's dependencies --
# so the import fails, and the check reports a ToS violation that did not happen. Resolve an
# interpreter that can actually import the package, and keep "could not run the check" strictly
# distinct from "the check failed": both block the release, but only one means a leak.
find_python() {
  for candidate in "${VIRTUAL_ENV:-}/bin/python" ./.venv/bin/python python3 python; do
    [ -n "$candidate" ] || continue
    command -v "$candidate" >/dev/null 2>&1 || continue
    if "$candidate" -c "import sys; sys.path.insert(0, 'src'); import tickflow.export" \
        >/dev/null 2>&1; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  return 1
}

# Matched as JSON *keys* only. SLO invariant labels (price_positive, high_ge_low) and CI bounds
# (ci_low, ci_high) are telemetry, not market data, and must not trip this -- see export.py.
#
# The pattern is generated from export.MARKET_FIELD_NAMES, never hand-copied: a second literal
# list drifts silently from the first, which is the defect class ADR-006 exists to record.
if PY=$(find_python); then
  MARKET_KEY_RE=$("$PY" -c "import sys; sys.path.insert(0, 'src'); \
from tickflow.export import market_key_regex; print(market_key_regex())")
else
  echo "[gate] ERROR: no interpreter available that can import tickflow."
  note "Cannot generate the market-data key pattern, so the scan below did NOT run."
  note "Blocking anyway: an unrun check is not a passed check."
  fail=1
  MARKET_KEY_RE=""
fi

echo "[gate] scanning published artifacts for market-data fields..."
if [ -z "$MARKET_KEY_RE" ]; then
  echo "[gate]   skip - pattern unavailable (already reported above)"
elif [ -d site ]; then
  if hits=$(grep -rnEi "$MARKET_KEY_RE" site/ 2>/dev/null); then
    echo "[gate] FAIL: market-data field in a published artifact (ToS §1):"
    note "$hits"
    fail=1
  else
    echo "[gate]   ok - site/ is telemetry-only"
  fi
else
  echo "[gate]   skip - no site/ directory"
fi

# The programmatic check is the authority; the grep above is a second, independent pass that can
# also catch a value smuggled in as page text rather than as a structured field.
#
echo "[gate] re-running the programmatic telemetry-only assertion..."
if [ -f site/telemetry.json ]; then
  if [ -n "${PY:-}" ]; then
    if "$PY" -c "
import json, sys
sys.path.insert(0, 'src')
from tickflow import export
export.assert_telemetry_only(json.load(open('site/telemetry.json')))
print('[gate]   ok - assert_telemetry_only passed')
"; then :; else
      echo "[gate] FAIL: assert_telemetry_only found market data in the published artifact"
      fail=1
    fi
  else
    echo "[gate] ERROR: no interpreter available that can import tickflow."
    note "The programmatic telemetry-only check did NOT run. This is an environment problem,"
    note "not a detected leak. Activate the venv or use 'uv run bash scripts/release_gate.sh'."
    note "Blocking anyway: an unrun check is not a passed check."
    fail=1
  fi
else
  echo "[gate]   skip - no site/telemetry.json"
fi

# ---------------------------------------------------------------------------------------------
# 3. The page must not claim a refresh cadence the best-effort cron cannot promise.
# ---------------------------------------------------------------------------------------------
echo "[gate] checking the dashboard makes no cadence claim..."
if [ -f site/index.html ]; then
  if hits=$(grep -nEi 'updated (daily|hourly|nightly|weekly|every)' site/index.html); then
    echo "[gate] FAIL: the dashboard claims a refresh cadence:"
    note "$hits"
    fail=1
  else
    echo "[gate]   ok - states a refresh timestamp, not a cadence"
  fi
fi

echo
if [ "$fail" -eq 0 ]; then
  echo "[gate] PASS - release is not blocked."
else
  echo "[gate] BLOCKED - fix the above before tagging."
fi
exit "$fail"
