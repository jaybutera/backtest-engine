#!/usr/bin/env bash
# Fail if anything private made it into the tree.
#
# This repo was extracted from a private trading codebase. The extraction
# stripped strategy logic, tuned parameters, research notes, and account
# identifiers. This script is the standing check that none of it came back —
# run in CI on every push, and locally before any commit.
#
# A hit is not automatically a leak: check the context. If a pattern matches
# something legitimate, narrow the pattern rather than deleting the check.

set -uo pipefail
cd "$(dirname "$0")/.."

fail=0

# A line ending in `leak-check: ok <reason>` is exempt. Use it only where the
# match is genuinely not private material — a doc quoting a banned pattern in
# order to teach the rule, or a test asserting on a computed value. Every use
# has to carry a reason, so the exemptions stay auditable.
EXEMPT='leak-check: ok'

report() {
    local label="$1" pattern="$2"
    shift 2
    local hits
    hits=$(grep -rniE "$pattern" \
        --include='*.rs' --include='*.toml' --include='*.py' \
        --include='*.html' --include='*.css' --include='*.sh' \
        --include='*.md' --include='*.json' --include='*.yml' \
        --exclude-dir=target --exclude-dir=.git --exclude-dir=.venv \
        --exclude-dir=__pycache__ --exclude-dir=data \
        --exclude='check-leaks.sh' \
        . 2>/dev/null | grep -v "$EXEMPT")
    if [ -n "$hits" ]; then
        echo "FAIL [$label]"
        echo "$hits" | sed 's/^/    /'
        fail=1
    fi
}

# ── Credentials and account identifiers ────────────────────────────────────
report "wallet address"      '0x[0-9a-fA-F]{40}'
report "private key"         '(PRIVATE_KEY|SECRET|PASSWORD|API_KEY)[[:space:]]*=[[:space:]]*[A-Za-z0-9]'
report "broker account"      '\b(2044256|X0314|DEMO[0-9]{7})\b'

# ── Infrastructure belonging to the private deployment ─────────────────────
report "tailnet host"        'tailbee2ae|\.ts\.net'
report "tailnet/LAN IP"      '\b(100\.85\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3})\b'
report "personal path"       '/home/(casper|ec2-user)'
report "remote host"         '(ssh|scp|rsync)[[:space:]]+[a-z]+@'

# ── Broker / venue integrations that do not belong in a backtest engine ────
report "venue integration"   '\b(hyperliquid|tradovate|tradier|databento|dukascopy)\b'

# ── Research output: campaign names, presets, and measured results ─────────
report "campaign name"       '\b(rlp-?[0-9]+|kgold|daybias|nobias|kirsten|mimic|jury|smt_|reclaim_)'
report "private preset"      '\b(v5_winning|v3_vote|live_reclaim|cme_daybias|idx_combo|rlp30_veto)\b'
# An R-figure is a leak when it reports what something EARNED over a sample.
# The same notation is legitimate in a test asserting a computed result, or in
# a doc explaining what R means, so match the claim shapes rather than the
# number: a result verb near an R-figure, or a per-trade/total/net framing.
report "R-figure claim"      '(net|total|gross|lifted|earned|cost|worth|gained|lost|book(ed|s)?|gives?|yields?|adds?|drops?)[^.]{0,50}[+-][0-9]+(\.[0-9]+)?R\b'
report "per-sample R"        '[+-]?[0-9]+(\.[0-9]+)?R[[:space:]]*/[[:space:]]*(trade|day|week|month|year|run)'
report "R over N trades"     '[+-][0-9]+(\.[0-9]+)?R\b[^.]{0,30}\b(over|across|on)\b[^.]{0,20}[0-9]+[[:space:]]*(trades|setups|samples|runs)'
report "research path"       'experiments/|briefs/|LEARNINGS'

# ── Dated findings: a date introducing a claim is a lab-notebook entry ─────
# Dates as data are fine — CLI examples, JSON timestamps, test fixtures. What
# is not fine is a date presented as when something was decided or measured,
# which is the shape a migrated research note takes. Match the annotation
# forms: a date opening a comment, or one following a decision verb.
report "dated annotation"    '(//|#)[[:space:]]*20[0-9]{2}-[0-9]{2}-[0-9]{2}[[:space:]]*[:,-]'
report "dated decision"      '(deployed|adopted|measured|tuned|validated|falsified|confirmed|switched|reverted)[^.]{0,40}20[0-9]{2}-[0-9]{2}-[0-9]{2}'

if [ "$fail" -eq 0 ]; then
    echo "OK — no private material found."
else
    echo
    echo "Leak check failed. Nothing above should ship in a public repo."
fi
exit "$fail"
