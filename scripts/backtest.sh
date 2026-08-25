#!/usr/bin/env bash
# Run a backtest and write the JSON report the visualizer reads.
#
# Usage:
#   scripts/backtest.sh [--from YYYY-MM-DD] [--to YYYY-MM-DD] [extra backtest args...]
#
# With no dates, the run covers the full range of whatever candle data is in
# data/. Anything this script doesn't recognize is forwarded to the binary
# untouched, so per-run flags (--warmup-days, --dataset, …) ride along.
#
# Environment:
#   BT_STRATEGY  strategy preset      (default: config/strategy/rsi_atr.toml)
#   BT_FILL      fill lens preset     (default: config/fill/market_on_open.toml)
#   BT_JSON      report path          (default: data/backtest_trades.json;
#                                      set empty to skip writing one)
#   BT_BIN       binary to run        (default: target/release/backtest)
#
# The three preset axes are independent by design: the same strategy graded
# through a pessimistic fill lens is one env var, not a fork.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

STRATEGY="${BT_STRATEGY:-config/strategy/rsi_atr.toml}"
FILL="${BT_FILL:-config/fill/market_on_open.toml}"

DATE_ARGS=()
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) DATE_ARGS+=(--from "$2"); shift 2 ;;
    --to)   DATE_ARGS+=(--to   "$2"); shift 2 ;;
    *)      EXTRA+=("$1"); shift ;;
  esac
done

BIN="${BT_BIN:-target/release/backtest}"
if [[ ! -x "$BIN" ]]; then
  echo "binary missing: $BIN — run: cargo build --release (or set BT_BIN)" >&2
  exit 1
fi

if [[ ${#DATE_ARGS[@]} -eq 0 ]]; then
  echo "Backtest: full data range (strategy: $STRATEGY | fill: $FILL)" >&2
else
  echo "Backtest: ${DATE_ARGS[*]} (strategy: $STRATEGY | fill: $FILL)" >&2
fi

# Always leave a JSON report behind, so the visualizer reflects whatever was
# last run without a separate export step. Set BT_JSON= (empty) to opt out.
REPORT="${BT_JSON-data/backtest_trades.json}"
REPORT_ARGS=()
if [[ -n "$REPORT" ]]; then
  mkdir -p "$(dirname "$REPORT")"
  REPORT_ARGS=(--json-sidecar "$REPORT")
fi

# ${arr[@]+"${arr[@]}"} — expanding an empty array is an "unbound variable"
# error under `set -u` on bash 3.2; this guard is a no-op on newer bash.
exec "$BIN" replay --strategy "$STRATEGY" --fill "$FILL" \
  ${REPORT_ARGS[@]+"${REPORT_ARGS[@]}"} \
  ${DATE_ARGS[@]+"${DATE_ARGS[@]}"} \
  ${EXTRA[@]+"${EXTRA[@]}"}
