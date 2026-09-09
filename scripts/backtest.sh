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
#   BT_STRATEGIES_DIR  strategies root: a tree of presets with sibling fill/
#                      and datasets/ dirs (default: config/strategy/private
#                      when it exists, else this repo's config/). Also accepted
#                      under its original name ICT_STRATEGIES_DIR.
#   BT_STRATEGY  strategy preset, relative to the root or absolute. Required
#                when a root is configured — a tree names its own presets, and
#                this script does not guess which one you meant.
#                (default without a root: config/strategy/rsi_atr.toml)
#   BT_FILL      fill lens, relative to the root's fill/ or absolute
#                (default without a root: config/fill/market_on_open.toml)
#   BT_JSON      report path          (default: data/backtest_trades.json;
#                                      set empty to skip writing one)
#   BT_BIN       binary to run        (default: target/release/backtest, built
#                                      on demand when absent)
#
# The three preset axes are independent by design: the same strategy graded
# through a pessimistic fill lens is one env var, not a fork.
#
# target/ is disposable: `cargo clean`, a disk sweep or a fresh clone all leave
# the default binary missing, and this script is run by tools (the trade-viz
# "run backtest" button) whose only channel for "go build it yourself" is a red
# error in someone else's UI. So a missing DEFAULT binary is built here rather
# than reported. An explicit BT_BIN is never built: naming a binary means you
# want that one, and silently compiling something else would hide the typo.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

# Resolve the strategies root the same way viz/server.py does, so a preset
# named in the UI and the same name typed here mean one file. With no root
# configured the repo's own demo presets are the defaults, which is what a
# fresh clone gets.
ROOT="${BT_STRATEGIES_DIR:-${ICT_STRATEGIES_DIR:-}}"
if [[ -z "$ROOT" && -d "config/strategy/private" ]]; then
  ROOT="config/strategy/private"
fi
if [[ -n "$ROOT" ]]; then
  ROOT="$(cd "$ROOT" 2>/dev/null && pwd)" || {
    echo "strategies root not found: $ROOT (unset BT_STRATEGIES_DIR to use the bundled demos)" >&2
    exit 1
  }
fi

# A name is taken relative to the given directory, absolute paths pass through,
# and an existing path relative to the repo root wins over both — so the old
# spelling ("config/strategy/example.toml") keeps working under a root.
resolve() {
  local name="$1" dir="$2"
  case "$name" in
    /*) echo "$name"; return ;;
  esac
  if [[ -n "$dir" && -f "$dir/$name" ]]; then echo "$dir/$name"; return; fi
  echo "$name"
}

if [[ -n "$ROOT" ]]; then
  [[ -n "${BT_STRATEGY:-}" ]] || {
    echo "BT_STRATEGY is required with a strategies root ($ROOT): name a preset under it," >&2
    echo "e.g. BT_STRATEGY=<family>/<preset>.toml" >&2
    exit 1
  }
  STRATEGY="$(resolve "$BT_STRATEGY" "$ROOT")"
  # No default lens under a root either: each tree's presets are graded under
  # their own, and the viz reads that from the preset's `[viz] fill`.
  [[ -n "${BT_FILL:-}" ]] || { echo "BT_FILL is required with a strategies root ($ROOT)" >&2; exit 1; }
  FILL="$(resolve "$BT_FILL" "$ROOT/fill")"
else
  STRATEGY="${BT_STRATEGY:-config/strategy/rsi_atr.toml}"
  FILL="${BT_FILL:-config/fill/market_on_open.toml}"
fi

DATE_ARGS=()
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) DATE_ARGS+=(--from "$2"); shift 2 ;;
    --to)   DATE_ARGS+=(--to   "$2"); shift 2 ;;
    *)      EXTRA+=("$1"); shift ;;
  esac
done

BIN="${BT_BIN:-}"
if [[ -n "$BIN" ]]; then
  [[ -x "$BIN" ]] || { echo "BT_BIN is not an executable: $BIN" >&2; exit 1; }
else
  BIN="target/release/backtest"
  if [[ ! -x "$BIN" ]]; then
    command -v cargo >/dev/null 2>&1 || {
      echo "binary missing: $BIN, and no cargo to build it — install Rust, or point BT_BIN at a built engine" >&2
      exit 1
    }
    # nice + a bounded job count so a build kicked off by a web request does
    # not take the machine away from whatever else is running; the flock keeps
    # two concurrent runs from fighting over the same target/ dir. flock is
    # util-linux and absent on macOS, so it is a wrapper when present and
    # skipped when not — cargo's own target lock still serializes the builds,
    # it just makes the loser wait silently instead of saying why.
    echo "binary missing: $BIN — building it (cargo build --release)" >&2
    LOCK=()
    if command -v flock >/dev/null 2>&1; then
      mkdir -p "$HOME/.cache"
      LOCK=(flock "$HOME/.cache/backtest-engine-build.lock")
    fi
    "${LOCK[@]+${LOCK[@]}}" nice -n 19 cargo build --release --jobs 4 --bin backtest >&2 || {
      echo "build failed — fix the build, or point BT_BIN at a working engine" >&2
      exit 1
    }
    [[ -x "$BIN" ]] || { echo "build reported success but $BIN is still missing" >&2; exit 1; }
  fi
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
