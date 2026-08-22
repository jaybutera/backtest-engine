#!/usr/bin/env bash
# Warmup-jitter ensemble wrapper around scripts/backtest.sh.
#
# Runs the same backtest N times at different --warmup-days offsets and reports
# the median and spread of total R.
#
# Why: a backtest's warmup window decides what detector state exists at the
# first real candle, which decides which setups are visible at all. Shifting
# the offset by a few days can move a full-year result by enough to flip its
# sign. A single run is one sample from that distribution, so headline numbers
# should come from the median here and be quoted with the spread.
#
# Usage:
#   scripts/ensemble-backtest.sh --from YYYY-MM-DD --to YYYY-MM-DD \
#       [--offsets "30 37 44 51 58"] [--outdir DIR] [extra backtest.sh args...]
#
# Env, passed through to backtest.sh: BT_STRATEGY, BT_FILL, BT_BIN.
#
# Runs are serialized under flock and niced: the ensemble is N full backtests,
# and running them concurrently on a laptop tends to end badly. Run from a
# repo checkout root.
set -euo pipefail

OFFSETS="30 37 44 51 58"
OUTDIR=""
FROM="" TO=""
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)    FROM="$2"; shift 2 ;;
    --to)      TO="$2"; shift 2 ;;
    --offsets) OFFSETS="$2"; shift 2 ;;
    --outdir)  OUTDIR="$2"; shift 2 ;;
    *)         EXTRA+=("$1"); shift ;;
  esac
done
[[ -n "$FROM" && -n "$TO" ]] || { echo "need --from and --to" >&2; exit 1; }
[[ -n "$OUTDIR" ]] || OUTDIR="ensemble_runs/${FROM}_${TO}_$(date +%H%M%S)"
mkdir -p "$OUTDIR"

echo "Ensemble: $FROM..$TO offsets=[$OFFSETS] -> $OUTDIR" >&2
for off in $OFFSETS; do
  [[ "$off" -gt 0 ]] || { echo "offsets must be >0 (0 = full-history warmup)" >&2; exit 1; }
  json="$OUTDIR/run_w${off}.json"
  if [[ -s "$json" ]]; then
    echo "  warmup=$off: exists, skipping" >&2
    continue
  fi
  echo "  warmup=$off ..." >&2
  BT_JSON="$json" \
    flock /tmp/backtest-heavy.lock nice -n 15 \
    scripts/backtest.sh --from "$FROM" --to "$TO" \
    --warmup-days "$off" ${EXTRA[@]+"${EXTRA[@]}"} \
    > "$OUTDIR/run_w${off}.log" 2>&1 \
    || { echo "  warmup=$off FAILED, see $OUTDIR/run_w${off}.log" >&2; exit 1; }
done

python3 - "$OUTDIR" <<'EOF'
import json, sys, glob, statistics as st
from collections import defaultdict

outdir = sys.argv[1]
runs = {}
for f in sorted(glob.glob(f"{outdir}/run_w*.json")):
    off = int(f.split("run_w")[1].split(".")[0])
    d = json.load(open(f))
    trades = d.get("trades", [])
    closed = [t for t in trades if t.get("gross_r_pnl") is not None]
    net = sum(t["gross_r_pnl"] - (t.get("fee_r") or 0) for t in closed)
    by_asset = defaultdict(float)
    by_dir = defaultdict(float)
    for t in closed:
        r = t["gross_r_pnl"] - (t.get("fee_r") or 0)
        by_asset[t["asset"]] += r
        by_dir[t.get("direction", "?")] += r
    runs[off] = {"gross": d.get("gross_r_pnl"), "net": net, "n": len(closed),
                 "by_asset": dict(by_asset), "by_dir": dict(by_dir)}

if not runs:
    sys.exit("no run jsons found")
print(f"\n=== ensemble summary ({outdir}) ===")
print(f"{'warmup':>7} {'net_R':>9} {'gross_R':>9} {'trades':>7}")
for off in sorted(runs):
    r = runs[off]
    print(f"{off:>7} {r['net']:>9.1f} {r['gross']:>9.1f} {r['n']:>7}")
nets = [r["net"] for r in runs.values()]
print(f"\nnet R: median {st.median(nets):.1f}  min {min(nets):.1f}  "
      f"max {max(nets):.1f}  spread {max(nets)-min(nets):.1f}")
assets = sorted({a for r in runs.values() for a in r["by_asset"]})
print("\nper-asset net R (median across runs):")
for a in assets:
    vals = [r["by_asset"].get(a, 0.0) for r in runs.values()]
    print(f"  {a:12} {st.median(vals):>8.1f}  [{min(vals):.1f} .. {max(vals):.1f}]")
print("\nper-direction net R (median across runs):")
for dr in ("bull", "bear"):
    vals = [r["by_dir"].get(dr, 0.0) for r in runs.values()]
    print(f"  {dr:5} {st.median(vals):>8.1f}  [{min(vals):.1f} .. {max(vals):.1f}]")
summary = {"runs": {str(k): {kk: vv for kk, vv in v.items() if kk != "by_asset"}
                    for k, v in runs.items()},
           "net_median": st.median(nets), "net_min": min(nets),
           "net_max": max(nets)}
json.dump(summary, open(f"{outdir}/summary.json", "w"), indent=1)
print(f"\nwrote {outdir}/summary.json")
EOF
