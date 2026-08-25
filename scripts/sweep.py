#!/usr/bin/env -S uv run --script
"""Run a strategy preset over a grid of [script] parameters and rank the results.

    uv run scripts/sweep.py config/strategy/rsi_atr.toml \
        --set tf_secs=900,1800,3600 --set ema_period=100,200 --set rr=2.0,2.5,3.0 \
        --from 2019-09-01 -- -a ES --warmup-days 60

Every combination gets its own temporary preset (the base file with the
named `[script]` keys replaced, in the shared table and in every
`[script.per_asset.*]` block that sets them, so a swept key holds for every
asset), is run through the engine one at a time under `nice`, and is scored
from the JSON sidecar: trades, win rate, gross and net R, fees, and the
deepest drawdown of the cumulative net R curve. Arguments after `--` go to
the engine untouched (`-a`, `--data-dir`, `--warmup-days`, ...); a
multi-asset preset is usually swept one leg at a time with `-a`. `--csv`
writes every row for a longer look.

This is an in-sample search. A grid's best cell is the most flattered
number in the table, not the expected result; the ensemble runner and a
window the grid never saw are how to find out what survives.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def parse_set(spec: str) -> tuple[str, list[str]]:
    key, _, vals = spec.partition("=")
    if not key or not vals:
        sys.exit(f"--set wants key=v1,v2,...: {spec!r}")
    return key, vals.split(",")


def rewrite(base: str, base_dir: Path, values: dict[str, str]) -> str:
    """Replace `key = ...` lines inside [script] and [script.per_asset.*]; make the script path absolute."""
    out, in_script, seen = [], False, set()
    for line in base.splitlines():
        m = re.match(r"\s*\[(.+)\]\s*$", line)
        if m:
            table = m.group(1).strip()
            in_script = table == "script" or table.startswith("script.")
        km = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", line)
        if km and km.group(1) == "script" and not in_script:
            rel = km.group(2).strip().strip('"')
            line = f'script = "{(base_dir / rel).resolve()}"'
        elif in_script and km and km.group(1) in values:
            line = f"{km.group(1)} = {values[km.group(1)]}"
            seen.add(km.group(1))
        out.append(line)
    missing = set(values) - seen
    if missing:
        sys.exit(f"keys not present under [script] in the base preset: {sorted(missing)}")
    return "\n".join(out) + "\n"


def score(sidecar: Path) -> dict:
    d = json.loads(sidecar.read_text())
    trades = sorted(d["trades"], key=lambda t: t.get("closed_at") or t["opened_at"])
    peak = cum = dd = 0.0
    for t in trades:
        cum += t["r_pnl"]
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    wins = sum(1 for t in trades if t["r_pnl"] > 0)
    return {
        "trades": len(trades),
        "win%": 100.0 * wins / len(trades) if trades else 0.0,
        "gross": d["gross_r_pnl"],
        "fees": d["total_fees"],
        "net": d["total_r_pnl"],
        "maxdd": dd,
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("preset")
    ap.add_argument("--set", action="append", default=[], help="key=v1,v2,... (repeatable)")
    ap.add_argument("--fill", default="config/fill/market_on_open.toml")
    ap.add_argument("--from", dest="start")
    ap.add_argument("--to", dest="end")
    ap.add_argument("--bin", default=os.environ.get("BT_BIN", "target/release/backtest"))
    ap.add_argument("--csv", type=Path)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("engine_args", nargs="*")
    a = ap.parse_args()

    base_path = Path(a.preset).resolve()
    base = base_path.read_text()
    grid = [parse_set(s) for s in a.set]
    keys = [k for k, _ in grid]
    combos = list(itertools.product(*[v for _, v in grid])) or [()]
    print(f"{len(combos)} runs", file=sys.stderr)

    rows = []
    with tempfile.TemporaryDirectory(prefix="sweep-") as tmp:
        for i, combo in enumerate(combos, 1):
            values = dict(zip(keys, combo, strict=True))
            preset = Path(tmp) / f"run{i}.toml"
            preset.write_text(rewrite(base, base_path.parent, values))
            sidecar = Path(tmp) / f"run{i}.json"
            cmd = ["nice", "-n", "15", a.bin, "replay", "--strategy", str(preset), "--fill", a.fill,
                   "--json-sidecar", str(sidecar), "--output", "json"]
            if a.start:
                cmd += ["--from", a.start]
            if a.end:
                cmd += ["--to", a.end]
            cmd += a.engine_args
            r = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO)
            if r.returncode != 0 or not sidecar.exists():
                print(f"run {i} {values}: engine failed\n{r.stderr[-800:]}", file=sys.stderr)
                continue
            row = {**values, **score(sidecar)}
            rows.append(row)
            print(
                f"[{i}/{len(combos)}] {values} -> {row['trades']} trades, "
                f"net {row['net']:+.1f}R, dd {row['maxdd']:.1f}R",
                file=sys.stderr,
            )

    rows.sort(key=lambda r: r["net"], reverse=True)
    cols = keys + ["trades", "win%", "gross", "fees", "net", "maxdd"]
    print("  ".join(f"{c:>9}" for c in cols))
    for r in rows[: a.top]:
        cells = [f"{r[c]:>9.2f}" if isinstance(r[c], float) else f"{r[c]:>9}" for c in cols]
        print("  ".join(cells))
    if a.csv:
        import csv
        with a.csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)


if __name__ == "__main__":
    main()
