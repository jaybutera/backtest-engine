#!/usr/bin/env -S uv run --script
"""Write a synthetic 1-minute candle file so the engine runs with no data at all.

    uv run scripts/synth-data.py                   # data/SYNTH_1m.parquet
    scripts/backtest.sh -a SYNTH

The series is a random walk shaped like a US equity session: 390 one-minute
bars a day from 13:30 UTC on weekdays, an overnight gap, a slow drift that
flips sign every few weeks, and volume heaviest at the open and close. It
carries no information; a strategy that makes money on it is fitting noise,
which is exactly what makes it useful as a control. Nothing about it is
real, and the default asset name says so. The generator refuses to
overwrite an existing file unless told to, so it cannot silently replace
real bars under the same stem.
"""

from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

BARS_PER_DAY = 390
OPEN_UTC = timedelta(hours=13, minutes=30)


def generate(start: date, end: date, seed: int, price: float) -> pl.DataFrame:
    rng = random.Random(seed)
    ts, o, h, lo_, c, v = [], [], [], [], [], []
    log_p = math.log(price)
    sigma = 0.00035  # per-minute, about 0.7% a day
    drift = 0.0
    days_left = 0
    day = start
    while day <= end:
        if day.weekday() < 5:
            if days_left == 0:
                # A new drift regime: a few weeks of mild trend either way,
                # at most about a third of a percent a day.
                drift = rng.choice([-1.0, 1.0]) * rng.uniform(0.0, 0.000003)
                days_left = rng.randint(5, 25)
            days_left -= 1
            log_p += rng.gauss(0.0, 0.004)  # overnight gap
            t0 = datetime.combine(day, datetime.min.time()) + OPEN_UTC
            for i in range(BARS_PER_DAY):
                # Volatility and volume follow the session's U shape.
                edge = abs(i - BARS_PER_DAY / 2) / (BARS_PER_DAY / 2)
                s = sigma * (0.7 + 0.8 * edge)
                bar_open = log_p
                hi = lo = bar_open
                for _ in range(4):
                    log_p += drift + rng.gauss(0.0, s / 2.0)
                    hi, lo = max(hi, log_p), min(lo, log_p)
                ts.append(t0 + timedelta(minutes=i))
                o.append(round(math.exp(bar_open), 2))
                h.append(round(math.exp(hi), 2))
                lo_.append(round(math.exp(lo), 2))
                c.append(round(math.exp(log_p), 2))
                v.append(int(rng.lognormvariate(math.log(2000 * (0.5 + edge)), 0.6)))
        day += timedelta(days=1)
    # Rounding can push a close a cent past the extremes it was inside of.
    h = [max(a, b, d) for a, b, d in zip(h, o, c, strict=True)]
    lo_ = [min(a, b, d) for a, b, d in zip(lo_, o, c, strict=True)]
    return pl.DataFrame(
        {"timestamp": ts, "open": o, "high": h, "low": lo_, "close": c, "volume": v},
        schema={"timestamp": pl.Datetime("us"), "open": pl.Float64, "high": pl.Float64,
                "low": pl.Float64, "close": pl.Float64, "volume": pl.Int64},
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--asset", default="SYNTH")
    ap.add_argument("--from", dest="start", type=date.fromisoformat, default=date(2024, 1, 2))
    ap.add_argument("--to", dest="end", type=date.fromisoformat, default=date(2024, 6, 28))
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--price", type=float, default=100.0, help="starting price")
    ap.add_argument("--out", type=Path, default=Path("data"))
    ap.add_argument("--force", action="store_true", help="overwrite an existing file")
    a = ap.parse_args()

    path = a.out / f"{a.asset.upper()}_1m.parquet"
    if path.exists() and not a.force:
        sys.exit(f"{path} exists; pass --force to overwrite it")
    df = generate(a.start, a.end, a.seed, a.price)
    a.out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd")
    print(
        f"{a.asset.upper()}: {df.height} bars, {df['timestamp'].min()} .. {df['timestamp'].max()}"
        f" UTC, seed {a.seed} -> {path} ({path.stat().st_size // 1024} KiB)"
    )


if __name__ == "__main__":
    main()
