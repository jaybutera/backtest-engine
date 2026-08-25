#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["databento>=0.40", "polars>=1.0"]
# ///
"""Fetch the demo's futures bars from Databento and write the engine's parquet files.

The demo runs on continuous front-month 1-minute bars of ES, NQ and GC built
from the GLBX.MDP3 dataset (CME Globex MDP 3.0), schema ohlcv-1m, parent
symbology (`ES.FUT` is every ES outright and calendar spread). This script
pulls that data with your own key, stitches each symbol's outrights into one
unadjusted front-month series, and writes `data/<SYM>_1m.parquet`. Nothing
is redistributed: Databento's terms license the bars to the account that
downloads them, which is why the repo ships this script and not the files.

    export DATABENTO_API_KEY=...
    uv run scripts/fetch-databento.py        # ES and NQ from 2019-08-01, GC from 2010-06-06
    uv run scripts/fetch-databento.py --start 2024-01-01 ES
    uv run scripts/fetch-databento.py --dbn ~/Downloads/glbx-mdp3-*.ohlcv-1m.dbn.zst --stem GC

The cost is quoted first (`metadata.get_cost`, the same number the Databento
portal shows) and the download waits for a `y` unless `--yes` is given. The
data is requested a calendar year at a time so a long GC history does not
sit in memory as one frame.

Stitching rule, the same one the demo's reference files were built with:
calendar spreads (symbols containing `-`) are dropped; for each UTC day the
outright contract with the highest volume that day is the front month; the
chosen expiry never moves backward. Unadjusted, so every roll leaves a price
step, which is what a preset's `roll_adjust = true` removes at load.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date
from pathlib import Path

import polars as pl

DATASET = "GLBX.MDP3"
SCHEMA = "ohlcv-1m"
# The ranges the published results were run on. Two of the three start where
# the reference files start; gold goes back to the first day the dataset has.
DEMO_START = {"ES": date(2019, 8, 1), "NQ": date(2019, 8, 1), "GC": date(2010, 6, 6)}

CANDLE_SCHEMA = {
    "timestamp": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}
MONTH_CODE = {c: i + 1 for i, c in enumerate("FGHJKMNQUVXZ")}
OUTRIGHT_RE = re.compile(r"^([A-Z0-9]+?)([FGHJKMNQUVXZ])(\d)$")


def expiry_ord(symbol: str) -> int | None:
    """Orderable expiry (year*12+month) for an outright like ESH5; None otherwise."""
    m = OUTRIGHT_RE.match(symbol)
    if not m:
        return None
    # Single-digit contract years: the dataset starts in 2010 and the data
    # decides which decade applies, so anchor on the decade of the bar itself
    # when stitching (see stitch_continuous); here only the month order matters.
    return int(m.group(3)) * 12 + MONTH_CODE[m.group(2)]


def stitch_continuous(bars: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Front-month series: per UTC day the highest-volume outright wins and the
    chosen expiry never moves backward. Returns the series and the roll count."""
    bars = bars.filter(~pl.col("symbol").str.contains("-", literal=True))
    day_vol = (
        bars.group_by("symbol", day=pl.col("ts").dt.date())
        .agg(v=pl.col("volume").sum())
        .sort("day")
    )
    picks: dict[date, str] = {}
    current: tuple[int, int] | None = None  # (decade-anchored year, month)
    for (day,), group in day_vol.group_by("day", maintain_order=True):
        cands = []
        for r in group.iter_rows(named=True):
            m = OUTRIGHT_RE.match(r["symbol"])
            if not m:
                continue
            digit, month = int(m.group(3)), MONTH_CODE[m.group(2)]
            # Resolve the single-digit year against the bar's own decade: a
            # contract can expire up to a few years out, never in the past.
            year = day.year - day.year % 10 + digit
            if year < day.year:
                year += 10
            key = (year, month)
            if current is None or key >= current:
                cands.append((r["v"], key, r["symbol"]))
        if cands:
            v, key, sym = max(cands)
            current = key
            picks[day] = sym
    picks_df = pl.DataFrame(
        {"day": list(picks.keys()), "symbol": list(picks.values())},
        schema={"day": pl.Date, "symbol": pl.String},
    )
    out = (
        bars.with_columns(day=pl.col("ts").dt.date())
        .join(picks_df, on=["day", "symbol"])
        .drop("day")
        .sort("ts")
    )
    rolls = out.get_column("symbol").rle().len() - 1
    return out, rolls


def frame_from_store(store) -> pl.DataFrame:
    df = store.to_df(price_type="float", map_symbols=True).reset_index()
    ts_col = "ts_event" if "ts_event" in df.columns else "ts_recv"
    lf = pl.from_pandas(df).lazy()
    dtype = lf.collect_schema()[ts_col]
    ts = pl.col(ts_col)
    if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
        ts = ts.dt.convert_time_zone("UTC").dt.replace_time_zone(None)
    return (
        lf.select(
            ts=ts.cast(pl.Datetime("us")),
            symbol=pl.col("symbol").cast(pl.String),
            open=pl.col("open").cast(pl.Float64),
            high=pl.col("high").cast(pl.Float64),
            low=pl.col("low").cast(pl.Float64),
            close=pl.col("close").cast(pl.Float64),
            volume=pl.col("volume").cast(pl.Float64),
        )
        .drop_nulls(["ts", "open", "high", "low", "close"])
        .collect()
    )


def write_candles(bars: pl.DataFrame, out: Path) -> None:
    df = (
        bars.sort("ts")
        .unique(subset="ts", keep="last", maintain_order=True)
        .select(timestamp="ts", open="open", high="high", low="low", close="close", volume="volume")
        .cast(CANDLE_SCHEMA)
    )
    # Modest row groups so the loader's statistics-based skip has something
    # to work with on warmup-window reads.
    df.write_parquet(out, statistics=True, row_group_size=10_000)
    print(f"{out}: {df.height} bars, {df['timestamp'].min()} .. {df['timestamp'].max()} UTC")


def year_chunks(start: date, end: date):
    a = start
    while a < end:
        b = min(date(a.year + 1, 1, 1), end)
        yield a, b
        a = b


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("symbols", nargs="*", default=list(DEMO_START), help="root symbols (ES NQ GC)")
    ap.add_argument("--start", type=date.fromisoformat, help="default: the demo's start per symbol")
    ap.add_argument("--end", type=date.fromisoformat, help="exclusive; default: today")
    ap.add_argument("--out", type=Path, default=Path("data"))
    ap.add_argument("--yes", action="store_true", help="download without asking after the quote")
    ap.add_argument("--dbn", type=Path, help="convert a batch download instead of calling the API")
    ap.add_argument("--stem", help="output stem for --dbn (default: the root of the symbols found)")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    import databento as db

    if a.dbn:
        bars = frame_from_store(db.DBNStore.from_file(a.dbn))
        cont, rolls = stitch_continuous(bars)
        stem = a.stem or re.sub(r"[FGHJKMNQUVXZ]\d$", "", cont["symbol"][0])
        write_candles(cont, a.out / f"{stem}_1m.parquet")
        print(f"{stem}: {rolls} rolls")
        return

    key = os.environ.get("DATABENTO_API_KEY")
    if not key:
        sys.exit("DATABENTO_API_KEY is not set (https://databento.com, Account > API keys).")
    client = db.Historical(key)
    end = a.end or date.today()
    plan = []
    for sym in a.symbols:
        sym = sym.upper()
        start = a.start or DEMO_START.get(sym)
        if start is None:
            sys.exit(f"{sym}: not a demo symbol; pass --start")
        kw = dict(dataset=DATASET, symbols=[f"{sym}.FUT"], schema=SCHEMA, stype_in="parent",
                  start=start.isoformat(), end=end.isoformat())
        cost = client.metadata.get_cost(**kw)
        size = client.metadata.get_billable_size(**kw) / 1e6
        print(f"{sym}.FUT {SCHEMA} {start} .. {end}: ${cost:.2f} ({size:.0f} MB)")
        plan.append((sym, start, cost))
    print(f"total ${sum(c for _, _, c in plan):.2f}")
    if not a.yes and input("download? [y/N] ").strip().lower() != "y":
        sys.exit("aborted")

    for sym, start, _ in plan:
        parts = []
        for lo, hi in year_chunks(start, end):
            store = client.timeseries.get_range(
                dataset=DATASET, symbols=[f"{sym}.FUT"], schema=SCHEMA, stype_in="parent",
                start=lo.isoformat(), end=hi.isoformat(),
            )
            part = frame_from_store(store)
            print(f"  {sym} {lo} .. {hi}: {part.height} rows", file=sys.stderr)
            parts.append(part)
        bars = pl.concat(parts)
        cont, rolls = stitch_continuous(bars)
        write_candles(cont, a.out / f"{sym}_1m.parquet")
        print(f"{sym}: {rolls} rolls")


if __name__ == "__main__":
    main()
