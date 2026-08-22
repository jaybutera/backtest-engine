"""Load an engine run's JSON sidecar into the visualizer's trade frame.

The replay binary writes a report (`--json-sidecar data/backtest_trades.json`)
whose `trades` array carries one entry per decided trade: entry/stop/tp levels,
the fill price, the close timestamp, and a P&L in R-multiples. There is no exit
PRICE in that record — R is the engine's unit of account — so we reverse-compute
one from the fill anchor and the gross R so chart overlays line up with candles.

`_SCHEMA` is the single column contract every consumer downstream reads. It has
slots for fields a live execution log could fill (size, fees, order ids); a
backtest leaves those null, and the columns exist so a run and a broker-derived
frame can be diffed column-for-column without a schema migration.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

DEFAULT_BACKTEST_PATH = Path("data/backtest_trades.json")


_SCHEMA: dict[str, type] = {
    "entry_time": pl.Datetime(time_unit="us", time_zone="UTC"),
    "asset": pl.Utf8,
    "direction": pl.Utf8,
    "entry_price": pl.Float64,
    "exit_time": pl.Datetime(time_unit="us", time_zone="UTC"),
    "exit_price": pl.Float64,
    "size": pl.Float64,
    "closed_pnl": pl.Float64,
    "fee": pl.Float64,
    "outcome": pl.Utf8,
    "open_oid": pl.Int64,
    "close_oid": pl.Int64,
    # Taker (crossed the spread) vs maker (resting limit hit) per leg. Null
    # for backtest trades, which price fills through the fill lens instead.
    "entry_crossed": pl.Boolean,
    "exit_crossed": pl.Boolean,
    # Engine-side context carried through from the sidecar.
    "opportunity_id": pl.Utf8,
    "score": pl.Float64,
    # Entry level at signal time; `entry_price` is where the fill actually
    # landed. They differ whenever the fill lens models slippage or a resting
    # limit filled away from its placed level.
    "intended_entry": pl.Float64,
    # Size the configured risk asked for. `size` is what actually filled, so
    # size/intended_size < 1 means the trade carried less than a full R.
    "intended_size": pl.Float64,
    "stop_price": pl.Float64,
    "tp_price": pl.Float64,
    "closed_via": pl.Utf8,
    # Free-form anomaly marker + its detail, for consumers that reconcile a
    # run against an execution record. Null for ordinary backtest trades.
    "flag": pl.Utf8,
    "flag_detail": pl.Utf8,
    # P&L in R-multiples — the backtest's native unit.
    "r_pnl": pl.Float64,
    # Signal that opened the trade (e.g. fvg_bull).
    "signal_type": pl.Utf8,
    # Present when the strategy compounds (risk_frac > 0): account balance
    # after this trade, and its dollar P&L at the size it was given.
    "equity": pl.Float64,
    "pnl_dollars": pl.Float64,
}


def empty_frame() -> pl.DataFrame:
    """An empty frame carrying the full `_SCHEMA` column contract."""
    return pl.DataFrame(schema=_SCHEMA)


_DIRECTION_MAP = {"bull": "long", "bear": "short", "long": "long", "short": "short"}
_RESULT_MAP = {
    "win": "win",
    "loss": "loss",
    "inconclusive": "scratch",
    "scratch": "scratch",
}


def _parse_naive_utc(s: str | None) -> datetime | None:
    """Parse the sidecar's naive ISO timestamps as UTC."""
    if not s:
        return None
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def load_backtest_trades(path: Path = DEFAULT_BACKTEST_PATH) -> pl.DataFrame:
    """Read a JSON sidecar and return its trades as a `_SCHEMA` DataFrame.

    A missing file yields an empty frame rather than raising, so a viz that
    starts before the first run renders an empty page instead of a 500.
    """
    if not path.exists():
        return empty_frame()
    with path.open() as f:
        report = json.load(f)
    raw = report.get("trades", [])
    rows = []
    for t in raw:
        entry = t.get("entry")
        stop = t.get("stop")
        direction = _DIRECTION_MAP.get(
            str(t.get("direction", "")).lower(), t.get("direction")
        )
        r_pnl = t.get("r_pnl")
        # A resting entry limit can fill many bars after the signal:
        # `opened_at` is signal time, `filled_at` the booking bar. Chart
        # markers want the fill point, so prefer it — same for the fill price
        # vs the placed level, which stays in `intended_entry`.
        fill_price = t.get("fill")
        # Reverse-compute the exit price from the R-multiple. The move is
        # anchored at the FILL and measured in GROSS R (net r_pnl folds fees
        # into the distance, which would paint a stop-out past the actual
        # stop); risk stays |entry − stop|, the planned R even under slippage.
        exit_price = None
        anchor = fill_price if fill_price is not None else entry
        gross_r = t.get("gross_r_pnl", r_pnl)
        if (
            anchor is not None
            and entry is not None
            and stop is not None
            and gross_r is not None
        ):
            risk = abs(entry - stop)
            exit_price = (
                anchor + gross_r * risk if direction == "long" else anchor - gross_r * risk
            )
        rows.append({
            "entry_time": _parse_naive_utc(t.get("filled_at") or t.get("opened_at")),
            "asset": t.get("asset"),
            "direction": direction,
            "entry_price": fill_price if fill_price is not None else entry,
            "exit_time": _parse_naive_utc(t.get("closed_at")),
            "exit_price": exit_price,
            "size": None,
            "closed_pnl": None,
            "fee": None,
            "outcome": _RESULT_MAP.get(str(t.get("result", "")).lower()),
            "open_oid": None,
            "close_oid": None,
            "entry_crossed": None,
            "exit_crossed": None,
            "opportunity_id": t.get("opportunity_id") or None,
            "score": t.get("score"),
            "intended_entry": entry,
            "intended_size": None,
            "stop_price": stop,
            "tp_price": t.get("tp"),
            "closed_via": None,
            "flag": None,
            "flag_detail": None,
            "r_pnl": r_pnl,
            "signal_type": t.get("signal_type"),
            "equity": t.get("equity"),
            "pnl_dollars": t.get("pnl_dollars"),
        })
    if not rows:
        return empty_frame()
    return pl.DataFrame(rows, schema=_SCHEMA).sort("entry_time", descending=True)
