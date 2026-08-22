"""Reconstruct round-trip trades from a stream of individual fills.

A fill is one execution. A trade — open through close — may span several of
them, and a single close can span several earlier opens. This module turns
one into the other with two passes:

  1. `_aggregate_orders` collapses fills sharing an order id into one logical
     order with a size-weighted average price. A resting limit filled in five
     pieces across three ticks is one order, not five trades.
  2. `pair_fills` walks those orders per instrument and matches closes against
     opens in FIFO order, splitting a close proportionally across every open
     lot it drains.

Realized P&L on a close fill is used verbatim rather than re-derived from
prices: a venue's own number accounts for fees and rounding that are lossy to
reproduce. Feed it whatever your execution source reports.

Nothing here performs I/O. Build `Fill` records from your own broker export,
CSV, or journal and pass the list in.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Fill:
    """One execution.

    `direction` is the four-state open/close verb the pairing keys off:
    "Open Long" | "Open Short" | "Close Long" | "Close Short". `side` is the
    raw buy/sell flag, carried through untouched for callers that need it.
    """
    coin: str
    side: str
    direction: str
    price: float
    size: float
    start_position: float
    closed_pnl: float
    time: datetime
    oid: int
    fee: float
    crossed: bool = False  # taker (crossed the spread) vs maker (resting)


@dataclass
class RoundTrip:
    coin: str
    direction: str     # "long" | "short"
    entry_time: datetime
    entry_price: float
    exit_time: datetime
    exit_price: float
    size: float
    closed_pnl: float
    fee: float
    outcome: str = "win"  # "win" | "loss" | "scratch"
    open_oid: int = 0
    close_oid: int = 0
    # Taker vs maker per leg.
    entry_crossed: bool = False
    exit_crossed: bool = False
    # The raw fills composing this round-trip, for a drill-down detail view.
    entry_fills: list[Fill] = field(default_factory=list)
    exit_fills: list[Fill] = field(default_factory=list)


@dataclass
class _Order:
    """One logical order: every fill sharing an oid, aggregated.

    Venues fill a single resting order in multiple pieces (same oid, same
    price or a sweep across a few ticks). Collapsing those into one order
    makes a trade show as a single entry/exit rather than one row per partial.
    """
    coin: str
    direction: str
    oid: int
    time: datetime          # last fill time (when the order fully filled)
    size: float             # total filled size
    notional: float         # size-weighted, for the VWAP price
    closed_pnl: float
    fee: float
    crossed: bool = False   # taker if ANY component fill crossed the spread
    fills: list[Fill] = field(default_factory=list)

    @property
    def price(self) -> float:
        return self.notional / self.size if self.size > 0 else 0.0


def _aggregate_orders(fills: list[Fill]) -> list[_Order]:
    """Collapse fills sharing a (coin, oid) into one logical order each.

    Returns orders sorted by the time of each order's EARLIEST fill, so FIFO
    pairing downstream sees opens before the closes that follow them.
    """
    orders: dict[tuple[str, int], _Order] = {}
    first_seen: dict[tuple[str, int], datetime] = {}
    for f in fills:
        key = (f.coin, f.oid)
        o = orders.get(key)
        if o is None:
            o = _Order(
                coin=f.coin,
                direction=f.direction,
                oid=f.oid,
                time=f.time,
                size=0.0,
                notional=0.0,
                closed_pnl=0.0,
                fee=0.0,
            )
            orders[key] = o
            first_seen[key] = f.time
        o.size += f.size
        o.notional += f.size * f.price
        o.closed_pnl += f.closed_pnl
        o.fee += f.fee
        o.crossed = o.crossed or f.crossed
        o.time = max(o.time, f.time)
        o.fills.append(f)
    return sorted(orders.values(), key=lambda o: (first_seen[(o.coin, o.oid)], o.oid))


@dataclass
class _OpenLot:
    """A still-open position lot waiting to be closed (one logical order)."""
    direction: str
    entry_time: datetime
    entry_price: float
    size_total: float
    size_remaining: float
    entry_fee: float
    open_oid: int
    entry_crossed: bool
    fills: list[Fill]


def pair_fills(fills: list[Fill]) -> list[RoundTrip]:
    """Pair Open/Close orders into round-trips, FIFO per instrument.

    Fills are first collapsed into logical orders by oid (`_aggregate_orders`)
    so a partially-filled entry, or a stop that sweeps several ticks, counts
    as a single order and therefore a single round-trip row. A close order
    spanning multiple earlier entry orders is still split per entry lot —
    those are genuinely distinct positions, with distinct entry prices.

    A close with no matching open (a position opened before the window the
    fills cover) is skipped rather than producing a half-trade.
    """
    orders = _aggregate_orders(fills)
    per_coin_opens: dict[str, deque[_OpenLot]] = defaultdict(deque)
    trips: list[RoundTrip] = []

    for o in orders:
        if o.direction.startswith("Open "):
            direction = "long" if o.direction == "Open Long" else "short"
            per_coin_opens[o.coin].append(_OpenLot(
                direction=direction,
                entry_time=o.time,
                entry_price=o.price,
                size_total=o.size,
                size_remaining=o.size,
                entry_fee=o.fee,
                open_oid=o.oid,
                entry_crossed=o.crossed,
                fills=o.fills,
            ))
        elif o.direction.startswith("Close "):
            # Drain the oldest open lot(s) to cover this close order.
            remaining = o.size
            # closed_pnl and fee on this close cover its ENTIRE size; split
            # them proportionally across the open lots they close.
            opens = per_coin_opens[o.coin]
            while remaining > 1e-12 and opens:
                lot = opens[0]
                take = min(lot.size_remaining, remaining)
                close_share = take / o.size if o.size > 0 else 0.0
                trip_pnl = o.closed_pnl * close_share
                close_fee = o.fee * close_share
                # Entry fee covers the whole lot; charge the portion closing now.
                entry_fee_share = (
                    lot.entry_fee * (take / lot.size_total) if lot.size_total > 0 else 0.0
                )
                if trip_pnl > 0:
                    outcome = "win"
                elif trip_pnl < 0:
                    outcome = "loss"
                else:
                    outcome = "scratch"
                trips.append(RoundTrip(
                    coin=o.coin,
                    direction=lot.direction,
                    entry_time=lot.entry_time,
                    entry_price=lot.entry_price,
                    exit_time=o.time,
                    exit_price=o.price,
                    size=take,
                    closed_pnl=trip_pnl,
                    fee=entry_fee_share + close_fee,
                    outcome=outcome,
                    open_oid=lot.open_oid,
                    close_oid=o.oid,
                    entry_crossed=lot.entry_crossed,
                    exit_crossed=o.crossed,
                    entry_fills=lot.fills,
                    exit_fills=o.fills,
                ))
                lot.size_remaining -= take
                remaining -= take
                if lot.size_remaining <= 1e-12:
                    opens.popleft()

    return trips
