# backtest-engine

A backtest harness for intraday trading strategies, with a web UI for reading
the results. Written in Rust; the visualizer is Python.

The engine does not come with a strategy. It comes with the machinery around
one: parquet candle loading, timeframe aggregation, a fill simulator you can
swap between optimistic and pessimistic models, a fee schedule, R-multiple
accounting, and a warmup-jitter ensemble runner. You supply the signal logic
behind a small trait; the harness handles everything downstream of "I want to
enter here, with this stop and this target."

## Why the fill model gets its own axis

Most backtest harnesses fill your order the moment price touches your limit.
Live, that is not what happens. Your order was placed a candle late, the wick
that "filled" you was a single print you never saw, and the stop you thought
sat at a price actually filled some distance past it once the trigger cascade
ran.

The gap between those two stories is usually larger than the difference
between two strategies. So a run here is three independent axes:

    strategy  ×  fill lens  ×  dataset

The **fill lens** is a config file, not a code path. `limit_only.toml` assumes
every resting limit fills — the optimistic bound. `market_hybrid.toml` models a
resting GTC order with a chase watchdog: it can fill at the open, aggress past
the limit and pay taker, seed-and-chase, or abandon the entry if price ran too
far. `worst_case_bound.toml` is the pessimistic floor. Grading the same
strategy through several lenses tells you how much of a result is edge and how
much is fill assumption.

How much they disagree depends on your entries. A strategy entering at market
sees the lenses move fill prices and fees only. One that rests a limit away
from price sees them disagree about which trades happened at all, which is much
the larger effect — a missed winner costs more than a slightly worse fill.

The **dataset** axis maps assets onto candle files, so the same strategy can be
run against a different venue's data with one flag rather than a fork.

## No single run is citable

A backtest's start date is an arbitrary choice, and it is load-bearing. Shift
the warmup window by a few days and detector state at the first real candle
differs, which changes which setups exist at all.

The ensemble runner exists because of that. It runs the same backtest at
several warmup offsets and reports the median and spread:

    scripts/ensemble-backtest.sh --from 2024-01-01 --to 2024-12-31

Use it for any number you plan to believe. A single run is a sample from a
distribution, and the spread is often wide enough to flip a result's sign.

## Quick start

    cargo build --release
    scripts/backtest.sh --from 2024-01-01 --to 2024-12-31

That runs the bundled example strategy over whatever candle data you have in
`data/`, and writes `data/backtest_trades.json`. Then:

    uv run viz serve

and open the printed URL to page through trades on a chart, with the equity
curve and each trade's entry, stop, target and fill marked.

## Supplying data

The engine reads parquet, one file per asset per interval, named
`data/<ASSET>_1m.parquet`. It needs five columns and resolves their names
case-insensitively:

| Column | Accepted names |
|---|---|
| timestamp | `timestamp`, `ts`, `time`, `datetime`, `date` |
| open | `open`, `o` |
| high | `high`, `h` |
| low | `low`, `l` |
| close | `close`, `c` |

Volume (`volume`, `vol`, `v`) is optional and defaults to zero. Asset names are
arbitrary strings — nothing about the symbol is hardcoded.

Tick data is optional. If `data/<ASSET>_1m_ticks.parquet` exists, the tick fill
lens resolves entries, stops and targets against real millisecond prints
instead of guessing intrabar order. Without it, tick mode degrades to the OHLC
model and says so.

No market data ships with this repo. Most of it is licensed per-user and none
of it is mine to redistribute.

## Writing a strategy

Implement one trait:

```rust
pub trait Strategy {
    fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity>;
}
```

An `Opportunity` is a proposed trade: direction, entry, stop, target, a score,
and whatever metadata you want carried through to the report. Everything after
that — whether the order fills, at what price, what it costs in fees, how the
position is managed, what it earns in R — belongs to the harness.

`src/example_strategy.rs` is a deliberately naive implementation included so
the engine runs end to end out of the box. It is a demonstration of the
interface. It is not an edge, and it is commented as such.

Strategy parameters live in `config/strategy/*.toml`, validated against a knob
registry at load time. A key absent from the registry is a hard load error
rather than a silently ignored line — a typo'd knob name should not quietly
grade a different strategy than you meant to run.

## Trade management

Independent of signal logic, the harness can move a stop to breakeven at a
chosen R, trail a locked-in profit, take partial size at a target, and close a
position after a maximum hold. These are configured per strategy and simulated
on the same path as the entry fill, so a partial that never filled does not
quietly bank profit.

## Layout

    src/            engine: data, fills, fees, accounting, config
    src/strategy.rs the trait, and the types crossing it
    viz/            web UI (Python)
    config/fill/    fill lenses
    config/strategy/ strategy presets
    scripts/        backtest and ensemble runners

## Provenance

This was extracted from a private trading system. The signal logic, its tuned
parameters, and the research behind them stayed behind; what is here is the
harness they ran on. Extraction was mechanical, so if you find a stray
reference to something that is not in this repo, it is an oversight — please
open an issue. `scripts/check-leaks.sh` runs in CI to keep that from
happening.

## License

MIT.

Nothing here is trading advice, and a backtest is not a prediction. The fill
lenses exist because I do not trust backtest results, including my own.
