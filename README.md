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

### Plugging it in

The binary does not know about your strategy, and you do not fork the engine
to teach it. You write a `StrategyFactory` and a three-line `main` in your own
crate:

```rust
use backtest_engine::{driver, knob};
use backtest_engine::params::{Knob, Value};
use backtest_engine::strategy::{BuildContext, Strategy, StrategyFactory};

static KNOBS: &[Knob] = &[
    knob!("lookback", U32, Value::U32(20), "Bars of history the signal reads."),
];

struct MyFactory;

impl StrategyFactory for MyFactory {
    fn name(&self) -> &str { "mine" }
    fn knobs(&self) -> &'static [Knob] { KNOBS }
    fn build(&self, ctx: &BuildContext<'_>) -> Box<dyn Strategy> {
        Box::new(MyStrategy::new(ctx.params.get_u32("lookback")))
    }
}

fn main() {
    driver::main(&[&MyFactory]);
}
```

That binary has the whole driver: config loading, data, fill lenses, fees,
the report, `--json-sidecar` for the visualizer. The shipped `backtest` binary
is exactly this with the example factory registered (`src/main.rs`).

A strategy file names its factory with a top-level `factory = "mine"` key, or
the run passes `--factory mine`; a binary with one factory registered needs
neither. The factory's knobs validate alongside the engine's: a preset may set
`lookback = 30`, and `lookbak = 30` is a load error with a did-you-mean hint,
same as a typo in `rr`. Built-in knob names are reserved, so a factory cannot
redefine what `max_hold` means. `ctx.engine` carries the file's `engine = ...`
path untouched, for a strategy whose configuration outgrows a flat knob bag.

Strategy code, presets and the factory binary can all live in a private crate
that depends on this one by path or version. Nothing about the engine needs
to be rebuilt or modified for it. Factories are linked in today; the trait is
object-safe and its inputs are plain data, which is deliberate, but loading
one from a separate library at runtime is not implemented.

Strategy parameters live in `config/strategy/*.toml`, validated against a knob
registry at load time. A key absent from the registry is a hard load error
rather than a silently ignored line — a typo'd knob name should not quietly
grade a different strategy than you meant to run.

### Scripting a strategy in Rhai

The same seam is reachable without a Rust crate. The shipped binary registers
a `rhai` factory that runs a strategy written as a [Rhai](https://rhai.rs)
script, selected from the strategy file:

```toml
factory = "rhai"
engine = "my_params.toml"          # optional; handed to the script as a path

[strategy]
script = "my_strategy.rhai"        # relative to this file
script_history = 500               # bars of history kept per timeframe

[script]                           # free-form, unvalidated; reaches init(cfg)
lookback = 20
```

A script implements the same things a native strategy does, with the same
information and the same honesty rules. It keeps its own state between bars,
sees every completed candle of its asset on the base timeframe and on every
`--timeframe` added to the run, reads candle history and other assets'
series, emits opportunities, decides their geometry, and manages the book
after each bar. Nothing is pre-computed for it: what to trade is entirely the
script's business. `scripts/rhai/macross.rhai` is the bundled example ported
line for line; run through `config/strategy/example_rhai.toml` it produces
the same trades as the native version.

```rhai
fn init(cfg) { #{ n: cfg.script.lookback, above: () } }   // state, bound to `this`

fn on_candle(c) {
    if c.tf != "1m" { return []; }
    let h = hist("1m");                  // newest first: h.close(0) == c.close
    if h.len < this.n { return []; }
    let o = opp("breakout", "1m", "bull", c.ts);
    o.entry = c.close; o.stop = c.close - h.atr(14); o.score = 1.0;
    [o]
}

fn admit(o, ctx) {                        // optional: geometry or a skip reason
    if o.score < ctx.min_score { return skip("below_min_score"); }
    take(o.entry, o.stop, o.entry + ctx.rr_target * (o.entry - o.stop))
}

fn on_bar_close(c, book) {                // optional: manage the book
    for t in book.open() { if t.r_open > 1.0 { book.set_stop(t.id, t.fill); } }
    for t in book.closed() { if t.stop_exit { /* arm a re-entry watch */ } }
}
```

What the engine supplies: `hist(tf)` (a ring of completed candles with
`open/high/low/close/ts(i)`, `atr(n)`, `highest(n)`, `lowest(n)`, `sma(n)`),
`market(asset)` (any loaded asset's series, read through windows that are
clamped to the current bar so a sibling's future stays out of reach),
`dt_utc/dt_offset/dt_tz` (wall-clock fields in a zone), `fee_in_r`,
`toml_load`, and the book: `closed()`, `open()`, `pending()`, `has_open()`,
`market_entry(#{...})`, `set_stop`, `set_tp`, `close`, `cancel`. Management
actions take effect on the next bar, as for a native strategy. Higher
timeframes arrive on the same `on_candle` stream with `c.tf` set, after the
base bar that closes them has been processed.

Rhai passes arguments by value, so state is mutated through `this`
(`this.levels.push(x)`) and helpers that mutate a sub-object are written as
methods on it. Two things to know before trusting a script's numbers: the
engine builds Rhai with its `unchecked` feature because the default float
comparison is approximate (two prices a few ulps apart compare equal, which
changes which bar a crossing fires on), and a script runs on the order of
ten to a hundred times slower per bar than the same logic in Rust, depending
on how much per-bar work it does.

### Contracts and continuous futures

Flat per-contract fees are declared in the strategy file, so a run's fee
model is readable from its config:

```toml
[[contract]]
asset = "EXAMPLE"
point_value = 5.0     # dollars per point, per contract
round_turn = 1.90     # all-in fee per contract, in and out
schedule = "futures"  # or "futures_full"
```

Declaring any contract selects the matching flat schedule unless
`fee_schedule` says otherwise. Without one, every asset is priced as basis
points of notional at placeholder rates. For an index future quoted in the
thousands with a stop a few points away, that bps charge is more than an
order of magnitude larger than the real per-contract fee, and it decides
the sign of the result.

A continuous futures file stitched unadjusted at contract rolls carries a
price jump at each roll. `roll_adjust = true` (or `--roll-adjust`) removes
them at load: each gap detected at a UTC-day boundary is added to every bar
before it, anchoring the series at the latest contract's prices. It rewrites
prices, so it is off by default.

## Trade management

Independent of signal logic, the harness can move a stop to breakeven at a
chosen R, trail a locked-in profit, take partial size at a target, and close a
position after a maximum hold. These are configured per strategy and simulated
on the same path as the entry fill, so a partial that never filled does not
quietly bank profit.

A strategy that wants its own management logic implements
`Strategy::on_bar_close`, called once per completed base bar after fills and
exits have been resolved. Its `Book` argument lists what closed on that bar
and what is open or resting, and accepts actions: move a stop or target,
close a position at the bar's close, cancel a resting entry, or open a
position at the bar's close as a taker fill. A moved stop keeps the planned
`|entry - stop|` as the R unit, so moving it to breakeven books a 0R exit
rather than rescaling the trade.

## Layout

    src/            engine: data, fills, fees, accounting, config
    src/driver.rs   the replay driver: CLI, config, threads, report
    src/strategy.rs the trait, the factory seam, and the types crossing them
    src/rhai_strategy.rs  the Rhai factory and the script-facing API
    scripts/rhai/   example scripts
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
