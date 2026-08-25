# backtest-engine

A backtest harness for intraday trading strategies, with a web UI for reading
the results. Written in Rust; the visualizer is Python.

The engine is the machinery around a strategy: parquet candle loading,
timeframe aggregation, a fill simulator you can swap between optimistic and
pessimistic models, a fee schedule, R-multiple accounting, and a warmup-jitter
ensemble runner. You supply the signal logic behind a small trait; the harness
handles everything downstream of "I want to enter here, with this stop and
this target." One real strategy ships with it as the demo, with the script that fetches
the futures bars it runs on and the results it produced on them.

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
    scripts/backtest.sh

That runs the demo strategy, `config/strategy/rsi_atr.toml`, on every
contract it lists (`data/ES_1m.parquet`, `NQ`, `GC`), prints the trade table
with a per-contract breakdown, and writes `data/backtest_trades.json`. `-a ES`
restricts it to one leg. The bars come from Databento with your own key (see
The demo); with no key, `uv run scripts/synth-data.py` writes a synthetic
file and `scripts/backtest.sh -a SYNTH` runs on that. Then:

    uv run viz serve

and open the printed URL to page through trades on a chart, with the equity
curve and each trade's entry, stop, target and fill marked. `--from` and
`--to` restrict the report window; `BT_STRATEGY` and `BT_FILL` pick another
preset or lens.

## The demo

The demo strategy is `scripts/rhai/rsi_atr.rhai`: an RSI-ATR pullback from
the private system this engine was extracted from, with the parameters each
leg ran with. It aggregates the 1-minute stream into working-timeframe
windows. At the first candle of a new window it buys when the last closed bar
is above its EMA, RSI 14 has just re-crossed the buy level from below, and
that bar closed up and above the previous bar's high; sells are the mirror.
The stop sits one ATR(14) from the signal close, the target at 2.5 R, one
position at a time. Entries are market-on-open orders;
`config/fill/market_on_open.toml` races them against the rest of the bar and
books a gapped exit at the open. The preset, `config/strategy/rsi_atr.toml`,
lists the three contracts the strategy ran on, each with the parameters it
ran with:

| Asset | Contract | Window | EMA | RSI levels | Cooldown |
|---|---|---|---|---|---|
| `ES` | E-mini S&P 500, $50/pt, $4.30 round turn | 30 min | 200 | 40 / 60 | 8 h after 2 losses |
| `NQ` | E-mini Nasdaq-100, $20/pt, $4.30 round turn | 15 min | 200 | 40 / 60 | none |
| `GC` | COMEX gold, $100/pt, $5.30 round turn | 30 min | 100 | 35 / 65 | none |

The shared keys sit in the preset's `[script]` table and each leg's changes
in `[script.per_asset.<ASSET>]`; the engine builds one script instance per
asset, and the script's `init()` merges its asset's block over the shared
keys. The legs run on their own threads and the report merges their ledgers,
with a per-asset table; `-a ES` runs one leg on its own.

### The data

The bars are continuous front-month 1-minute series from Databento's
`GLBX.MDP3` dataset (CME Globex), schema `ohlcv-1m`, requested with parent
symbology (`ES.FUT`, `NQ.FUT`, `GC.FUT`) and stitched unadjusted at a
volume-based daily roll; `roll_adjust = true` in the preset removes the roll
steps at load. Databento licenses the bars to the account that downloads
them, so the repo ships the fetch script and not the files:

    export DATABENTO_API_KEY=...
    uv run scripts/fetch-databento.py        # ES and NQ from 2019-08-01, GC from 2010-06-06

The script quotes the cost before it downloads anything. Quoted by
Databento's `metadata.get_cost` on 2026-08-24 at the dataset's
`ohlcv-1m` rate of $70 per GB: ES $14.52, NQ $13.17, GC $56.90 from
2010-06-06 (or $28.73 from 2019-08-01), for the full ranges through that
day. ES and NQ start where the reference files start, GC on the dataset's
first day; every window runs to the day you fetch, and there is no chosen
end date.

### The results

Output of the preset above, one leg at a time, on bars through 2026-08-23,
fees included, with the commands that produce them:

    scripts/backtest.sh -a ES --from 2019-09-01 --warmup-days 60
    scripts/backtest.sh -a NQ --from 2019-09-01 --warmup-days 60
    scripts/backtest.sh -a GC --from 2010-07-01 --warmup-days 60

| Leg | Report window | Candles | Trades | Win rate | Gross, R | Fees, R | Net, R | Deepest drawdown, R |
|---|---|---|---|---|---|---|---|---|
| ES | 2019-09-01 to 2026-08-23 | 2,492,102 | 867 | 29.9% | +36.8 | 10.1 | +26.7 | -34.9 |
| NQ | 2019-09-01 to 2026-08-23 | 2,492,071 | 1,920 | 30.2% | +105.6 | 20.5 | +85.1 | -56.1 |
| GC | 2010-07-01 to 2026-08-23 | 5,649,974 | 349 | 35.8% | +90.6 | 6.7 | +83.9 | -22.4 |

The drawdown is the deepest fall of the cumulative net R curve from its
running peak. The `--from` dates sit a month after the first bar so the
indicators are seeded before the report starts; `--warmup-days` loads the
bars before it. The no-argument quick-start run covers the full range of
every file instead, so it also counts the trades from that first month
(ES 877, NQ 1,940, GC 350) and reports the three legs merged. Refetching
extends the window and the numbers move with it.
The parameters were not fitted to these windows: they are the ones the legs
ran with, and `scripts/sweep.py` will show you what a fit looks like. Every
leg's win rate is around a third, which is what a 2.5 R target buys; the
result lives in the payoff, not the hit rate, and the ensemble section is how
to find out how much of it survives a shifted start.

The demo exists to show the harness working on a real strategy. It is not a
recommendation of the strategy.

## Supplying data

Nothing in `data/` is tracked. `scripts/fetch-databento.py` fills it for the
demo, and `--start`, `--end` and other root symbols (`SI`, `CL`, ...) fetch
what you want from the same dataset; `--dbn` converts a batch download you
already have. With no key, `uv run scripts/synth-data.py` writes a random-walk
file shaped like an equity session and `scripts/backtest.sh -a SYNTH` runs on
it. It carries no information, which is what makes it a control: a strategy
that earns on it is fitting noise.

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

No market data ships with this repo. It is licensed per-user by the vendor
and none of it can be redistributed; the fetch script is the substitute.

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

`src/example_strategy.rs` is a deliberately naive moving-average crossover
included as a demonstration of the interface and as a control for engine
changes (`config/strategy/example.toml` runs it on the ES bars). It
is not an edge, and it is commented as such.

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

A market entry fills at the bar's close by default. `market_entry(#{...,
at: "open"})` is a market-on-open order: it fills at the bar's open and is
raced against the rest of that bar (stop first, by the lens tie-break),
which is what an order decided at the previous close and sent at the open
gets. The engine cannot check that the decision did not read the bar it
fills in; that is the script's contract. A script that only ever books
market orders can define `on_bar_close` alone and skip `on_candle`.

### Windows: script-owned timeframes

A strategy that acts at bar boundaries of its own timeframe, on its own
grid, pays the interpreter on every base candle if it aggregates in the
script: a 1.8M-candle run spent 4.4 s of 5.5 s updating a forming bar. A
`window(secs, anchor_secs, keep)` in the state is aggregated natively
instead, grouping base candles by `(ts + anchor) div secs` (so a 4-hour
window can start at 22:00 UTC rather than midnight), keeping the last
`keep` closed bars, and the script is called in `on_bar_close` only on a
candle that rolled a window, closed a trade, or while `w.wake(true)`
stands. Bars are read MQL-style, shift 1 being the last closed one:
`w.o(i)`, `w.h(i)`, `w.l(i)`, `w.c(i)`, `w.ts(i)`, `w.bar(i)`; `w.rolled`,
`w.start`, `w.closed`, `w.count`, `w.forming`, `w.forming_end`,
`w.finalize_through(t)` close a forming bar by the clock; `w.pivot_high(i,
n)` / `w.pivot_low(i, n)`, `w.recent_pivots(n, scan)` and
`w.first_pivot(highs, n, from, to)` are the swing queries. The same RSI
strategy on a window runs in 1.8 s on that file, against 0.96 s for an
empty script.

Rhai passes arguments by value, so state is mutated through `this`
(`this.levels.push(x)`) and helpers that mutate a sub-object are written as
methods on it. One thing to know before trusting a script's numbers: the
engine builds Rhai with its `unchecked` feature because the default float
comparison is approximate (two prices a few ulps apart compare equal, which
changes which bar a crossing fires on).

### Native scanner services for scripts

A script that scans its own data structures on every bar pays the
interpreter for every element of every scan. A strategy of the
sweep-and-retrace kind keeps a registry of a few hundred price levels and a
few hundred fair-value gaps and walks both on every candle; written that way
as a script, a 910k-candle run took 38 minutes where the same strategy in
Rust took 12 seconds. The per-bar bookkeeping is not where a strategy's
identity lives, so the engine offers it natively, parameterized by the
script. `src/liquidity.rs` is a scanner with a level registry
(cluster-merged, decaying, refreshed on retests), a sweep detector over it,
a gap tracker with mitigation and inversion, session and previous-period
levels, swing / gap / structure-break primitives, equal-high / equal-low
clustering, a ranked draw-on-liquidity map and a UTC day tracker. Every
table it uses (source significance, timeframe multipliers, session hours,
caps, cadences) comes from the script.

A script opts in by putting a scanner in its state and replacing
`on_candle` with event hooks; the engine steps the scanner on every candle
and calls the script only on bars that carry a sweep or a structure break,
and on bars the script asked to be woken for:

```rhai
fn init(cfg) {
    #{ scan: scanner(#{ primitives: #{ atr_period: 14, swing_lookback: 5, … },
                        significance: #{ base: #{ pdh: 4.0, … }, … },
                        levels: #{ cluster_atr_tolerance: 0.5, decay_candles: 300, … },
                        sweep: #{ noise_atr: 1.0, max_multi_candle: 3 },
                        sessions: #{ enabled: true, tz_offset_secs: -18000, sessions: [ … ] },
                        fvg: #{ cap: 500 } }),
       watching: [] }
}

fn on_bar(c, atr, sweeps, breaks) {       // detection, before the draw map rebuild
    for sw in sweeps { this.watching.push(#{ sweep: sw, … }); }
    let f = this.scan.fvg_first(c.tf, "bull", false, since_ts, 0.0, -inf(), inf(), px, atr * 0.5);
    let t = this.scan.find_target("bull", entry, stop, 2.0, 0.0);
    this.scan.wake(c.tf, this.watching.len() > 0);   // keep calling me on this timeframe
    [ #{ entry: entry, stop: stop, target: t.price } ]   // candidates for emit()
}

fn emit(c, atr, cand) {                   // scoring, after the rebuild; () drops it
    let o = opp("sweep_fvg", c.tf, "bull", c.ts);
    o.entry = cand.entry; o.stop = cand.stop; o.target = cand.target; o.score = 3.0;
    o
}
```

`scripts/rhai/sweep_fvg.rhai` with `config/strategy/example_sweep.toml` is
the bundled demonstration: a swept session or period level, the first gap
after it, entered on the retrace, targeting the nearest opposing level. The
queries a script has: `significance`, `add_level`, `level`,
`levels_beyond`, `nearest_above` / `nearest_below`, `find_target`, `fvg`,
`fvg_status`, `fvgs_since`, `fvg_first`, `draw_map`, `draw_bias`, `day`,
`day_high` / `day_low`, `last_atr`, `candle_count`, `stats`; an `on_day(c,
prev)` hook reports each completed UTC day; `wake_book(true)` asks for
`on_bar_close` on every bar rather than only on bars that closed a trade.
The module docs of `src/rhai_strategy.rs` give the exact per-candle order.
`BACKTEST_RHAI_STATS=1` prints per-hook call counts and time at the end of a
run, which is how to find out what a script is still paying for.

With the scanner, the 38-minute strategy above runs in 11 seconds on the
same data with the same 228 trades, the same fills and the same net result
to the last digit; its script shrank from 2,000 lines to 1,000, and what
remained is the part that was the strategy: which sweeps to act on, the
entry models, the bias, the scoring, the gates, and the re-entry rule.

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

A fill lens can also decide what a gapped exit costs. By default a stop
that the bar opens through is booked at the stop price, and a target the
bar opens through at the target. `exit_gap_at_open = true` books both at
the bar's open instead, which is where a resting stop or target order
actually fills when the market jumps over it; the stop is checked before
the target.

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
    scripts/rhai/   the demo strategy (rsi_atr.rhai) and the example scripts
    viz/            web UI (Python)
    config/fill/    fill lenses
    config/strategy/ strategy presets
    scripts/        backtest and ensemble runners, the Databento fetcher,
                    the synthetic generator, the parameter sweep

## Provenance

This was extracted from a private trading system. Most of the signal logic
and the research behind it stayed behind; what is here is the harness it ran
on, plus the one strategy released as the demo. Extraction was mechanical, so if you find a stray
reference to something that is not in this repo, it is an oversight — please
open an issue. `scripts/check-leaks.sh` runs in CI to keep that from
happening.

## License

MIT.

Nothing here is trading advice, and a backtest is not a prediction. The fill
lenses exist because I do not trust backtest results, including my own.
