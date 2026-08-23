//! Strategies written in [Rhai](https://rhai.rs): the whole strategy, not a
//! verdict, lives in a script.
//!
//! # What a script can do
//!
//! A script implements the same three things a native [`Strategy`] does,
//! with the same information: it sees every completed candle of its asset
//! on every configured timeframe, keeps whatever state it likes between
//! bars, emits opportunities, decides their geometry, and manages the book
//! after each bar. The engine supplies data access (candle history per
//! timeframe, other assets' series, the clock), generic helpers (ATR,
//! extremes, fees, time zones, TOML), the order API (limit entries via
//! opportunities, market entries, stop and target changes, closes,
//! cancels), and, for scripts that want it, the native scanner services of
//! [`crate::liquidity`]. Everything that decides what to trade is the
//! script's.
//!
//! # Script shape: per bar
//!
//! ```rhai
//! fn init(cfg) {            // optional: build the state map. cfg carries
//!     #{ bars: 0 }          // asset, params, engine, timeframes, history.
//! }
//! fn on_candle(c) {          // `this` is the state map; returns an array
//!     this.bars += 1;        // of opportunities (often empty).
//!     let h = hist(c.tf);    // candle history for this timeframe, newest
//!     if h.len < 20 { return []; }   // first: h.close(0) is `c.close`.
//!     []
//! }
//! fn admit(o, ctx) {         // optional: geometry or a skip reason.
//!     take(o.entry, o.stop, o.entry + ctx.rr_target * (o.entry - o.stop))
//! }
//! fn on_bar_close(c, book) { // optional: manage the book after the bar.
//!     for t in book.closed() { if t.stop_exit { /* … */ } }
//! }
//! ```
//!
//! # Script shape: on events, with the native scanner
//!
//! A script that would otherwise rescan its own levels and gaps on every
//! bar puts a [`Scanner`] in its state under `scan` and replaces
//! `on_candle` with event hooks. The engine then steps the scanner on every
//! candle itself and calls the script only when there is something to
//! decide:
//!
//! ```rhai
//! fn init(cfg) {
//!     #{ scan: scanner(#{ primitives: #{ … }, significance: #{ … }, levels: #{ … },
//!                        sweep: #{ … }, fvg: #{ … }, sessions: #{ … }, draw: #{ … } }),
//!        watching: [] }
//! }
//! fn on_bar(c, atr, sweeps, breaks) {   // bars with a sweep or a structure
//!     for sw in sweeps { … }             // break, plus bars the script asked
//!     let f = this.scan.fvg_first(c.tf, "bull", false, since, 0.0, -inf(), inf(), px, atr * 0.5);
//!     this.scan.wake(c.tf, this.watching.len() > 0);   // keep being called
//!     [ #{ entry: …, stop: … } ]          // candidates for emit()
//! }
//! fn emit(c, atr, cand) {               // after the draw map rebuild:
//!     let o = opp("sweep_fvg", c.tf, "bull", c.ts);   // score and emit, or ()
//!     o.entry = cand.entry; o
//! }
//! fn on_day(c, prev) { … }              // optional: each completed UTC day
//! ```
//!
//! The order within one candle is: scanner step (history, ATR, day
//! tracker, sessions, swings / gaps / breaks, levels, equal levels, gap
//! lifecycle, sweeps) → `on_day` → `on_bar` → draw map rebuild when due →
//! `emit` per candidate → registry maintenance (retest refresh, decay,
//! pruning). `on_bar` runs when the bar carries a sweep or a break, or the
//! script has `wake`d that timeframe (`"*"` for all). With a scanner,
//! `on_bar_close` runs only on bars that closed a trade, or on every bar
//! while `wake_book(true)` stands. A candidate that is already an `opp(…)`
//! value is emitted as is, so a script without `emit` returns opportunities
//! from `on_bar` directly.
//!
//! Scanner queries: `significance(src, tf, touch)`, `add_level(price, side,
//! src, tf, ts, touch, atr)`, `level(id)`, `levels_beyond(side, price,
//! min_sig)`, `nearest_above(price)` / `nearest_below(price)`,
//! `find_target(dir, entry, stop, min_rr, min_sig)`, `fvg(id)`,
//! `fvg_status(id)`, `fvgs_since(tf, dir, inverted, since_ts)`,
//! `fvg_first(tf, dir, inverted, since_ts, min_gap, zone_lo, zone_hi,
//! clear_from, min_clear)`, `draw_map()`, `draw_bias()`, `day()`,
//! `day_high()` / `day_low()`, `last_atr()`, `candle_count()`, `stats()`.
//! Setting `BACKTEST_RHAI_STATS=1` prints per-hook call counts and time per
//! asset at the end of a run.
//!
//! The state map is bound to `this` in every hook, so a script mutates it
//! in place (`this.levels.push(x)`) rather than passing it around; Rhai
//! passes function arguments by value, so a helper that needs to mutate a
//! sub-object is written as a method and called on it
//! (`this.registry.add(lvl)` with `fn add(lvl) { this.levels.push(lvl) }`).
//!
//! # Script shape: at window boundaries
//!
//! A strategy that decides once per bar of its own timeframe puts one or
//! more `window(secs, anchor, keep)` aggregators in its state and defines
//! `on_bar_close` alone. The engine steps every window natively on each
//! base candle and calls the script only on candles that rolled a window,
//! closed a trade, or while a window's `wake(true)` stands:
//!
//! ```rhai
//! fn init(cfg) { #{ w: window(1800, 0, 2), in_pos: false } }
//! fn on_bar_close(c, book) {
//!     for t in book.closed() { this.in_pos = false; }
//!     if !this.w.rolled { return; }          // c is the first candle of a window
//!     let b = this.w.closed;                 // the bar it closed, or ()
//!     …                                      // decide on closed bars only, then
//!     book.market_entry(#{ id: "x" + this.w.start, signal_type: "s", direction: "bull",
//!                          stop: sl, tp: tp, at: "open" });   // fill at c.open
//! }
//! ```
//!
//! `market_entry` with `at: "open"` is a market-on-open fill, raced against
//! the same bar; the default `at: "close"` fills at the close and is raced
//! from the next bar.
//!
//! # Selecting a script
//!
//! A strategy file names the factory and the script:
//!
//! ```toml
//! factory = "rhai"
//! engine = "my_strategy_config.toml"   # optional, handed to the script
//! [strategy]
//! script = "my_strategy.rhai"           # relative to the strategy file
//! script_history = 500                  # bars of history kept per timeframe
//! [script]                              # free-form, reaches init(cfg) as
//! lookback = 20                         # cfg.script, unvalidated
//! ```
//!
//! Higher timeframes come from `--timeframe` on the command line (the
//! script sees each as it closes, on the same `on_candle` stream, with
//! `c.tf` telling them apart).
//!
//! # Honesty
//!
//! Cross-asset reads go through [`MarketData`], whose windows are clamped
//! to the bar being processed, so a script cannot read the future of a
//! sibling series. Its own history is a ring of completed bars. Management
//! actions take effect on the next bar, as for native strategies.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, TimeZone, Timelike};
use rhai::{Array, CallFnOptions, Dynamic, Engine, EvalAltResult, ImmutableString, Map, Scope, AST};

use crate::fees::{self, EntryFeeSide};
use crate::knob;
use crate::liquidity::{self, BarEvents, Scanner, ScannerCfg, Side};
use crate::models::{
    asset_name, sig_type_id, sig_type_name, tf_id, tf_name, Candle, Direction, Opportunity,
    PaperTrade,
};
use crate::params::{Knob, Value};
use crate::strategy::{
    AdmitContext, Book, BuildContext, Decision, SkipReason, Strategy, StrategyFactory,
    TakeParams,
};

/// The knobs the Rhai factory adds to `[strategy]`.
pub static RHAI_KNOBS: &[Knob] = &[
    knob!(
        "script",
        Str,
        Value::Str(String::new()),
        "Path of the Rhai strategy script, relative to the strategy file's directory (or the working directory)."
    ),
    knob!(
        "script_history",
        U32,
        Value::U32(500),
        "Completed bars of history kept per timeframe for the script's `hist()` calls."
    ),
];

/// Builds [`RhaiStrategy`] from a preset. Registered under the name `"rhai"`.
pub struct RhaiFactory;

impl StrategyFactory for RhaiFactory {
    fn name(&self) -> &str {
        "rhai"
    }

    fn knobs(&self) -> &'static [Knob] {
        RHAI_KNOBS
    }

    fn build(&self, ctx: &BuildContext<'_>) -> Box<dyn Strategy> {
        let script = ctx.params.get_str("script");
        if script.is_empty() {
            eprintln!("error: factory \"rhai\" needs `script = \"...\"` under [strategy]");
            std::process::exit(1);
        }
        let path = resolve_relative(&script, ctx.strategy_file);
        let engine_path = ctx
            .engine
            .map(|e| resolve_relative(e, ctx.strategy_file));
        match RhaiStrategy::new(
            &path,
            ctx.asset,
            ctx.params,
            engine_path.as_deref(),
            ctx.timeframes,
            ctx.params.get_u32("script_history") as usize,
            ctx.market.series.clone(),
            ctx.script,
        ) {
            Ok(s) => Box::new(s),
            Err(e) => {
                eprintln!("error: rhai script {}: {e}", path.display());
                std::process::exit(1);
            }
        }
    }
}

/// Resolve `p` against the strategy file's directory when it is relative
/// and exists there; otherwise as given (relative to the working directory).
fn resolve_relative(p: &str, strategy_file: Option<&Path>) -> PathBuf {
    let raw = PathBuf::from(p);
    if raw.is_absolute() {
        return raw;
    }
    if let Some(dir) = strategy_file.and_then(|f| f.parent()) {
        let cand = dir.join(&raw);
        if cand.exists() {
            return cand;
        }
    }
    raw
}

// ─── Timeframe names without the global intern lock ─────────────────────────
// `tf_id` / `tf_name` take a process-wide mutex that every asset thread
// contends for; scripts read `c.tf` and pass timeframe names on every bar,
// so each thread keeps its own copy of the (tiny, append-only) table.

thread_local! {
    static TF_NAMES: RefCell<Vec<Option<ImmutableString>>> = const { RefCell::new(Vec::new()) };
    static TF_IDS: RefCell<HashMap<ImmutableString, u16>> = RefCell::new(HashMap::new());
}

fn tf_str(id: u16) -> ImmutableString {
    TF_NAMES.with(|t| {
        let mut t = t.borrow_mut();
        let i = id as usize;
        if i >= t.len() {
            t.resize(i + 1, None);
        }
        if let Some(s) = &t[i] {
            return s.clone();
        }
        let s: ImmutableString = tf_name(id).to_string().into();
        t[i] = Some(s.clone());
        s
    })
}

fn tf_of(name: &str) -> u16 {
    TF_IDS.with(|t| {
        let mut t = t.borrow_mut();
        if let Some(&id) = t.get(name) {
            return id;
        }
        let id = tf_id(name);
        t.insert(name.into(), id);
        id
    })
}

// ─── Data types handed to scripts ───────────────────────────────────────────

/// A ring of completed candles for one timeframe, newest last. Scripts index
/// it from the newest bar: `close(0)` is the current bar, `close(1)` the one
/// before.
#[derive(Clone)]
pub struct Series(Rc<RefCell<VecDeque<Candle>>>);

impl Series {
    fn new(cap: usize) -> Self {
        Series(Rc::new(RefCell::new(VecDeque::with_capacity(cap + 1))))
    }
    fn from_slice(c: &[Candle]) -> Self {
        Series(Rc::new(RefCell::new(c.iter().cloned().collect())))
    }
    fn push(&self, c: &Candle, cap: usize) {
        let mut b = self.0.borrow_mut();
        b.push_back(c.clone());
        while b.len() > cap {
            b.pop_front();
        }
    }
    fn len(&self) -> i64 {
        self.0.borrow().len() as i64
    }
    fn get(&self, i: i64) -> Option<Candle> {
        let b = self.0.borrow();
        if i < 0 || i as usize >= b.len() {
            return None;
        }
        Some(b[b.len() - 1 - i as usize].clone())
    }
    fn field(&self, i: i64, f: fn(&Candle) -> f64) -> Result<f64, Box<EvalAltResult>> {
        let b = self.0.borrow();
        if i < 0 || i as usize >= b.len() {
            return Err(format!("series index {i} out of range (len {})", b.len()).into());
        }
        Ok(f(&b[b.len() - 1 - i as usize]))
    }
    /// Plain ATR over the last `period` bars with the previous bar's close in
    /// the true range, `period` capped at `len - 1`; 0 with under two bars.
    fn atr(&self, period: i64) -> f64 {
        let b = self.0.borrow();
        let n = (period.max(0) as usize).min(b.len().saturating_sub(1));
        if n < 1 {
            return 0.0;
        }
        let start = b.len() - n;
        let mut sum = 0.0;
        for i in start..b.len() {
            let h = b[i].high;
            let l = b[i].low;
            let pc = b[i - 1].close;
            let tr = (h - l).max((h - pc).abs()).max((l - pc).abs());
            sum += tr;
        }
        sum / n as f64
    }
    /// Highest high over the last `n` bars (including the current one).
    fn highest(&self, n: i64) -> f64 {
        let b = self.0.borrow();
        let n = (n.max(0) as usize).min(b.len());
        b.iter()
            .rev()
            .take(n)
            .map(|c| c.high)
            .fold(f64::NEG_INFINITY, f64::max)
    }
    fn lowest(&self, n: i64) -> f64 {
        let b = self.0.borrow();
        let n = (n.max(0) as usize).min(b.len());
        b.iter()
            .rev()
            .take(n)
            .map(|c| c.low)
            .fold(f64::INFINITY, f64::min)
    }
    /// Highest high over bars `from..=to` counted from the newest (0).
    fn highest_range(&self, from: i64, to: i64) -> f64 {
        let b = self.0.borrow();
        let mut m = f64::NEG_INFINITY;
        for i in from.max(0)..=to {
            if (i as usize) < b.len() {
                m = m.max(b[b.len() - 1 - i as usize].high);
            }
        }
        m
    }
    fn lowest_range(&self, from: i64, to: i64) -> f64 {
        let b = self.0.borrow();
        let mut m = f64::INFINITY;
        for i in from.max(0)..=to {
            if (i as usize) < b.len() {
                m = m.min(b[b.len() - 1 - i as usize].low);
            }
        }
        m
    }
    fn sma_close(&self, n: i64) -> f64 {
        let b = self.0.borrow();
        let n = (n.max(0) as usize).min(b.len());
        if n == 0 {
            return 0.0;
        }
        b.iter().rev().take(n).map(|c| c.close).sum::<f64>() / n as f64
    }
}

/// Another asset's complete series (or this asset's), read through windows
/// that never extend past the bar currently being processed.
#[derive(Clone)]
pub struct MarketSeries {
    candles: Arc<Vec<Candle>>,
    now: Rc<Cell<i64>>,
}

impl MarketSeries {
    /// Candles with timestamps in `(lo, hi]`, `hi` clamped to the current bar.
    fn slice(&self, lo: i64, hi: i64) -> &[Candle] {
        let hi = hi.min(self.now.get());
        let start = self
            .candles
            .partition_point(|c| c.timestamp.and_utc().timestamp() <= lo);
        let end = self
            .candles
            .partition_point(|c| c.timestamp.and_utc().timestamp() <= hi);
        if start >= end {
            &[]
        } else {
            &self.candles[start..end]
        }
    }
    fn count(&self, lo: i64, hi: i64) -> i64 {
        self.slice(lo, hi).len() as i64
    }
    fn lowest_low(&self, lo: i64, hi: i64) -> Dynamic {
        let s = self.slice(lo, hi);
        if s.is_empty() {
            return Dynamic::UNIT;
        }
        Dynamic::from_float(s.iter().map(|c| c.low).fold(f64::INFINITY, f64::min))
    }
    fn highest_high(&self, lo: i64, hi: i64) -> Dynamic {
        let s = self.slice(lo, hi);
        if s.is_empty() {
            return Dynamic::UNIT;
        }
        Dynamic::from_float(
            s.iter()
                .map(|c| c.high)
                .fold(f64::NEG_INFINITY, f64::max),
        )
    }
    fn window(&self, lo: i64, hi: i64) -> Series {
        Series::from_slice(self.slice(lo, hi))
    }
}

/// A wall-clock reading of a timestamp in some zone.
#[derive(Clone)]
pub struct Dt(NaiveDateTime);

fn secs_to_ndt(ts: i64) -> NaiveDateTime {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|d| d.naive_utc())
        .unwrap_or(NaiveDateTime::MIN)
}

fn ndt_secs(t: NaiveDateTime) -> i64 {
    t.and_utc().timestamp()
}

/// An opportunity under construction in a script: the engine's
/// [`Opportunity`] plus a free-form `meta` map the script's own `admit`
/// reads back.
#[derive(Clone)]
pub struct ScriptOpp {
    pub inner: Opportunity,
    pub meta: Map,
}

fn dir_from_str(s: &str) -> Result<Direction, Box<EvalAltResult>> {
    match s {
        "bull" | "long" | "buy" => Ok(Direction::Bull),
        "bear" | "short" | "sell" => Ok(Direction::Bear),
        other => Err(format!("direction must be \"bull\" or \"bear\", got {other:?}").into()),
    }
}

fn dir_str(d: Direction) -> ImmutableString {
    match d {
        Direction::Bull => "bull".into(),
        Direction::Bear => "bear".into(),
    }
}

fn opt_f64(v: Option<f64>) -> Dynamic {
    match v {
        Some(x) => Dynamic::from_float(x),
        None => Dynamic::UNIT,
    }
}

/// The book during `on_bar_close`, valid only for the duration of the call.
#[derive(Clone)]
pub struct ScriptBook {
    ptr: Rc<Cell<*mut Book<'static>>>,
}

impl ScriptBook {
    fn with<R>(&self, f: impl FnOnce(&mut Book<'_>) -> R) -> Result<R, Box<EvalAltResult>> {
        let p = self.ptr.get();
        if p.is_null() {
            return Err("book used outside on_bar_close".into());
        }
        // SAFETY: the pointer is set for exactly the synchronous duration of
        // one on_bar_close call and nulled before the Book it points at is
        // dropped; the strategy runs on one thread.
        let book: &mut Book<'_> = unsafe { &mut *p };
        Ok(f(book))
    }
}

fn trade_map(t: &PaperTrade, stop_exit: Option<bool>) -> Map {
    let mut m = Map::new();
    m.insert("id".into(), t.opportunity_id.clone().into());
    m.insert("signal_type".into(), sig_type_name(t.signal_type).to_string().into());
    m.insert("asset".into(), asset_name(t.asset).to_string().into());
    m.insert("tf".into(), tf_name(t.timeframe).to_string().into());
    m.insert("direction".into(), dir_str(t.direction).into());
    m.insert("entry".into(), t.entry.into());
    m.insert("stop".into(), t.stop.into());
    m.insert("tp".into(), t.tp.into());
    m.insert("fill".into(), t.fill.into());
    m.insert("score".into(), t.score.into());
    m.insert("opened_at".into(), ndt_secs(t.opened_at).into());
    m.insert(
        "filled_at".into(),
        t.filled_at.map(|x| Dynamic::from(ndt_secs(x))).unwrap_or(Dynamic::UNIT),
    );
    m.insert(
        "closed_at".into(),
        t.closed_at.map(|x| Dynamic::from(ndt_secs(x))).unwrap_or(Dynamic::UNIT),
    );
    m.insert("result".into(), t.result.as_str().into());
    m.insert("r_pnl".into(), t.r_pnl.into());
    m.insert("fee_r".into(), t.fee_r.into());
    if let Some(se) = stop_exit {
        m.insert("stop_exit".into(), se.into());
    }
    m
}

fn map_get_f64(m: &Map, k: &str) -> Result<f64, Box<EvalAltResult>> {
    let v = m
        .get(k)
        .ok_or_else(|| Box::<EvalAltResult>::from(format!("missing field {k:?}")))?;
    if let Some(f) = v.clone().try_cast::<f64>() {
        return Ok(f);
    }
    if let Some(i) = v.clone().try_cast::<i64>() {
        return Ok(i as f64);
    }
    Err(format!("field {k:?} must be a number").into())
}

fn map_get_str(m: &Map, k: &str) -> Result<String, Box<EvalAltResult>> {
    m.get(k)
        .and_then(|v| v.clone().into_immutable_string().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| format!("missing string field {k:?}").into())
}

/// Convert a TOML document into nested Rhai maps/arrays.
fn toml_to_dynamic(v: &toml::Value) -> Dynamic {
    match v {
        toml::Value::String(s) => s.clone().into(),
        toml::Value::Integer(i) => (*i).into(),
        toml::Value::Float(f) => (*f).into(),
        toml::Value::Boolean(b) => (*b).into(),
        toml::Value::Datetime(d) => d.to_string().into(),
        toml::Value::Array(a) => Dynamic::from_array(a.iter().map(toml_to_dynamic).collect()),
        toml::Value::Table(t) => {
            let mut m = Map::new();
            for (k, v) in t {
                m.insert(k.as_str().into(), toml_to_dynamic(v));
            }
            Dynamic::from_map(m)
        }
    }
}

fn value_to_dynamic(v: &Value) -> Dynamic {
    match v {
        Value::Bool(b) => (*b).into(),
        Value::F64(f) => (*f).into(),
        Value::U32(u) => (*u as i64).into(),
        Value::I64(i) => (*i).into(),
        Value::Str(s) => s.clone().into(),
        Value::VecStr(v) => Dynamic::from_array(v.iter().map(|s| s.clone().into()).collect()),
        Value::VecU32(v) => Dynamic::from_array(v.iter().map(|u| (*u as i64).into()).collect()),
    }
}

// ─── The strategy ───────────────────────────────────────────────────────────

/// A strategy whose behavior is a Rhai script. See the module docs.
pub struct RhaiStrategy {
    engine: Engine,
    ast: AST,
    scope: Scope<'static>,
    state: RefCell<Dynamic>,
    hist: Rc<RefCell<HashMap<u16, Series>>>,
    history: usize,
    now: Rc<Cell<i64>>,
    book_ptr: Rc<Cell<*mut Book<'static>>>,
    /// Script-side metadata for emitted opportunities, by id, until admitted.
    meta: RefCell<HashMap<String, Map>>,
    has_admit: bool,
    has_on_candle: bool,
    has_bar_close: bool,
    has_on_bar: bool,
    has_emit: bool,
    has_on_day: bool,
    /// The native scanner the script put in its state under `scan`, if any.
    scan: Option<ScanHandle>,
    /// Native window aggregators found in the state (top-level values), in
    /// key order. Stepped on every base candle before the script runs.
    windows: Vec<WinHandle>,
    /// Whether any window rolled on the candle being processed; gates
    /// `on_bar_close` for window-driven scripts.
    rolled: Cell<bool>,
    asset: String,
    /// Per-hook call counts and time, reported on drop when
    /// `BACKTEST_RHAI_STATS` is set.
    stats: RefCell<HashMap<&'static str, (u64, std::time::Duration)>>,
    stats_on: bool,
}

impl Drop for RhaiStrategy {
    fn drop(&mut self) {
        if !self.stats_on {
            return;
        }
        let st = self.stats.borrow();
        let mut rows: Vec<_> = st.iter().collect();
        rows.sort_by(|a, b| b.1 .1.cmp(&a.1 .1));
        for (name, (n, t)) in rows {
            eprintln!(
                "rhai stats {}: {:<14} {:>9} calls {:>8.3} s {:>7.1} us/call",
                self.asset,
                name,
                n,
                t.as_secs_f64(),
                if *n > 0 { t.as_secs_f64() * 1e6 / *n as f64 } else { 0.0 }
            );
        }
    }
}

impl RhaiStrategy {
    /// Compile `path` and run its top level plus `init`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: &Path,
        asset: &str,
        params: &crate::params::Params,
        engine_path: Option<&Path>,
        timeframes: &[String],
        history: usize,
        market: HashMap<String, Arc<Vec<Candle>>>,
        script_table: &toml::value::Table,
    ) -> Result<Self, Box<EvalAltResult>> {
        let mut engine = Engine::new();
        engine.set_optimization_level(rhai::OptimizationLevel::Full);
        engine.on_print(|s| eprintln!("{s}"));
        engine.on_debug(|s, src, pos| eprintln!("{s} @ {src:?} {pos:?}"));

        let hist: Rc<RefCell<HashMap<u16, Series>>> = Rc::new(RefCell::new(HashMap::new()));
        let now = Rc::new(Cell::new(i64::MIN));
        let book_ptr: Rc<Cell<*mut Book<'static>>> = Rc::new(Cell::new(std::ptr::null_mut()));
        let asset_for_opp = asset.to_string();

        register_api(&mut engine);

        // hist(tf): this asset's ring for a timeframe.
        {
            let hist = hist.clone();
            let cap = history;
            engine.register_fn("hist", move |tf: &str| -> Series {
                let id = tf_of(tf);
                hist.borrow_mut()
                    .entry(id)
                    .or_insert_with(|| Series::new(cap))
                    .clone()
            });
        }
        // market(asset): any loaded series, windowed to the present.
        {
            let now_m = now.clone();
            let market = market.clone();
            engine.register_fn(
                "market",
                move |asset: &str| -> Result<MarketSeries, Box<EvalAltResult>> {
                    match market.get(asset) {
                        Some(c) => Ok(MarketSeries {
                            candles: c.clone(),
                            now: now_m.clone(),
                        }),
                        None => Err(format!("market({asset:?}): no such loaded asset").into()),
                    }
                },
            );
            let now2 = now.clone();
            engine.register_fn("now", move || -> i64 { now2.get() });
        }
        // opp(signal_type, tf, direction, ts): a new opportunity for this asset.
        {
            let a = asset_for_opp.clone();
            engine.register_fn(
                "opp",
                move |signal_type: &str,
                      tf: &str,
                      direction: &str,
                      ts: i64|
                      -> Result<ScriptOpp, Box<EvalAltResult>> {
                    let d = dir_from_str(direction)?;
                    Ok(ScriptOpp {
                        inner: Opportunity::new(signal_type, &a, tf, d, secs_to_ndt(ts)),
                        meta: Map::new(),
                    })
                },
            );
        }

        let ast = engine.compile_file(path.to_path_buf())?;
        let mut scope = Scope::new();
        let _ = engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)?;

        let has_fn = |n: &str| ast.iter_functions().any(|f| f.name == n);
        let has_init = has_fn("init");
        let has_admit = has_fn("admit");
        let has_bar_close = has_fn("on_bar_close");
        let has_on_bar = has_fn("on_bar");
        let has_emit = has_fn("emit");
        let has_on_day = has_fn("on_day");
        let has_on_candle = has_fn("on_candle");
        if !has_on_candle && !has_on_bar && !has_bar_close {
            return Err(
                "script defines none of `on_candle(c)`, `on_bar(c, atr, sweeps, breaks)` \
                 or `on_bar_close(c, book)`"
                    .into(),
            );
        }

        // The configuration the script starts from.
        let mut cfg = Map::new();
        cfg.insert("asset".into(), asset.to_string().into());
        let mut pm = Map::new();
        for k in crate::params::all_knobs() {
            pm.insert(k.name.into(), value_to_dynamic(&params.get(k.name)));
        }
        cfg.insert("params".into(), Dynamic::from_map(pm));
        cfg.insert(
            "engine".into(),
            engine_path
                .map(|p| Dynamic::from(p.to_string_lossy().to_string()))
                .unwrap_or(Dynamic::UNIT),
        );
        cfg.insert(
            "timeframes".into(),
            Dynamic::from_array(timeframes.iter().map(|t| t.clone().into()).collect()),
        );
        cfg.insert("history".into(), (history as i64).into());
        cfg.insert(
            "script".into(),
            toml_to_dynamic(&toml::Value::Table(script_table.clone())),
        );

        let state: Dynamic = if has_init {
            engine.call_fn_with_options::<Dynamic>(
                CallFnOptions::new().eval_ast(false),
                &mut scope,
                &ast,
                "init",
                (Dynamic::from_map(cfg),),
            )?
        } else {
            Dynamic::from_map(Map::new())
        };

        let scan = state
            .read_lock::<Map>()
            .and_then(|m| m.get("scan").and_then(|v| v.clone().try_cast::<ScanHandle>()));
        let windows: Vec<WinHandle> = state
            .read_lock::<Map>()
            .map(|m| {
                m.values()
                    .filter_map(|v| v.clone().try_cast::<WinHandle>())
                    .collect()
            })
            .unwrap_or_default();
        if scan.is_none() && has_on_bar && !has_on_candle {
            return Err("script defines `on_bar` but its state has no `scan` scanner".into());
        }

        Ok(Self {
            engine,
            ast,
            scope,
            state: RefCell::new(state),
            hist,
            history,
            now,
            book_ptr,
            meta: RefCell::new(HashMap::new()),
            has_admit,
            has_on_candle,
            has_bar_close,
            has_on_bar,
            has_emit,
            has_on_day,
            scan,
            windows,
            rolled: Cell::new(false),
            asset: asset.to_string(),
            stats: RefCell::new(HashMap::new()),
            stats_on: std::env::var_os("BACKTEST_RHAI_STATS").is_some(),
        })
    }

    fn call(
        &self,
        name: &'static str,
        args: impl rhai::FuncArgs,
    ) -> Result<Dynamic, Box<EvalAltResult>> {
        if self.stats_on {
            let t0 = std::time::Instant::now();
            let r = self.call_inner(name, args);
            let mut st = self.stats.borrow_mut();
            let e = st.entry(name).or_insert((0, std::time::Duration::ZERO));
            e.0 += 1;
            e.1 += t0.elapsed();
            return r;
        }
        self.call_inner(name, args)
    }

    fn call_inner(
        &self,
        name: &str,
        args: impl rhai::FuncArgs,
    ) -> Result<Dynamic, Box<EvalAltResult>> {
        let name = name.split('(').next().unwrap_or(name);
        let mut state = self.state.borrow_mut();
        let mut scope = Scope::new();
        // The scope is rebuilt per call: the script's persistent state lives
        // in `this`, and top-level constants were folded in at compile time.
        let _ = &self.scope;
        self.engine.call_fn_with_options::<Dynamic>(
            CallFnOptions::new()
                .eval_ast(false)
                .bind_this_ptr(&mut state),
            &mut scope,
            &self.ast,
            name,
            args,
        )
    }

    fn fail(&self, hook: &str, e: Box<EvalAltResult>) -> ! {
        eprintln!("error: rhai {}: {hook}: {e}", self.asset);
        std::process::exit(1)
    }
}

impl RhaiStrategy {
    /// Turn a hook's return value into opportunities: `()` or an array of
    /// `opp(...)` values.
    fn collect_opps(&self, out: Dynamic, hook: &str) -> Vec<Opportunity> {
        let mut opps = Vec::new();
        if out.is_unit() {
            return opps;
        }
        let arr: Array = match out.try_cast::<Array>() {
            Some(a) => a,
            None => self.fail(hook, "must return an array of opportunities (or ())".into()),
        };
        for item in arr {
            match item.try_cast::<ScriptOpp>() {
                Some(o) => {
                    if !o.meta.is_empty() {
                        self.meta.borrow_mut().insert(o.inner.id.clone(), o.meta);
                    }
                    opps.push(o.inner);
                }
                None => self.fail(hook, "array item is not an opportunity".into()),
            }
        }
        opps
    }

    /// One bar through the native scanner and the script's event hooks.
    /// Returns what `emit` (or `on_bar`, for scripts without `emit`) produced.
    fn drive_scanner(&self, candle: &Candle, scan: &ScanHandle) -> Dynamic {
        let ev: BarEvents = scan.0.borrow_mut().process(candle);
        if let (Some(day), true) = (ev.day_closed, self.has_on_day) {
            if let Err(e) = self.call("on_day", (candle.clone(), day_map(&day))) {
                self.fail("on_day", e);
            }
        }
        let awake = {
            let s = scan.0.borrow();
            !ev.sweeps.is_empty()
                || !ev.breaks.is_empty()
                || s.wake.contains(&candle.timeframe)
                || s.wake.contains(&u16::MAX)
        };
        let mut cands: Array = Array::new();
        if awake && self.has_on_bar {
            let sweeps: Array = ev.sweeps.iter().map(|s| Dynamic::from_map(sweep_map(s))).collect();
            let breaks: Array = ev.breaks.iter().map(|b| Dynamic::from_map(break_map(b))).collect();
            let hook: &'static str = if ev.sweeps.is_empty() && ev.breaks.is_empty() {
                "on_bar(wake)"
            } else {
                "on_bar"
            };
            match self.call(hook, (candle.clone(), ev.atr, sweeps, breaks)) {
                Ok(v) => {
                    if !v.is_unit() {
                        match v.try_cast::<Array>() {
                            Some(a) => cands = a,
                            None => self.fail("on_bar", "must return an array (or ())".into()),
                        }
                    }
                }
                Err(e) => self.fail("on_bar", e),
            }
        }
        scan.0.borrow_mut().rebuild_draw(candle, &ev);
        let mut out: Array = Array::new();
        for cand in cands {
            if cand.is::<ScriptOpp>() || !self.has_emit {
                out.push(cand);
                continue;
            }
            match self.call("emit", (candle.clone(), ev.atr, cand)) {
                Ok(v) => {
                    if !v.is_unit() {
                        out.push(v);
                    }
                }
                Err(e) => self.fail("emit", e),
            }
        }
        scan.0.borrow_mut().after_hooks(candle, ev.atr);
        Dynamic::from_array(out)
    }
}

impl Strategy for RhaiStrategy {
    fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
        if !candle.complete {
            return Vec::new();
        }
        let ts = ndt_secs(candle.timestamp);
        if ts > self.now.get() {
            self.now.set(ts);
        }
        {
            let mut h = self.hist.borrow_mut();
            let cap = self.history;
            h.entry(candle.timeframe)
                .or_insert_with(|| Series::new(cap))
                .push(candle, cap);
        }
        if candle.timeframe == crate::models::base_tf_id() {
            let mut rolled = false;
            for w in &self.windows {
                rolled |= w.0.borrow_mut().step(candle);
            }
            self.rolled.set(rolled);
        }
        let out: Dynamic = match self.scan.clone() {
            Some(scan) => self.drive_scanner(candle, &scan),
            // A script of only `on_bar_close` (market orders, no limit
            // opportunities) is not called per candle at all.
            None if !self.has_on_candle => return Vec::new(),
            None => match self.call("on_candle", (candle.clone(),)) {
                Ok(v) => v,
                Err(e) => self.fail("on_candle", e),
            },
        };
        self.collect_opps(out, "on_candle")
    }

    fn admit(&self, opp: &Opportunity, ctx: &AdmitContext<'_>) -> Decision {
        if !self.has_admit {
            return crate::strategy::default_admit(opp, ctx);
        }
        let meta = self.meta.borrow_mut().remove(&opp.id).unwrap_or_default();
        let so = ScriptOpp {
            inner: opp.clone(),
            meta,
        };
        let mut c = Map::new();
        c.insert("min_score".into(), ctx.min_score.into());
        c.insert("rr_target".into(), ctx.rr_target.into());
        let mut pm = Map::new();
        for k in crate::params::all_knobs() {
            pm.insert(k.name.into(), value_to_dynamic(&ctx.params.get(k.name)));
        }
        c.insert("params".into(), Dynamic::from_map(pm));
        c.insert(
            "recent".into(),
            Dynamic::from(Series::from_slice(ctx.recent_candles.unwrap_or(&[]))),
        );
        let out: Dynamic = match self.call("admit", (so, Dynamic::from_map(c))) {
            Ok(v) => v,
            Err(e) => self.fail("admit", e),
        };
        let Some(m) = out.try_cast::<Map>() else {
            self.fail("admit", "must return take(entry, stop, tp) or skip(reason)".into())
        };
        if let Some(r) = m.get("skip") {
            let reason = r
                .clone()
                .into_immutable_string()
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "skip".to_string());
            return Decision::Skip(match reason.as_str() {
                "below_min_score" => SkipReason::BelowMinScore,
                "no_entry_stop" => SkipReason::NoEntryStop,
                "non_positive_risk" => SkipReason::NonPositiveRisk,
                "invalid_geometry" => SkipReason::InvalidGeometry,
                _ => SkipReason::Custom(reason),
            });
        }
        let take = (|| -> Result<TakeParams, Box<EvalAltResult>> {
            Ok(TakeParams {
                entry: map_get_f64(&m, "entry")?,
                stop: map_get_f64(&m, "stop")?,
                tp: map_get_f64(&m, "tp")?,
            })
        })();
        match take {
            Ok(t) => {
                if !t.is_valid(opp.direction) {
                    Decision::Skip(SkipReason::InvalidGeometry)
                } else {
                    Decision::Take(t)
                }
            }
            Err(e) => self.fail("admit", e),
        }
    }

    fn on_bar_close(&mut self, candle: &Candle, book: &mut Book<'_>) {
        if !self.has_bar_close {
            return;
        }
        if let Some(scan) = &self.scan {
            // Event-driven: only when something closed, or the script asked
            // to be woken on every bar.
            if !scan.0.borrow().wake_book && book.closed().is_empty() {
                return;
            }
        } else if !self.windows.is_empty() {
            // Window-driven: only when a window rolled on this bar, something
            // closed, or a window asked for every bar.
            let awake = self.rolled.get()
                || self.windows.iter().any(|w| w.0.borrow().wake)
                || !book.closed().is_empty();
            if !awake {
                return;
            }
        }
        // Erase the borrow's lifetime for the duration of the call; see
        // ScriptBook::with for the invariant.
        let raw: *mut Book<'_> = book;
        let raw: *mut Book<'static> = raw.cast();
        self.book_ptr.set(raw);
        let sb = ScriptBook {
            ptr: self.book_ptr.clone(),
        };
        let r = self.call("on_bar_close", (candle.clone(), sb));
        self.book_ptr.set(std::ptr::null_mut());
        if let Err(e) = r {
            self.fail("on_bar_close", e);
        }
    }
}

/// Register every type and helper a script can use.
fn register_api(engine: &mut Engine) {
    // ── Candle ──────────────────────────────────────────────────────────────
    engine
        .register_type_with_name::<Candle>("Candle")
        .register_get("open", |c: &mut Candle| c.open)
        .register_get("high", |c: &mut Candle| c.high)
        .register_get("low", |c: &mut Candle| c.low)
        .register_get("close", |c: &mut Candle| c.close)
        .register_get("volume", |c: &mut Candle| c.volume)
        .register_get("o", |c: &mut Candle| c.open)
        .register_get("h", |c: &mut Candle| c.high)
        .register_get("l", |c: &mut Candle| c.low)
        .register_get("c", |c: &mut Candle| c.close)
        .register_get("ts", |c: &mut Candle| ndt_secs(c.timestamp))
        .register_get("complete", |c: &mut Candle| c.complete)
        .register_get("tf", |c: &mut Candle| -> ImmutableString { tf_str(c.timeframe) })
        .register_get("asset", |c: &mut Candle| -> ImmutableString {
            asset_name(c.asset).to_string().into()
        })
        .register_get("is_bullish", |c: &mut Candle| c.close > c.open)
        .register_get("body_high", |c: &mut Candle| c.open.max(c.close))
        .register_get("body_low", |c: &mut Candle| c.open.min(c.close));

    // ── Series ──────────────────────────────────────────────────────────────
    engine
        .register_type_with_name::<Series>("Series")
        .register_get("len", |s: &mut Series| s.len())
        .register_fn("at", |s: &mut Series, i: i64| -> Dynamic {
            s.get(i).map(Dynamic::from).unwrap_or(Dynamic::UNIT)
        })
        .register_fn("open", |s: &mut Series, i: i64| s.field(i, |c| c.open))
        .register_fn("high", |s: &mut Series, i: i64| s.field(i, |c| c.high))
        .register_fn("low", |s: &mut Series, i: i64| s.field(i, |c| c.low))
        .register_fn("close", |s: &mut Series, i: i64| s.field(i, |c| c.close))
        .register_fn("volume", |s: &mut Series, i: i64| s.field(i, |c| c.volume))
        .register_fn("ts", |s: &mut Series, i: i64| {
            s.field(i, |c| ndt_secs(c.timestamp) as f64).map(|f| f as i64)
        })
        .register_fn("atr", |s: &mut Series, period: i64| s.atr(period))
        .register_fn("highest", |s: &mut Series, n: i64| s.highest(n))
        .register_fn("lowest", |s: &mut Series, n: i64| s.lowest(n))
        .register_fn("highest_range", |s: &mut Series, a: i64, b: i64| {
            s.highest_range(a, b)
        })
        .register_fn("lowest_range", |s: &mut Series, a: i64, b: i64| {
            s.lowest_range(a, b)
        })
        .register_fn("sma", |s: &mut Series, n: i64| s.sma_close(n));

    // ── MarketSeries ────────────────────────────────────────────────────────
    engine
        .register_type_with_name::<MarketSeries>("MarketSeries")
        .register_fn("count", |m: &mut MarketSeries, lo: i64, hi: i64| m.count(lo, hi))
        .register_fn("lowest_low", |m: &mut MarketSeries, lo: i64, hi: i64| {
            m.lowest_low(lo, hi)
        })
        .register_fn("highest_high", |m: &mut MarketSeries, lo: i64, hi: i64| {
            m.highest_high(lo, hi)
        })
        .register_fn("window", |m: &mut MarketSeries, lo: i64, hi: i64| {
            m.window(lo, hi)
        });

    // ── Time ────────────────────────────────────────────────────────────────
    engine
        .register_type_with_name::<Dt>("Dt")
        .register_get("year", |d: &mut Dt| d.0.year() as i64)
        .register_get("month", |d: &mut Dt| d.0.month() as i64)
        .register_get("day", |d: &mut Dt| d.0.day() as i64)
        .register_get("hour", |d: &mut Dt| d.0.hour() as i64)
        .register_get("minute", |d: &mut Dt| d.0.minute() as i64)
        .register_get("second", |d: &mut Dt| d.0.second() as i64)
        .register_get("weekday", |d: &mut Dt| {
            d.0.weekday().num_days_from_monday() as i64
        })
        .register_get("iso_week", |d: &mut Dt| d.0.iso_week().week() as i64)
        .register_get("date", |d: &mut Dt| -> ImmutableString {
            d.0.format("%Y-%m-%d").to_string().into()
        })
        .register_get("ts", |d: &mut Dt| ndt_secs(d.0))
        .register_fn("to_string", |d: &mut Dt| d.0.format("%Y-%m-%dT%H:%M:%S").to_string());
    engine.register_fn("dt_utc", |ts: i64| Dt(secs_to_ndt(ts)));
    engine.register_fn("dt_offset", |ts: i64, offset_secs: i64| {
        Dt(secs_to_ndt(ts) + chrono::Duration::seconds(offset_secs))
    });
    engine.register_fn(
        "dt_tz",
        |ts: i64, tz: &str| -> Result<Dt, Box<EvalAltResult>> {
            let z: chrono_tz::Tz = tz
                .parse()
                .map_err(|_| Box::<EvalAltResult>::from(format!("unknown time zone {tz:?}")))?;
            Ok(Dt(z.from_utc_datetime(&secs_to_ndt(ts)).naive_local()))
        },
    );

    // ── Opportunity ─────────────────────────────────────────────────────────
    engine
        .register_type_with_name::<ScriptOpp>("Opp")
        .register_get("id", |o: &mut ScriptOpp| -> ImmutableString { o.inner.id.clone().into() })
        .register_set("id", |o: &mut ScriptOpp, v: &str| o.inner.id = v.to_string())
        .register_get("signal_type", |o: &mut ScriptOpp| -> ImmutableString {
            sig_type_name(o.inner.signal_type).to_string().into()
        })
        .register_get("asset", |o: &mut ScriptOpp| -> ImmutableString {
            asset_name(o.inner.asset).to_string().into()
        })
        .register_get("tf", |o: &mut ScriptOpp| -> ImmutableString {
            tf_name(o.inner.timeframe).to_string().into()
        })
        .register_get("direction", |o: &mut ScriptOpp| dir_str(o.inner.direction))
        .register_get("ts", |o: &mut ScriptOpp| ndt_secs(o.inner.created_at))
        .register_get("score", |o: &mut ScriptOpp| o.inner.score)
        .register_set("score", |o: &mut ScriptOpp, v: f64| o.inner.score = v)
        .register_get("entry", |o: &mut ScriptOpp| opt_f64(o.inner.entry))
        .register_set("entry", |o: &mut ScriptOpp, v: f64| o.inner.entry = Some(v))
        .register_get("stop", |o: &mut ScriptOpp| opt_f64(o.inner.stop))
        .register_set("stop", |o: &mut ScriptOpp, v: f64| o.inner.stop = Some(v))
        .register_get("target", |o: &mut ScriptOpp| opt_f64(o.inner.target))
        .register_set("target", |o: &mut ScriptOpp, v: f64| o.inner.target = Some(v))
        .register_get("developing", |o: &mut ScriptOpp| o.inner.developing)
        .register_set("developing", |o: &mut ScriptOpp, v: bool| o.inner.developing = v)
        .register_get("meta", |o: &mut ScriptOpp| o.meta.clone())
        .register_set("meta", |o: &mut ScriptOpp, m: Map| o.meta = m);

    // ── Decisions ───────────────────────────────────────────────────────────
    engine.register_fn("take", |entry: f64, stop: f64, tp: f64| -> Map {
        let mut m = Map::new();
        m.insert("entry".into(), entry.into());
        m.insert("stop".into(), stop.into());
        m.insert("tp".into(), tp.into());
        m
    });
    engine.register_fn("skip", |reason: &str| -> Map {
        let mut m = Map::new();
        m.insert("skip".into(), reason.to_string().into());
        m
    });

    // ── Book ────────────────────────────────────────────────────────────────
    engine
        .register_type_with_name::<ScriptBook>("Book")
        .register_fn("closed", |b: &mut ScriptBook| -> Result<Array, Box<EvalAltResult>> {
            b.with(|bk| {
                bk.closed()
                    .iter()
                    .map(|c| Dynamic::from_map(trade_map(&c.trade, Some(c.stop_exit))))
                    .collect()
            })
        })
        .register_fn("open", |b: &mut ScriptBook| -> Result<Array, Box<EvalAltResult>> {
            b.with(|bk| {
                bk.open()
                    .iter()
                    .map(|t| {
                        let mut m = trade_map(t, None);
                        m.insert("effective_stop".into(), bk.effective_stop(t).into());
                        // Unrealized R at this bar's close, over the planned risk.
                        let risk = (t.entry - t.stop).abs();
                        let close = bk.candle().close;
                        let r_open = if risk > 0.0 {
                            match t.direction {
                                Direction::Bull => (close - t.fill) / risk,
                                Direction::Bear => (t.fill - close) / risk,
                            }
                        } else {
                            0.0
                        };
                        m.insert("r_open".into(), r_open.into());
                        Dynamic::from_map(m)
                    })
                    .collect()
            })
        })
        .register_fn("pending", |b: &mut ScriptBook| -> Result<Array, Box<EvalAltResult>> {
            b.with(|bk| bk.pending_ids().into_iter().map(Dynamic::from).collect())
        })
        .register_fn("has_open", |b: &mut ScriptBook| b.with(|bk| bk.has_open()))
        .register_fn(
            "market_entry",
            |b: &mut ScriptBook, spec: Map| -> Result<bool, Box<EvalAltResult>> {
                let id = map_get_str(&spec, "id")?;
                let st = map_get_str(&spec, "signal_type")?;
                let dir = dir_from_str(&map_get_str(&spec, "direction")?)?;
                let stop = map_get_f64(&spec, "stop")?;
                let tp = map_get_f64(&spec, "tp")?;
                let score = map_get_f64(&spec, "score").unwrap_or(0.0);
                let at_open = match spec.get("at").map(|v| v.to_string()).as_deref() {
                    None | Some("close") => false,
                    Some("open") => true,
                    Some(other) => {
                        return Err(format!(
                            "market_entry: `at` must be \"close\" or \"open\", got {other:?}"
                        )
                        .into())
                    }
                };
                b.with(|bk| {
                    if at_open {
                        bk.market_entry_at_open(&id, sig_type_id(&st), dir, stop, tp, score)
                    } else {
                        bk.market_entry(&id, sig_type_id(&st), dir, stop, tp, score)
                    }
                })
            },
        )
        .register_fn("set_stop", |b: &mut ScriptBook, id: &str, px: f64| {
            b.with(|bk| bk.set_stop(id, px))
        })
        .register_fn("set_tp", |b: &mut ScriptBook, id: &str, px: f64| {
            b.with(|bk| bk.set_tp(id, px))
        })
        .register_fn("close", |b: &mut ScriptBook, id: &str| b.with(|bk| bk.close(id)))
        .register_fn("cancel", |b: &mut ScriptBook, id: &str| {
            b.with(|bk| bk.cancel(id))
        });

    // ── Fees, config, logging ───────────────────────────────────────────────
    engine.register_fn("fee_in_r", |asset: &str, entry: f64, stop: f64| {
        fees::fee_in_r(crate::models::asset_id(asset), entry, stop)
    });
    engine.register_fn("fee_in_r_taker", |asset: &str, entry: f64, stop: f64| {
        fees::fee_in_r_side(
            crate::models::asset_id(asset),
            entry,
            stop,
            EntryFeeSide::Taker,
        )
    });
    engine.register_fn(
        "toml_load",
        |path: &str| -> Result<Dynamic, Box<EvalAltResult>> {
            let text = std::fs::read_to_string(path)
                .map_err(|e| Box::<EvalAltResult>::from(format!("toml_load({path:?}): {e}")))?;
            let v: toml::Value = text
                .parse()
                .map_err(|e| Box::<EvalAltResult>::from(format!("toml_load({path:?}): {e}")))?;
            Ok(toml_to_dynamic(&v))
        },
    );
    engine.register_fn("log", |s: &str| eprintln!("{s}"));
    engine.register_fn("to_int_trunc", |f: f64| f as i64);
    engine.register_fn("fmin", |a: f64, b: f64| a.min(b));
    engine.register_fn("fmax", |a: f64, b: f64| a.max(b));
    // Round through single precision: for reproducing an f32 accumulator.
    engine.register_fn("f32", |x: f64| (x as f32) as f64);
    engine.register_fn("inf", || f64::INFINITY);
    engine.register_fn("nan", || f64::NAN);

    register_scanner_api(engine);
    register_window_api(engine);
}

// ─── Native window aggregators ───────────────────────────────────────────────

/// One closed or forming bar of a [`Window`].
#[derive(Clone, Copy, Debug)]
pub struct WinBar {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// A script-owned timeframe aggregator the engine steps natively on every
/// base candle: bars are grouped by `(ts + anchor) div dur`, a bar's `ts` is
/// its window start, and a candle in a new group closes the forming bar and
/// starts the next. Closed bars are kept in a ring of `keep` entries and read
/// MQL-style, shift 1 being the last closed bar. A script that puts one or
/// more of these in its state (any top-level key) is called in
/// `on_bar_close` only on candles that rolled a window, closed a trade, or
/// while `wake(true)` stands, so a strategy that acts at bar boundaries pays
/// the interpreter only there.
pub struct Window {
    dur: i64,
    anchor: i64,
    keep: usize,
    cur_group: i64,
    forming: Option<WinBar>,
    bars: VecDeque<WinBar>,
    /// Closed bars ever, including those the ring dropped.
    total: usize,
    /// The candle just stepped started a new window.
    rolled: bool,
    /// Window start of the candle just stepped.
    start: i64,
    /// The bar the last step closed, if any.
    closed: Option<WinBar>,
    wake: bool,
}

impl Window {
    pub fn new(dur: i64, anchor: i64, keep: usize) -> Self {
        Window {
            dur,
            anchor,
            keep: keep.max(1),
            cur_group: i64::MIN,
            forming: None,
            bars: VecDeque::with_capacity(keep.max(1)),
            total: 0,
            rolled: false,
            start: 0,
            closed: None,
            wake: false,
        }
    }

    fn push_closed(&mut self, b: WinBar) {
        if self.bars.len() == self.keep {
            self.bars.pop_front();
        }
        self.bars.push_back(b);
        self.total += 1;
    }

    /// Feed one base candle. Returns whether it started a new window.
    pub fn step(&mut self, c: &Candle) -> bool {
        let ts = ndt_secs(c.timestamp);
        let g = (ts + self.anchor).div_euclid(self.dur);
        self.closed = None;
        if g != self.cur_group {
            if let Some(b) = self.forming.take() {
                self.push_closed(b);
                self.closed = Some(b);
            }
            self.cur_group = g;
            self.start = g * self.dur - self.anchor;
            self.forming = Some(WinBar {
                ts: self.start,
                open: c.open,
                high: c.high,
                low: c.low,
                close: c.close,
            });
            self.rolled = true;
        } else {
            // A forming bar finalized by the clock takes nothing newer.
            if let Some(f) = self.forming.as_mut() {
                if c.high > f.high {
                    f.high = c.high;
                }
                if c.low < f.low {
                    f.low = c.low;
                }
                f.close = c.close;
            }
            self.rolled = false;
        }
        self.rolled
    }

    /// Window end (exclusive) of the forming bar.
    pub fn forming_end(&self) -> Option<i64> {
        self.forming.map(|_| (self.cur_group + 1) * self.dur - self.anchor)
    }

    /// Close the forming bar by the clock: if its window ended at or before
    /// `t`, push it as closed without waiting for a newer candle. Returns
    /// whether a bar was closed.
    pub fn finalize_through(&mut self, t: i64) -> bool {
        match self.forming_end() {
            Some(end) if end <= t => {
                let b = self.forming.take().expect("forming when its end is known");
                self.push_closed(b);
                true
            }
            _ => false,
        }
    }

    /// Closed bar at `shift` (1 = last closed). None beyond the ring.
    pub fn bar(&self, shift: usize) -> Option<&WinBar> {
        if shift == 0 || shift > self.bars.len() {
            return None;
        }
        self.bars.get(self.bars.len() - shift)
    }

    /// Is the bar at `shift` a pivot high of `strength` (strictly above the
    /// `strength` bars on each side)? False where a neighbour is missing.
    pub fn pivot_high(&self, shift: usize, strength: usize) -> bool {
        let Some(h) = self.bar(shift).map(|b| b.high) else {
            return false;
        };
        for k in 1..=strength {
            if shift <= k {
                return false;
            }
            let (Some(l), Some(r)) = (self.bar(shift - k), self.bar(shift + k)) else {
                return false;
            };
            if h <= l.high || h <= r.high {
                return false;
            }
        }
        true
    }

    /// Mirror of [`Window::pivot_high`].
    pub fn pivot_low(&self, shift: usize, strength: usize) -> bool {
        let Some(lo) = self.bar(shift).map(|b| b.low) else {
            return false;
        };
        for k in 1..=strength {
            if shift <= k {
                return false;
            }
            let (Some(l), Some(r)) = (self.bar(shift - k), self.bar(shift + k)) else {
                return false;
            };
            if lo >= l.low || lo >= r.low {
                return false;
            }
        }
        true
    }

    /// The two most recent pivot highs and the two most recent pivot lows,
    /// scanning shifts `strength + 1 .. scan - strength` from the newest bar
    /// out, stopping once both pairs are found. Returns
    /// `(last_high, prev_high, last_low, prev_low)` with 0.0 for a slot not
    /// found, as the MQL swing scan does.
    pub fn recent_pivots(&self, strength: usize, scan: usize) -> (f64, f64, f64, f64) {
        let (mut lh, mut ph, mut ll, mut pl) = (0.0, 0.0, 0.0, 0.0);
        if scan <= strength {
            return (lh, ph, ll, pl);
        }
        for i in (strength + 1)..(scan - strength) {
            if lh == 0.0 && self.pivot_high(i, strength) {
                lh = self.bar(i).map(|b| b.high).unwrap_or(0.0);
            } else if lh != 0.0 && ph == 0.0 && self.pivot_high(i, strength) {
                ph = self.bar(i).map(|b| b.high).unwrap_or(0.0);
            }
            if ll == 0.0 && self.pivot_low(i, strength) {
                ll = self.bar(i).map(|b| b.low).unwrap_or(0.0);
            } else if ll != 0.0 && pl == 0.0 && self.pivot_low(i, strength) {
                pl = self.bar(i).map(|b| b.low).unwrap_or(0.0);
            }
            if ph != 0.0 && pl != 0.0 {
                break;
            }
        }
        (lh, ph, ll, pl)
    }

    /// The nearest pivot (high if `highs`, else low) at shifts `from ..
    /// to` (exclusive), as `(shift, price)`.
    pub fn first_pivot(&self, highs: bool, strength: usize, from: usize, to: usize) -> Option<(usize, f64)> {
        for i in from..to {
            let hit = if highs {
                self.pivot_high(i, strength)
            } else {
                self.pivot_low(i, strength)
            };
            if hit {
                let b = self.bar(i)?;
                return Some((i, if highs { b.high } else { b.low }));
            }
        }
        None
    }
}

/// A script's handle on a [`Window`]. Cloning the handle shares the window.
#[derive(Clone)]
pub struct WinHandle(pub Rc<RefCell<Window>>);

fn winbar_map(b: &WinBar) -> Map {
    let mut m = Map::new();
    m.insert("ts".into(), b.ts.into());
    m.insert("open".into(), b.open.into());
    m.insert("high".into(), b.high.into());
    m.insert("low".into(), b.low.into());
    m.insert("close".into(), b.close.into());
    m
}

fn opt_winbar(b: Option<WinBar>) -> Dynamic {
    match b {
        Some(b) => Dynamic::from_map(winbar_map(&b)),
        None => Dynamic::UNIT,
    }
}

fn shift_usize(i: i64) -> usize {
    if i < 0 {
        0
    } else {
        i as usize
    }
}

fn register_window_api(engine: &mut Engine) {
    engine.register_fn("window", |dur: i64, anchor: i64, keep: i64| -> WinHandle {
        WinHandle(Rc::new(RefCell::new(Window::new(dur, anchor, shift_usize(keep)))))
    });
    engine.register_fn("window", |dur: i64, anchor: i64| -> WinHandle {
        WinHandle(Rc::new(RefCell::new(Window::new(dur, anchor, 2))))
    });
    engine
        .register_type_with_name::<WinHandle>("Window")
        .register_get("rolled", |w: &mut WinHandle| w.0.borrow().rolled)
        .register_get("start", |w: &mut WinHandle| w.0.borrow().start)
        .register_get("closed", |w: &mut WinHandle| opt_winbar(w.0.borrow().closed))
        .register_get("forming", |w: &mut WinHandle| opt_winbar(w.0.borrow().forming))
        .register_get("forming_end", |w: &mut WinHandle| -> Dynamic {
            match w.0.borrow().forming_end() {
                Some(e) => e.into(),
                None => Dynamic::UNIT,
            }
        })
        .register_get("count", |w: &mut WinHandle| w.0.borrow().total as i64)
        .register_get("len", |w: &mut WinHandle| w.0.borrow().bars.len() as i64)
        .register_fn("finalize_through", |w: &mut WinHandle, t: i64| {
            w.0.borrow_mut().finalize_through(t)
        })
        .register_fn("wake", |w: &mut WinHandle, on: bool| w.0.borrow_mut().wake = on)
        .register_fn("bar", |w: &mut WinHandle, shift: i64| {
            opt_winbar(w.0.borrow().bar(shift_usize(shift)).copied())
        })
        .register_fn("o", |w: &mut WinHandle, shift: i64| {
            w.0.borrow().bar(shift_usize(shift)).map(|b| b.open).unwrap_or(f64::NAN)
        })
        .register_fn("h", |w: &mut WinHandle, shift: i64| {
            w.0.borrow().bar(shift_usize(shift)).map(|b| b.high).unwrap_or(f64::NAN)
        })
        .register_fn("l", |w: &mut WinHandle, shift: i64| {
            w.0.borrow().bar(shift_usize(shift)).map(|b| b.low).unwrap_or(f64::NAN)
        })
        .register_fn("c", |w: &mut WinHandle, shift: i64| {
            w.0.borrow().bar(shift_usize(shift)).map(|b| b.close).unwrap_or(f64::NAN)
        })
        .register_fn("ts", |w: &mut WinHandle, shift: i64| {
            w.0.borrow().bar(shift_usize(shift)).map(|b| b.ts).unwrap_or(0)
        })
        .register_fn("pivot_high", |w: &mut WinHandle, shift: i64, strength: i64| {
            w.0.borrow().pivot_high(shift_usize(shift), shift_usize(strength))
        })
        .register_fn("pivot_low", |w: &mut WinHandle, shift: i64, strength: i64| {
            w.0.borrow().pivot_low(shift_usize(shift), shift_usize(strength))
        })
        .register_fn("recent_pivots", |w: &mut WinHandle, strength: i64, scan: i64| -> Array {
            let (a, b, c, d) = w.0.borrow().recent_pivots(shift_usize(strength), shift_usize(scan));
            vec![a.into(), b.into(), c.into(), d.into()]
        })
        .register_fn(
            "first_pivot",
            |w: &mut WinHandle, highs: bool, strength: i64, from: i64, to: i64| -> Dynamic {
                match w.0.borrow().first_pivot(highs, shift_usize(strength), shift_usize(from), shift_usize(to)) {
                    Some((i, px)) => {
                        let mut m = Map::new();
                        m.insert("shift".into(), (i as i64).into());
                        m.insert("price".into(), px.into());
                        Dynamic::from_map(m)
                    }
                    None => Dynamic::UNIT,
                }
            },
        );
}

// ─── The native scanner ─────────────────────────────────────────────────────

/// A script's handle on a [`Scanner`]. Cloning the handle shares the scanner.
#[derive(Clone)]
pub struct ScanHandle(pub Rc<RefCell<Scanner>>);

fn dir_name(d: Direction) -> ImmutableString {
    dir_str(d)
}

fn sweep_map(s: &liquidity::Sweep) -> Map {
    let mut m = Map::new();
    m.insert("level_id".into(), s.level_id.into());
    m.insert("level_price".into(), s.level_price.into());
    m.insert("level_source".into(), s.level_source.clone().into());
    m.insert("level_sig".into(), s.level_sig.into());
    m.insert("level_dir".into(), s.level_side.as_str().into());
    m.insert("dir".into(), dir_name(s.dir).into());
    m.insert("start_ts".into(), s.start_ts.into());
    m.insert("extreme_ts".into(), s.extreme_ts.into());
    m.insert("extreme_price".into(), s.extreme_price.into());
    m.insert("magnitude_atr".into(), s.magnitude_atr.into());
    m.insert("level_formed_at".into(), s.level_formed_at.into());
    m
}

fn break_map(b: &liquidity::Break) -> Map {
    let mut m = Map::new();
    m.insert("tf".into(), tf_str(b.tf).into());
    m.insert("ts".into(), b.ts.into());
    m.insert("level".into(), b.level.into());
    m.insert("dir".into(), dir_name(b.dir).into());
    m
}

fn day_map(d: &liquidity::Day) -> Map {
    let mut m = Map::new();
    m.insert("day_ms".into(), d.day_ms.into());
    m.insert("open".into(), d.open.into());
    m.insert("high".into(), d.high.into());
    m.insert("low".into(), d.low.into());
    m.insert("close".into(), d.close.into());
    m
}

fn fvg_map(f: &liquidity::Fvg) -> Map {
    let mut m = Map::new();
    m.insert("id".into(), f.id.into());
    m.insert("dir".into(), dir_name(f.dir).into());
    m.insert("tf".into(), tf_str(f.tf).into());
    m.insert("ts".into(), f.ts.into());
    m.insert("near".into(), f.near.into());
    m.insert("far".into(), f.far.into());
    m.insert("ce".into(), f.ce.into());
    m.insert("status".into(), f.status.as_str().into());
    m.insert("is_ifvg".into(), f.is_inverted_kind.into());
    m.insert("c1_stop".into(), opt_f64(f.c1_stop));
    m
}

fn level_map(scan: &Scanner, l: &liquidity::Level) -> Map {
    let mut m = Map::new();
    m.insert("id".into(), l.id.into());
    m.insert("price".into(), l.price.into());
    m.insert("dir".into(), l.side.as_str().into());
    m.insert("source".into(), scan.source_name_owned(l).into());
    m.insert("tf".into(), tf_str(l.tf).into());
    m.insert("ts".into(), l.ts.into());
    m.insert("touch".into(), l.touch.into());
    m.insert("sig".into(), l.sig.into());
    m.insert("last_touch".into(), l.last_touch.into());
    m
}

fn draw_map_map(dm: &liquidity::DrawMap) -> Map {
    let mut m = Map::new();
    m.insert("ts".into(), dm.ts.into());
    m.insert("price".into(), dm.price.into());
    m.insert("atr".into(), dm.atr.into());
    let targets: Array = dm
        .targets
        .iter()
        .map(|t| {
            let mut tm = Map::new();
            tm.insert("level_id".into(), t.level_id.into());
            tm.insert("price".into(), t.price.into());
            tm.insert("direction".into(), dir_name(t.dir).into());
            tm.insert("distance_atr".into(), t.distance_atr.into());
            tm.insert("significance".into(), t.significance.into());
            tm.insert("draw_score".into(), t.draw_score.into());
            Dynamic::from_map(tm)
        })
        .collect();
    m.insert("targets".into(), Dynamic::from_array(targets));
    m
}

fn side_from_str(s: &str) -> Result<Side, Box<EvalAltResult>> {
    Side::parse(s).ok_or_else(|| format!("level side must be \"BSL\" or \"SSL\", got {s:?}").into())
}

fn register_scanner_api(engine: &mut Engine) {
    engine.register_type_with_name::<ScanHandle>("Scanner");
    engine.register_fn(
        "scanner",
        |cfg: Map| -> Result<ScanHandle, Box<EvalAltResult>> {
            let parsed: ScannerCfg = rhai::serde::from_dynamic(&Dynamic::from_map(cfg))
                .map_err(|e| Box::<EvalAltResult>::from(format!("scanner config: {e}")))?;
            Ok(ScanHandle(Rc::new(RefCell::new(Scanner::new(parsed)))))
        },
    );
    engine
        // Hook control.
        .register_fn("wake", |s: &mut ScanHandle, tf: &str, on: bool| {
            let id = if tf == "*" { u16::MAX } else { tf_of(tf) };
            let mut sc = s.0.borrow_mut();
            if on {
                sc.wake.insert(id);
            } else {
                sc.wake.remove(&id);
            }
        })
        .register_fn("is_awake", |s: &mut ScanHandle, tf: &str| -> bool {
            s.0.borrow().wake.contains(&tf_of(tf))
        })
        .register_fn("wake_book", |s: &mut ScanHandle, on: bool| {
            s.0.borrow_mut().wake_book = on;
        })
        .register_fn("candle_count", |s: &mut ScanHandle| -> i64 {
            s.0.borrow().candle_count()
        })
        .register_fn("last_atr", |s: &mut ScanHandle| -> f64 { s.0.borrow().last_atr() })
        .register_fn("stats", |s: &mut ScanHandle| -> Map {
            let (l, la, f, fa, sg) = s.0.borrow().sizes();
            let mut m = Map::new();
            m.insert("levels".into(), (l as i64).into());
            m.insert("active_levels".into(), (la as i64).into());
            m.insert("fvgs".into(), (f as i64).into());
            m.insert("active_fvgs".into(), (fa as i64).into());
            m.insert("signals".into(), (sg as i64).into());
            m
        })
        // Significance and levels.
        .register_fn(
            "significance",
            |s: &mut ScanHandle, source: &str, tf: &str, touch: i64| -> f64 {
                s.0.borrow_mut().significance_by_name(source, tf_of(tf), touch)
            },
        )
        .register_fn(
            "add_level",
            |s: &mut ScanHandle,
             price: f64,
             side: &str,
             source: &str,
             tf: &str,
             ts: i64,
             touch: i64,
             atr: f64|
             -> Result<i64, Box<EvalAltResult>> {
                let side = side_from_str(side)?;
                Ok(s.0
                    .borrow_mut()
                    .add_level_named(price, side, source, tf_of(tf), ts, touch, atr))
            },
        )
        .register_fn("level", |s: &mut ScanHandle, id: i64| -> Dynamic {
            let sc = s.0.borrow();
            match sc.level(id) {
                Some(l) => Dynamic::from_map(level_map(&sc, l)),
                None => Dynamic::UNIT,
            }
        })
        .register_fn(
            "levels_beyond",
            |s: &mut ScanHandle, side: &str, price: f64, min_sig: f64| -> Result<Array, Box<EvalAltResult>> {
                let side = side_from_str(side)?;
                let sc = s.0.borrow();
                Ok(sc
                    .levels_beyond(side, price, min_sig)
                    .into_iter()
                    .map(|l| Dynamic::from_map(level_map(&sc, l)))
                    .collect())
            },
        )
        .register_fn("nearest_above", |s: &mut ScanHandle, price: f64| -> Dynamic {
            opt_f64(s.0.borrow().nearest_beyond(Side::Bsl, price).map(|l| l.price))
        })
        .register_fn("nearest_below", |s: &mut ScanHandle, price: f64| -> Dynamic {
            opt_f64(s.0.borrow().nearest_beyond(Side::Ssl, price).map(|l| l.price))
        })
        .register_fn(
            "find_target",
            |s: &mut ScanHandle, dir: &str, entry: f64, stop: f64, min_rr: f64, min_sig: f64|
             -> Result<Dynamic, Box<EvalAltResult>> {
                let d = dir_from_str(dir)?;
                Ok(match s.0.borrow().find_target(d, entry, stop, min_rr, min_sig) {
                    Some((price, id, rr)) => {
                        let mut m = Map::new();
                        m.insert("price".into(), price.into());
                        m.insert("level_id".into(), id.into());
                        m.insert("rr".into(), rr.into());
                        Dynamic::from_map(m)
                    }
                    None => Dynamic::UNIT,
                })
            },
        )
        // Gaps.
        .register_fn("fvg", |s: &mut ScanHandle, id: i64| -> Dynamic {
            match s.0.borrow().fvg(id) {
                Some(f) => Dynamic::from_map(fvg_map(f)),
                None => Dynamic::UNIT,
            }
        })
        .register_fn("fvg_status", |s: &mut ScanHandle, id: i64| -> Dynamic {
            match s.0.borrow().fvg(id) {
                Some(f) => f.status.as_str().into(),
                None => Dynamic::UNIT,
            }
        })
        .register_fn(
            "fvgs_since",
            |s: &mut ScanHandle, tf: &str, dir: &str, inverted: bool, since_ts: i64|
             -> Result<Array, Box<EvalAltResult>> {
                let d = dir_from_str(dir)?;
                Ok(s.0
                    .borrow()
                    .fvgs_since(tf_of(tf), d, inverted, since_ts)
                    .into_iter()
                    .map(|f| Dynamic::from_map(fvg_map(f)))
                    .collect())
            },
        )
        .register_fn(
            "fvg_first",
            |s: &mut ScanHandle,
             tf: &str,
             dir: &str,
             inverted: bool,
             since_ts: i64,
             min_gap: f64,
             zone_lo: f64,
             zone_hi: f64,
             clear_from: f64,
             min_clear: f64|
             -> Result<Dynamic, Box<EvalAltResult>> {
                let d = dir_from_str(dir)?;
                Ok(match s.0.borrow().fvg_first(
                    tf_of(tf),
                    d,
                    inverted,
                    since_ts,
                    min_gap,
                    zone_lo,
                    zone_hi,
                    clear_from,
                    min_clear,
                ) {
                    Some(f) => Dynamic::from_map(fvg_map(f)),
                    None => Dynamic::UNIT,
                })
            },
        )
        // Draw map and day.
        .register_fn("draw_bias", |s: &mut ScanHandle| -> Dynamic {
            let mut m = Map::new();
            match s.0.borrow().draw_bias() {
                Some((d, conf)) => {
                    m.insert("direction".into(), dir_name(d).into());
                    m.insert("confidence".into(), conf.into());
                }
                None => {
                    m.insert("direction".into(), Dynamic::UNIT);
                    m.insert("confidence".into(), 0.0.into());
                }
            }
            Dynamic::from_map(m)
        })
        .register_fn("draw_map", |s: &mut ScanHandle| -> Dynamic {
            match s.0.borrow().draw_map() {
                Some(dm) => Dynamic::from_map(draw_map_map(dm)),
                None => Dynamic::UNIT,
            }
        })
        .register_fn("day", |s: &mut ScanHandle| -> Dynamic {
            match s.0.borrow().day() {
                Some(d) => Dynamic::from_map(day_map(d)),
                None => Dynamic::UNIT,
            }
        })
        .register_fn("day_high", |s: &mut ScanHandle| -> Dynamic {
            opt_f64(s.0.borrow().day().map(|d| d.high))
        })
        .register_fn("day_low", |s: &mut ScanHandle| -> Dynamic {
            opt_f64(s.0.borrow().day().map(|d| d.low))
        });
}

#[cfg(test)]
mod window_tests {
    use super::*;

    fn c(min: i64, o: f64, h: f64, l: f64, cl: f64) -> Candle {
        Candle {
            asset: 0,
            timeframe: crate::models::base_tf_id(),
            timestamp: chrono::DateTime::from_timestamp(min * 60, 0).unwrap().naive_utc(),
            open: o,
            high: h,
            low: l,
            close: cl,
            volume: 0.0,
            complete: true,
        }
    }

    #[test]
    fn window_rolls_on_the_first_candle_of_a_new_group() {
        let mut w = Window::new(300, 0, 10);
        assert!(w.step(&c(0, 1.0, 2.0, 0.5, 1.5)));
        assert!(w.closed.is_none());
        assert!(!w.step(&c(1, 1.5, 3.0, 1.0, 2.0)));
        assert!(!w.step(&c(4, 2.0, 2.5, 0.2, 0.4)));
        // Minute 5 starts the next 5-minute group: the first bar closes.
        assert!(w.step(&c(5, 0.4, 0.6, 0.3, 0.5)));
        let b = w.closed.unwrap();
        assert_eq!((b.ts, b.open, b.high, b.low, b.close), (0, 1.0, 3.0, 0.2, 0.4));
        assert_eq!(w.start, 300);
        assert_eq!(w.total, 1);
        assert_eq!(w.bar(1).unwrap().high, 3.0);
        assert!(w.bar(2).is_none());
    }

    #[test]
    fn window_anchor_shifts_the_grid_and_a_gap_still_rolls() {
        // 4h anchored at 22:00 UTC: the group boundary sits at 22:00, 02:00, …
        let mut w = Window::new(14400, 7200, 10);
        w.step(&c(21 * 60 + 59, 1.0, 1.0, 1.0, 1.0));
        assert_eq!(w.start, 18 * 3600);
        assert!(w.step(&c(22 * 60, 1.0, 1.0, 1.0, 1.0)));
        assert_eq!(w.start, 22 * 3600);
        // A data gap past the next boundary: the next candle rolls, and the
        // partial bar closes with what it had.
        assert!(w.step(&c(30 * 60, 5.0, 6.0, 4.0, 5.5)));
        assert_eq!(w.closed.unwrap().ts, 22 * 3600);
        // The 02:00 group had no candles; the candle's own group starts 06:00.
        assert_eq!(w.start, 30 * 3600);
    }

    #[test]
    fn finalize_through_closes_a_forming_bar_by_the_clock() {
        let mut w = Window::new(300, 0, 10);
        w.step(&c(0, 1.0, 2.0, 0.5, 1.5));
        assert!(!w.finalize_through(299));
        assert!(w.finalize_through(300));
        assert_eq!(w.total, 1);
        assert!(w.forming.is_none());
        assert!(!w.finalize_through(1000));
    }

    #[test]
    fn pivots_and_recent_pivots_follow_the_shift_convention() {
        let mut w = Window::new(60, 0, 50);
        // Highs: 1 2 5 2 1 | 1 2 7 2 1 | 3 (newest). Lows mirror negatively.
        let highs = [1.0, 2.0, 5.0, 2.0, 1.0, 1.0, 2.0, 7.0, 2.0, 1.0, 3.0];
        for (i, h) in highs.iter().enumerate() {
            w.step(&c(i as i64, *h, *h, -*h, *h));
        }
        // Close them all: one more candle rolls the last one.
        w.step(&c(100, 0.0, 0.0, 0.0, 0.0));
        assert_eq!(w.total, 11);
        // shift 1 = 3.0 (newest), shift 4 = 7.0, shift 9 = 5.0.
        assert_eq!(w.bar(4).unwrap().high, 7.0);
        assert!(w.pivot_high(4, 2));
        assert!(w.pivot_high(9, 2));
        assert!(!w.pivot_high(9, 3)); // shift 12 does not exist
        assert!(!w.pivot_high(1, 1)); // no right neighbour
        let (lh, ph, ll, pl) = w.recent_pivots(2, 12);
        assert_eq!((lh, ph), (7.0, 5.0));
        assert_eq!((ll, pl), (-7.0, -5.0));
        assert_eq!(w.first_pivot(true, 2, 3, 11), Some((4, 7.0)));
    }
}
