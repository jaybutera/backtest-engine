//! Strategies written in [Rhai](https://rhai.rs): the whole strategy, not a
//! verdict, lives in a script.
//!
//! # What a script can do
//!
//! A script implements the same three things a native [`Strategy`] does,
//! with the same information: it sees every completed candle of its asset
//! on every configured timeframe, keeps whatever state it likes between
//! bars, emits opportunities, decides their geometry, and manages the book
//! after each bar. Nothing is pre-computed for it. The engine supplies
//! data access (candle history per timeframe, other assets' series, the
//! clock), generic helpers (ATR, extremes, fees, time zones, TOML), and
//! the order API (limit entries via opportunities, market entries, stop and
//! target changes, closes, cancels). Everything that decides what to trade
//! is the script's.
//!
//! # Script shape
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
//! The state map is bound to `this` in every hook, so a script mutates it
//! in place (`this.levels.push(x)`) rather than passing it around; Rhai
//! passes function arguments by value, so a helper that needs to mutate a
//! sub-object is written as a method and called on it
//! (`this.registry.add(lvl)` with `fn add(lvl) { this.levels.push(lvl) }`).
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
    has_bar_close: bool,
    asset: String,
}

impl RhaiStrategy {
    /// Compile `path` and run its top level plus `init`.
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
                let id = tf_id(tf);
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

        let has_init = ast.iter_functions().any(|f| f.name == "init");
        let has_admit = ast.iter_functions().any(|f| f.name == "admit");
        let has_bar_close = ast.iter_functions().any(|f| f.name == "on_bar_close");
        if !ast.iter_functions().any(|f| f.name == "on_candle") {
            return Err("script defines no `on_candle(c)` function".into());
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
            has_bar_close,
            asset: asset.to_string(),
        })
    }

    fn call(
        &self,
        name: &str,
        args: impl rhai::FuncArgs,
    ) -> Result<Dynamic, Box<EvalAltResult>> {
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
        let out: Dynamic = match self.call("on_candle", (candle.clone(),)) {
            Ok(v) => v,
            Err(e) => self.fail("on_candle", e),
        };
        let mut opps = Vec::new();
        if out.is_unit() {
            return opps;
        }
        let arr: Array = match out.try_cast::<Array>() {
            Some(a) => a,
            None => self.fail(
                "on_candle",
                "must return an array of opportunities (or ())".into(),
            ),
        };
        for item in arr {
            match item.try_cast::<ScriptOpp>() {
                Some(o) => {
                    if !o.meta.is_empty() {
                        self.meta
                            .borrow_mut()
                            .insert(o.inner.id.clone(), o.meta);
                    }
                    opps.push(o.inner);
                }
                None => self.fail("on_candle", "array item is not an opportunity".into()),
            }
        }
        opps
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
        // Erase the borrow's lifetime for the duration of the call; see
        // ScriptBook::with for the invariant.
        let raw: *mut Book<'_> = book;
        self.book_ptr
            .set(raw as *mut Book<'static>);
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
        .register_get("tf", |c: &mut Candle| -> ImmutableString {
            tf_name(c.timeframe).to_string().into()
        })
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
                b.with(|bk| bk.market_entry(&id, sig_type_id(&st), dir, stop, tp, score))
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
}
