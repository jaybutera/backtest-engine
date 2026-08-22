//! Strategy knob bag + registry.
//!
//! # Why this exists
//!
//! A configurable knob normally costs a wall of mechanical plumbing: a serde
//! struct field, a merge arm, a resolved-config field, a CLI argument, a
//! local binding in the run function, a field on the component that reads it,
//! and a line in whatever builder assembles that component. Seven or eight
//! edits across as many files, none of which the compiler checks against each
//! other. Miss one link and you ship a knob that silently does nothing.
//!
//! `Params` replaces those hand-threaded fields with ONE validated bag that
//! travels from the config loader to the code that reads it unchanged. There
//! is no per-knob plumbing left to forget, and any number of consumers can
//! read the same bag without a chance of disagreeing about its contents.
//!
//! # Adding a knob
//!
//! 1. Add one `knob!` line to [`REGISTRY`] below: name, type, default, and a
//!    doc string describing what it does.
//! 2. Read it where it applies: `params.get_bool("my_knob")`.
//!
//! That is the whole change.
//!
//! # Validation
//!
//! The registry doubles as the validator. [`Params::from_table`] hard-errors
//! on any key not in the registry and on any key whose TOML type does not
//! match its registered type, with a did-you-mean hint for near-miss typos. A
//! config file therefore means exactly what it says or fails loudly — a
//! silently-ignored typo can never quietly change what a run does.
//!
//! # Defaults
//!
//! A missing key returns its REGISTERED default, never a zero value. Every
//! gate is registered default-off, so a config that does not mention a knob
//! behaves exactly as it did before that knob existed.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// A knob's declared type. Determines both how a TOML value is validated on
/// load and which accessor may read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Bool,
    F64,
    U32,
    I64,
    Str,
    /// A list of strings — the `["ASSET:50", "OTHER:2"]` shape used by the
    /// per-asset overrides, plus plain string lists.
    VecStr,
    /// A list of non-negative integers.
    VecU32,
}

/// A resolved knob value. Mirrors [`Kind`] one-to-one.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    F64(f64),
    U32(u32),
    I64(i64),
    Str(String),
    VecStr(Vec<String>),
    VecU32(Vec<u32>),
}

impl Value {
    fn kind(&self) -> Kind {
        match self {
            Value::Bool(_) => Kind::Bool,
            Value::F64(_) => Kind::F64,
            Value::U32(_) => Kind::U32,
            Value::I64(_) => Kind::I64,
            Value::Str(_) => Kind::Str,
            Value::VecStr(_) => Kind::VecStr,
            Value::VecU32(_) => Kind::VecU32,
        }
    }
}

/// One registry entry: the single place a knob is declared.
pub struct Knob {
    pub name: &'static str,
    pub kind: Kind,
    /// Built-in default, returned by the accessors when a config does not set
    /// the key. Must keep every gate inert, so a config that never mentions a
    /// knob behaves exactly as it did before that knob existed.
    pub default: fn() -> Value,
    /// What this knob does, mechanically. Shown in error messages and by any
    /// tool that dumps the registry.
    pub doc: &'static str,
}

/// Declare one knob: name, [`Kind`], default, and the reason it exists.
///
/// Exported so a strategy crate can declare its own knobs for
/// [`register_knobs`] / [`crate::strategy::StrategyFactory::knobs`] in the
/// same form the engine uses:
///
/// ```
/// use backtest_engine::knob;
/// use backtest_engine::params::{Knob, Value};
///
/// static MY_KNOBS: &[Knob] = &[
///     knob!("lookback", U32, Value::U32(20), "Bars of history the signal reads."),
/// ];
/// assert_eq!(MY_KNOBS[0].name, "lookback");
/// ```
#[macro_export]
macro_rules! knob {
    ($name:literal, $kind:ident, $default:expr, $doc:literal) => {
        $crate::params::Knob {
            name: $name,
            kind: $crate::params::Kind::$kind,
            default: || $default,
            doc: $doc,
        }
    };
}

fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}

/// THE knob registry: name, type, default, and the reason it exists.
///
/// Every `[strategy]` key a config may set appears here exactly once. Adding a
/// row is the entire cost of adding a knob; a key absent from this table is a
/// hard load error.
pub static REGISTRY: &[Knob] = &[
    // ─── Core selection ────────────────────────────────────────────────────
    knob!(
        "min_score",
        F64,
        Value::F64(0.0),
        "Minimum opportunity score to place an order. The driver may override \
         this per run, so a strategy reads the effective value from \
         `AdmitContext::min_score` rather than from the bag."
    ),
    knob!(
        "rr",
        F64,
        Value::F64(2.0),
        "Reward:risk target multiple used to project the take-profit from the \
         entry and stop."
    ),
    knob!(
        "max_hold",
        U32,
        Value::U32(300),
        "Maximum base-timeframe bars a filled position may stay open before it \
         is closed at market as a timeout. The result is inconclusive — a \
         timeout is neither a win nor a stop-out."
    ),
    // ─── Fees ──────────────────────────────────────────────────────────────
    knob!(
        "fees",
        Bool,
        Value::Bool(true),
        "Charge round-trip fees against every closed trade. The default is \
         context-dependent: a backtest turns it ON, a paper-only run OFF (see \
         `Params::apply_context_default_fees`)."
    ),
    knob!(
        "fee_schedule",
        Str,
        s(""),
        "Named fee schedule to price trades under. Empty = infer from the data \
         source (see `fees::FeeSchedule`); an explicit value always wins."
    ),
    // ─── Stop geometry floors ──────────────────────────────────────────────
    knob!(
        "min_stop_atr",
        F64,
        Value::F64(0.0),
        "Floor on the stop distance, in ATR units of the recent candle buffer \
         (0 = off). A strategy applies this in its own `admit`."
    ),
    knob!(
        "min_stop_pct",
        F64,
        Value::F64(0.0),
        "Floor on the stop distance, as a percent of the entry price \
         (0 = off). Wider stops carry proportionally less fee drag per R."
    ),
    // ─── Trade management ──────────────────────────────────────────────────
    knob!(
        "breakeven_r",
        F64,
        Value::F64(0.0),
        "Open profit in R at which the stop moves up to lock in `trail_lock_r` \
         (0 = off). Measured from the actual fill, not the planned entry."
    ),
    knob!(
        "trail_lock_r",
        F64,
        Value::F64(0.0),
        "R the trailing stop locks in once `breakeven_r` is reached. 0 means \
         plain breakeven; 0.5 locks half an R."
    ),
    knob!(
        "partial_tp_r",
        F64,
        Value::F64(0.0),
        "Bank half the position when open profit reaches this many R, letting \
         the rest run to its stop or target (0 = off)."
    ),
    knob!(
        "derisk_after_min",
        U32,
        Value::U32(0),
        "After a position has been open this many base-timeframe bars, close it \
         at market if its unrealized R at the bar close is below \
         `derisk_below_r` (0 = off). Models standing down from a stale trade \
         that is going nowhere rather than waiting for the full stop."
    ),
    knob!(
        "derisk_below_r",
        F64,
        Value::F64(0.0),
        "Unrealized-R threshold for the de-risk exit. Only meaningful when \
         `derisk_after_min > 0`; 0.0 closes stale trades that are flat or under \
         water."
    ),
    // ─── Cancel watchdogs ──────────────────────────────────────────────────
    knob!(
        "cancel_on_target_consumed",
        Bool,
        Value::Bool(false),
        "Cancel a pending entry limit when a completed bar closes beyond the \
         strategy's stamped `min_target` — the move ran without us. Hybrid fill \
         lens only; acts on bars strictly before the fill bar."
    ),
    knob!(
        "cancel_on_setup_invalidated",
        Bool,
        Value::Bool(false),
        "Cancel a pending entry limit when the driver reports the strategy's \
         setup invalidated on a completed bar. Hybrid fill lens only; acts on \
         bars strictly before the fill bar."
    ),
    // ─── Reporting-only (does not change which trades fire) ────────────────
    knob!(
        "risk_frac",
        F64,
        Value::F64(0.0),
        "Compounding-risk sizing: when > 0 the report embeds a dollar equity \
         curve where each trade risks this fraction of the running balance, \
         sized off the balance realized at the trade's open, starting from \
         `account_size`. 0 = off (pure R-multiple report). Affects REPORTING \
         ONLY — never which trades fire."
    ),
    knob!(
        "account_size",
        F64,
        Value::F64(10_000.0),
        "Starting account balance for the compounding equity curve."
    ),
    // ─── Fill lens ─────────────────────────────────────────────────────────
    // The canonical home for these is a separate fill-lens file (see
    // `strategy_config::load_fill`); they are registered here so a strategy
    // file that carries them still loads, with a deprecation warning.
    knob!(
        "allow_signal_bar_fill",
        Bool,
        Value::Bool(false),
        "Whether the entry limit may fill on the signal bar itself. false is \
         the honest default: that bar's high and low printed before the order \
         existed, so the fill search starts on the bar after the signal."
    ),
    knob!(
        "entry_slippage_r",
        F64,
        Value::F64(0.0),
        "Entry slippage in R-multiples of the planned risk. The fill price \
         moves toward the losing side by `slip · R`; the divisor stays the \
         planned R, so slippage reads as a uniform drag, never a rescale."
    ),
    knob!(
        "intrabar_stop_first",
        Bool,
        Value::Bool(true),
        "Tie-break when one candle spans both the stop and the take-profit and \
         the intrabar order is unknowable. true resolves to the stop \
         (pessimistic)."
    ),
    knob!(
        "entry_fill_mode",
        Str,
        s("limit"),
        "How an entry order fills. \"limit\" = a pure resting limit that fills \
         when price reaches it. \"hybrid\" = the order-management model: pays \
         taker when it aggresses past the entry, models a seed-and-chase \
         watchdog, and abandons on target/stop-gap/age. \"tick\" = resolve \
         entries and exits against real trade prints in true time order. \
         \"rest_on_ready\" = hybrid, plus a maker fill on the signal bar when \
         the order demonstrably rested before it."
    ),
    knob!(
        "chase_r",
        F64,
        Value::F64(0.1),
        "Chase gate and fill cap for the hybrid lens, in R-multiples of \
         |entry − stop|. When price sits in front of the entry, a fill is only \
         taken out to `entry ± chase_r · R`, taker at that boundary."
    ),
    knob!(
        "chase_requires_seed",
        Bool,
        Value::Bool(true),
        "Require a seed touch — the signal bar or its predecessor traded to the \
         entry — before any chase is armed."
    ),
    knob!(
        "immediate_chase_at_open",
        Bool,
        Value::Bool(true),
        "When armed and the decision bar opens in front of the entry but inside \
         the chase cap, fill immediately at the open as a taker, modelling a \
         watchdog firing on its first poll."
    ),
    knob!(
        "race_maker_first",
        Bool,
        Value::Bool(false),
        "When one bar reaches BOTH the entry and the chase boundary, and the \
         intrabar order is unknowable, resolve to the maker fill at the entry \
         instead of the pessimistic boundary chase."
    ),
    knob!(
        "deferred_chase_at_open",
        Bool,
        Value::Bool(false),
        "Deferred open-chase: a decision bar opening in front of the entry \
         within the cap carries its open as the limit's chase price for the \
         limit's whole life. A maker touch at the entry still wins the race, \
         but every other resolution books taker at the carried open instead of \
         the full-cap boundary, and never abandons."
    ),
    knob!(
        "past_entry_fee",
        Str,
        s("taker"),
        "Fee side when the decision bar opens at or past the entry. \"taker\" \
         because a resting limit is marketable once price is already through \
         it, so it aggresses; \"maker\" is a counterfactual."
    ),
    knob!(
        "rest_min_lead_secs",
        I64,
        Value::I64(60),
        "Minimum lead, in seconds, between a setup becoming final and the \
         signal bar, before the rest-on-ready lens grants a maker fill on that \
         bar. Larger is stricter."
    ),
    knob!(
        "tick_chase",
        Bool,
        Value::Bool(true),
        "In tick mode, model the entry chase on real tick order: an armed limit \
         whose chase boundary is reached before any tick touches the entry \
         fills taker at the boundary rather than never filling."
    ),
    knob!(
        "stop_gap_bps_default",
        F64,
        Value::F64(0.0),
        "Stop-exit gap penalty in basis points of the stop price, applied to \
         every asset without a per-asset override. On a genuine stop-loss exit \
         the fill is widened toward the loss before P&L and the exit fee are \
         computed, modelling the trigger-cascade slippage a resting stop \
         suffers. 0 = off."
    ),
    knob!(
        "stop_gap_bps_asset",
        VecStr,
        Value::VecStr(Vec::new()),
        "Per-asset `stop_gap_bps` overrides as `ASSET:bps` strings. Assets with \
         no match use `stop_gap_bps_default`."
    ),
    // ─── Sizing ────────────────────────────────────────────────────────────
    knob!(
        "risk_usd",
        F64,
        Value::F64(-1.0),
        "Dollar risk per trade, for a driver that sizes positions. -1 (unset) \
         reads back as None. The backtest reports in R-multiples and ignores \
         it."
    ),
    knob!(
        "risk_usd_asset",
        VecStr,
        Value::VecStr(Vec::new()),
        "Per-asset `risk_usd` overrides as `ASSET:usd` strings, parsed on the \
         LAST ':' so a prefixed asset name also works. Assets with no match \
         size at `risk_usd`."
    ),
    // ─── Depth-aware sizing (l2book.rs) ────────────────────────────────────
    knob!(
        "l2_sizing_mode",
        Str,
        s("off"),
        "\"off\" (default) | \"log\" (compute and journal the depth cap, size \
         unchanged) | \"cap\" (size = min(risk sizing, depth cap)) | \"max\" \
         (size = the depth cap outright)."
    ),
    knob!(
        "l2_slip_cap_bps",
        F64,
        Value::F64(2.0),
        "Slippage budget per taker leg, in basis points of mid, including the \
         half-spread. Both the entry leg and the stop-exit leg must fit inside \
         it: size that can get in but not out is not size you can carry."
    ),
    knob!(
        "l2_adverse_bps",
        F64,
        Value::F64(0.7),
        "Flat offset added to the book-walk slippage, in basis points — the \
         latency and adverse-selection tax a real taker pays over what a \
         pre-fill book snapshot predicts."
    ),
    knob!(
        "l2_max_staleness_s",
        F64,
        Value::F64(90.0),
        "Maximum book-snapshot age, in seconds, the sizing will still act on. \
         Anything older falls back to the risk-based size and journals no cap."
    ),
];

/// Knobs declared by a strategy rather than by the engine, registered at
/// startup through [`register_knobs`].
///
/// The engine's own registry is a static table because its knobs are fixed
/// at compile time. A strategy plugged in through
/// [`crate::strategy::StrategyFactory`] brings its own knobs, and they have to
/// be visible to the same validation, typo hints and default lookup as the
/// built-in ones — otherwise a private `[strategy]` key is either a hard load
/// error or a silently ignored line, and neither is acceptable. This table is
/// the extension point: it is consulted after `REGISTRY` everywhere a knob is
/// looked up by name.
static EXTRA: LazyLock<Mutex<Vec<&'static Knob>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Register strategy-declared knobs so they validate and resolve like built-in
/// ones. Call before loading any config that sets them — the driver does this
/// for the selected factory's [`crate::strategy::StrategyFactory::knobs`].
///
/// A name that collides with a built-in knob is an error: the engine's
/// meaning of `rr` or `max_hold` must not be redefinable by a plugin.
/// Re-registering the same slice is a no-op, so a driver that builds several
/// per-asset strategies may call this freely.
pub fn register_knobs(knobs: &'static [Knob]) -> Result<(), String> {
    let mut extra = EXTRA.lock().unwrap();
    for k in knobs {
        if REGISTRY.iter().any(|r| r.name == k.name) {
            return Err(format!(
                "strategy knob \"{}\" collides with a built-in engine knob",
                k.name
            ));
        }
        if let Some(existing) = extra.iter().find(|e| e.name == k.name) {
            if !std::ptr::eq(*existing, k) {
                return Err(format!(
                    "strategy knob \"{}\" registered twice with different definitions",
                    k.name
                ));
            }
            continue;
        }
        extra.push(k);
    }
    Ok(())
}

/// Forget every strategy-declared knob. Mainly for tests.
pub fn clear_registered_knobs() {
    EXTRA.lock().unwrap().clear();
}

/// Every knob currently known: the built-in registry followed by any
/// strategy-declared ones.
pub fn all_knobs() -> Vec<&'static Knob> {
    let mut v: Vec<&'static Knob> = REGISTRY.iter().collect();
    v.extend(EXTRA.lock().unwrap().iter().copied());
    v
}

/// Look up a knob spec by name, built-in first, then strategy-declared.
pub fn spec(name: &str) -> Option<&'static Knob> {
    REGISTRY.iter().find(|k| k.name == name).or_else(|| {
        EXTRA
            .lock()
            .unwrap()
            .iter()
            .copied()
            .find(|k| k.name == name)
    })
}

/// The registered default for `name`.
///
/// # Panics
/// Panics if `name` is not registered — an accessor typo is a programmer bug,
/// caught by the `every_accessed_knob_is_registered` test rather than shipped.
fn registered_default(name: &str) -> Value {
    match spec(name) {
        Some(k) => (k.default)(),
        None => {
            panic!("params: knob \"{name}\" is not registered — add a knob!() row in params.rs, or register it via params::register_knobs")
        }
    }
}

/// A validated bag of strategy knobs.
///
/// Cloned freely — it is small, a few dozen entries at most — and carried
/// verbatim from the config loader to whatever reads it. Only keys a config
/// actually SET are stored; everything else resolves to its registered default
/// on read, which is what keeps an unmentioning config identical to the
/// pre-knob engine.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Params {
    set: HashMap<String, Value>,
}

impl Params {
    /// An empty bag — every knob at its registered default.
    pub fn new() -> Self {
        Params {
            set: HashMap::new(),
        }
    }

    /// The resolved value of a knob: what the config set, else the registered
    /// default.
    ///
    /// # Panics
    /// Panics if `name` is not registered.
    pub fn get(&self, name: &str) -> Value {
        match self.set.get(name) {
            Some(v) => v.clone(),
            None => registered_default(name),
        }
    }

    /// Whether a config explicitly set this key, as opposed to inheriting the
    /// registered default. Needed by the handful of knobs whose "unset" state
    /// is semantically distinct from their default value — `min_score` (unset
    /// ⇒ defer to the engine's own value) and `risk_usd` (unset ⇒ a driver
    /// that needs a size has none, and should refuse to run).
    pub fn is_set(&self, name: &str) -> bool {
        self.set.contains_key(name)
    }

    /// Set a knob directly, bypassing TOML — for CLI flags that map onto a
    /// knob, and for tests.
    ///
    /// # Panics
    /// Panics on an unregistered name or a type mismatch — both are programmer
    /// bugs, not user input.
    pub fn set(&mut self, name: &str, v: Value) {
        let k =
            spec(name).unwrap_or_else(|| panic!("params: set() on unregistered knob \"{name}\""));
        assert_eq!(
            v.kind(),
            k.kind,
            "params: set(\"{name}\") type mismatch — registry says {:?}, got {:?}",
            k.kind,
            v.kind()
        );
        self.set.insert(name.to_string(), v);
    }

    /// Overlay `child`'s explicitly-set keys onto `self`, child winning.
    ///
    /// This IS the `base = "..."` inheritance rule: a key the child does not
    /// mention is left at the base's value; a key the child sets replaces the
    /// base's wholesale. Lists are replaced, never concatenated.
    pub fn overlay(&mut self, child: &Params) {
        for (k, v) in &child.set {
            self.set.insert(k.clone(), v.clone());
        }
    }

    // ─── Typed accessors ───────────────────────────────────────────────────
    //
    // Each returns the set value, or the registered default when unset. A type
    // mismatch cannot occur: `from_table` and `set` both validate against the
    // registry on the way in.

    pub fn get_bool(&self, name: &str) -> bool {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::Bool(b) => b,
            other => panic!(
                "params: get_bool(\"{name}\") but knob is {:?}",
                other.kind()
            ),
        }
    }

    pub fn get_f64(&self, name: &str) -> f64 {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::F64(v) => v,
            other => panic!("params: get_f64(\"{name}\") but knob is {:?}", other.kind()),
        }
    }

    pub fn get_u32(&self, name: &str) -> u32 {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::U32(v) => v,
            other => panic!("params: get_u32(\"{name}\") but knob is {:?}", other.kind()),
        }
    }

    pub fn get_i64(&self, name: &str) -> i64 {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::I64(v) => v,
            other => panic!("params: get_i64(\"{name}\") but knob is {:?}", other.kind()),
        }
    }

    pub fn get_str(&self, name: &str) -> String {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::Str(v) => v,
            other => panic!("params: get_str(\"{name}\") but knob is {:?}", other.kind()),
        }
    }

    pub fn get_vec_str(&self, name: &str) -> Vec<String> {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::VecStr(v) => v,
            other => panic!(
                "params: get_vec_str(\"{name}\") but knob is {:?}",
                other.kind()
            ),
        }
    }

    pub fn get_vec_u32(&self, name: &str) -> Vec<u32> {
        match self
            .set
            .get(name)
            .cloned()
            .unwrap_or_else(|| registered_default(name))
        {
            Value::VecU32(v) => v,
            other => panic!(
                "params: get_vec_u32(\"{name}\") but knob is {:?}",
                other.kind()
            ),
        }
    }

    /// `get_f64`, but a sentinel-valued knob reads back as `None`. The two
    /// knobs whose absence is meaningful (`max_fee_r`, `risk_usd`) register a
    /// negative sentinel and expose it as an `Option<f64>` here, preserving the
    /// `Option` semantics the old resolved struct carried.
    pub fn get_opt_f64(&self, name: &str, sentinel: f64) -> Option<f64> {
        let v = self.get_f64(name);
        if v == sentinel {
            None
        } else {
            Some(v)
        }
    }

    /// `get_str`, but the empty string reads back as `None` — for the knobs
    /// whose unset state means "leave the CLI/engine default alone"
    /// (`signal_max_age`, `pancake_bias`).
    pub fn get_opt_str(&self, name: &str) -> Option<String> {
        let v = self.get_str(name);
        if v.is_empty() {
            None
        } else {
            Some(v)
        }
    }

    /// Parse a `ASSET:value` list knob into a map.
    ///
    /// Splits on the FIRST ':' and **drops** an entry whose value does not
    /// parse as a float, rather than substituting a fallback. A typo therefore
    /// disables one override instead of silently installing a number nobody
    /// wrote — the failure that is easiest to notice.
    pub fn get_asset_map(&self, name: &str) -> HashMap<String, f64> {
        self.get_vec_str(name)
            .iter()
            .filter_map(|s| {
                s.split_once(':')
                    .and_then(|(a, v)| v.parse::<f64>().ok().map(|x| (a.to_string(), x)))
            })
            .collect()
    }

    /// Parse a `ASSET:signal` list knob into (asset, signal) tuples.
    pub fn get_asset_pairs(&self, name: &str) -> Vec<(String, String)> {
        self.get_vec_str(name)
            .iter()
            .filter_map(|s| {
                s.split_once(':')
                    .map(|(a, b)| (a.to_string(), b.to_string()))
            })
            .collect()
    }

    /// Build a bag from a TOML `[strategy]` table, validating every key against
    /// the registry.
    ///
    /// Two classes of hard error:
    ///   - an UNKNOWN key, which would otherwise be a typo that silently
    ///     changes nothing;
    ///   - a key whose TOML type does not match its registered type.
    ///
    /// `src` names the file in error messages.
    pub fn from_table(tbl: &toml::value::Table, src: &str) -> Result<Self, String> {
        let mut p = Params::new();
        for (key, raw) in tbl {
            let k = spec(key).ok_or_else(|| {
                format!(
                    "strategy config {src}: unknown [strategy] key \"{key}\"{}",
                    nearest_hint(key)
                )
            })?;
            let v = coerce(raw, k.kind).ok_or_else(|| {
                format!(
                    "strategy config {src}: [strategy] key \"{key}\" expects {:?}, got {}",
                    k.kind,
                    raw.type_str()
                )
            })?;
            p.set.insert(key.clone(), v);
        }
        Ok(p)
    }

    /// Apply the one knob whose DEFAULT (not value) depends on run context:
    /// `fees` defaults ON for a backtest and OFF otherwise. A config that sets
    /// `fees` explicitly is untouched.
    pub fn apply_context_default_fees(&mut self, replay: bool) {
        if !self.is_set("fees") {
            self.set.insert("fees".to_string(), Value::Bool(replay));
        }
    }
}

/// Suggest a close registered name for an unknown key, so a typo says what it
/// probably meant.
fn nearest_hint(key: &str) -> String {
    let best = all_knobs()
        .into_iter()
        .map(|k| (edit_distance(key, k.name), k.name))
        .filter(|(d, _)| *d <= 3)
        .min_by_key(|(d, _)| *d);
    match best {
        Some((_, name)) => format!(" (did you mean \"{name}\"?)"),
        None => String::new(),
    }
}

/// Plain Levenshtein distance — used only for typo hints in error messages.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// Convert a raw TOML value to the registered [`Kind`], or `None` if the TOML
/// type is incompatible.
///
/// The one deliberate coercion is integer → float: TOML writes `rr = 3` as an
/// integer and it must land on the float knob. Float → integer is NOT accepted
/// — a `max_hold = 180.5` should say so rather than silently truncate.
fn coerce(raw: &toml::Value, kind: Kind) -> Option<Value> {
    match kind {
        Kind::Bool => raw.as_bool().map(Value::Bool),
        Kind::F64 => raw
            .as_float()
            .or_else(|| raw.as_integer().map(|i| i as f64))
            .map(Value::F64),
        Kind::U32 => raw
            .as_integer()
            .filter(|i| *i >= 0 && *i <= u32::MAX as i64)
            .map(|i| Value::U32(i as u32)),
        Kind::I64 => raw.as_integer().map(Value::I64),
        Kind::Str => raw.as_str().map(|s| Value::Str(s.to_string())),
        Kind::VecStr => raw.as_array().and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Option<Vec<String>>>()
                .map(Value::VecStr)
        }),
        Kind::VecU32 => raw.as_array().and_then(|a| {
            a.iter()
                .map(|v| {
                    v.as_integer()
                        .filter(|i| *i >= 0 && *i <= u32::MAX as i64)
                        .map(|i| i as u32)
                })
                .collect::<Option<Vec<u32>>>()
                .map(Value::VecU32)
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The extension registry is process-global; tests that touch it
    /// serialize on this lock and start from an empty table.
    static EXTRA_GUARD: Mutex<()> = Mutex::new(());

    static PLUGIN_KNOBS: &[Knob] = &[
        knob!(
            "plug_depth",
            U32,
            Value::U32(7),
            "A strategy-declared knob."
        ),
        knob!("plug_ratio", F64, Value::F64(0.5), "Another one."),
    ];

    #[test]
    fn registered_knobs_validate_and_default_like_builtin_ones() {
        let _g = EXTRA_GUARD.lock().unwrap();
        clear_registered_knobs();
        // Before registration: unknown, and a hard error.
        assert!(spec("plug_depth").is_none());
        assert!(Params::from_table(&table("plug_depth = 3"), "t").is_err());

        register_knobs(PLUGIN_KNOBS).unwrap();
        let p = Params::from_table(&table("plug_depth = 3"), "t").unwrap();
        assert_eq!(p.get_u32("plug_depth"), 3);
        // Unset resolves to the declared default, same as a built-in.
        assert_eq!(p.get_f64("plug_ratio"), 0.5);
        assert!(!p.is_set("plug_ratio"));
        // Wrong type is still a hard error.
        assert!(Params::from_table(&table("plug_ratio = \"x\""), "t").is_err());
        // Typo hints see strategy knobs too.
        let err = Params::from_table(&table("plug_dept = 3"), "t").unwrap_err();
        assert!(err.contains("plug_depth"), "got: {err}");
        // Registering the same slice again is a no-op.
        register_knobs(PLUGIN_KNOBS).unwrap();
        assert_eq!(
            all_knobs()
                .iter()
                .filter(|k| k.name == "plug_depth")
                .count(),
            1
        );
        clear_registered_knobs();
    }

    #[test]
    fn a_strategy_knob_may_not_shadow_a_builtin() {
        let _g = EXTRA_GUARD.lock().unwrap();
        clear_registered_knobs();
        static CLASH: &[Knob] = &[knob!("rr", F64, Value::F64(9.0), "Redefines rr.")];
        let err = register_knobs(CLASH).unwrap_err();
        assert!(err.contains("collides"), "got: {err}");
        assert!(spec("rr").map(|k| (k.default)()) == Some(Value::F64(2.0)));
        clear_registered_knobs();
    }

    fn table(toml_src: &str) -> toml::value::Table {
        toml_src
            .parse::<toml::Value>()
            .unwrap()
            .as_table()
            .unwrap()
            .clone()
    }

    #[test]
    fn registry_names_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for k in REGISTRY {
            assert!(
                seen.insert(k.name),
                "duplicate knob in REGISTRY: {}",
                k.name
            );
        }
    }

    #[test]
    fn registered_defaults_match_their_declared_kind() {
        for k in REGISTRY {
            assert_eq!(
                (k.default)().kind(),
                k.kind,
                "knob {} default is a different type than its declared kind",
                k.name
            );
        }
    }

    #[test]
    fn unset_keys_read_their_registered_default() {
        let p = Params::new();
        assert!(!p.get_bool("cancel_on_target_consumed"));
        assert_eq!(p.get_f64("rr"), 2.0);
        assert_eq!(p.get_u32("max_hold"), 300);
        assert_eq!(p.get_str("entry_fill_mode"), "limit");
        assert!(p.get_vec_str("stop_gap_bps_asset").is_empty());
        assert_eq!(p.get_i64("rest_min_lead_secs"), 60);
    }

    #[test]
    fn unknown_key_is_a_hard_error() {
        // The property everything else rests on: a typo must FAIL loudly, never
        // be silently ignored.
        let err = Params::from_table(&table("min_scor = 5.0"), "typo.toml").unwrap_err();
        assert!(err.contains("unknown [strategy] key"), "got: {err}");
        assert!(err.contains("min_scor"), "got: {err}");
        // And it points at the key it probably meant.
        assert!(err.contains("min_score"), "got: {err}");
    }

    #[test]
    fn wrong_type_is_a_hard_error() {
        let err = Params::from_table(&table("rr = \"two\""), "bad.toml").unwrap_err();
        assert!(err.contains("expects F64"), "got: {err}");
        let err2 = Params::from_table(&table("race_maker_first = 1"), "bad2.toml").unwrap_err();
        assert!(err2.contains("expects Bool"), "got: {err2}");
    }

    #[test]
    fn integer_literals_read_as_floats() {
        // A config writing `rr = 3` and `l2_max_staleness_s = 90` must land on
        // the float knobs without complaint.
        let p = Params::from_table(&table("rr = 3\nl2_max_staleness_s = 90"), "x.toml").unwrap();
        assert_eq!(p.get_f64("rr"), 3.0);
        assert_eq!(p.get_f64("l2_max_staleness_s"), 90.0);
    }

    #[test]
    fn float_does_not_silently_truncate_into_an_integer_knob() {
        let err = Params::from_table(&table("max_hold = 180.5"), "x.toml").unwrap_err();
        assert!(err.contains("expects U32"), "got: {err}");
    }

    #[test]
    fn overlay_is_child_wins_per_key() {
        let mut base = Params::from_table(
            &table("min_score = 5.0\nrace_maker_first = true\nbreakeven_r = 1.5"),
            "base.toml",
        )
        .unwrap();
        let child = Params::from_table(&table("race_maker_first = false"), "child.toml").unwrap();
        base.overlay(&child);
        assert!(!base.get_bool("race_maker_first"), "child wins");
        assert_eq!(base.get_f64("min_score"), 5.0, "unmentioned key inherited");
        assert_eq!(base.get_f64("breakeven_r"), 1.5);
    }

    #[test]
    fn overlay_replaces_lists_wholesale_never_concatenates() {
        let mut base =
            Params::from_table(&table("stop_gap_bps_asset = [\"A:1\", \"B:2\"]"), "b").unwrap();
        let child = Params::from_table(&table("stop_gap_bps_asset = [\"C:3\"]"), "c").unwrap();
        base.overlay(&child);
        assert_eq!(base.get_vec_str("stop_gap_bps_asset"), vec!["C:3"]);
    }

    #[test]
    fn is_set_distinguishes_unset_from_default_valued() {
        let p = Params::from_table(&table("min_score = 0.0"), "x").unwrap();
        assert!(
            p.is_set("min_score"),
            "explicitly set, even to the default value"
        );
        assert!(!Params::new().is_set("min_score"));
    }

    #[test]
    fn sentinel_options_round_trip() {
        // `risk_usd` registers a negative sentinel because "unset" is
        // semantically distinct from any real value: a driver that needs a
        // size and finds None should refuse to run, not invent one.
        let p = Params::new();
        assert_eq!(p.get_opt_f64("risk_usd", -1.0), None);
        let p2 = Params::from_table(&table("risk_usd = 25"), "x").unwrap();
        assert_eq!(p2.get_opt_f64("risk_usd", -1.0), Some(25.0));
    }

    #[test]
    fn every_registered_default_is_readable_through_its_typed_accessor() {
        // Guards the failure mode where a knob's declared Kind and the
        // accessor callers actually use drift apart: reading through the wrong
        // one panics, so exercising all of them keeps the registry honest.
        let p = Params::new();
        for k in REGISTRY {
            match k.kind {
                Kind::Bool => {
                    p.get_bool(k.name);
                }
                Kind::F64 => {
                    p.get_f64(k.name);
                }
                Kind::U32 => {
                    p.get_u32(k.name);
                }
                Kind::I64 => {
                    p.get_i64(k.name);
                }
                Kind::Str => {
                    p.get_str(k.name);
                }
                Kind::VecStr => {
                    p.get_vec_str(k.name);
                }
                Kind::VecU32 => {
                    p.get_vec_u32(k.name);
                }
            }
        }
    }

    #[test]
    fn every_knob_documents_itself() {
        // A registry row is also the knob's documentation; an empty doc string
        // would ship a knob nobody can find out the meaning of.
        for k in REGISTRY {
            assert!(
                !k.doc.trim().is_empty(),
                "knob {} has no doc string",
                k.name
            );
            assert!(
                k.doc.len() > 20,
                "knob {} has a uselessly short doc",
                k.name
            );
        }
    }

    #[test]
    fn context_default_fees_only_fills_an_unset_key() {
        let mut replay = Params::new();
        replay.apply_context_default_fees(true);
        assert!(replay.get_bool("fees"), "backtest defaults fees ON");

        let mut live = Params::new();
        live.apply_context_default_fees(false);
        assert!(!live.get_bool("fees"), "non-backtest defaults fees OFF");

        let mut explicit = Params::from_table(&table("fees = false"), "x").unwrap();
        explicit.apply_context_default_fees(true);
        assert!(!explicit.get_bool("fees"), "explicit value survives");
    }

    #[test]
    fn asset_map_parsing_drops_malformed_entries() {
        let p = Params::from_table(
            &table("stop_gap_bps_asset = [\"AAA:0.25\", \"BBB:0.5\"]"),
            "x",
        )
        .unwrap();
        let m = p.get_asset_map("stop_gap_bps_asset");
        assert_eq!(m.get("AAA"), Some(&0.25));
        assert_eq!(m.get("BBB"), Some(&0.5));
        // A value that does not parse as a float is DROPPED, never defaulted —
        // a typo disables one override rather than silently installing a
        // number nobody wrote.
        let p2 = Params::from_table(
            &table("stop_gap_bps_asset = [\"AAA:bad\", \"BBB:1.5\"]"),
            "x",
        )
        .unwrap();
        let m2 = p2.get_asset_map("stop_gap_bps_asset");
        assert_eq!(
            m2.get("AAA"),
            None,
            "malformed entry dropped, not defaulted"
        );
        assert_eq!(m2.get("BBB"), Some(&1.5));
    }

    #[test]
    fn asset_map_splits_on_the_first_colon() {
        // A namespaced asset name keeps only its prefix as the KEY, and the
        // rest is read as the value. `get_asset_map` is therefore wrong for
        // namespaced names; `risk_usd_asset` splits on the LAST ':' instead
        // and is parsed by its own caller.
        let p = Params::from_table(&table("stop_gap_bps_asset = [\"ns:AAA\"]"), "x").unwrap();
        // "ns" -> "AAA" does not parse as a float, so the entry drops.
        assert!(p.get_asset_map("stop_gap_bps_asset").is_empty());
    }

    #[test]
    fn asset_pairs_parsing() {
        let p = Params::from_table(
            &table("stop_gap_bps_asset = [\"AAA:one\", \"BBB:two\"]"),
            "x",
        )
        .unwrap();
        assert_eq!(
            p.get_asset_pairs("stop_gap_bps_asset"),
            vec![
                ("AAA".to_string(), "one".to_string()),
                ("BBB".to_string(), "two".to_string())
            ]
        );
    }
}
