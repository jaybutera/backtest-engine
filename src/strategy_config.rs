//! Config loading: strategy parameters and fill-lens parameters, from TOML
//! files rather than a wall of CLI flags.
//!
//! # Two files, deliberately separate
//!
//! A **strategy file** holds what decides *which* trades to take: the knob bag,
//! the asset list, a pointer to an engine config, and any data-source
//! overrides. A **fill file** holds how an order *executes* once a strategy has
//! decided to place it: resting versus chasing, maker versus taker, the stop
//! gap, the intrabar tie-break.
//!
//! Keeping them apart is what makes a comparison honest. Run one strategy under
//! several fill lenses and the difference is entirely execution; run several
//! strategies under one lens and the difference is entirely selection. Mixing
//! the two in one file makes it impossible to say which half moved a result, so
//! each loader rejects the other's keys outright rather than quietly accepting
//! them.
//!
//! Operational parameters — dates, paths, output destinations — stay as CLI
//! flags, because they describe one invocation rather than the method.
//!
//! # Override layering
//!
//! A strategy file may set `base = "<other file>"`; keys it sets win over the
//! base's. One level only — a base may not itself declare a base, so resolving
//! a config never walks an arbitrary chain.

use crate::params::{Params, Value};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

/// Raw deserialized strategy file. Every field optional so an override file sets
/// only what it changes. `deny_unknown_fields` turns typos into hard errors.
///
/// NOTE the `[strategy]` table is deliberately a generic `toml::Table` rather
/// than a typed struct: its keys are validated against `params::REGISTRY`
/// instead (see `Params::from_table`), which catches BOTH unknown keys and
/// wrong-typed values, and — crucially — needs no per-knob code here. Adding a
/// knob is one registry row, not a field + a merge arm + a resolver line.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyFile {
    /// Path to another strategy file to inherit from (one level only).
    base: Option<String>,
    /// Path to the engine config (the `[v2]` TOML).
    engine: Option<String>,
    /// Which registered [`crate::strategy::StrategyFactory`] builds this
    /// preset's strategy, by name. Optional when the driver has exactly one
    /// factory registered.
    #[serde(rename = "factory")]
    strategy_impl: Option<String>,
    /// Asset watchlist for this preset.
    assets: Option<Vec<String>>,
    /// Per-contract fee specs (`[[contract]]` tables) for flat-fee futures
    /// schedules. Registered with [`crate::fees`] by the driver at load time,
    /// so a run's fee model is readable from its config rather than hidden in
    /// a `register_contract` call somewhere in a binary.
    #[serde(default)]
    contract: Vec<ContractEntry>,
    /// Remove contract-roll discontinuities from every loaded series before
    /// the strategy sees it (see [`crate::data::roll_adjust`]). Off by
    /// default: it rewrites prices, so a run must opt in knowingly.
    roll_adjust: Option<bool>,
    /// Optional config-driven data-source overrides (`[[source]]` tables).
    /// Each maps an asset to one or more backing parquet file stems with an
    /// optional price scale/offset, so a backtest can run an asset against a
    /// donor or proxy feed rescaled into its own price terms. Absent assets
    /// keep the default behavior, backed by `{asset}_{interval}.parquet`.
    #[serde(default)]
    source: Vec<SourceEntry>,
    /// The raw `[strategy]` table. Validated by `Params::from_table`, not serde.
    #[serde(default)]
    strategy: toml::value::Table,
    /// Free-form metadata for downstream tooling. The engine ignores this
    /// table entirely; it is accepted only so `deny_unknown_fields` does not
    /// reject a config that carries one.
    #[serde(default)]
    #[allow(dead_code)]
    viz: Option<toml::Value>,
    /// Free-form parameters for a strategy whose knobs are not registered
    /// with the engine — a script's. Not validated; handed to the factory
    /// verbatim via [`crate::strategy::BuildContext::script`]. Child keys
    /// override base keys one by one, like `[strategy]`.
    #[serde(default)]
    script: toml::value::Table,
}

/// The fill-lens key names. A strategy file carrying any of them is using the
/// pre-split layout and earns a deprecation warning; the canonical home is a
/// separate fill file's `[fill]` table. Kept in sync with `FillParams` below.
const FILL_KEYS: &[&str] = &[
    "entry_fill_mode",
    "chase_r",
    "chase_requires_seed",
    "immediate_chase_at_open",
    "race_maker_first",
    "deferred_chase_at_open",
    "past_entry_fee",
    "allow_signal_bar_fill",
    "entry_slippage_r",
    "intrabar_stop_first",
    "rest_min_lead_secs",
    "tick_chase",
];

/// One `[[source]]` table. `files` are SOURCE symbols (filename stems before
/// the `_{interval}.parquet` suffix), loaded in order with the first to provide a
/// given timestamp winning. `scale`/`offset` transform OHLC into the target
/// asset's price terms: `price = src_price * scale + offset`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceEntry {
    /// The asset name this source backs.
    asset: String,
    /// Backing file stems, in splice/precedence order.
    files: Vec<String>,
    #[serde(default = "default_scale")]
    scale: f64,
    #[serde(default)]
    offset: f64,
}

fn default_scale() -> f64 {
    1.0
}

/// One `[[contract]]` table: a flat per-contract fee spec for one asset.
///
/// ```toml
/// [[contract]]
/// asset = "EXAMPLE"
/// point_value = 5.0      # dollars per one point of price, per contract
/// round_turn = 1.90      # all-in fee per contract, in and out
/// schedule = "futures"   # or "futures_full"; default "futures"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractEntry {
    asset: String,
    point_value: f64,
    round_turn: f64,
    #[serde(default = "default_contract_schedule")]
    schedule: String,
}

fn default_contract_schedule() -> String {
    "futures".to_string()
}

/// A resolved `[[contract]]` entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ContractSpecEntry {
    pub asset: String,
    pub point_value: f64,
    pub round_turn: f64,
    /// `"futures"` or `"futures_full"` — which flat schedule this spec
    /// belongs to.
    pub schedule: String,
}

/// A resolved data-source override for one asset, ready to feed the loader.
#[derive(Debug, Clone)]
pub struct AssetSource {
    pub asset: String,
    pub files: Vec<String>,
    pub scale: f64,
    pub offset: f64,
}

// ─── Fill lens ─────────────────────────────────────────────────────────────
//
// The fill lens describes how the simulator models entry fills. It lives in its
// own file under a `[fill]` table so a strategy and a lens can be crossed
// freely — N strategies × M lenses — instead of hand-writing every combination
// as its own config. That separation is also what lets a result be attributed:
// hold the strategy fixed and vary the lens, and any difference is execution.

/// Raw deserialized fill file. Only a `[fill]` table is allowed; a `[strategy]`
/// table (or any other top-level key) is a hard error — a fill file must carry
/// no strategy params. `deny_unknown_fields` turns a stray `[strategy]` (or a
/// typo) into a parse error, which `load_fill` translates into a clear message.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FillFile {
    #[serde(default)]
    fill: FillParams,
}

/// The `[fill]` table. All optional; unset fields fall back to the built-in
/// fill defaults (the same values the old combined `[strategy]` table used).
/// `deny_unknown_fields` means a STRATEGY key placed in a fill file (e.g.
/// `min_score`) is rejected as an unknown field — the strict validation the
/// spec requires.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FillParams {
    allow_signal_bar_fill: Option<bool>,
    entry_slippage_r: Option<f64>,
    intrabar_stop_first: Option<bool>,
    entry_fill_mode: Option<String>,
    chase_r: Option<f64>,
    chase_requires_seed: Option<bool>,
    immediate_chase_at_open: Option<bool>,
    race_maker_first: Option<bool>,
    deferred_chase_at_open: Option<bool>,
    past_entry_fee: Option<String>,
    rest_min_lead_secs: Option<i64>,
    tick_chase: Option<bool>,
    /// Stop-exit gap penalty, bps of the stop price. See
    /// `ResolvedFill::stop_gap_bps_default`.
    stop_gap_bps_default: Option<f64>,
    /// Per-asset override, `ASSET:bps` strings (substring key match, parsed on
    /// the LAST ':' so a namespaced `ns:AAA:5.27` also works — same convention
    /// as `risk_usd_asset`). Unmatched assets use `stop_gap_bps_default`.
    stop_gap_bps_asset: Option<Vec<String>>,
}

/// Fully-resolved fill lens — no optionals. Semantics identical to the fill
/// fields that used to live on `ResolvedStrategy`.
#[derive(Debug, Clone)]
pub struct ResolvedFill {
    pub allow_signal_bar_fill: bool,
    pub entry_slippage_r: f64,
    pub intrabar_stop_first: bool,
    pub entry_fill_mode: String,
    pub chase_r: f64,
    pub chase_requires_seed: bool,
    pub immediate_chase_at_open: bool,
    /// Hybrid only: resolve a both-reachable (entry + chase boundary in one
    /// bar) race to the maker fill at the entry instead of the pessimistic
    /// boundary chase. See `PaperTrader::race_maker_first`.
    pub race_maker_first: bool,
    /// Hybrid only: deferred open-chase — a B0 decision bar carries its open
    /// as the limit's chase fill price (maker touch still wins first; no
    /// boundary-priced chases, no abandons). Only acts when
    /// `immediate_chase_at_open = false`. See
    /// `PaperTrader::deferred_chase_at_open`.
    pub deferred_chase_at_open: bool,
    pub past_entry_fee: String,
    /// rest-on-Ready lens only: minimum Ready→touch lead (seconds) for the
    /// rested-maker signal-bar fill. See `PaperTrader::rest_min_lead_secs`.
    pub rest_min_lead_secs: i64,
    /// Tick lens only: model the live entry-chase on real tick order (a resting
    /// limit whose chase boundary is reached before the entry fills taker at the
    /// boundary). `true` = live-faithful default; `false` = pure resting maker
    /// (the pre-chase tick model). See `PaperTrader::tick_chase`.
    pub tick_chase: bool,
    /// Stop-exit gap penalty, bps of the stop price: on a genuine stop-loss
    /// exit (not TP, not a de-risk/timeout close), the fill price is moved
    /// `stop*(1 - bps*1e-4)` for longs / `stop*(1 + bps*1e-4)` for shorts
    /// before r_pnl and the exit fee are computed. Models the live trigger-
    /// cascade slippage a resting stop suffers vs. a clean fill exactly at the
    /// stop price. Flat, deterministic — no randomness. Default 0 (off,
    /// matches every existing lens). See `PaperTrader::stop_gap_bps_default`.
    pub stop_gap_bps_default: f64,
    /// Per-asset override for `stop_gap_bps_default`, raw `ASSET:bps` strings
    /// (parsed on the LAST ':' in `main.rs`, same as `risk_usd_asset`).
    pub stop_gap_bps_asset: Vec<String>,
}

impl ResolvedFill {
    /// The built-in fill defaults (mode="limit", no signal-bar fill, no slip,
    /// stop-first, hybrid knobs at their canonical values). Used when neither a
    /// `--fill` file nor a default fill file is available.
    pub fn builtin_default() -> Self {
        ResolvedFill {
            allow_signal_bar_fill: false,
            entry_slippage_r: 0.0,
            intrabar_stop_first: true,
            entry_fill_mode: "limit".to_string(),
            chase_r: 0.1,
            chase_requires_seed: true,
            immediate_chase_at_open: true,
            race_maker_first: false,
            deferred_chase_at_open: false,
            past_entry_fee: "taker".to_string(),
            rest_min_lead_secs: 60,
            tick_chase: true,
            stop_gap_bps_default: 0.0,
            stop_gap_bps_asset: Vec::new(),
        }
    }
}

/// Load and resolve a fill lens from a TOML file.
///
/// STRICT validation (both directions):
///   - A strategy key in the fill file (e.g. `min_score`) → error (the
///     `[fill]` table `deny_unknown_fields` rejects it; a bare top-level
///     strategy key or a `[strategy]` table is rejected by `FillFile`'s
///     `deny_unknown_fields`). The parse error is surfaced verbatim.
///   - A `[strategy]` table in the fill file → error (same mechanism).
pub fn load_fill(path: &Path) -> Result<ResolvedFill, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("reading fill config {}: {e}", path.display()))?;
    let f: FillFile = toml::from_str(&content).map_err(|e| {
        format!(
            "parsing fill config {}: {e}\n  (a fill file may contain ONLY a [fill] table with \
             fill-lens keys — selection keys like min_score and rr belong in a \
             strategy file)",
            path.display()
        )
    })?;
    let p = f.fill;
    let d = ResolvedFill::builtin_default();
    Ok(ResolvedFill {
        allow_signal_bar_fill: p.allow_signal_bar_fill.unwrap_or(d.allow_signal_bar_fill),
        entry_slippage_r: p.entry_slippage_r.unwrap_or(d.entry_slippage_r),
        intrabar_stop_first: p.intrabar_stop_first.unwrap_or(d.intrabar_stop_first),
        entry_fill_mode: p.entry_fill_mode.unwrap_or(d.entry_fill_mode),
        chase_r: p.chase_r.unwrap_or(d.chase_r),
        chase_requires_seed: p.chase_requires_seed.unwrap_or(d.chase_requires_seed),
        immediate_chase_at_open: p
            .immediate_chase_at_open
            .unwrap_or(d.immediate_chase_at_open),
        race_maker_first: p.race_maker_first.unwrap_or(d.race_maker_first),
        deferred_chase_at_open: p.deferred_chase_at_open.unwrap_or(d.deferred_chase_at_open),
        past_entry_fee: p.past_entry_fee.unwrap_or(d.past_entry_fee),
        rest_min_lead_secs: p.rest_min_lead_secs.unwrap_or(d.rest_min_lead_secs),
        tick_chase: p.tick_chase.unwrap_or(d.tick_chase),
        stop_gap_bps_default: p.stop_gap_bps_default.unwrap_or(d.stop_gap_bps_default),
        stop_gap_bps_asset: p.stop_gap_bps_asset.unwrap_or(d.stop_gap_bps_asset),
    })
}

/// Scan a strategy file's TOML for any legacy fill keys under `[strategy]`.
/// Returns the offending key names (empty = clean, new-style file). Used to
/// print a loud one-time deprecation warning: fill fields belong in a
/// a fill file now, not the strategy file. Reads the file directly — a cheap
/// re-parse into a generic Value — so it works regardless of `base`.
pub fn legacy_fill_keys_in_strategy(path: &Path) -> Vec<String> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(value) = content.parse::<toml::Value>() else {
        return Vec::new();
    };
    let mut found = Vec::new();
    if let Some(tbl) = value.get("strategy").and_then(|v| v.as_table()) {
        for k in FILL_KEYS {
            if tbl.contains_key(*k) {
                found.push((*k).to_string());
            }
        }
    }
    found
}

/// Read the top-level `factory = "<name>"` key from a strategy file without
/// validating the rest of it, following one level of `base`.
///
/// The driver needs the factory name BEFORE the full load, because the
/// factory's own knobs have to be registered for the `[strategy]` table to
/// validate. A cheap generic re-parse breaks that cycle.
pub fn peek_strategy_factory(path: &Path) -> Option<String> {
    fn read(path: &Path) -> Option<toml::Value> {
        std::fs::read_to_string(path)
            .ok()?
            .parse::<toml::Value>()
            .ok()
    }
    let child = read(path)?;
    if let Some(name) = child.get("factory").and_then(|v| v.as_str()) {
        return Some(name.to_string());
    }
    let base_rel = child.get("base").and_then(|v| v.as_str())?;
    read(&resolve_relative(path, base_rel))?
        .get("factory")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Which run context we're resolving defaults for. The `fees` default differs:
/// backtests default fees ON, live defaults fees OFF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Context {
    Replay,
    Live,
}

/// Fully-resolved strategy: the structural bits the loader computes (paths,
/// asset list, data sources) plus the validated knob bag.
///
/// The ~80 hand-threaded knob fields this struct used to carry are gone —
/// they live in `params` now, and the accessors below derive the handful of
/// non-gate values (engine wiring, sizing, management) that consumers still
/// need as concrete Rust types. Every DECISION-gate knob is read straight off
/// `params` by `decide()`, so there is no per-knob code on the replay or live
/// path that could be forgotten on one side.
#[derive(Debug, Clone)]
pub struct ResolvedStrategy {
    pub engine: Option<String>,
    /// The factory name from the file's top-level `factory = "..."`, if set.
    pub strategy_impl: Option<String>,
    pub assets: Option<Vec<String>>,
    /// Per-contract fee specs from `[[contract]]` tables. Empty when none.
    pub contracts: Vec<ContractSpecEntry>,
    /// Whether the loader should roll-adjust every series (`roll_adjust`).
    pub roll_adjust: bool,
    /// Config-driven data-source overrides, one per asset that declares a
    /// `[[source]]`. Empty when none are declared (loader uses defaults).
    pub sources: Vec<AssetSource>,
    /// The validated knob bag — the single carrier for everything a
    /// `[strategy]` table can set. Handed verbatim to whatever consumes the
    /// preset, so there is no second place a knob has to be wired up.
    pub params: Params,
    /// The unvalidated `[script]` table, for factories whose parameters are
    /// not registered knobs. Empty when the file has none.
    pub script: toml::value::Table,
}

impl ResolvedStrategy {
    /// The configured score floor, or `None` to defer to whatever the engine
    /// config says. A config that names `min_score` — even at 0.0 — takes over.
    pub fn min_score(&self) -> Option<f64> {
        if self.params.is_set("min_score") {
            Some(self.params.get_f64("min_score"))
        } else {
            None
        }
    }

    // ─── Core selection ────────────────────────────────────────────────────
    pub fn rr(&self) -> f64 {
        self.params.get_f64("rr")
    }
    pub fn max_hold(&self) -> usize {
        self.params.get_u32("max_hold") as usize
    }

    // ─── Fees ──────────────────────────────────────────────────────────────
    pub fn use_fees(&self) -> bool {
        self.params.get_bool("fees")
    }
    /// Empty = infer the schedule from the data sources.
    pub fn fee_schedule(&self) -> String {
        self.params.get_str("fee_schedule")
    }

    // ─── Stop geometry floors ──────────────────────────────────────────────
    pub fn min_stop_atr(&self) -> f64 {
        self.params.get_f64("min_stop_atr")
    }
    pub fn min_stop_pct(&self) -> f64 {
        self.params.get_f64("min_stop_pct")
    }

    // ─── Trade management ──────────────────────────────────────────────────
    pub fn breakeven_r(&self) -> f64 {
        self.params.get_f64("breakeven_r")
    }
    pub fn trail_lock_r(&self) -> f64 {
        self.params.get_f64("trail_lock_r")
    }
    pub fn partial_tp_r(&self) -> f64 {
        self.params.get_f64("partial_tp_r")
    }
    pub fn derisk_after_min(&self) -> usize {
        self.params.get_u32("derisk_after_min") as usize
    }
    pub fn derisk_below_r(&self) -> f64 {
        self.params.get_f64("derisk_below_r")
    }

    // ─── Cancel watchdogs ──────────────────────────────────────────────────
    pub fn cancel_on_target_consumed(&self) -> bool {
        self.params.get_bool("cancel_on_target_consumed")
    }
    pub fn cancel_on_setup_invalidated(&self) -> bool {
        self.params.get_bool("cancel_on_setup_invalidated")
    }

    // ─── Reporting-only ────────────────────────────────────────────────────
    pub fn risk_frac(&self) -> f64 {
        self.params.get_f64("risk_frac")
    }
    pub fn account_size(&self) -> f64 {
        self.params.get_f64("account_size")
    }

    // ─── Fill lens (pre-split layout; the canonical home is a fill file) ───
    pub fn allow_signal_bar_fill(&self) -> bool {
        self.params.get_bool("allow_signal_bar_fill")
    }
    pub fn entry_slippage_r(&self) -> f64 {
        self.params.get_f64("entry_slippage_r")
    }
    pub fn intrabar_stop_first(&self) -> bool {
        self.params.get_bool("intrabar_stop_first")
    }
    pub fn entry_fill_mode(&self) -> String {
        self.params.get_str("entry_fill_mode")
    }
    pub fn chase_r(&self) -> f64 {
        self.params.get_f64("chase_r")
    }
    pub fn chase_requires_seed(&self) -> bool {
        self.params.get_bool("chase_requires_seed")
    }
    pub fn immediate_chase_at_open(&self) -> bool {
        self.params.get_bool("immediate_chase_at_open")
    }
    pub fn past_entry_fee(&self) -> String {
        self.params.get_str("past_entry_fee")
    }

    // ─── Sizing ────────────────────────────────────────────────────────────
    /// `None` when unset — a driver that needs a size should refuse to run
    /// rather than invent one.
    pub fn risk_usd(&self) -> Option<f64> {
        self.params.get_opt_f64("risk_usd", -1.0)
    }
    pub fn risk_usd_asset(&self) -> Vec<String> {
        self.params.get_vec_str("risk_usd_asset")
    }

    // ─── Depth-aware sizing ────────────────────────────────────────────────
    pub fn l2_sizing_mode(&self) -> String {
        self.params.get_str("l2_sizing_mode")
    }
    pub fn l2_slip_cap_bps(&self) -> f64 {
        self.params.get_f64("l2_slip_cap_bps")
    }
    pub fn l2_adverse_bps(&self) -> f64 {
        self.params.get_f64("l2_adverse_bps")
    }
    pub fn l2_max_staleness_s(&self) -> f64 {
        self.params.get_f64("l2_max_staleness_s")
    }
}

/// Merge a child's set fields over an accumulator (the base). Only `Some` fields
/// in `child` overwrite.
fn merge(into: &mut StrategyFile, child: StrategyFile) {
    if child.engine.is_some() {
        into.engine = child.engine;
    }
    if child.strategy_impl.is_some() {
        into.strategy_impl = child.strategy_impl;
    }
    if child.roll_adjust.is_some() {
        into.roll_adjust = child.roll_adjust;
    }
    if child.assets.is_some() {
        into.assets = child.assets;
    }
    // Contracts: same all-or-nothing replacement as sources.
    if !child.contract.is_empty() {
        into.contract = child.contract;
    }
    // Source overrides: a child that declares any `[[source]]` replaces the
    // base's set wholesale (matches the all-or-nothing nature of `assets`).
    if !child.source.is_empty() {
        into.source = child.source;
    }
    // `[strategy]` keys: child's keys win, per key. This is the whole of what
    // the old 70-line `take!(field);` wall did — the generic form needs no
    // per-knob line, so adding a knob costs nothing here.
    for (k, v) in child.strategy {
        into.strategy.insert(k, v);
    }
    for (k, v) in child.script {
        into.script.insert(k, v);
    }
}

/// Resolve a relative path against the directory of the file that referenced it,
/// so `base`/`engine` paths are relative to the strategy file, not the cwd.
fn resolve_relative(base_file: &Path, p: &str) -> PathBuf {
    let pb = PathBuf::from(p);
    if pb.is_absolute() {
        pb
    } else {
        base_file.parent().map(|d| d.join(&pb)).unwrap_or(pb)
    }
}

fn read_file(path: &Path) -> Result<StrategyFile, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("reading strategy config {}: {e}", path.display()))?;
    toml::from_str(&content).map_err(|e| format!("parsing strategy config {}: {e}", path.display()))
}

/// File stems that imply a flat per-contract (futures) fee schedule.
///
/// Empty by default, so nothing is inferred and every run uses the
/// proportional schedule unless a config says otherwise. Register the stems of
/// your futures datasets here, and a run backed by one of them picks the
/// matching schedule automatically instead of silently pricing futures as
/// basis points of notional.
static FUTURES_STEMS: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Same, for full-size contract datasets.
static FUTURES_FULL_STEMS: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Register file stems whose presence implies the `Futures` fee schedule.
pub fn register_futures_stems(stems: &[&str]) {
    let mut v = FUTURES_STEMS.lock().unwrap();
    v.extend(stems.iter().map(|s| s.to_string()));
}

/// Register file stems whose presence implies the `FuturesFullSize` schedule.
pub fn register_futures_full_stems(stems: &[&str]) {
    let mut v = FUTURES_FULL_STEMS.lock().unwrap();
    v.extend(stems.iter().map(|s| s.to_string()));
}

/// Forget every registered stem. Mainly for tests.
pub fn clear_futures_stems() {
    FUTURES_STEMS.lock().unwrap().clear();
    FUTURES_FULL_STEMS.lock().unwrap().clear();
}

/// Whether any resolved `[[source]]` entry is backed by a registered futures
/// dataset stem. Such a run is implicitly on a flat per-contract fee schedule
/// — see the `fee_schedule` inference in `load_strategy`.
pub fn sources_use_futures(sources: &[AssetSource]) -> bool {
    let stems = FUTURES_STEMS.lock().unwrap();
    sources
        .iter()
        .any(|s| s.files.iter().any(|f| stems.iter().any(|k| k == f)))
}

/// Same inference for the registered full-size contract stems.
pub fn sources_use_futures_full(sources: &[AssetSource]) -> bool {
    let stems = FUTURES_FULL_STEMS.lock().unwrap();
    sources
        .iter()
        .any(|s| s.files.iter().any(|f| stems.iter().any(|k| k == f)))
}

/// Load and fully resolve a strategy config, applying one level of `base`
/// inheritance and filling unset fields from context defaults.
pub fn load_strategy(path: &Path, context: Context) -> Result<ResolvedStrategy, String> {
    let child = read_file(path)?;

    // One level of base inheritance. Track the DIRECTORY of the file that
    // declares `engine`, so an inherited `engine` path resolves against the
    // base file's dir, not the child's. Without this, a child in `a/b/x.toml`
    // inheriting from `c/d/base.toml` would resolve the base's
    // `engine = "../eng.toml"` against `a/` instead of `c/` — pointing at a
    // path that usually does not exist, and failing in a way that looks like a
    // missing file rather than a resolution bug. The declaring file is the
    // child when the CHILD set engine, else the base.
    let child_declares_engine = child.engine.is_some();
    let mut engine_decl_path = path.to_path_buf();
    let mut acc = if let Some(ref base_rel) = child.base {
        let base_path = resolve_relative(path, base_rel);
        let base = read_file(&base_path)?;
        if base.base.is_some() {
            return Err(format!(
                "strategy config {} declares a base, but it is itself used as a base \
                 by {} — only one level of inheritance is allowed",
                base_path.display(),
                path.display()
            ));
        }
        // If the child does NOT override engine but the base sets one, that
        // engine path was written relative to the BASE file.
        if !child_declares_engine && base.engine.is_some() {
            engine_decl_path = base_path.clone();
        }
        base
    } else {
        StrategyFile::default()
    };
    merge(&mut acc, child);

    // Resolve the engine path against the file that DECLARED it (child if the
    // child set it, else the base — tracked above).
    let engine = acc.engine.as_ref().map(|e| {
        resolve_relative(&engine_decl_path, e)
            .to_string_lossy()
            .into_owned()
    });

    let sources: Vec<AssetSource> = acc
        .source
        .into_iter()
        .map(|e| AssetSource {
            asset: e.asset,
            files: e.files,
            scale: e.scale,
            offset: e.offset,
        })
        .collect();

    let contracts: Vec<ContractSpecEntry> = acc
        .contract
        .into_iter()
        .map(|c| ContractSpecEntry {
            asset: c.asset,
            point_value: c.point_value,
            round_turn: c.round_turn,
            schedule: c.schedule,
        })
        .collect();
    for c in &contracts {
        if c.schedule != "futures" && c.schedule != "futures_full" {
            return Err(format!(
                "strategy config {}: [[contract]] for {} names schedule \"{}\" \
                 (expected \"futures\" or \"futures_full\")",
                path.display(),
                c.asset,
                c.schedule
            ));
        }
        if !c.point_value.is_finite() || c.point_value <= 0.0 || c.round_turn < 0.0 {
            return Err(format!(
                "strategy config {}: [[contract]] for {} needs point_value > 0 and round_turn >= 0",
                path.display(),
                c.asset
            ));
        }
    }

    // Validate the merged `[strategy]` table against the knob registry. This
    // replaces serde's `deny_unknown_fields` and is strictly stronger: it
    // rejects unknown keys AND wrong-typed values, with a did-you-mean hint.
    // A typo must fail loudly rather than be silently ignored — every result
    // rests on a config meaning exactly what it says.
    let mut params = Params::from_table(&acc.strategy, &path.display().to_string())?;

    // The one context-dependent DEFAULT: a backtest defaults fees ON, any
    // other context OFF. An explicit `fees` in the config is untouched.
    params.apply_context_default_fees(context == Context::Replay);

    // The fee schedule is implicit in the data: a run backed by a registered
    // futures dataset is priced per contract, everything else proportionally.
    // An explicit `fee_schedule` in the config still wins — the registered
    // default is the empty string precisely so "unset" is distinguishable
    // from a deliberate choice.
    // Declared `[[contract]]` specs imply the flat schedule they belong to:
    // a config that prices its assets per contract does not also have to say
    // so twice. A mixed declaration (some full-size, some micro) picks the
    // full-size schedule only if every contract is full-size.
    if !params.is_set("fee_schedule") {
        let inferred = if !contracts.is_empty() {
            if contracts.iter().all(|c| c.schedule == "futures_full") {
                "futures_full"
            } else {
                "futures"
            }
        } else if sources_use_futures(&sources) {
            "futures"
        } else if sources_use_futures_full(&sources) {
            "futures_full"
        } else {
            "perp"
        };
        params.set("fee_schedule", Value::Str(inferred.to_string()));
    }

    Ok(ResolvedStrategy {
        engine,
        strategy_impl: acc.strategy_impl,
        assets: acc.assets,
        contracts,
        roll_adjust: acc.roll_adjust.unwrap_or(false),
        sources,
        params,
        script: acc.script,
    })
}

// Context-specific defaults used to live in a `Defaults` struct here. Only
// `fees` ever actually differed by context (rr/max_hold were 2.0/300 in both
// arms); it now lives in the registry with the context flip applied by
// `Params::apply_context_default_fees` in `load_strategy`.

/// Parse `ASSET:value` strings into a HashMap, matching the existing CLI parse
/// behavior (including its fallback-on-malformed defaults).
pub fn parse_asset_map(items: &[String], malformed_default: f64) -> HashMap<String, f64> {
    items
        .iter()
        .filter_map(|s| {
            s.split_once(':')
                .map(|(a, v)| (a.to_string(), v.parse().unwrap_or(malformed_default)))
        })
        .collect()
}

/// Parse `ASSET:signal` strings into (asset, signal) tuples.
pub fn parse_asset_signals(items: &[String]) -> Vec<(String, String)> {
    items
        .iter()
        .filter_map(|s| {
            s.split_once(':')
                .map(|(a, b)| (a.to_string(), b.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stem registrations are process-global, so tests that touch them run
    /// under one lock and reset it on the way in.
    static STEM_GUARD: Mutex<()> = Mutex::new(());

    fn write_tmp(dir: &Path, name: &str, content: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let p = dir.join(name);
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn contracts_factory_and_roll_adjust_resolve() {
        let _g = STEM_GUARD.lock().unwrap();
        clear_futures_stems();
        let d = tmpdir("contracts");
        let p = write_tmp(
            &d,
            "s.toml",
            r#"
factory = "macross"
roll_adjust = true
assets = ["X"]

[[contract]]
asset = "X"
point_value = 5.0
round_turn = 1.9

[[contract]]
asset = "Y"
point_value = 50.0
round_turn = 4.3
schedule = "futures_full"

[strategy]
rr = 2.0
"#,
        );
        assert_eq!(peek_strategy_factory(&p).as_deref(), Some("macross"));
        let r = load_strategy(&p, Context::Replay).unwrap();
        assert_eq!(r.strategy_impl.as_deref(), Some("macross"));
        assert!(r.roll_adjust);
        assert_eq!(r.contracts.len(), 2);
        assert_eq!(r.contracts[0].schedule, "futures");
        assert_eq!(r.contracts[1].schedule, "futures_full");
        // Mixed micro/full declarations imply the micro schedule.
        assert_eq!(r.params.get_str("fee_schedule"), "futures");
    }

    #[test]
    fn all_full_size_contracts_imply_the_full_schedule_unless_overridden() {
        let _g = STEM_GUARD.lock().unwrap();
        clear_futures_stems();
        let d = tmpdir("contracts_full");
        let p = write_tmp(
            &d,
            "s.toml",
            "[[contract]]\nasset = \"Y\"\npoint_value = 50.0\nround_turn = 4.3\nschedule = \"futures_full\"\n",
        );
        let r = load_strategy(&p, Context::Replay).unwrap();
        assert_eq!(r.params.get_str("fee_schedule"), "futures_full");
        let p2 = write_tmp(
            &d,
            "s2.toml",
            "[[contract]]\nasset = \"Y\"\npoint_value = 50.0\nround_turn = 4.3\nschedule = \"futures_full\"\n[strategy]\nfee_schedule = \"perp\"\n",
        );
        assert_eq!(
            load_strategy(&p2, Context::Replay)
                .unwrap()
                .params
                .get_str("fee_schedule"),
            "perp"
        );
    }

    #[test]
    fn a_bad_contract_is_a_hard_error() {
        let d = tmpdir("contracts_bad");
        let p = write_tmp(
            &d,
            "s.toml",
            "[[contract]]\nasset = \"Y\"\npoint_value = 0.0\nround_turn = 1.0\n",
        );
        assert!(load_strategy(&p, Context::Replay)
            .unwrap_err()
            .contains("point_value"));
        let p = write_tmp(&d, "s2.toml", "[[contract]]\nasset = \"Y\"\npoint_value = 1.0\nround_turn = 1.0\nschedule = \"spot\"\n");
        assert!(load_strategy(&p, Context::Replay)
            .unwrap_err()
            .contains("schedule"));
        let p = write_tmp(
            &d,
            "s3.toml",
            "[[contract]]\nasset = \"Y\"\npoint_value = 1.0\nround_turn = 1.0\nbogus = 1\n",
        );
        assert!(load_strategy(&p, Context::Replay).is_err());
    }

    #[test]
    fn factory_key_is_inherited_from_base_and_peekable() {
        let d = tmpdir("factory_base");
        write_tmp(
            &d,
            "base.toml",
            "factory = \"macross\"\n[strategy]\nrr = 2.0\n",
        );
        let child = write_tmp(
            &d,
            "child.toml",
            "base = \"base.toml\"\n[strategy]\nrr = 3.0\n",
        );
        assert_eq!(peek_strategy_factory(&child).as_deref(), Some("macross"));
        let r = load_strategy(&child, Context::Replay).unwrap();
        assert_eq!(r.strategy_impl.as_deref(), Some("macross"));
        assert!(!r.roll_adjust);
        assert!(r.contracts.is_empty());
        let child2 = write_tmp(
            &d,
            "child2.toml",
            "base = \"base.toml\"\nfactory = \"other\"\n",
        );
        assert_eq!(peek_strategy_factory(&child2).as_deref(), Some("other"));
    }

    /// A unique temp dir per test, so two tests never race on the same files
    /// and a failure leaves its inputs behind for inspection.
    fn tmpdir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "backtest_engine_cfg_{}_{}_{}",
            tag,
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    const BASE: &str = r#"
engine = "engine/main.toml"
assets = ["AAA", "BBB"]

[strategy]
min_score = 5.0
rr = 3.0
max_hold = 180
breakeven_r = 1.0
"#;

    #[test]
    fn a_config_resolves_to_the_values_it_declares() {
        let dir = tmpdir("resolve");
        let f = write_tmp(&dir, "base.toml", BASE);
        let r = load_strategy(&f, Context::Replay).unwrap();
        assert_eq!(r.min_score(), Some(5.0));
        assert_eq!(r.rr(), 3.0);
        assert_eq!(r.max_hold(), 180);
        assert_eq!(r.breakeven_r(), 1.0);
        assert_eq!(
            r.assets.as_deref().unwrap(),
            &["AAA".to_string(), "BBB".to_string()]
        );
        assert!(r.engine.as_deref().unwrap().ends_with("engine/main.toml"));
        // Unmentioned knobs sit at their registered defaults.
        assert_eq!(r.trail_lock_r(), 0.0);
        assert_eq!(r.partial_tp_r(), 0.0);
    }

    #[test]
    fn an_override_changes_only_the_key_it_names() {
        let dir = tmpdir("override");
        write_tmp(&dir, "base.toml", BASE);
        let child = write_tmp(
            &dir,
            "child.toml",
            "base = \"base.toml\"\n[strategy]\nrr = 2.0\n",
        );
        let r = load_strategy(&child, Context::Replay).unwrap();
        assert_eq!(r.rr(), 2.0, "the child's key wins");
        assert_eq!(r.min_score(), Some(5.0), "an unmentioned key is inherited");
        assert_eq!(r.breakeven_r(), 1.0, "so is this one");
        assert_eq!(r.assets.unwrap(), vec!["AAA", "BBB"], "and the asset list");
    }

    #[test]
    fn a_base_may_not_itself_declare_a_base() {
        let dir = tmpdir("grandparent");
        write_tmp(&dir, "root.toml", BASE);
        write_tmp(
            &dir,
            "mid.toml",
            "base = \"root.toml\"\n[strategy]\nrr = 2.0\n",
        );
        let leaf = write_tmp(&dir, "leaf.toml", "base = \"mid.toml\"\n[strategy]\n");
        let err = load_strategy(&leaf, Context::Replay).unwrap_err();
        assert!(err.contains("only one level"), "got: {err}");
    }

    #[test]
    fn an_unknown_strategy_key_is_a_hard_error_with_a_hint() {
        // The property everything else rests on: a typo must FAIL, never be
        // silently ignored into a run that does something other than what the
        // file says.
        let dir = tmpdir("typo");
        let f = write_tmp(&dir, "x.toml", "[strategy]\nmin_scor = 5.0\n");
        let err = load_strategy(&f, Context::Replay).unwrap_err();
        assert!(err.contains("unknown [strategy] key"), "got: {err}");
        assert!(
            err.contains("min_score"),
            "should suggest the near miss: {err}"
        );
    }

    #[test]
    fn an_unknown_key_in_a_base_file_also_errors() {
        // A typo cannot hide behind inheritance either.
        let dir = tmpdir("basetypo");
        write_tmp(&dir, "base.toml", "[strategy]\nbreakeven_rr = 1.0\n");
        let child = write_tmp(
            &dir,
            "child.toml",
            "base = \"base.toml\"\n[strategy]\nrr = 2.0\n",
        );
        let err = load_strategy(&child, Context::Replay).unwrap_err();
        assert!(err.contains("unknown [strategy] key"), "got: {err}");
        assert!(err.contains("breakeven_rr"), "got: {err}");
    }

    #[test]
    fn a_wrong_typed_value_is_a_hard_error() {
        let dir = tmpdir("wrongtype");
        let f = write_tmp(&dir, "x.toml", "[strategy]\nrace_maker_first = \"yes\"\n");
        let err = load_strategy(&f, Context::Replay).unwrap_err();
        assert!(err.contains("expects Bool"), "got: {err}");
    }

    #[test]
    fn fees_default_differs_by_context() {
        let dir = tmpdir("fees");
        let f = write_tmp(&dir, "x.toml", "[strategy]\nrr = 2.0\n");
        assert!(
            load_strategy(&f, Context::Replay).unwrap().use_fees(),
            "a backtest charges fees unless told otherwise"
        );
        assert!(
            !load_strategy(&f, Context::Live).unwrap().use_fees(),
            "any other context does not"
        );
        // An explicit value survives both.
        let e = write_tmp(&dir, "e.toml", "[strategy]\nfees = false\n");
        assert!(!load_strategy(&e, Context::Replay).unwrap().use_fees());
    }

    #[test]
    fn min_score_unset_defers_to_the_engine() {
        let dir = tmpdir("minscore");
        let unset = write_tmp(&dir, "u.toml", "[strategy]\nrr = 2.0\n");
        assert_eq!(
            load_strategy(&unset, Context::Replay).unwrap().min_score(),
            None
        );
        // Naming it — even at the default value — takes over.
        let zero = write_tmp(&dir, "z.toml", "[strategy]\nmin_score = 0.0\n");
        assert_eq!(
            load_strategy(&zero, Context::Replay).unwrap().min_score(),
            Some(0.0)
        );
    }

    #[test]
    fn an_inherited_engine_resolves_against_the_base_dir_not_the_child() {
        // The footgun this guards: a child in a different directory inheriting
        // a relative `engine` path would otherwise resolve it against its OWN
        // directory and point at a file that does not exist.
        let dir = tmpdir("enginedir");
        let base_dir = dir.join("cfg");
        let child_dir = dir.join("runs").join("one");
        write_tmp(
            &base_dir,
            "base.toml",
            "engine = \"../engine.toml\"\n[strategy]\nrr = 2.0\n",
        );
        let child = write_tmp(
            &child_dir,
            "child.toml",
            "base = \"../../cfg/base.toml\"\n[strategy]\nmin_score = 7.0\n",
        );
        let r = load_strategy(&child, Context::Replay).unwrap();
        let engine = r.engine.clone().unwrap();
        assert!(
            engine.contains("cfg") || !engine.contains("runs"),
            "engine resolved against the child's dir: {engine}"
        );
        assert!(engine.ends_with("engine.toml"), "got {engine}");
        assert_eq!(r.min_score(), Some(7.0));
    }

    #[test]
    fn a_child_declared_engine_resolves_against_the_child_dir() {
        let dir = tmpdir("childengine");
        let base_dir = dir.join("cfg");
        let child_dir = dir.join("runs");
        write_tmp(
            &base_dir,
            "base.toml",
            "engine = \"../engine.toml\"\n[strategy]\nrr = 2.0\n",
        );
        let child = write_tmp(
            &child_dir,
            "child.toml",
            "base = \"../cfg/base.toml\"\nengine = \"own.toml\"\n[strategy]\n",
        );
        let r = load_strategy(&child, Context::Replay).unwrap();
        assert!(r.engine.unwrap().ends_with("own.toml"));
    }

    #[test]
    fn source_overrides_resolve_with_scale_and_offset() {
        let dir = tmpdir("sources");
        let f = write_tmp(
            &dir,
            "x.toml",
            r#"
[[source]]
asset = "AAA"
files = ["DONOR", "AAA"]
scale = 10.0
offset = 1.5

[[source]]
asset = "BBB"
files = ["BBB"]

[strategy]
rr = 2.0
"#,
        );
        let r = load_strategy(&f, Context::Replay).unwrap();
        assert_eq!(r.sources.len(), 2);
        assert_eq!(r.sources[0].asset, "AAA");
        assert_eq!(r.sources[0].files, vec!["DONOR", "AAA"]);
        assert_eq!(r.sources[0].scale, 10.0);
        assert_eq!(r.sources[0].offset, 1.5);
        // Unset scale/offset are the identity transform, not zero — a zero
        // scale would silently flatten every price to the offset.
        assert_eq!(r.sources[1].scale, 1.0);
        assert_eq!(r.sources[1].offset, 0.0);
    }

    #[test]
    fn registered_futures_stems_infer_the_fee_schedule() {
        let _g = STEM_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        clear_futures_stems();
        register_futures_stems(&["FUT1"]);

        let dir = tmpdir("feeinfer");
        let f = write_tmp(
            &dir,
            "x.toml",
            "[[source]]\nasset = \"AAA\"\nfiles = [\"FUT1\"]\n\n[strategy]\nrr = 2.0\n",
        );
        assert_eq!(
            load_strategy(&f, Context::Replay).unwrap().fee_schedule(),
            "futures"
        );

        // A source with no registered stem stays proportional.
        let g = write_tmp(
            &dir,
            "y.toml",
            "[[source]]\nasset = \"AAA\"\nfiles = [\"OTHER\"]\n\n[strategy]\nrr = 2.0\n",
        );
        assert_eq!(
            load_strategy(&g, Context::Replay).unwrap().fee_schedule(),
            "perp"
        );
        clear_futures_stems();
    }

    #[test]
    fn an_explicit_fee_schedule_beats_the_inference() {
        let _g = STEM_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        clear_futures_stems();
        register_futures_stems(&["FUT1"]);

        let dir = tmpdir("feeexplicit");
        let f = write_tmp(
            &dir,
            "x.toml",
            "[[source]]\nasset = \"AAA\"\nfiles = [\"FUT1\"]\n\n\
             [strategy]\nfee_schedule = \"perp\"\n",
        );
        assert_eq!(
            load_strategy(&f, Context::Replay).unwrap().fee_schedule(),
            "perp",
            "an explicit choice must never be overridden by inference"
        );
        clear_futures_stems();
    }

    #[test]
    fn a_metadata_table_is_accepted_and_ignored() {
        let dir = tmpdir("viz");
        let f = write_tmp(
            &dir,
            "x.toml",
            "[viz]\nanything = [\"at all\"]\n\n[strategy]\nrr = 2.0\n",
        );
        let r = load_strategy(&f, Context::Replay).unwrap();
        assert_eq!(r.rr(), 2.0);
    }

    // ─── Fill lens ─────────────────────────────────────────────────────────

    #[test]
    fn a_fill_file_resolves_and_defaults_the_rest() {
        let dir = tmpdir("fill");
        let f = write_tmp(
            &dir,
            "lens.toml",
            "[fill]\nentry_fill_mode = \"hybrid\"\nchase_r = 0.3\n",
        );
        let r = load_fill(&f).unwrap();
        assert_eq!(r.entry_fill_mode, "hybrid");
        assert_eq!(r.chase_r, 0.3);
        // Everything unmentioned sits at the built-in default.
        let d = ResolvedFill::builtin_default();
        assert_eq!(r.allow_signal_bar_fill, d.allow_signal_bar_fill);
        assert_eq!(r.intrabar_stop_first, d.intrabar_stop_first);
        assert_eq!(r.past_entry_fee, d.past_entry_fee);
        assert_eq!(r.rest_min_lead_secs, d.rest_min_lead_secs);
    }

    #[test]
    fn builtin_fill_defaults_are_the_conservative_ones() {
        // These defaults decide what an unconfigured run assumes about its own
        // fills, so they are asserted explicitly rather than left to drift.
        let d = ResolvedFill::builtin_default();
        assert!(
            !d.allow_signal_bar_fill,
            "the signal bar printed before the order existed"
        );
        assert_eq!(d.entry_slippage_r, 0.0);
        assert!(
            d.intrabar_stop_first,
            "an unknowable intrabar race resolves to the stop"
        );
        assert_eq!(d.entry_fill_mode, "limit");
        assert_eq!(d.stop_gap_bps_default, 0.0);
    }

    #[test]
    fn a_fill_file_carrying_a_strategy_key_is_rejected() {
        let dir = tmpdir("fillbad");
        let f = write_tmp(&dir, "bad.toml", "[fill]\nmin_score = 5.0\n");
        let err = load_fill(&f).unwrap_err();
        assert!(err.contains("parsing fill config"), "got: {err}");
    }

    #[test]
    fn a_fill_file_carrying_a_strategy_table_is_rejected() {
        let dir = tmpdir("fillbad2");
        let f = write_tmp(
            &dir,
            "bad.toml",
            "[strategy]\nmin_score = 5.0\n\n[fill]\nchase_r = 0.2\n",
        );
        let err = load_fill(&f).unwrap_err();
        assert!(err.contains("parsing fill config"), "got: {err}");
    }

    #[test]
    fn stop_gap_resolves_default_and_per_asset() {
        let dir = tmpdir("stopgap");
        let f = write_tmp(
            &dir,
            "lens.toml",
            "[fill]\nstop_gap_bps_default = 1.0\nstop_gap_bps_asset = [\"AAA:5.0\", \"BBB:2.0\"]\n",
        );
        let r = load_fill(&f).unwrap();
        assert_eq!(r.stop_gap_bps_default, 1.0);
        assert_eq!(r.stop_gap_bps_asset, vec!["AAA:5.0", "BBB:2.0"]);
    }

    #[test]
    fn tick_chase_can_be_turned_off() {
        let dir = tmpdir("tickchase");
        let f = write_tmp(
            &dir,
            "lens.toml",
            "[fill]\nentry_fill_mode = \"tick\"\ntick_chase = false\n",
        );
        let r = load_fill(&f).unwrap();
        assert_eq!(r.entry_fill_mode, "tick");
        assert!(!r.tick_chase);
        assert!(ResolvedFill::builtin_default().tick_chase, "on by default");
    }

    #[test]
    fn a_strategy_file_carrying_fill_keys_is_flagged_but_still_loads() {
        let dir = tmpdir("legacy");
        let f = write_tmp(
            &dir,
            "x.toml",
            "[strategy]\nmin_score = 5.0\nchase_r = 0.2\nentry_fill_mode = \"hybrid\"\n",
        );
        let mut found = legacy_fill_keys_in_strategy(&f);
        found.sort();
        assert_eq!(
            found,
            vec!["chase_r".to_string(), "entry_fill_mode".to_string()]
        );
        // Flagged, but not rejected: the pre-split layout still resolves.
        assert_eq!(load_strategy(&f, Context::Replay).unwrap().chase_r(), 0.2);
    }

    #[test]
    fn a_clean_strategy_file_flags_nothing() {
        let dir = tmpdir("clean");
        let f = write_tmp(&dir, "x.toml", "[strategy]\nmin_score = 5.0\nrr = 3.0\n");
        assert!(legacy_fill_keys_in_strategy(&f).is_empty());
    }

    // ─── Parsing helpers ───────────────────────────────────────────────────

    #[test]
    fn asset_map_parsing_with_a_malformed_fallback() {
        let items = vec![
            "AAA:0.25".to_string(),
            "BBB:0.5".to_string(),
            "CCC:junk".to_string(),
        ];
        let m = parse_asset_map(&items, 9.0);
        assert_eq!(m.get("AAA"), Some(&0.25));
        assert_eq!(m.get("BBB"), Some(&0.5));
        assert_eq!(
            m.get("CCC"),
            Some(&9.0),
            "malformed takes the caller's fallback"
        );
    }

    #[test]
    fn asset_signal_pair_parsing() {
        let items = vec![
            "AAA:one".to_string(),
            "BBB:two".to_string(),
            "nocolon".to_string(),
        ];
        assert_eq!(
            parse_asset_signals(&items),
            vec![
                ("AAA".to_string(), "one".to_string()),
                ("BBB".to_string(), "two".to_string()),
            ],
            "an entry with no ':' is dropped rather than half-parsed"
        );
    }
}
