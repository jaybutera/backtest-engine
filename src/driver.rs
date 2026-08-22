//! The replay driver: everything between a CLI invocation and a report.
//!
//! This is the body of the `backtest` binary, exposed as a library so a
//! strategy crate gets the whole thing by registering its factories:
//!
//! ```no_run
//! # fn main() {
//! use backtest_engine::driver;
//! use backtest_engine::example_strategy::MaCrossoverFactory;
//!
//! driver::main(&[&MaCrossoverFactory]);
//! # }
//! ```
//!
//! One thread per asset, each running the same pipeline: strategy emits,
//! admission decides, the fill simulator books. Assets do not interact, so
//! nothing is shared or locked; the per-asset ledgers are merged at the end
//! into one report.
//!
//! # Factory selection
//!
//! A run builds one strategy per asset from exactly one
//! [`StrategyFactory`], chosen in this order: the `--factory` flag, then the
//! strategy file's top-level `factory = "<name>"` key, then the only
//! registered factory if there is just one. Anything else is an error naming
//! the registered factories, so a config never silently grades a strategy
//! other than the one it names.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::models::{self, Candle};
use crate::paper::{PaperTrader, TickStore};
use crate::pipeline::Pipeline;
use crate::strategy::{BuildContext, MarketData, Strategy, StrategyFactory};
use crate::timeframe::TimeframeBuilder;
use crate::{data, fees, params, strategy_config};

use chrono::NaiveDateTime;
use clap::{Parser, Subcommand};

/// The command line of the `backtest` binary (and of any binary built on
/// [`main`]).
#[derive(Parser)]
#[command(
    name = "backtest",
    about = "Replay historical candles through a strategy",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Replay historical parquet candles through the strategy and report.
    Replay(ReplayArgs),
}

/// Arguments of the `replay` subcommand.
#[derive(Parser)]
pub struct ReplayArgs {
    /// The validated knob bag, filled from `--strategy`. Not a CLI argument:
    /// every knob lives in the config file so a run is reproducible from it.
    #[arg(skip)]
    pub params: params::Params,

    /// Restrict the run to these assets (repeatable). Default: every asset
    /// found in the data directories.
    #[arg(short, long)]
    pub asset: Vec<String>,

    /// Exclude these assets (repeatable).
    #[arg(long)]
    pub exclude_asset: Vec<String>,

    /// Directory to load candle parquets from (repeatable, searched in order).
    #[arg(long = "data-dir", default_value = "data")]
    pub data_dir: Vec<String>,

    /// Smallest timeframe present in the data. Higher timeframes are
    /// aggregated from it; anything smaller cannot be built.
    #[arg(long = "base-interval", default_value = "1m")]
    pub base_interval: String,

    /// Higher timeframes to aggregate and feed to the strategy (repeatable).
    #[arg(long = "timeframe")]
    pub timeframe: Vec<String>,

    /// Report format: "text" or "json".
    #[arg(long, default_value = "text")]
    pub output: String,

    /// Also write the full JSON report to this path.
    #[arg(long = "json-sidecar")]
    pub json_sidecar: Option<String>,

    /// Strategy config TOML. Supplies the knob bag, the asset list and any
    /// data-source overrides.
    #[arg(long = "strategy")]
    pub config_strategy: Option<String>,

    /// Fill-lens TOML. Decides how orders execute, independently of what the
    /// strategy selects. Omitted = the built-in resting-limit defaults.
    #[arg(long = "fill")]
    pub config_fill: Option<String>,

    /// Start of the reporting window, `YYYY-MM-DD`. Trades signalled before it
    /// are dropped from the report; candles before it still warm the strategy.
    #[arg(long)]
    pub from: Option<String>,

    /// End of the reporting window, `YYYY-MM-DD` (inclusive).
    #[arg(long)]
    pub to: Option<String>,

    /// Days of candles to load before `--from` purely to warm the strategy's
    /// detectors. Their trades are dropped from the report.
    #[arg(long, default_value = "0")]
    pub warmup_days: u32,

    /// Fee schedule name. Omitted = take it from the config, which in turn
    /// infers it from the data sources.
    #[arg(long = "fee-schedule")]
    pub fee_schedule: Option<String>,

    /// Which registered strategy factory to build. Omitted = the strategy
    /// file's `factory = "..."` key, or the only registered factory.
    #[arg(long)]
    pub factory: Option<String>,

    /// Back-adjust contract-roll gaps out of every loaded series
    /// (see `data::roll_adjust`). Omitted = the strategy file's `roll_adjust`.
    #[arg(long = "roll-adjust")]
    pub roll_adjust: bool,
}

/// Parse the process command line and run it against these factories.
///
/// This is the entire body of the shipped binary. A strategy crate's `main`
/// calls it with its own factories. Exits the process with status 1 on a
/// configuration error.
pub fn main(factories: &[&dyn StrategyFactory]) {
    env_logger::init();
    let cli = Cli::parse();
    run(cli, factories);
}

/// Run an already-parsed command line against these factories.
pub fn run(cli: Cli, factories: &[&dyn StrategyFactory]) {
    match cli.command {
        Commands::Replay(args) => run_replay(args, factories),
    }
}

/// Pick the factory a run uses: `--factory`, else the strategy file's
/// `factory` key, else the sole registered one.
pub fn select_factory<'f>(
    explicit: Option<&str>,
    config: Option<&str>,
    factories: &'f [&'f dyn StrategyFactory],
) -> Result<&'f dyn StrategyFactory, String> {
    let names: Vec<&str> = factories.iter().map(|f| f.name()).collect();
    if factories.is_empty() {
        return Err("no strategy factories registered with the driver".to_string());
    }
    let wanted = match explicit.or(config) {
        Some(n) => n,
        None if factories.len() == 1 => return Ok(factories[0]),
        None => {
            return Err(format!(
                "several strategy factories are registered ({}); pick one with \
                 --factory or a top-level `factory = \"...\"` in the strategy file",
                names.join(", ")
            ))
        }
    };
    factories
        .iter()
        .copied()
        .find(|f| f.name() == wanted)
        .ok_or_else(|| {
            format!(
                "unknown strategy factory \"{wanted}\" (registered: {})",
                names.join(", ")
            )
        })
}

/// Seed every knob the registry declares into the bag at its registered
/// default, so a run's effective configuration is fully materialized before
/// the config file overlays its own values on top.
fn seed_args_from_registry(p: &mut params::Params) {
    for knob in params::REGISTRY {
        if !p.is_set(knob.name) {
            p.set(knob.name, (knob.default)());
        }
    }
}

/// Parse a `YYYY-MM-DD` date into a naive datetime at midnight.
fn parse_date(s: &str, what: &str) -> NaiveDateTime {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap_or_else(|e| panic!("--{what} must be YYYY-MM-DD: {e}"))
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn run_replay(mut args: ReplayArgs, factories: &[&dyn StrategyFactory]) {
    let started = Instant::now();
    models::set_base_interval(&args.base_interval);

    // ── Factory ────────────────────────────────────────────────────────────
    // Chosen before the config loads: the factory's own knobs must be
    // registered for the `[strategy]` table to validate.
    let peeked = args
        .config_strategy
        .as_deref()
        .and_then(|p| strategy_config::peek_strategy_factory(Path::new(p)));
    let factory = select_factory(args.factory.as_deref(), peeked.as_deref(), factories)
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    if let Err(e) = params::register_knobs(factory.knobs()) {
        eprintln!("error: factory \"{}\": {e}", factory.name());
        std::process::exit(1);
    }

    // ── Strategy config ────────────────────────────────────────────────────
    let mut resolved: Option<strategy_config::ResolvedStrategy> = None;
    if let Some(ref path) = args.config_strategy {
        let p = Path::new(path);
        let legacy = strategy_config::legacy_fill_keys_in_strategy(p);
        if !legacy.is_empty() {
            eprintln!(
                "warning: {path} carries fill-lens keys under [strategy]: {}. \
                 They still load, but their home is a --fill file — keeping selection and \
                 execution in separate files is what lets a result be attributed to one or \
                 the other.",
                legacy.join(", ")
            );
        }
        let r = strategy_config::load_strategy(p, strategy_config::Context::Replay).unwrap_or_else(
            |e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            },
        );
        if let Some(ref name) = r.strategy_impl {
            if name != factory.name() {
                eprintln!(
                    "error: {path} names factory \"{name}\" but --factory selected \"{}\"",
                    factory.name()
                );
                std::process::exit(1);
            }
        }
        args.params = r.params.clone();
        if args.asset.is_empty() {
            if let Some(ref assets) = r.assets {
                args.asset = assets.clone();
            }
        }
        // Per-contract fee specs declared in the config.
        for c in &r.contracts {
            let spec = fees::ContractSpec {
                point_value: c.point_value,
                round_turn: c.round_turn,
            };
            if c.schedule == "futures_full" {
                fees::register_contract_full(&c.asset, spec);
            } else {
                fees::register_contract(&c.asset, spec);
            }
        }
        if r.roll_adjust {
            args.roll_adjust = true;
        }
        resolved = Some(r);
    }
    seed_args_from_registry(&mut args.params);

    // ── Fill lens ──────────────────────────────────────────────────────────
    let fill = match args.config_fill {
        Some(ref path) => strategy_config::load_fill(Path::new(path)).unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        }),
        None => strategy_config::ResolvedFill::builtin_default(),
    };

    // ── Fee schedule ───────────────────────────────────────────────────────
    let schedule_name = args
        .fee_schedule
        .clone()
        .unwrap_or_else(|| args.params.get_str("fee_schedule"));
    if !schedule_name.is_empty() {
        match fees::parse_schedule(&schedule_name) {
            Ok(s) => fees::set_schedule(s),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
    }

    // ── Data discovery ─────────────────────────────────────────────────────
    let data_dirs: Vec<PathBuf> = args.data_dir.iter().map(PathBuf::from).collect();
    let dir_refs: Vec<&Path> = data_dirs.iter().map(|p| p.as_path()).collect();

    let sources = build_source_map(&resolved);
    let mut assets: Vec<String> = if args.asset.is_empty() {
        let mut found: Vec<String> = data::discover_parquet_files(&dir_refs)
            .into_iter()
            .map(|(_, asset)| asset)
            .collect();
        found.sort();
        found.dedup();
        found
    } else {
        args.asset.clone()
    };
    assets.retain(|a| !args.exclude_asset.iter().any(|x| a.contains(x.as_str())));
    if assets.is_empty() {
        eprintln!(
            "error: no assets to run. Point --data-dir at a directory of \
             `{{ASSET}}_{{interval}}.parquet` files, or name assets with -a."
        );
        std::process::exit(1);
    }

    // ── Window bounds ──────────────────────────────────────────────────────
    let window_start = args.from.as_deref().map(|s| parse_date(s, "from"));
    let window_end = args
        .to
        .as_deref()
        .map(|s| parse_date(s, "to") + chrono::Duration::days(1));
    // Warmup extends the LOAD bound backwards without moving the REPORT bound.
    let load_start = window_start.map(|s| s - chrono::Duration::days(args.warmup_days as i64));

    // ── Load ───────────────────────────────────────────────────────────────
    let tick_mode = fill.entry_fill_mode == "tick";
    let mut asset_candles: Vec<(String, Vec<Candle>)> = Vec::new();
    let mut asset_ticks: HashMap<String, Vec<models::Tick>> = HashMap::new();
    for asset in &assets {
        let candles = data::load_asset_candles_with_sources(
            asset, &dir_refs, load_start, window_end, &sources,
        );
        if candles.is_empty() {
            eprintln!("  {asset}: no candles in range, skipping");
            continue;
        }
        let mut candles = candles;
        if args.roll_adjust {
            let n = data::roll_adjust(&mut candles, 600);
            eprintln!(
                "  {asset}: {} candles ({n} roll splices adjusted)",
                candles.len()
            );
        } else {
            eprintln!("  {asset}: {} candles", candles.len());
        }
        if tick_mode {
            let ticks = data::load_asset_ticks(asset, &dir_refs, load_start, window_end, &sources);
            if ticks.is_empty() {
                eprintln!(
                    "  {asset}: tick fill mode requested but no tick file found — \
                     this asset falls back to bar-resolution fills"
                );
            } else {
                eprintln!("  {asset}: {} ticks", ticks.len());
                asset_ticks.insert(asset.clone(), ticks);
            }
        }
        asset_candles.push((asset.clone(), candles));
    }
    if asset_candles.is_empty() {
        eprintln!("error: no candles loaded for any asset in the requested range.");
        std::process::exit(1);
    }

    let total_candles: usize = asset_candles.iter().map(|(_, c)| c.len()).sum();
    let min_score = args.params.get_f64("min_score");
    let rr = args.params.get_f64("rr");
    let max_hold = args.params.get_u32("max_hold") as usize;
    // Higher timeframes: the factory's declared needs, plus any the CLI adds.
    let mut timeframes = factory.timeframes();
    for tf in &args.timeframe {
        if !timeframes.contains(tf) {
            timeframes.push(tf.clone());
        }
    }
    let engine_path: Option<String> = resolved.as_ref().and_then(|r| r.engine.clone());
    let script_table: toml::value::Table = resolved
        .as_ref()
        .map(|r| r.script.clone())
        .unwrap_or_default();
    let script_table = &script_table;
    let strategy_file: Option<PathBuf> = args.config_strategy.as_ref().map(PathBuf::from);

    // Every series, shared read-only with every strategy for cross-asset
    // reads. The per-asset loops below iterate their own Arc.
    let asset_candles: Vec<(String, std::sync::Arc<Vec<Candle>>)> = asset_candles
        .into_iter()
        .map(|(a, c)| (a, std::sync::Arc::new(c)))
        .collect();
    let mut market = MarketData::new();
    for (a, c) in &asset_candles {
        market.insert(a, c.clone());
    }
    let market = &market;

    // ── One thread per asset ───────────────────────────────────────────────
    // Scoped threads: the factory is borrowed, not `'static`, and every
    // per-asset strategy is built on its own thread from that borrow.
    let traders: Vec<PaperTrader> = std::thread::scope(|scope| {
        let handles: Vec<_> = asset_candles
            .into_iter()
            .map(|(asset_str, candles)| {
                let ticks = asset_ticks.remove(&asset_str).unwrap_or_default();
                let params = args.params.clone();
                let fill = fill.clone();
                let timeframes = timeframes.clone();
                let engine_path = engine_path.clone();
                let strategy_file = strategy_file.clone();

                scope.spawn(move || {
                    let aid = models::asset_id(&asset_str);

                    let mut pt = PaperTrader::new(min_score, rr, max_hold);
                    apply_config(&mut pt, &params, &fill);
                    if tick_mode && !ticks.is_empty() {
                        pt.tick_store = Some(TickStore::new(ticks));
                    }

                    let strategy: Box<dyn Strategy> = factory.build(&BuildContext {
                        asset: &asset_str,
                        params: &params,
                        engine: engine_path.as_deref(),
                        timeframes: &timeframes,
                        strategy_file: strategy_file.as_deref(),
                        market,
                        script: script_table,
                    });
                    let mut pipeline = Pipeline::new();
                    pipeline.insert_asset(aid, strategy, TimeframeBuilder::new(&timeframes), pt);

                    for candle in candles.iter() {
                        pipeline.process_candle(candle);
                    }

                    let pt = pipeline.finish(aid).expect("the asset was registered");
                    eprintln!(
                        "  {asset_str} done: {} candles, {} trades",
                        candles.len(),
                        pt.opportunities_taken
                    );
                    pt
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("an asset thread panicked"))
            .collect()
    });

    // ── Merge ──────────────────────────────────────────────────────────────
    let mut merged = PaperTrader::new(min_score, rr, max_hold);
    apply_config(&mut merged, &args.params, &fill);
    for pt in traders {
        merged.trades.extend(pt.trades);
        merged.opportunities_seen += pt.opportunities_seen;
        merged.opportunities_taken += pt.opportunities_taken;
        merged.hybrid_counters.merge(&pt.hybrid_counters);
        merged.resting_intervals.extend(pt.resting_intervals);
        merged.trade_ready_at.extend(pt.trade_ready_at);
        merged.tick_resolved_bars += pt.tick_resolved_bars;
        merged.tick_fallback_bars += pt.tick_fallback_bars;
        merged.tick_walked += pt.tick_walked;
        for (reason, n) in pt.skips {
            *merged.skips.entry(reason).or_insert(0) += n;
        }
    }

    // Warmup trades were never part of the reporting window: their signals
    // predate it, and their only job was to populate the strategy's state.
    if let Some(ws) = window_start {
        let before = merged.trades.len();
        merged.trades.retain(|t| t.opened_at >= ws);
        let dropped = before - merged.trades.len();
        if dropped > 0 {
            eprintln!("Dropped {dropped} warmup trades signalled before {ws}");
            merged.opportunities_taken = merged.trades.len();
        }
    }

    if tick_mode {
        let seen = merged.tick_resolved_bars + merged.tick_fallback_bars;
        if seen > 0 {
            eprintln!(
                "Tick fills: {} bars resolved on ticks, {} fell back to bars ({:.1}% coverage), {} ticks walked",
                merged.tick_resolved_bars,
                merged.tick_fallback_bars,
                merged.tick_resolved_bars as f64 / seen as f64 * 100.0,
                merged.tick_walked
            );
        }
    }

    let elapsed = started.elapsed();
    eprintln!(
        "Replayed {total_candles} candles in {:.2}s ({:.0} candles/s)",
        elapsed.as_secs_f64(),
        total_candles as f64 / elapsed.as_secs_f64().max(1e-9)
    );

    // ── Report ─────────────────────────────────────────────────────────────
    let label = format!(
        "{} | {} | {} candles",
        factory.name(),
        assets.join(", "),
        total_candles
    );
    let want_json = args.output == "json";
    let json = if want_json || args.json_sidecar.is_some() {
        Some(merged.render_json(&label))
    } else {
        None
    };

    if let (Some(path), Some(ref body)) = (args.json_sidecar.as_ref(), json.as_ref()) {
        if let Some(parent) = Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::write(path, body) {
            Ok(_) => eprintln!("Wrote JSON report: {path}"),
            Err(e) => eprintln!("warning: could not write {path}: {e}"),
        }
    }

    match json {
        Some(body) if want_json => println!("{body}"),
        _ => merged.render_text(&label),
    }
}

/// Build the data-source map from any `[[source]]` overrides in the config.
fn build_source_map(resolved: &Option<strategy_config::ResolvedStrategy>) -> data::SourceMap {
    let mut map = data::SourceMap::new();
    if let Some(r) = resolved {
        for s in &r.sources {
            let sources: Vec<data::DataSource> = s
                .files
                .iter()
                .map(|stem| data::DataSource {
                    stem: stem.clone(),
                    scale: s.scale,
                    offset: s.offset,
                })
                .collect();
            map.insert(&s.asset, sources);
        }
    }
    map
}

/// Copy the resolved knob bag and fill lens onto a trader.
///
/// One function, used for both the per-asset traders and the merged reporting
/// trader, so the report header can never describe a configuration different
/// from the one that ran.
fn apply_config(pt: &mut PaperTrader, p: &params::Params, fill: &strategy_config::ResolvedFill) {
    pt.params = p.clone();
    pt.use_fees = p.get_bool("fees");
    pt.risk_frac = p.get_f64("risk_frac");
    pt.account_size = p.get_f64("account_size");

    // Trade management.
    pt.breakeven_r = p.get_f64("breakeven_r");
    pt.trail_lock_r = p.get_f64("trail_lock_r");
    pt.partial_tp_r = p.get_f64("partial_tp_r");
    pt.derisk_after_min = p.get_u32("derisk_after_min") as usize;
    pt.derisk_below_r = p.get_f64("derisk_below_r");
    pt.cancel_on_target_consumed = p.get_bool("cancel_on_target_consumed");
    pt.cancel_on_setup_invalidated = p.get_bool("cancel_on_setup_invalidated");

    // Fill lens.
    pt.allow_signal_bar_fill = fill.allow_signal_bar_fill;
    pt.entry_slippage_r = fill.entry_slippage_r;
    pt.intrabar_stop_first = fill.intrabar_stop_first;
    pt.hybrid_fill = fill.entry_fill_mode == "hybrid" || fill.entry_fill_mode == "rest_on_ready";
    pt.rest_on_ready_fill = fill.entry_fill_mode == "rest_on_ready";
    pt.tick_fill = fill.entry_fill_mode == "tick";
    pt.chase_r = fill.chase_r;
    pt.chase_requires_seed = fill.chase_requires_seed;
    pt.immediate_chase_at_open = fill.immediate_chase_at_open;
    pt.race_maker_first = fill.race_maker_first;
    pt.deferred_chase_at_open = fill.deferred_chase_at_open;
    pt.rest_min_lead_secs = fill.rest_min_lead_secs;
    pt.tick_chase = fill.tick_chase;
    pt.past_entry_fee = if fill.past_entry_fee == "maker" {
        fees::EntryFeeSide::Maker
    } else {
        fees::EntryFeeSide::Taker
    };
    pt.stop_gap_bps_default = fill.stop_gap_bps_default;
    pt.stop_gap_bps_asset = strategy_config::parse_asset_map(&fill.stop_gap_bps_asset, 0.0);
}
