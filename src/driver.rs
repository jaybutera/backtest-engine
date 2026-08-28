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
use crate::paper::{self, PaperTrader, TickStore};
use crate::session::{self, Session};
use crate::strategy::{BuildContext, MarketData, SharedSeries, Strategy, StrategyFactory};
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

    /// Feed every asset through ONE streaming session, candles grouped by
    /// timestamp — the exact loop a live driver runs over a feed — instead
    /// of one batch thread per asset. The results are identical by
    /// construction; the flag exists so that identity can be proven on
    /// real data (diff the two runs' sidecars).
    #[arg(long)]
    pub streaming: bool,
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

/// Everything a replay run resolves before any data loads: the factory,
/// the validated knob bag, the fill lens, the asset list, the window
/// bounds. A batch run and a streaming driver resolve identically —
/// [`resolve`] is the one place the precedence chains live — and then
/// differ only in where their candles come from.
pub struct ResolvedReplay<'f> {
    pub factory: &'f dyn StrategyFactory,
    pub params: params::Params,
    pub fill: strategy_config::ResolvedFill,
    pub assets: Vec<String>,
    pub sources: data::SourceMap,
    /// Split-feed execution sources (`[[exec_source]]`). Empty = single-feed.
    pub exec_sources: data::SourceMap,
    /// True when the config declared any `[[exec_source]]`.
    pub split_feed: bool,
    pub data_dirs: Vec<PathBuf>,
    pub window_start: Option<NaiveDateTime>,
    pub window_end: Option<NaiveDateTime>,
    /// `window_start` minus the warmup days: candles load from here, but
    /// trades signalled before `window_start` are dropped from the report.
    pub load_start: Option<NaiveDateTime>,
    pub timeframes: Vec<String>,
    pub engine_path: Option<String>,
    pub script_table: toml::value::Table,
    pub strategy_file: Option<PathBuf>,
    pub roll_adjust: bool,
    pub tick_mode: bool,
    pub min_score: f64,
    pub rr: f64,
    pub max_hold: usize,
}

impl ResolvedReplay<'_> {
    /// A fill simulator configured exactly as this run specifies. Used for
    /// every per-asset trader and for the merged reporting trader, so the
    /// report header can never describe a configuration different from the
    /// one that ran.
    pub fn build_trader(&self) -> PaperTrader {
        let mut pt = PaperTrader::new(self.min_score, self.rr, self.max_hold);
        apply_config(&mut pt, &self.params, &self.fill);
        pt
    }

    /// Build one per-asset strategy instance from the run's factory.
    pub fn build_strategy(&self, asset: &str, market: &MarketData) -> Box<dyn Strategy> {
        self.factory.build(&BuildContext {
            asset,
            params: &self.params,
            engine: self.engine_path.as_deref(),
            timeframes: &self.timeframes,
            strategy_file: self.strategy_file.as_deref(),
            market,
            script: &self.script_table,
        })
    }
}

/// Resolve a replay command line into a [`ResolvedReplay`]: pick the
/// factory, load and validate the strategy and fill configs, register fee
/// schedules and contracts, discover assets, and fix the window bounds.
/// Everything the process-global side of a run needs (base interval, knob
/// registry, fee tables) is in place when this returns.
pub fn resolve<'f>(
    mut args: ReplayArgs,
    factories: &'f [&'f dyn StrategyFactory],
) -> Result<ResolvedReplay<'f>, String> {
    models::set_base_interval(&args.base_interval);

    // ── Factory ────────────────────────────────────────────────────────────
    // Chosen before the config loads: the factory's own knobs must be
    // registered for the `[strategy]` table to validate.
    let peeked = args
        .config_strategy
        .as_deref()
        .and_then(|p| strategy_config::peek_strategy_factory(Path::new(p)));
    let factory = select_factory(args.factory.as_deref(), peeked.as_deref(), factories)?;
    params::register_knobs(factory.knobs())
        .map_err(|e| format!("factory \"{}\": {e}", factory.name()))?;

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
        let r = strategy_config::load_strategy(p, strategy_config::Context::Replay)?;
        if let Some(ref name) = r.strategy_impl {
            if name != factory.name() {
                return Err(format!(
                    "{path} names factory \"{name}\" but --factory selected \"{}\"",
                    factory.name()
                ));
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
        Some(ref path) => strategy_config::load_fill(Path::new(path))?,
        None => strategy_config::ResolvedFill::builtin_default(),
    };

    // ── Fee schedule ───────────────────────────────────────────────────────
    let schedule_name = args
        .fee_schedule
        .clone()
        .unwrap_or_else(|| args.params.get_str("fee_schedule"));
    if !schedule_name.is_empty() {
        fees::set_schedule(fees::parse_schedule(&schedule_name)?);
    }

    // ── Data discovery ─────────────────────────────────────────────────────
    let data_dirs: Vec<PathBuf> = args.data_dir.iter().map(PathBuf::from).collect();
    let dir_refs: Vec<&Path> = data_dirs.iter().map(|p| p.as_path()).collect();

    let sources = build_source_map(&resolved);
    let exec_sources = build_exec_source_map(&resolved);
    let split_feed = !exec_sources.is_empty();
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
        return Err(
            "no assets to run. Point --data-dir at a directory of \
             `{ASSET}_{interval}.parquet` files, or name assets with -a."
                .to_string(),
        );
    }

    // ── Window bounds ──────────────────────────────────────────────────────
    let window_start = args.from.as_deref().map(|s| parse_date(s, "from"));
    let window_end = args
        .to
        .as_deref()
        .map(|s| parse_date(s, "to") + chrono::Duration::days(1));
    // Warmup extends the LOAD bound backwards without moving the REPORT bound.
    let load_start = window_start.map(|s| s - chrono::Duration::days(args.warmup_days as i64));

    // Higher timeframes: the factory's declared needs, plus any the CLI adds.
    let mut timeframes = factory.timeframes();
    for tf in &args.timeframe {
        if !timeframes.contains(tf) {
            timeframes.push(tf.clone());
        }
    }

    Ok(ResolvedReplay {
        factory,
        min_score: args.params.get_f64("min_score"),
        rr: args.params.get_f64("rr"),
        max_hold: args.params.get_u32("max_hold") as usize,
        tick_mode: fill.entry_fill_mode == "tick",
        engine_path: resolved.as_ref().and_then(|r| r.engine.clone()),
        script_table: resolved.map(|r| r.script).unwrap_or_default(),
        strategy_file: args.config_strategy.as_ref().map(PathBuf::from),
        params: args.params,
        fill,
        assets,
        sources,
        exec_sources,
        split_feed,
        data_dirs,
        window_start,
        window_end,
        load_start,
        timeframes,
        roll_adjust: args.roll_adjust,
    })
}

fn run_replay(args: ReplayArgs, factories: &[&dyn StrategyFactory]) {
    let started = Instant::now();
    let want_json = args.output == "json";
    let json_sidecar = args.json_sidecar.clone();
    let streaming = args.streaming;
    let run = resolve(args, factories).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    // ── Load ───────────────────────────────────────────────────────────────
    let (asset_candles, mut asset_ticks) = load_run_data(&run);
    if asset_candles.is_empty() {
        eprintln!("error: no candles loaded for any asset in the requested range.");
        std::process::exit(1);
    }

    let total_candles: usize = asset_candles.iter().map(|(_, c)| c.len()).sum();

    // ── Split-feed execution book ──────────────────────────────────────────
    // Loaded over the SAME window as the decision feed so a trade filling at
    // the very end of the run still finds its execution bar. Missing assets
    // are a hard error: silently replaying an asset on its decision feed
    // while its siblings execute on the venue would mix two price worlds in
    // one book and there would be no sign of it in the output.
    if run.split_feed && run.tick_mode {
        eprintln!(
            "error: tick fill mode cannot be combined with a split feed — the \
             execution venue is replayed from 1m bars, so there are no \
             execution-side ticks to resolve fills against."
        );
        std::process::exit(1);
    }
    let exec_book: HashMap<String, paper::ExecSeries> = if run.split_feed {
        let dir_refs: Vec<&Path> = run.data_dirs.iter().map(|p| p.as_path()).collect();
        let mut book = HashMap::new();
        for asset in &run.assets {
            if run.exec_sources.sources_for(asset).is_empty() {
                continue;
            }
            let candles = data::load_asset_candles_with_sources(
                asset,
                &dir_refs,
                run.load_start,
                run.window_end,
                &run.exec_sources,
            );
            if candles.is_empty() {
                eprintln!(
                    "error: split-feed run has no execution candles for {asset} \
                     — check its [[exec_source]] stems and the data dirs."
                );
                std::process::exit(1);
            }
            eprintln!("  {asset}: {} execution candles", candles.len());
            book.insert(asset.clone(), paper::ExecSeries::new(&candles));
        }
        book
    } else {
        HashMap::new()
    };
    let exec_book = &exec_book;

    // Every series, shared with every strategy for cross-asset reads. The
    // per-asset loops below iterate their own handle under a read guard —
    // nothing writes after this point.
    let mut market = MarketData::new();
    let asset_series: Vec<(String, SharedSeries)> = asset_candles
        .into_iter()
        .map(|(a, c)| {
            let s = SharedSeries::new(c);
            market.insert(&a, s.clone());
            (a, s)
        })
        .collect();
    let market = &market;
    let run = &run;

    // ── One thread per asset ───────────────────────────────────────────────
    // Scoped threads: the factory is borrowed, not `'static`, and every
    // per-asset strategy is built on its own thread from that borrow. Each
    // thread runs a single-asset Session — the identical loop body a
    // streaming driver runs over a live feed.
    let traders: Vec<PaperTrader> = if streaming {
        run_streaming(run, asset_series, &mut asset_ticks, exec_book)
    } else {
        std::thread::scope(|scope| {
        let handles: Vec<_> = asset_series
            .into_iter()
            .map(|(asset_str, series)| {
                let ticks = asset_ticks.remove(&asset_str).unwrap_or_default();
                scope.spawn(move || {
                    let mut pt = run.build_trader();
                    if run.tick_mode && !ticks.is_empty() {
                        pt.tick_store = Some(TickStore::new(ticks));
                    }
                    if let Some(ex) = exec_book.get(&asset_str) {
                        pt.exec_book
                            .insert(models::asset_id(&asset_str), ex.clone());
                        if let Some(tick) = exec_tick_for(run, &asset_str) {
                            pt.exec_tick.insert(models::asset_id(&asset_str), tick);
                        }
                    }
                    let strategy = run.build_strategy(&asset_str, market);

                    let mut session = Session::new(market.clone(), false, false);
                    session.add_asset(&asset_str, strategy, &run.timeframes, pt);

                    let guard = series.read();
                    for candle in guard.iter() {
                        session.push(candle);
                    }
                    let n_candles = guard.len();
                    drop(guard);

                    let (_, pt) = session
                        .finish()
                        .pop()
                        .expect("the asset was registered");
                    eprintln!(
                        "  {asset_str} done: {} candles, {} trades",
                        n_candles, pt.opportunities_taken
                    );
                    pt
                })
            })
            .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("an asset thread panicked"))
                .collect()
        })
    };

    // ── Merge ──────────────────────────────────────────────────────────────
    let merged = session::merge_traders(run.build_trader(), traders, run.window_start);

    if run.tick_mode {
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
        run.factory.name(),
        run.assets.join(", "),
        total_candles
    );
    let json = if want_json || json_sidecar.is_some() {
        Some(merged.render_json(&label))
    } else {
        None
    };

    if let (Some(path), Some(ref body)) = (json_sidecar.as_ref(), json.as_ref()) {
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

/// The `--streaming` form of a replay: one multi-asset [`Session`] over an
/// initially EMPTY market, fed same-timestamp groups in time order — the
/// live loop, replayed. The market grows by appending, exactly as it does
/// on a feed, so a diff of this run's sidecar against the batch run's
/// proves the streaming path end to end: the push loop, the market append,
/// and the clamped cross-asset reads.
fn run_streaming(
    run: &ResolvedReplay<'_>,
    asset_series: Vec<(String, SharedSeries)>,
    asset_ticks: &mut HashMap<String, Vec<models::Tick>>,
    exec_book: &HashMap<String, paper::ExecSeries>,
) -> Vec<PaperTrader> {
    let mut market = MarketData::new();
    for (a, _) in &asset_series {
        market.insert(a, SharedSeries::default());
    }
    let mut session = Session::new(market.clone(), true, false);
    let mut feed: Vec<Candle> = Vec::new();
    for (asset_str, series) in &asset_series {
        let ticks = asset_ticks.remove(asset_str).unwrap_or_default();
        let mut pt = run.build_trader();
        if run.tick_mode && !ticks.is_empty() {
            pt.tick_store = Some(TickStore::new(ticks));
        }
        if let Some(ex) = exec_book.get(asset_str) {
            pt.exec_book.insert(models::asset_id(asset_str), ex.clone());
            if let Some(tick) = exec_tick_for(run, asset_str) {
                pt.exec_tick.insert(models::asset_id(asset_str), tick);
            }
        }
        let strategy = run.build_strategy(asset_str, &market);
        session.add_asset(asset_str, strategy, &run.timeframes, pt);
        feed.extend(series.read().iter().cloned());
    }
    // Concatenated in asset order, then stably sorted by time: candles of
    // one timestamp stay in asset registration order, the same tie-break a
    // per-minute feed batch delivers.
    feed.sort_by_key(|c| c.timestamp);
    session::push_sorted(&mut session, &feed);
    session
        .finish()
        .into_iter()
        .map(|(asset, pt)| {
            eprintln!("  {asset} done (streaming): {} trades", pt.opportunities_taken);
            pt
        })
        .collect()
}

/// Load every asset's candles (and ticks, in tick mode) for a resolved
/// run: the same sources, window, and roll adjustment whether the candles
/// are then replayed batch or pushed through a streaming session. Assets
/// with no candles in range are skipped with a note.
#[allow(clippy::type_complexity)]
pub fn load_run_data(
    run: &ResolvedReplay<'_>,
) -> (
    Vec<(String, Vec<Candle>)>,
    HashMap<String, Vec<models::Tick>>,
) {
    let dir_refs: Vec<&Path> = run.data_dirs.iter().map(|p| p.as_path()).collect();
    let mut asset_candles: Vec<(String, Vec<Candle>)> = Vec::new();
    let mut asset_ticks: HashMap<String, Vec<models::Tick>> = HashMap::new();
    for asset in &run.assets {
        let mut candles = data::load_asset_candles_with_sources(
            asset,
            &dir_refs,
            run.load_start,
            run.window_end,
            &run.sources,
        );
        if candles.is_empty() {
            eprintln!("  {asset}: no candles in range, skipping");
            continue;
        }
        if run.roll_adjust {
            let n = data::roll_adjust(&mut candles, 600);
            eprintln!(
                "  {asset}: {} candles ({n} roll splices adjusted)",
                candles.len()
            );
        } else {
            eprintln!("  {asset}: {} candles", candles.len());
        }
        if run.tick_mode {
            let ticks = data::load_asset_ticks(
                asset,
                &dir_refs,
                run.load_start,
                run.window_end,
                &run.sources,
            );
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
    (asset_candles, asset_ticks)
}

/// Execution-contract tick size for an asset, derived LITERALLY from its
/// first `[[exec_source]]` stem — the same rule the live mirror uses to pick
/// a contract (`tv_mirror.rs`, `Product::from_root`): the stem names the
/// product, so `ES` rounds to the full-size E-mini tick and `MES` to the
/// micro's. Both classes of a product share a tick size; what differs is the
/// dollar multiplier, which the fee/contract tables already carry.
///
/// Unknown stems return None and the bracket is left unrounded rather than
/// rounded to a guess.
fn exec_tick_for(run: &ResolvedReplay<'_>, asset: &str) -> Option<f64> {
    let stem = run
        .exec_sources
        .sources_for(asset)
        .first()
        .map(|s| s.stem.clone())?;
    let root = stem.trim_start_matches('M');
    Some(match root {
        // index futures quote in quarter points
        "ES" | "NQ" => 0.25,
        // COMEX metals
        "GC" => 0.1,
        "SI" | "SIL" => 0.005,
        // NYMEX crude
        "CL" => 0.01,
        _ => return None,
    })
}

/// Build the data-source map from any `[[source]]` overrides in the config.
fn build_source_map(resolved: &Option<strategy_config::ResolvedStrategy>) -> data::SourceMap {
    source_map_from(resolved.as_ref().map(|r| r.sources.as_slice()).unwrap_or(&[]))
}

/// Build the execution-feed map from any `[[exec_source]]` overrides.
fn build_exec_source_map(
    resolved: &Option<strategy_config::ResolvedStrategy>,
) -> data::SourceMap {
    source_map_from(
        resolved
            .as_ref()
            .map(|r| r.exec_sources.as_slice())
            .unwrap_or(&[]),
    )
}

fn source_map_from(entries: &[strategy_config::AssetSource]) -> data::SourceMap {
    let mut map = data::SourceMap::new();
    {
        for s in entries {
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
    pt.exit_gap_at_open = fill.exit_gap_at_open;
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
