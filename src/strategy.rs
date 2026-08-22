//! The strategy seam: the one interface a user-supplied strategy implements,
//! and the small vocabulary of types it speaks to the fill simulator in.
//!
//! # The two halves of a strategy
//!
//! A strategy does two separable things, and this trait keeps them separate:
//!
//! 1. **Detection** — read the candle stream, decide that a setup exists, and
//!    emit an [`Opportunity`] describing it. That is [`Strategy::on_candle`].
//! 2. **Admission** — given an emitted opportunity, decide whether to actually
//!    place an order for it, and at what entry / stop / take-profit prices.
//!    That is [`Strategy::admit`].
//!
//! Splitting them matters because the engine treats the two differently. An
//! emitted opportunity is a fact about the market and is always journaled;
//! an admitted one is a commitment of capital that goes on to be filled,
//! raced, and booked by [`crate::paper::PaperTrader`]. A strategy that wants
//! to emit for diagnostics but trade only a subset returns `None` from
//! `admit` for the rest.
//!
//! # Why admission lives on the strategy, not on the trader
//!
//! The fill simulator is deliberately ignorant of *why* a trade was taken. It
//! receives geometry ([`TakeParams`]: entry, stop, tp) and models what would
//! have happened to a real order at those prices — resting limits, chases,
//! taker fees, intrabar races, stop gaps. None of that reasoning needs to know
//! whether the setup was a moving-average crossover or something far more
//! elaborate. Keeping admission behind this trait means
//! [`crate::paper::PaperTrader::evaluate`] takes a decided verdict as an
//! argument rather than computing one, so the trader carries no strategy
//! state and the two halves can be tested and replaced independently.
//!
//! # Implementing one
//!
//! ```
//! use backtest_engine::models::{Candle, Direction, Opportunity};
//! use backtest_engine::strategy::{Decision, Strategy, TakeParams};
//!
//! struct AlwaysFlat;
//!
//! impl Strategy for AlwaysFlat {
//!     fn on_candle(&mut self, _candle: &Candle) -> Vec<Opportunity> {
//!         Vec::new()
//!     }
//! }
//! ```
//!
//! `admit` has a default implementation ([`default_admit`]) that takes any
//! opportunity clearing a minimum score and derives the take-profit from the
//! configured reward:risk multiple, so the minimal strategy above is only one
//! method. See [`crate::example_strategy`] for a runnable end-to-end example.

use crate::models::{Candle, Direction, Opportunity};
use crate::params::{Knob, Params};

/// Everything a strategy is allowed to know about the run's configuration when
/// it decides whether to admit an opportunity.
///
/// This is the validated knob bag plus the two values that carry a per-run
/// precedence chain the bag does not model: `min_score` and `rr_target` can be
/// overridden after the config is loaded (by a CLI flag, or by a scan-all mode
/// that drops the score floor to zero mid-run), so they are passed explicitly
/// rather than read back out of `params`.
#[derive(Clone, Debug, Default)]
pub struct AdmitContext<'a> {
    /// Effective minimum score after the config → CLI precedence chain.
    pub min_score: f64,
    /// Effective reward:risk target after the same chain.
    pub rr_target: f64,
    /// The validated knob bag, for strategies that read their own knobs.
    pub params: Params,
    /// The most recent completed candles for this opportunity's asset, oldest
    /// first, for strategies whose admission rule needs recent context (an ATR
    /// stop floor, say). `None` when the trader has no history buffered yet.
    pub recent_candles: Option<&'a [Candle]>,
}

impl<'a> AdmitContext<'a> {
    /// A context carrying only the score floor and the R:R target — everything
    /// else at its default. Enough for the majority of strategies.
    pub fn new(min_score: f64, rr_target: f64) -> Self {
        Self {
            min_score,
            rr_target,
            params: Params::new(),
            recent_candles: None,
        }
    }

    /// Attach the validated knob bag.
    pub fn with_params(mut self, params: Params) -> Self {
        self.params = params;
        self
    }

    /// Attach the recent-candle buffer for this asset.
    pub fn with_recent(mut self, recent: Option<&'a [Candle]>) -> Self {
        self.recent_candles = recent;
        self
    }
}

/// The geometry of an admitted trade: where the order goes, where it gives up,
/// and where it takes profit.
///
/// The fill simulator derives everything else from these three numbers. In
/// particular the risk unit `R = |entry - stop|` is fixed here and is the ONLY
/// divisor used for profit and loss, so entry slippage shows up as a uniform
/// drag on results rather than silently rescaling every trade's R.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TakeParams {
    /// Price the entry order is placed at.
    pub entry: f64,
    /// Price at which the trade is abandoned for a loss.
    pub stop: f64,
    /// Price at which the trade is closed for a profit.
    pub tp: f64,
}

impl TakeParams {
    /// The planned risk unit, `|entry - stop|`.
    pub fn risk(&self) -> f64 {
        (self.entry - self.stop).abs()
    }

    /// The planned reward:risk multiple, `|tp - entry| / |entry - stop|`.
    /// Zero when the risk is degenerate.
    pub fn planned_rr(&self) -> f64 {
        let r = self.risk();
        if r > 0.0 {
            (self.tp - self.entry).abs() / r
        } else {
            0.0
        }
    }

    /// Whether the geometry is coherent: positive risk, and a take-profit on
    /// the profitable side of the entry for the given direction.
    pub fn is_valid(&self, direction: Direction) -> bool {
        // Finite-and-positive rather than a negated comparison: NaN geometry
        // must be rejected, and NaN fails every comparison it appears in.
        let r = self.risk();
        if !r.is_finite() || r <= 0.0 {
            return false;
        }
        match direction {
            Direction::Bull => self.stop < self.entry && self.tp > self.entry,
            Direction::Bear => self.stop > self.entry && self.tp < self.entry,
        }
    }
}

/// A strategy's verdict on one opportunity.
#[derive(Debug, Clone)]
pub enum Decision {
    /// Place an order with this geometry.
    Take(TakeParams),
    /// Do not trade this opportunity, for the given reason. The reason is
    /// journaled, so a gate that rejects a lot of flow is visible rather than
    /// silently invisible.
    Skip(SkipReason),
}

impl Decision {
    /// The take geometry, or `None` for a skip.
    pub fn take(&self) -> Option<&TakeParams> {
        match self {
            Decision::Take(t) => Some(t),
            Decision::Skip(_) => None,
        }
    }

    /// Whether this decision places an order.
    pub fn is_take(&self) -> bool {
        matches!(self, Decision::Take(_))
    }
}

/// Why an opportunity was not traded.
///
/// The engine defines the handful of reasons its own generic machinery can
/// produce; a strategy with its own vocabulary of gates uses
/// [`SkipReason::Custom`], whose payload is carried through to the report
/// verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// The opportunity's score is below the configured floor.
    BelowMinScore,
    /// The strategy could not derive an entry and stop from the setup.
    NoEntryStop,
    /// `|entry - stop|` came out zero or negative — the geometry is unusable.
    NonPositiveRisk,
    /// The take-profit sits on the losing side of the entry, or the stop on
    /// the winning side.
    InvalidGeometry,
    /// A strategy-defined gate. The string names the gate and is emitted as-is
    /// in the report, so keep it short and stable (`"outside_session"`).
    Custom(String),
}

impl SkipReason {
    /// A short, stable, lowercase name for reports and journals.
    pub fn as_str(&self) -> &str {
        match self {
            SkipReason::BelowMinScore => "below_min_score",
            SkipReason::NoEntryStop => "no_entry_stop",
            SkipReason::NonPositiveRisk => "non_positive_risk",
            SkipReason::InvalidGeometry => "invalid_geometry",
            SkipReason::Custom(s) => s,
        }
    }

    /// Build a strategy-defined reason.
    pub fn custom(name: impl Into<String>) -> Self {
        SkipReason::Custom(name.into())
    }
}

/// A trading strategy: the one thing a user of this engine writes.
///
/// The engine drives an implementation once per candle, in strict ascending
/// time order, and never rewinds. Implementations are therefore free to keep
/// mutable incremental state (rolling averages, tracked levels, whatever the
/// method needs) without worrying about being replayed.
///
/// # Warmup
///
/// The driver feeds a warmup prefix of candles before the reporting window
/// begins, so detectors that need history are populated by the time results
/// start counting. Nothing distinguishes a warmup candle at this interface —
/// the strategy sees one uniform stream, and the engine discards trades whose
/// signal falls before the window.
pub trait Strategy {
    /// Consume one candle and return any opportunities it completes.
    ///
    /// Called for every candle of every asset the run covers, in ascending
    /// time order. Returning an empty vector is the common case. The candles
    /// include aggregated higher-timeframe bars when the run configures them,
    /// distinguishable by [`Candle::timeframe`].
    fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity>;

    /// Decide whether to trade an opportunity this strategy emitted, and at
    /// what prices.
    ///
    /// The default implementation is [`default_admit`]: take anything scoring
    /// at or above `ctx.min_score`, using the entry and stop the opportunity
    /// carries and a take-profit at `ctx.rr_target` multiples of the risk.
    /// Override this to add gates, or to derive geometry some other way.
    fn admit(&self, opp: &Opportunity, ctx: &AdmitContext<'_>) -> Decision {
        default_admit(opp, ctx)
    }
}

/// A boxed strategy is a strategy, so a driver can hold strategies of
/// different concrete types behind one [`crate::pipeline::Pipeline`] type
/// parameter. [`StrategyFactory::build`] returns this form.
impl Strategy for Box<dyn Strategy> {
    fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
        (**self).on_candle(candle)
    }
    fn admit(&self, opp: &Opportunity, ctx: &AdmitContext<'_>) -> Decision {
        (**self).admit(opp, ctx)
    }
}

/// What a factory is handed to build one per-asset strategy instance.
///
/// Everything here is resolved configuration: the factory reads its own
/// knobs from `params`, its engine pointer from `engine`, and builds state
/// for `asset` alone — the driver runs one instance per asset, on its own
/// thread, and they never share state.
#[derive(Clone, Debug)]
pub struct BuildContext<'a> {
    /// The asset this instance trades, as the name that appears in the data
    /// file stem and the report.
    pub asset: &'a str,
    /// The validated knob bag: built-in knobs plus the factory's own.
    pub params: &'a Params,
    /// The resolved `engine = "..."` path from the strategy file, if any. The
    /// engine does not interpret it; a factory that keeps its own larger
    /// configuration outside the knob bag loads it from here.
    pub engine: Option<&'a str>,
    /// Higher timeframes the driver will aggregate and feed to
    /// [`Strategy::on_candle`], after merging the factory's
    /// [`StrategyFactory::timeframes`] with any the CLI added.
    pub timeframes: &'a [String],
    /// The strategy file the run was configured from, if one was given. A
    /// factory whose configuration outgrows the knob bag can re-read it with
    /// its own loader; the engine has already validated the `[strategy]`
    /// table against the factory's declared knobs.
    pub strategy_file: Option<&'a std::path::Path>,
}

/// Builds [`Strategy`] instances, and declares what they need from config.
///
/// This is the seam a strategy crate plugs into. The engine's binary is a
/// thin wrapper around [`crate::driver::main`] that registers one factory (the
/// bundled example); a private crate registers its own factories with the
/// same call and gets the whole driver — config loading, data, fills, report
/// — without copying any of it. A strategy file selects the factory by name
/// with a top-level `strategy = "<name>"` key.
///
/// # Knobs
///
/// A factory declares the `[strategy]` keys it reads via [`Self::knobs`].
/// The driver registers them with [`crate::params::register_knobs`] before
/// the config loads, so they validate exactly like built-in knobs: unknown
/// keys are a hard error, wrong types are a hard error, and a key the config
/// never mentions resolves to its declared default. Built-in knob names are
/// reserved and cannot be redeclared.
///
/// # Toward out-of-process plugins
///
/// Nothing here requires the factory to live in the same crate as the
/// driver; the trait is object-safe and its inputs are plain data. A future
/// loader could hand the same [`BuildContext`] across a library boundary.
/// That is not implemented: today a factory is linked in.
pub trait StrategyFactory: Send + Sync {
    /// The name a strategy file selects this factory by (`strategy = "..."`).
    /// Short, lowercase, stable.
    fn name(&self) -> &str;

    /// Knobs this strategy reads from `[strategy]`, beyond the built-in ones.
    /// Declare with [`crate::knob!`]. Default: none.
    fn knobs(&self) -> &'static [Knob] {
        &[]
    }

    /// Higher timeframes the strategy wants aggregated and delivered to
    /// [`Strategy::on_candle`] (`"5m"`, `"1h"`, …). Merged with any `--timeframe`
    /// the CLI adds. Default: none — the strategy sees base-interval bars only.
    fn timeframes(&self) -> Vec<String> {
        Vec::new()
    }

    /// Build one instance for one asset.
    fn build(&self, ctx: &BuildContext<'_>) -> Box<dyn Strategy>;
}

/// The engine's built-in admission rule, and the default body of
/// [`Strategy::admit`].
///
/// Three steps, in order:
///
/// 1. Reject anything scoring below `ctx.min_score`.
/// 2. Take the entry and stop from the opportunity's own geometry (see
///    [`Opportunity::entry_stop`]); reject if it carries none, or if the two
///    are equal.
/// 3. Project the take-profit `ctx.rr_target` risk-multiples beyond the entry,
///    in the trade's direction.
///
/// The result is checked for coherence before it is returned, so a strategy
/// that stamps a nonsensical entry/stop pair gets an `InvalidGeometry` skip
/// rather than a trade the fill simulator cannot account for.
pub fn default_admit(opp: &Opportunity, ctx: &AdmitContext<'_>) -> Decision {
    if opp.score < ctx.min_score {
        return Decision::Skip(SkipReason::BelowMinScore);
    }
    let Some((entry, stop)) = opp.entry_stop() else {
        return Decision::Skip(SkipReason::NoEntryStop);
    };
    let risk = (entry - stop).abs();
    if !risk.is_finite() || risk <= 0.0 {
        return Decision::Skip(SkipReason::NonPositiveRisk);
    }
    let tp = match opp.direction {
        Direction::Bull => entry + ctx.rr_target * risk,
        Direction::Bear => entry - ctx.rr_target * risk,
    };
    let take = TakeParams { entry, stop, tp };
    if !take.is_valid(opp.direction) {
        return Decision::Skip(SkipReason::InvalidGeometry);
    }
    Decision::Take(take)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{asset_id, tf_id};
    use chrono::NaiveDate;

    fn at() -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
    }

    fn opp(dir: Direction, score: f64, entry: f64, stop: f64) -> Opportunity {
        let mut o = Opportunity::new("test", "TEST", "1m", dir, at());
        o.score = score;
        o.entry = Some(entry);
        o.stop = Some(stop);
        o
    }

    #[test]
    fn take_params_geometry_helpers() {
        let t = TakeParams {
            entry: 100.0,
            stop: 98.0,
            tp: 106.0,
        };
        assert_eq!(t.risk(), 2.0);
        assert_eq!(t.planned_rr(), 3.0);
        assert!(t.is_valid(Direction::Bull));
        // The same numbers read as an incoherent short.
        assert!(!t.is_valid(Direction::Bear));
    }

    #[test]
    fn degenerate_risk_reports_zero_rr_and_is_invalid() {
        let t = TakeParams {
            entry: 100.0,
            stop: 100.0,
            tp: 106.0,
        };
        assert_eq!(t.risk(), 0.0);
        assert_eq!(t.planned_rr(), 0.0);
        assert!(!t.is_valid(Direction::Bull));
    }

    #[test]
    fn default_admit_projects_tp_from_rr_target() {
        let ctx = AdmitContext::new(0.0, 2.5);
        // Long: entry 100, stop 98 → R = 2, tp = 100 + 2.5*2 = 105.
        match default_admit(&opp(Direction::Bull, 10.0, 100.0, 98.0), &ctx) {
            Decision::Take(t) => {
                assert_eq!(t.entry, 100.0);
                assert_eq!(t.stop, 98.0);
                assert!((t.tp - 105.0).abs() < 1e-12);
                assert!((t.planned_rr() - 2.5).abs() < 1e-12);
            }
            Decision::Skip(r) => panic!("expected a take, got {}", r.as_str()),
        }
        // Short is the exact mirror: tp = 100 - 2.5*2 = 95.
        match default_admit(&opp(Direction::Bear, 10.0, 100.0, 102.0), &ctx) {
            Decision::Take(t) => assert!((t.tp - 95.0).abs() < 1e-12),
            Decision::Skip(r) => panic!("expected a take, got {}", r.as_str()),
        }
    }

    #[test]
    fn default_admit_enforces_the_score_floor() {
        let ctx = AdmitContext::new(7.0, 2.0);
        let d = default_admit(&opp(Direction::Bull, 6.9, 100.0, 98.0), &ctx);
        assert!(matches!(d, Decision::Skip(SkipReason::BelowMinScore)));
        // Exactly at the floor is admitted — the comparison is `<`, not `<=`.
        assert!(default_admit(&opp(Direction::Bull, 7.0, 100.0, 98.0), &ctx).is_take());
    }

    #[test]
    fn default_admit_rejects_missing_and_degenerate_geometry() {
        let ctx = AdmitContext::new(0.0, 2.0);
        let mut bare = Opportunity::new("test", "TEST", "1m", Direction::Bull, at());
        bare.score = 10.0;
        assert!(matches!(
            default_admit(&bare, &ctx),
            Decision::Skip(SkipReason::NoEntryStop)
        ));
        // entry == stop → zero risk.
        assert!(matches!(
            default_admit(&opp(Direction::Bull, 10.0, 100.0, 100.0), &ctx),
            Decision::Skip(SkipReason::NonPositiveRisk)
        ));
        // A long whose stop sits ABOVE the entry is incoherent: the projected
        // tp lands below the entry, so the coherence check catches it.
        assert!(matches!(
            default_admit(&opp(Direction::Bull, 10.0, 100.0, 102.0), &ctx),
            Decision::Skip(SkipReason::InvalidGeometry)
        ));
    }

    #[test]
    fn skip_reason_names_are_stable_and_custom_passes_through() {
        assert_eq!(SkipReason::BelowMinScore.as_str(), "below_min_score");
        assert_eq!(SkipReason::NoEntryStop.as_str(), "no_entry_stop");
        assert_eq!(SkipReason::NonPositiveRisk.as_str(), "non_positive_risk");
        assert_eq!(SkipReason::InvalidGeometry.as_str(), "invalid_geometry");
        assert_eq!(
            SkipReason::custom("outside_session").as_str(),
            "outside_session"
        );
    }

    /// The trait's default `admit` must be exactly `default_admit` — a
    /// strategy that overrides only `on_candle` still gets the documented
    /// admission rule.
    #[test]
    fn trait_default_admit_matches_the_free_function() {
        struct Bare;
        impl Strategy for Bare {
            fn on_candle(&mut self, _c: &Candle) -> Vec<Opportunity> {
                Vec::new()
            }
        }
        let ctx = AdmitContext::new(1.0, 3.0);
        let o = opp(Direction::Bull, 5.0, 200.0, 190.0);
        let via_trait = Bare.admit(&o, &ctx);
        let via_fn = default_admit(&o, &ctx);
        assert_eq!(via_trait.take().copied(), via_fn.take().copied());
        assert!(via_trait.is_take());
    }

    /// A strategy is free to ignore the default entirely and compute its own
    /// geometry; nothing in the engine second-guesses it.
    #[test]
    fn a_strategy_can_override_admit_wholesale() {
        struct FixedGeometry;
        impl Strategy for FixedGeometry {
            fn on_candle(&mut self, _c: &Candle) -> Vec<Opportunity> {
                Vec::new()
            }
            fn admit(&self, _o: &Opportunity, _ctx: &AdmitContext<'_>) -> Decision {
                Decision::Take(TakeParams {
                    entry: 10.0,
                    stop: 9.0,
                    tp: 13.0,
                })
            }
        }
        let ctx = AdmitContext::new(1e9, 2.0); // floor a default would reject on
        let d = FixedGeometry.admit(&opp(Direction::Bull, 0.0, 1.0, 0.5), &ctx);
        let t = d.take().expect("override ignores the score floor");
        assert_eq!(t.planned_rr(), 3.0);
    }

    #[test]
    fn admit_context_builders_compose() {
        let mut p = Params::new();
        p.set("rr", crate::params::Value::F64(4.0));
        let c = Candle {
            asset: asset_id("TEST"),
            timeframe: tf_id("1m"),
            open: 1.0,
            high: 2.0,
            low: 0.5,
            close: 1.5,
            volume: 1.0,
            timestamp: at(),
            complete: true,
        };
        let buf = vec![c];
        let ctx = AdmitContext::new(3.0, 2.0)
            .with_params(p)
            .with_recent(Some(&buf));
        assert_eq!(ctx.min_score, 3.0);
        assert_eq!(ctx.params.get_f64("rr"), 4.0);
        assert_eq!(ctx.recent_candles.map(|r| r.len()), Some(1));
    }
}
