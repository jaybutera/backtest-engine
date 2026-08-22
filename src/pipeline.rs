//! The per-candle loop: strategy → admission → fill simulator.
//!
//! This is the whole engine, and it is short on purpose. One candle goes in;
//! the strategy may emit opportunities; each is admitted or skipped; admitted
//! ones become resting orders; then the completed bar advances every open
//! position and every resting order. Everything interesting lives in
//! [`crate::paper::PaperTrader`] and in the strategy — this module only fixes
//! the ORDER those things happen in, which is where lookahead bugs come from.
//!
//! Two ordering rules are load-bearing:
//!
//! 1. **Emit before advancing prices.** A strategy sees a candle and places its
//!    orders before that same candle is used to fill anything. An order can
//!    therefore never be filled by the bar that created it unless the fill lens
//!    explicitly allows a signal-bar fill.
//! 2. **Aggregate after the base bar.** Higher-timeframe bars are built from
//!    completed base bars and fed to the strategy only once the base bar that
//!    closes them has been fully processed, so an aggregated bar can never
//!    carry information back into the bar it was built from.

use std::collections::HashMap;

use crate::models::{asset_name, sig_type_name, Candle, Direction, Opportunity};
use crate::paper::PaperTrader;
use crate::strategy::Strategy;
use crate::timeframe::TimeframeBuilder;

/// Per-asset runtime for one backtest: the strategy, the timeframe aggregator,
/// and the fill simulator, keyed by interned asset id.
///
/// A driver normally builds one of these per asset and runs each on its own
/// thread — assets do not interact, so nothing has to be shared or locked.
pub struct Pipeline<S: Strategy> {
    /// One strategy instance per asset.
    pub strategies: HashMap<u16, S>,
    /// One higher-timeframe aggregator per asset.
    pub tf_builders: HashMap<u16, TimeframeBuilder>,
    /// One fill simulator and trade ledger per asset.
    pub traders: HashMap<u16, PaperTrader>,
    /// Opportunities still considered live, by id.
    pub active_opps: HashMap<String, Opportunity>,
    /// Recently invalidated opportunities, newest last, capped.
    pub expired_opps: Vec<Opportunity>,
    /// Print one line per emitted opportunity to stderr. Off by default: a
    /// multi-asset run drowns the terminal otherwise.
    pub verbose: bool,
}

/// How many expired opportunities to retain before dropping the oldest.
const EXPIRED_CAP: usize = 50;

impl<S: Strategy> Default for Pipeline<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Strategy> Pipeline<S> {
    /// An empty pipeline. Register each asset with [`Self::insert_asset`].
    pub fn new() -> Self {
        Self {
            strategies: HashMap::new(),
            tf_builders: HashMap::new(),
            traders: HashMap::new(),
            active_opps: HashMap::new(),
            expired_opps: Vec::new(),
            verbose: false,
        }
    }

    /// Register one asset's strategy, aggregator and trader.
    pub fn insert_asset(
        &mut self,
        asset: u16,
        strategy: S,
        tf_builder: TimeframeBuilder,
        trader: PaperTrader,
    ) {
        self.strategies.insert(asset, strategy);
        self.tf_builders.insert(asset, tf_builder);
        self.traders.insert(asset, trader);
    }

    /// Process one candle for its asset, returning the opportunities that were
    /// ADMITTED (an order was placed). Emitted-but-skipped ones are tallied on
    /// the trader and not returned.
    pub fn process_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
        let aid = candle.asset;
        let mut admitted = Vec::new();

        // ── 1) The strategy sees the candle and may emit ────────────────────
        let opps = match self.strategies.get_mut(&aid) {
            Some(s) => s.on_candle(candle),
            None => return admitted, // asset not registered; nothing to do
        };
        self.offer(aid, &opps, &mut admitted);

        // Only a COMPLETED bar advances prices or feeds the aggregator. An
        // in-progress bar's high and low are not final, so racing a stop
        // against them would resolve trades on prices that may not stand.
        if !candle.complete {
            return admitted;
        }

        // ── 2) The completed bar advances fills, races and exits ────────────
        if let Some(pt) = self.traders.get_mut(&aid) {
            pt.update_prices(candle);
            self.reap_closed(aid);
        }

        // ── 3) Aggregate, and offer any higher-timeframe bar this one closed ─
        let htfs: Vec<Candle> = match self.tf_builders.get_mut(&aid) {
            Some(tfb) => tfb.process(candle),
            None => Vec::new(),
        };
        for htf in &htfs {
            let htf_opps = match self.strategies.get_mut(&aid) {
                Some(s) => s.on_candle(htf),
                None => Vec::new(),
            };
            self.offer(aid, &htf_opps, &mut admitted);
        }

        admitted
    }

    /// Offer each opportunity to the asset's trader for admission, recording
    /// the admitted ones.
    fn offer(&mut self, aid: u16, opps: &[Opportunity], admitted: &mut Vec<Opportunity>) {
        if opps.is_empty() {
            return;
        }
        let Some(strategy) = self.strategies.get(&aid) else {
            return;
        };
        let Some(pt) = self.traders.get_mut(&aid) else {
            return;
        };
        for opp in opps {
            if self.verbose {
                let dir = if opp.direction == Direction::Bull {
                    "LONG"
                } else {
                    "SHORT"
                };
                eprintln!(
                    "[opp] {} {} {} score={:.1}",
                    asset_name(opp.asset),
                    dir,
                    sig_type_name(opp.signal_type),
                    opp.score
                );
            }
            // A developing setup is journaled but never traded — it is a
            // partial observation, not a decision.
            if !opp.developing && pt.evaluate_with(opp, strategy).is_some() {
                admitted.push(opp.clone());
            }
            self.active_opps.insert(opp.id.clone(), opp.clone());
        }
    }

    /// Move opportunities whose trade has closed out of the active set.
    fn reap_closed(&mut self, aid: u16) {
        let Some(pt) = self.traders.get(&aid) else {
            return;
        };
        let closed: Vec<String> = pt
            .trades
            .iter()
            .filter(|t| t.closed_at.is_some())
            .map(|t| t.opportunity_id.clone())
            .collect();
        for id in &closed {
            if let Some(mut opp) = self.active_opps.remove(id) {
                opp.invalidated = true;
                opp.invalidation_reason = Some("trade_closed".to_string());
                self.expired_opps.push(opp);
                if self.expired_opps.len() > EXPIRED_CAP {
                    self.expired_opps.remove(0);
                }
            }
        }
    }

    /// Close every still-open position at the end of the run and return the
    /// asset's finished trader.
    pub fn finish(&mut self, aid: u16) -> Option<PaperTrader> {
        let mut pt = self.traders.remove(&aid)?;
        pt.close_remaining();
        Some(pt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{asset_id, tf_id};
    use crate::strategy::{AdmitContext, Decision, SkipReason, TakeParams};
    use chrono::{Duration, NaiveDate};

    fn bar(asset: u16, min: i64, o: f64, h: f64, l: f64, c: f64) -> Candle {
        let t = NaiveDate::from_ymd_opt(2026, 5, 4)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::minutes(min);
        Candle {
            asset,
            timeframe: tf_id("1m"),
            open: o,
            high: h,
            low: l,
            close: c,
            volume: 1.0,
            timestamp: t,
            complete: true,
        }
    }

    /// Emits one long on the Nth candle it sees, with fixed geometry.
    struct EmitOnce {
        fire_on: usize,
        seen: usize,
        developing: bool,
        entry: f64,
        stop: f64,
    }

    impl EmitOnce {
        fn new(fire_on: usize, entry: f64, stop: f64) -> Self {
            Self {
                fire_on,
                seen: 0,
                developing: false,
                entry,
                stop,
            }
        }
    }

    impl Strategy for EmitOnce {
        fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
            self.seen += 1;
            if self.seen != self.fire_on {
                return Vec::new();
            }
            let mut o = Opportunity::new(
                "test_long",
                &asset_name(candle.asset),
                "1m",
                Direction::Bull,
                candle.timestamp,
            )
            .with_score(5.0)
            .with_entry_stop(self.entry, self.stop);
            o.developing = self.developing;
            vec![o]
        }
    }

    fn pipeline_with(s: EmitOnce, aid: u16) -> Pipeline<EmitOnce> {
        let mut p = Pipeline::new();
        let pt = PaperTrader::new(0.0, 2.0, 300);
        p.insert_asset(aid, s, TimeframeBuilder::new(&[]), pt);
        p
    }

    #[test]
    fn an_admitted_opportunity_becomes_a_resting_order_then_a_trade() {
        let aid = asset_id("PIPE_A");
        let mut p = pipeline_with(EmitOnce::new(1, 100.0, 98.0), aid);

        // Bar 1 emits; the order rests. The signal bar itself must not fill it.
        let admitted = p.process_candle(&bar(aid, 0, 101.0, 101.5, 99.0, 101.0));
        assert_eq!(admitted.len(), 1, "the opportunity was admitted");
        assert!(
            p.traders[&aid].trades.is_empty(),
            "the signal bar must not fill"
        );

        // Bar 2 trades down through the entry: now it fills.
        p.process_candle(&bar(aid, 1, 101.0, 101.2, 99.5, 100.5));
        assert_eq!(p.traders[&aid].trades.len(), 1, "the next bar fills it");
    }

    #[test]
    fn a_developing_opportunity_is_journaled_but_never_traded() {
        let aid = asset_id("PIPE_DEV");
        let mut s = EmitOnce::new(1, 100.0, 98.0);
        s.developing = true;
        let mut p = pipeline_with(s, aid);

        let admitted = p.process_candle(&bar(aid, 0, 101.0, 101.5, 99.0, 101.0));
        assert!(admitted.is_empty(), "a developing setup is not admitted");
        assert_eq!(p.active_opps.len(), 1, "but it is still journaled");
        p.process_candle(&bar(aid, 1, 101.0, 101.2, 99.5, 100.5));
        assert!(
            p.traders[&aid].trades.is_empty(),
            "and never becomes a trade"
        );
    }

    /// Rejects everything, to prove skips are tallied rather than swallowed.
    struct AlwaysSkip;
    impl Strategy for AlwaysSkip {
        fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
            vec![Opportunity::new(
                "nope",
                &asset_name(candle.asset),
                "1m",
                Direction::Bull,
                candle.timestamp,
            )
            .with_score(1.0)
            .with_entry_stop(100.0, 98.0)]
        }
        fn admit(&self, _o: &Opportunity, _c: &AdmitContext<'_>) -> Decision {
            Decision::Skip(SkipReason::custom("test_gate"))
        }
    }

    #[test]
    fn skips_are_counted_by_reason() {
        let aid = asset_id("PIPE_SKIP");
        let mut p: Pipeline<AlwaysSkip> = Pipeline::new();
        p.insert_asset(
            aid,
            AlwaysSkip,
            TimeframeBuilder::new(&[]),
            PaperTrader::new(0.0, 2.0, 300),
        );
        for i in 0..3 {
            assert!(p
                .process_candle(&bar(aid, i, 100.0, 101.0, 99.0, 100.0))
                .is_empty());
        }
        let pt = &p.traders[&aid];
        assert_eq!(pt.opportunities_seen, 3);
        assert_eq!(
            pt.skips.get("test_gate"),
            Some(&3),
            "the gate's rejections are visible"
        );
        assert!(pt.trades.is_empty());
    }

    /// Overrides `admit` to prove the pipeline uses the STRATEGY's verdict and
    /// does not recompute geometry of its own.
    struct FixedGeometry;
    impl Strategy for FixedGeometry {
        fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
            if candle.timestamp.and_utc().timestamp() % 120 != 0 {
                return Vec::new();
            }
            // Deliberately stamp geometry the override will ignore.
            vec![Opportunity::new(
                "override",
                &asset_name(candle.asset),
                "1m",
                Direction::Bull,
                candle.timestamp,
            )
            .with_score(0.0)
            .with_entry_stop(1.0, 0.5)]
        }
        fn admit(&self, _o: &Opportunity, _c: &AdmitContext<'_>) -> Decision {
            Decision::Take(TakeParams {
                entry: 100.0,
                stop: 98.0,
                tp: 110.0,
            })
        }
    }

    #[test]
    fn the_strategys_own_admit_decides_the_geometry() {
        let aid = asset_id("PIPE_OVR");
        let mut p: Pipeline<FixedGeometry> = Pipeline::new();
        // A score floor the emitted 0.0 would fail — the override ignores it.
        p.insert_asset(
            aid,
            FixedGeometry,
            TimeframeBuilder::new(&[]),
            PaperTrader::new(99.0, 2.0, 300),
        );
        p.process_candle(&bar(aid, 0, 101.0, 101.5, 100.5, 101.0));
        p.process_candle(&bar(aid, 1, 101.0, 101.2, 99.5, 100.5));
        let t = p.traders[&aid].trades.first().expect("the override admits");
        assert_eq!(t.entry, 100.0, "the override's entry, not the emitted 1.0");
        assert_eq!(
            t.tp, 110.0,
            "and the override's tp, not one projected from rr"
        );
    }

    #[test]
    fn an_incomplete_bar_never_advances_prices() {
        let aid = asset_id("PIPE_INC");
        let mut p = pipeline_with(EmitOnce::new(1, 100.0, 98.0), aid);
        p.process_candle(&bar(aid, 0, 101.0, 101.5, 100.5, 101.0));

        // An in-progress bar whose low reaches the entry must NOT fill it: its
        // low is not final, so a fill there could be undone by the rest of the
        // bar.
        let mut partial = bar(aid, 1, 101.0, 101.2, 99.0, 100.5);
        partial.complete = false;
        p.process_candle(&partial);
        assert!(
            p.traders[&aid].trades.is_empty(),
            "an incomplete bar must not fill"
        );

        // The same bar, completed, does fill.
        p.process_candle(&bar(aid, 1, 101.0, 101.2, 99.0, 100.5));
        assert_eq!(p.traders[&aid].trades.len(), 1);
    }

    #[test]
    fn a_closed_trade_moves_from_active_to_expired() {
        let aid = asset_id("PIPE_REAP");
        let mut p = pipeline_with(EmitOnce::new(1, 100.0, 98.0), aid);
        p.process_candle(&bar(aid, 0, 101.0, 101.5, 100.5, 101.0));
        p.process_candle(&bar(aid, 1, 101.0, 101.2, 99.5, 100.5)); // fills
        assert_eq!(p.active_opps.len(), 1);
        // Drive it into the stop.
        p.process_candle(&bar(aid, 2, 100.0, 100.1, 97.0, 97.5));
        assert!(
            p.active_opps.is_empty(),
            "the closed trade's opp left the active set"
        );
        assert_eq!(p.expired_opps.len(), 1);
        assert_eq!(
            p.expired_opps[0].invalidation_reason.as_deref(),
            Some("trade_closed")
        );
    }

    #[test]
    fn an_unregistered_asset_is_ignored_rather_than_panicking() {
        let aid = asset_id("PIPE_KNOWN");
        let other = asset_id("PIPE_UNKNOWN");
        let mut p = pipeline_with(EmitOnce::new(1, 100.0, 98.0), aid);
        assert!(p
            .process_candle(&bar(other, 0, 100.0, 101.0, 99.0, 100.0))
            .is_empty());
    }

    #[test]
    fn finish_closes_open_positions_and_yields_the_trader() {
        let aid = asset_id("PIPE_FIN");
        let mut p = pipeline_with(EmitOnce::new(1, 100.0, 98.0), aid);
        p.process_candle(&bar(aid, 0, 101.0, 101.5, 100.5, 101.0));
        p.process_candle(&bar(aid, 1, 101.0, 101.2, 99.5, 100.5)); // fills, stays open
        let pt = p.finish(aid).expect("the asset was registered");
        assert_eq!(pt.trades.len(), 1);
        assert!(
            pt.open_trades.is_empty(),
            "nothing is left open at the end of a run"
        );
        assert!(p.finish(aid).is_none(), "and the asset is consumed");
    }
}
