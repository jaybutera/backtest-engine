//! A streaming session: the same per-candle machinery as a batch replay,
//! fed one candle at a time, with the book's transitions exposed as
//! [`BookEvent`]s as they happen.
//!
//! # Why this exists
//!
//! The batch driver ([`crate::driver`]) loads a whole series and loops over
//! it; a live process receives the same candles one at a time from a feed.
//! If those two run *different* code, every later change to one must be
//! mirrored by hand in the other, and the property that makes a backtest
//! trustworthy — "the backtest is the live loop, replayed" — silently rots.
//! A `Session` is the shared loop body: the batch driver runs one Session
//! per asset over a preloaded series, a live driver runs one Session over
//! the feed, and both do exactly [`Pipeline::process_candle`] per bar.
//!
//! # Cross-asset reads and push order
//!
//! Strategies may read sibling series through
//! [`MarketData`](crate::strategy::MarketData), clamped to the bar being
//! processed. In batch the whole sibling series is preloaded, so a bar of
//! asset A at time T sees sibling bars up to and including T. A streaming
//! session reproduces that by appending every candle of a same-timestamp
//! group to the market *before* processing any of them —
//! [`Session::push_group`]. Feed it groups of same-timestamp candles and
//! the visibility rule is identical to batch by construction. (A session
//! created with `append_market = false` — the batch case, where the series
//! are already complete — never writes to the market.)
//!
//! # Events
//!
//! When `record_events` is on, every push returns the book transitions the
//! candle caused, in order: skips, placements, fills, cancels, stop/target
//! moves, closes. A consumer that mirrors the book onto a real venue acts
//! on these; a journal writes them out. With it off (the batch default)
//! nothing is recorded and nothing is returned.

use std::collections::HashMap;

use crate::events::BookEvent;
use crate::models::{self, Candle};
use crate::paper::PaperTrader;
use crate::pipeline::Pipeline;
use crate::strategy::{MarketData, Strategy};
use crate::timeframe::TimeframeBuilder;

/// One streaming run: per-asset pipelines around a shared market view.
pub struct Session {
    pipeline: Pipeline<Box<dyn Strategy>>,
    market: MarketData,
    /// Append completed base bars to `market` as they are pushed (the live
    /// case). False for batch, where the series are preloaded and complete.
    append_market: bool,
    /// Whether the per-asset traders record [`BookEvent`]s.
    record_events: bool,
    /// Assets in registration order, for deterministic iteration.
    assets: Vec<(u16, String)>,
}

impl Session {
    /// A session over `market`. `append_market` is true for a live feed
    /// (completed base bars are appended to the shared series as they
    /// arrive) and false for a batch replay over preloaded series.
    /// `record_events` turns on the per-candle event stream.
    pub fn new(market: MarketData, append_market: bool, record_events: bool) -> Self {
        Self {
            pipeline: Pipeline::new(),
            market,
            append_market,
            record_events,
            assets: Vec::new(),
        }
    }

    /// The shared market view this session reads (and appends, when live).
    pub fn market(&self) -> &MarketData {
        &self.market
    }

    /// Register one asset: its strategy, the higher timeframes to
    /// aggregate for it, and its configured fill simulator.
    pub fn add_asset(
        &mut self,
        asset: &str,
        strategy: Box<dyn Strategy>,
        timeframes: &[String],
        mut trader: PaperTrader,
    ) {
        let aid = models::asset_id(asset);
        trader.record_events = self.record_events;
        self.pipeline
            .insert_asset(aid, strategy, TimeframeBuilder::new(timeframes), trader);
        self.assets.push((aid, asset.to_string()));
    }

    /// Feed one candle through its asset's pipeline and return the book
    /// transitions it caused (empty unless `record_events`).
    ///
    /// When several assets have a candle for the same timestamp, use
    /// [`Self::push_group`] so sibling bars are visible cross-asset exactly
    /// as they would be in a batch run.
    pub fn push(&mut self, candle: &Candle) -> Vec<BookEvent> {
        self.append(candle);
        self.process(candle)
    }

    /// Feed a group of same-timestamp candles: every candle is appended to
    /// the market first, then each is processed in the order given. This
    /// reproduces the batch visibility rule — a strategy processing time T
    /// sees every sibling bar up to and including T.
    pub fn push_group(&mut self, candles: &[Candle]) -> Vec<BookEvent> {
        for c in candles {
            self.append(c);
        }
        let mut events = Vec::new();
        for c in candles {
            events.extend(self.process(c));
        }
        events
    }

    /// Append a completed base bar to the shared market series (live only).
    fn append(&mut self, candle: &Candle) {
        if !self.append_market || !candle.complete || candle.timeframe != models::base_tf_id() {
            return;
        }
        let name = models::asset_name(candle.asset);
        if let Some(series) = self.market.get(&name) {
            series.push(candle.clone());
        }
    }

    fn process(&mut self, candle: &Candle) -> Vec<BookEvent> {
        self.pipeline.process_candle(candle);
        if !self.record_events {
            return Vec::new();
        }
        match self.pipeline.traders.get_mut(&candle.asset) {
            Some(pt) => pt.take_events(),
            None => Vec::new(),
        }
    }

    /// The current book of one asset, for snapshots and diagnostics.
    pub fn trader(&self, asset: &str) -> Option<&PaperTrader> {
        self.pipeline.traders.get(&models::asset_id(asset))
    }

    /// Every registered asset's current book, in registration order.
    pub fn traders(&self) -> Vec<(&str, &PaperTrader)> {
        self.assets
            .iter()
            .filter_map(|(aid, name)| {
                self.pipeline
                    .traders
                    .get(aid)
                    .map(|pt| (name.as_str(), pt))
            })
            .collect()
    }

    /// Close every still-open position and return the finished per-asset
    /// traders in registration order (the batch end-of-run step; a live
    /// session never calls this).
    pub fn finish(mut self) -> Vec<(String, PaperTrader)> {
        let assets = std::mem::take(&mut self.assets);
        assets
            .into_iter()
            .filter_map(|(aid, name)| self.pipeline.finish(aid).map(|pt| (name, pt)))
            .collect()
    }
}

/// Merge finished per-asset traders into one reporting trader, exactly as
/// the batch driver does: ledgers concatenated in the order given, counters
/// summed, then trades signalled before `window_start` dropped (they only
/// existed to warm the strategies). The template supplies the
/// configuration so the report header always describes the run.
pub fn merge_traders(
    template: PaperTrader,
    per_asset: Vec<PaperTrader>,
    window_start: Option<chrono::NaiveDateTime>,
) -> PaperTrader {
    let mut merged = template;
    for pt in per_asset {
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
    merged
}

/// Group a time-sorted candle stream into same-timestamp batches and push
/// each through the session. This is the replay form of the live loop: a
/// feed delivers each minute's candles together, and this delivers them
/// identically from a preloaded, merged, time-sorted series.
pub fn push_sorted(session: &mut Session, candles: &[Candle]) -> HashMap<String, Vec<BookEvent>> {
    let mut out: HashMap<String, Vec<BookEvent>> = HashMap::new();
    let mut i = 0;
    while i < candles.len() {
        let t = candles[i].timestamp;
        let mut j = i + 1;
        while j < candles.len() && candles[j].timestamp == t {
            j += 1;
        }
        for ev in session.push_group(&candles[i..j]) {
            out.entry(models::asset_name(ev.asset()).to_string())
                .or_default()
                .push(ev);
        }
        i = j;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{asset_id, asset_name, tf_id, Direction, Opportunity};
    use crate::strategy::SharedSeries;
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
        entry: f64,
        stop: f64,
    }

    impl Strategy for EmitOnce {
        fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
            self.seen += 1;
            if self.seen != self.fire_on {
                return Vec::new();
            }
            vec![Opportunity::new(
                "test_long",
                &asset_name(candle.asset),
                "1m",
                Direction::Bull,
                candle.timestamp,
            )
            .with_score(5.0)
            .with_entry_stop(self.entry, self.stop)]
        }
    }

    /// The same candles through a raw pipeline (the pre-session loop body)
    /// and through a Session must book identical trades: the Session adds
    /// event recording around the loop, never behavior.
    #[test]
    fn a_session_books_the_same_trades_as_a_raw_pipeline() {
        let aid = asset_id("SESS_EQ");
        let candles = [
            bar(aid, 0, 101.0, 101.5, 100.5, 101.0), // signal
            bar(aid, 1, 101.0, 101.2, 99.5, 100.5),  // fills at 100
            bar(aid, 2, 100.0, 100.1, 97.0, 97.5),   // stops out at 98
        ];

        let mut pipeline: crate::pipeline::Pipeline<Box<dyn Strategy>> =
            crate::pipeline::Pipeline::new();
        pipeline.insert_asset(
            aid,
            Box::new(EmitOnce {
                fire_on: 1,
                seen: 0,
                entry: 100.0,
                stop: 98.0,
            }),
            crate::timeframe::TimeframeBuilder::new(&[]),
            PaperTrader::new(0.0, 2.0, 300),
        );
        for c in &candles {
            pipeline.process_candle(c);
        }
        let raw = pipeline.finish(aid).unwrap();

        let mut session = Session::new(MarketData::new(), false, true);
        session.add_asset(
            "SESS_EQ",
            Box::new(EmitOnce {
                fire_on: 1,
                seen: 0,
                entry: 100.0,
                stop: 98.0,
            }),
            &[],
            PaperTrader::new(0.0, 2.0, 300),
        );
        let mut events = Vec::new();
        for c in &candles {
            events.extend(session.push(c));
        }
        let (_, streamed) = session.finish().pop().unwrap();

        assert_eq!(raw.trades.len(), streamed.trades.len());
        for (a, b) in raw.trades.iter().zip(streamed.trades.iter()) {
            assert_eq!(a.opportunity_id.is_empty(), b.opportunity_id.is_empty());
            assert_eq!(a.entry, b.entry);
            assert_eq!(a.fill, b.fill);
            assert_eq!(a.r_pnl, b.r_pnl);
            assert_eq!(a.result, b.result);
            assert_eq!(a.closed_at, b.closed_at);
        }

        // And the event stream narrates that life: placed, filled, closed.
        let kinds: Vec<&str> = events.iter().map(|e| e.kind()).collect();
        assert_eq!(kinds, vec!["entry_placed", "entry_filled", "closed"]);
        match &events[2] {
            BookEvent::Closed { reason, r_pnl, .. } => {
                assert_eq!(reason.as_str(), "stop");
                assert!((*r_pnl - -1.0).abs() < 1e-9);
            }
            other => panic!("expected a Closed event, got {other:?}"),
        }
    }

    /// Reads the sibling series' visible length on every candle, proving
    /// what a cross-asset consumer could see at that moment.
    struct SiblingLen {
        sibling: String,
        market: MarketData,
        seen: std::rc::Rc<std::cell::RefCell<Vec<usize>>>,
    }

    impl Strategy for SiblingLen {
        fn on_candle(&mut self, _candle: &Candle) -> Vec<Opportunity> {
            let n = self.market.get(&self.sibling).map(|s| s.len()).unwrap_or(0);
            self.seen.borrow_mut().push(n);
            Vec::new()
        }
    }

    /// push_group appends every candle of the group to the market BEFORE
    /// processing any of them, so a strategy at time T sees its sibling's
    /// bar for T — the batch visibility rule.
    #[test]
    fn push_group_makes_same_timestamp_siblings_visible() {
        let a = asset_id("SESS_GA");
        let b = asset_id("SESS_GB");
        let mut market = MarketData::new();
        market.insert("SESS_GA", SharedSeries::default());
        market.insert("SESS_GB", SharedSeries::default());

        let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut session = Session::new(market.clone(), true, false);
        session.add_asset(
            "SESS_GA",
            Box::new(SiblingLen {
                sibling: "SESS_GB".into(),
                market: market.clone(),
                seen: seen.clone(),
            }),
            &[],
            PaperTrader::new(0.0, 2.0, 300),
        );
        session.add_asset(
            "SESS_GB",
            Box::new(SiblingLen {
                sibling: "SESS_GA".into(),
                market,
                seen: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            }),
            &[],
            PaperTrader::new(0.0, 2.0, 300),
        );

        // Two minutes, both assets each minute. A is processed FIRST in
        // each group, yet must already see B's bar for that minute.
        session.push_group(&[
            bar(a, 0, 1.0, 1.0, 1.0, 1.0),
            bar(b, 0, 1.0, 1.0, 1.0, 1.0),
        ]);
        session.push_group(&[
            bar(a, 1, 1.0, 1.0, 1.0, 1.0),
            bar(b, 1, 1.0, 1.0, 1.0, 1.0),
        ]);

        assert_eq!(
            *seen.borrow(),
            vec![1, 2],
            "asset A's strategy sees B's same-minute bar in both groups"
        );
    }

    /// merge_traders concatenates ledgers in order and drops warmup trades.
    #[test]
    fn merge_traders_drops_warmup_trades() {
        let aid = asset_id("SESS_MG");
        let mut session = Session::new(MarketData::new(), false, false);
        session.add_asset(
            "SESS_MG",
            Box::new(EmitOnce {
                fire_on: 1,
                seen: 0,
                entry: 100.0,
                stop: 98.0,
            }),
            &[],
            PaperTrader::new(0.0, 2.0, 300),
        );
        for c in [
            bar(aid, 0, 101.0, 101.5, 100.5, 101.0),
            bar(aid, 1, 101.0, 101.2, 99.5, 100.5),
            bar(aid, 2, 100.0, 100.1, 97.0, 97.5),
        ] {
            session.push(&c);
        }
        let traders: Vec<PaperTrader> = session.finish().into_iter().map(|(_, pt)| pt).collect();

        // Window starting after the signal: the trade is warmup and drops.
        let after = bar(aid, 10, 0.0, 0.0, 0.0, 0.0).timestamp;
        let merged = merge_traders(PaperTrader::new(0.0, 2.0, 300), traders.clone(), Some(after));
        assert!(merged.trades.is_empty());
        assert_eq!(merged.opportunities_taken, 0);

        // No window: the trade survives.
        let merged = merge_traders(PaperTrader::new(0.0, 2.0, 300), traders, None);
        assert_eq!(merged.trades.len(), 1);
    }
}
