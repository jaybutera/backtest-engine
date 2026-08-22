//! A deliberately naive reference strategy, so the engine runs end to end out
//! of the box.
//!
//! # This is a demonstration, not an edge
//!
//! `MaCrossover` is here to show what implementing [`Strategy`] looks like and
//! to give the binary something to drive. It is a textbook moving-average
//! crossover: buy when a fast average crosses above a slow one, sell when it
//! crosses below, stop at a fixed multiple of the recent range. That rule has
//! been public for decades and is comprehensively arbitraged. Expect it to
//! lose money net of fees, and treat any run where it does not as a sign that
//! your data or your fee model is wrong rather than as a discovery.
//!
//! What it IS good for: a known-quantity control. When you change the fill
//! model or the fee schedule, running this strategy before and after tells you
//! what the engine change did, uncontaminated by a strategy you are also
//! tuning.
//!
//! # What to copy from it
//!
//! Three things generalize:
//!
//! 1. **Per-asset state.** `on_candle` is called for every asset in the run, so
//!    incremental state is keyed by `candle.asset`, never kept as one global.
//! 2. **Base-timeframe filtering.** Aggregated higher-timeframe bars arrive on
//!    the same stream; a strategy that does not want them must say so.
//! 3. **Geometry at emission.** Entry and stop are stamped on the
//!    [`Opportunity`], which lets the default [`Strategy::admit`] project the
//!    take-profit from the run's reward:risk target without the strategy
//!    knowing what that target is.
//!
//! # It is a poor demonstration of the fill lenses
//!
//! This strategy enters at the current close, so price is already at the entry
//! when the order is placed and essentially everything fills at once. Nothing
//! ever rests unfilled, which is exactly the case the lenses disagree about.
//! Run it across several and you will see the fill PRICE move — the worst-case
//! lens shifts every fill adversarially, the hybrid lens charges taker fees —
//! but the trade SET stays identical.
//!
//! That is a property of entering at market, not evidence the lenses are
//! decorative. A strategy that rests a limit away from price (at a retracement
//! level, a prior extreme, a band edge) will see the lenses disagree about
//! which trades happened at all, which is the larger effect and the reason the
//! axis exists.

use std::collections::HashMap;

use crate::models::{is_1m, Candle, Direction, Opportunity};
use crate::strategy::Strategy;

/// Rolling state for one asset.
#[derive(Debug, Clone)]
struct AssetState {
    /// Closes seen so far, oldest first, truncated to the slow window.
    closes: Vec<f64>,
    /// True ranges over the same window, for the stop distance.
    ranges: Vec<f64>,
    /// Sign of the last crossover: `Some(true)` = fast was above slow.
    /// `None` until both averages exist.
    fast_above: Option<bool>,
}

/// A moving-average crossover. See the module docs: naive on purpose.
pub struct MaCrossover {
    fast: usize,
    slow: usize,
    /// Stop distance as a multiple of the mean true range over the slow window.
    stop_atr_mult: f64,
    state: HashMap<u16, AssetState>,
}

impl MaCrossover {
    /// A crossover with the given windows, in base-timeframe bars.
    ///
    /// # Panics
    /// If `fast` is not strictly less than `slow`, or either is zero — a
    /// crossover of an average with itself has no signal, and silently
    /// producing none would look like a data problem.
    pub fn new(fast: usize, slow: usize, stop_atr_mult: f64) -> Self {
        assert!(fast > 0 && slow > 0, "both windows must be non-zero");
        assert!(
            fast < slow,
            "the fast window must be shorter than the slow one"
        );
        Self {
            fast,
            slow,
            stop_atr_mult,
            state: HashMap::new(),
        }
    }
}

impl Default for MaCrossover {
    /// 20 bars against 60, stopping at 2x the mean range.
    fn default() -> Self {
        Self::new(20, 60, 2.0)
    }
}

impl Strategy for MaCrossover {
    fn on_candle(&mut self, candle: &Candle) -> Vec<Opportunity> {
        // Higher-timeframe bars arrive on the same stream; this strategy works
        // on the base grain only, so it ignores them rather than mixing grains
        // into one average.
        if !is_1m(candle.timeframe) || !candle.complete {
            return Vec::new();
        }

        let slow = self.slow;
        let st = self
            .state
            .entry(candle.asset)
            .or_insert_with(|| AssetState {
                closes: Vec::with_capacity(slow),
                ranges: Vec::with_capacity(slow),
                fast_above: None,
            });

        st.closes.push(candle.close);
        st.ranges.push(candle.high - candle.low);
        if st.closes.len() > slow {
            st.closes.remove(0);
            st.ranges.remove(0);
        }
        if st.closes.len() < slow {
            return Vec::new(); // still warming up
        }

        let mean = |xs: &[f64]| xs.iter().sum::<f64>() / xs.len() as f64;
        let fast_ma = mean(&st.closes[st.closes.len() - self.fast..]);
        let slow_ma = mean(&st.closes);
        let now_above = fast_ma > slow_ma;

        let Some(was_above) = st.fast_above.replace(now_above) else {
            return Vec::new(); // first complete window: establish the sign only
        };
        if was_above == now_above {
            return Vec::new(); // no crossing this bar
        }

        // A crossing. Stop at a multiple of the mean range, floored so a run of
        // flat bars cannot produce a zero-risk trade the accounting cannot
        // divide by.
        let atr = mean(&st.ranges);
        // `is_normal_positive` rather than `<= 0.0`: a NaN risk must be
        // rejected too, and every comparison against NaN is false.
        let risk = (self.stop_atr_mult * atr).max(candle.close.abs() * 1e-6);
        if !risk.is_finite() || risk <= 0.0 {
            return Vec::new();
        }

        let direction = if now_above {
            Direction::Bull
        } else {
            Direction::Bear
        };
        let entry = candle.close;
        let stop = match direction {
            Direction::Bull => entry - risk,
            Direction::Bear => entry + risk,
        };

        // Score is flat: this strategy has no view on which crossings are
        // better than others, and inventing one would be a fabricated ranking.
        // A real strategy varies it and the engine's `min_score` gate becomes
        // meaningful.
        vec![Opportunity::new(
            if now_above {
                "ma_cross_up"
            } else {
                "ma_cross_down"
            },
            &crate::models::asset_name(candle.asset),
            &crate::models::tf_name(candle.timeframe),
            direction,
            candle.timestamp,
        )
        .with_score(1.0)
        .with_entry_stop(entry, stop)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{asset_id, tf_id};
    use crate::strategy::{AdmitContext, Decision};
    use chrono::{Duration, NaiveDate};

    fn bar(asset: &str, min: i64, close: f64) -> Candle {
        let t = NaiveDate::from_ymd_opt(2026, 4, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::minutes(min);
        Candle {
            asset: asset_id(asset),
            timeframe: tf_id("1m"),
            open: close,
            high: close + 0.5,
            low: close - 0.5,
            close,
            volume: 1.0,
            timestamp: t,
            complete: true,
        }
    }

    /// Feed a price path and collect everything the strategy emits.
    fn run(s: &mut MaCrossover, asset: &str, prices: &[f64]) -> Vec<Opportunity> {
        prices
            .iter()
            .enumerate()
            .flat_map(|(i, p)| s.on_candle(&bar(asset, i as i64, *p)))
            .collect()
    }

    #[test]
    fn emits_nothing_until_the_slow_window_is_full() {
        let mut s = MaCrossover::new(2, 5, 2.0);
        // Four bars is one short of the slow window.
        assert!(run(&mut s, "WARM", &[1.0, 2.0, 3.0, 4.0]).is_empty());
    }

    /// A price path that establishes a DOWN state, then reverses up, then back
    /// down — so both crossings are real transitions rather than the initial
    /// sign being read off a one-directional series.
    fn down_up_down() -> Vec<f64> {
        let mut prices: Vec<f64> = (0..10).map(|i| 100.0 - i as f64).collect(); // falling
        prices.extend((0..14).map(|i| 91.0 + 2.0 * i as f64)); // rising through
        prices.extend((0..16).map(|i| 117.0 - 3.0 * i as f64)); // and back down
        prices
    }

    #[test]
    fn a_reversal_up_then_down_emits_a_long_then_a_short() {
        let mut s = MaCrossover::new(2, 5, 2.0);
        let opps = run(&mut s, "CROSS", &down_up_down());

        assert!(
            !opps.is_empty(),
            "a full round trip must cross at least once"
        );
        assert_eq!(
            opps[0].direction,
            Direction::Bull,
            "the first crossing out of a downtrend is upward"
        );
        assert!(
            opps.iter().any(|o| o.direction == Direction::Bear),
            "and the reversal back down must cross the other way"
        );
    }

    #[test]
    fn a_one_directional_series_never_crosses() {
        // The subtlety this pins: a monotonically rising series has the fast
        // average above the slow one from the moment the window fills, so the
        // sign is ESTABLISHED rather than crossed. Emitting there would be a
        // phantom signal on the strategy's first complete window.
        let mut s = MaCrossover::new(2, 5, 2.0);
        let rising: Vec<f64> = (0..40).map(|i| 100.0 + i as f64).collect();
        assert!(
            run(&mut s, "MONO", &rising).is_empty(),
            "establishing the initial sign is not a crossing"
        );
    }

    #[test]
    fn a_flat_market_never_crosses() {
        let mut s = MaCrossover::new(3, 10, 2.0);
        assert!(run(&mut s, "FLAT", &[100.0; 60]).is_empty());
    }

    #[test]
    fn emitted_geometry_admits_and_is_coherent() {
        let mut s = MaCrossover::new(2, 5, 2.0);
        let opps = run(&mut s, "GEOM", &down_up_down());
        let o = opps
            .iter()
            .find(|o| o.direction == Direction::Bull)
            .expect("the upward reversal emits");

        // Stop below entry for a long, and a positive risk the accounting can
        // divide by.
        let (entry, stop) = o.entry_stop().expect("geometry is stamped at emission");
        assert!(stop < entry, "a long stops below its entry");
        assert!(entry - stop > 0.0);

        // And the default admission rule turns it into a valid trade whose
        // take-profit honors the run's reward:risk target.
        let ctx = AdmitContext::new(0.0, 3.0);
        match s.admit(o, &ctx) {
            Decision::Take(t) => {
                assert!((t.planned_rr() - 3.0).abs() < 1e-9);
                assert!(t.is_valid(Direction::Bull));
            }
            Decision::Skip(r) => panic!("expected a take, got {}", r.as_str()),
        }
    }

    #[test]
    fn per_asset_state_does_not_bleed_between_assets() {
        let mut s = MaCrossover::new(2, 5, 2.0);
        // Interleave a climbing asset with a flat one. The flat asset must stay
        // silent no matter what the other one is doing — the failure this
        // guards is one global buffer shared across every asset in the run.
        let moving = down_up_down();
        let mut flat_emitted = 0;
        let mut move_emitted = 0;
        for (i, p) in moving.iter().enumerate() {
            move_emitted += s.on_candle(&bar("MOVE", i as i64, *p)).len();
            flat_emitted += s.on_candle(&bar("STILL", i as i64, 50.0)).len();
        }
        assert!(move_emitted > 0, "the reversing asset should cross");
        assert_eq!(flat_emitted, 0, "the flat asset must emit nothing");
    }

    #[test]
    fn higher_timeframe_bars_are_ignored() {
        let mut s = MaCrossover::new(2, 5, 2.0);
        let mut htf = bar("HTF", 0, 100.0);
        htf.timeframe = tf_id("15m");
        // Even a long run of 15m bars never fills the base-grain window.
        for i in 0..50 {
            htf.timestamp += Duration::minutes(15);
            htf.close = 100.0 + i as f64;
            assert!(s.on_candle(&htf).is_empty());
        }
    }

    #[test]
    #[should_panic(expected = "shorter than")]
    fn an_inverted_window_pair_is_rejected_loudly() {
        // Silently emitting nothing would look like a data problem, so this
        // fails at construction instead.
        let _ = MaCrossover::new(60, 20, 2.0);
    }
}
