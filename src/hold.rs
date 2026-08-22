//! Max-hold timeout clock — the one implementation of the hold-count
//! decision, so that anything managing a position (the backtest trade
//! manager in `paper.rs`, or a live execution path) agrees on when a trade
//! has been open too long.
//!
//! A filled position's clock counts completed base-tf candles of its asset,
//! starting
//! with the FIRST candle after the one on which the entry filled (the fill
//! bar itself is never counted — paper books the fill after the bar's race,
//! live sees the enclosing bar complete only after the fill event). The
//! position times out when the count strictly exceeds `max_hold_candles`,
//! closing at that bar's close with result "inconclusive" — a timeout is
//! neither a win nor a stop-out, and counting it as either would misreport
//! the strategy.
//!
//! The caller owns everything execution-flavored: which candles belong to the
//! trade, when the clock starts, and how a `Timeout` becomes an actual close.
//! Keeping those out of this module is what lets a simulated and a real
//! execution path share the boundary decision verbatim, rather than
//! reimplementing "has this been open too long" twice and drifting.

use crate::models::Direction;

/// The hold clock of one filled position. The backtest keeps one per open
/// trade, keyed by opportunity id. Serde derives exist so an execution path
/// that needs to survive a restart can snapshot them; the backtest itself
/// never serializes these.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HoldClock {
    /// Completed candles of the trade's asset seen since the fill bar.
    pub candles_held: usize,
}

/// Outcome of ticking the clock against one completed candle of the asset.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HoldTick {
    /// Keep holding.
    Hold,
    /// The count now strictly exceeds `max_hold_candles` — close on this bar.
    Timeout,
}

impl HoldClock {
    /// Advance the clock by one completed candle of the trade's asset and
    /// decide whether this bar is the timeout bar. The caller guarantees the
    /// candle belongs to the trade's asset and comes AFTER the fill bar.
    ///
    /// The boundary is strict (`count > max_hold_candles`), so a position
    /// survives exactly `max_hold_candles` full bars after the fill bar and
    /// times out on the next one — `max_hold = 180` on 1m bars closes 181
    /// minutes after the fill bar's open.
    pub fn tick(&mut self, max_hold_candles: usize) -> HoldTick {
        self.candles_held += 1;
        if self.candles_held > max_hold_candles {
            HoldTick::Timeout
        } else {
            HoldTick::Hold
        }
    }
}

/// R booked by a timeout close at `close`, measured from the actual fill and
/// divided by planned risk — the same convention as every other paper exit.
/// Zero when `risk` is degenerate.
pub fn timeout_r(direction: Direction, fill: f64, close: f64, risk: f64) -> f64 {
    if risk > 0.0 {
        if direction == Direction::Bull {
            (close - fill) / risk
        } else {
            (fill - close) / risk
        }
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_out_strictly_after_max_hold() {
        let mut c = HoldClock::default();
        for i in 1..=3 {
            assert_eq!(c.tick(3), HoldTick::Hold, "bar {i} should hold");
        }
        assert_eq!(c.tick(3), HoldTick::Timeout);
        assert_eq!(c.candles_held, 4);
    }

    #[test]
    fn zero_max_hold_times_out_on_first_bar() {
        let mut c = HoldClock::default();
        assert_eq!(c.tick(0), HoldTick::Timeout);
    }

    #[test]
    fn timeout_r_measured_from_fill() {
        // Long filled at 100, risk 2: close 100.8 books +0.4R. // leak-check: ok test arithmetic
        assert!((timeout_r(Direction::Bull, 100.0, 100.8, 2.0) - 0.4).abs() < 1e-12);
        // Short symmetric: close below the fill is positive.
        assert!((timeout_r(Direction::Bear, 100.0, 99.0, 2.0) - 0.5).abs() < 1e-12);
        // Degenerate risk books flat.
        assert_eq!(timeout_r(Direction::Bull, 100.0, 105.0, 0.0), 0.0);
    }
}
