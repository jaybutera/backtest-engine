use crate::models::Candle;

/// Compute Average True Range over the last `period` candles.
pub fn compute_atr(candles: &[Candle], period: usize) -> f64 {
    let n = period.min(candles.len().saturating_sub(1));
    if n < 1 {
        return 0.0;
    }

    let start = candles.len() - n;
    let mut sum = 0.0;
    for i in start..candles.len() {
        let h = candles[i].high;
        let l = candles[i].low;
        let pc = candles[i - 1].close;
        let tr = (h - l).max((h - pc).abs()).max((l - pc).abs());
        sum += tr;
    }
    sum / n as f64
}

/// ATR with optional exclusion of zero-range bars (high == low).
///
/// Thin CME books print a 1m bar only when a trade happens, and quiet minutes
/// print single-tick bars with high == low; those bars drag the ATR toward
/// zero and implicitly tighten every ATR-denominated detector gate (sweep
/// noise_atr, equal-H/L cluster tolerance, registry clustering, FVG floors).
/// With `skip_zero_range`, the ATR is computed over the last `period` bars
/// that actually traded a range, using the previous *kept* bar's close for
/// the true-range gap term. Feeds with no zero-range bars (HL perps) produce
/// bit-identical output either way.
pub fn compute_atr_filtered(candles: &[Candle], period: usize, skip_zero_range: bool) -> f64 {
    if !skip_zero_range {
        return compute_atr(candles, period);
    }
    // Collect indices of the last `period + 1` bars with a real range,
    // scanning backward (the extra one supplies the first prev-close).
    let mut kept: Vec<usize> = Vec::with_capacity(period + 1);
    for i in (0..candles.len()).rev() {
        if candles[i].high > candles[i].low {
            kept.push(i);
            if kept.len() == period + 1 {
                break;
            }
        }
    }
    if kept.len() < 2 {
        return 0.0;
    }
    kept.reverse();
    let mut sum = 0.0;
    for w in kept.windows(2) {
        let c = &candles[w[1]];
        let pc = candles[w[0]].close;
        let tr = (c.high - c.low).max((c.high - pc).abs()).max((c.low - pc).abs());
        sum += tr;
    }
    sum / (kept.len() - 1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{asset_id, tf_id, Candle};

    fn c(o: f64, h: f64, l: f64, cl: f64) -> Candle {
        Candle {
            asset: asset_id("TEST"),
            timeframe: tf_id("1m"),
            timestamp: chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            open: o,
            high: h,
            low: l,
            close: cl,
            volume: 1.0,
            complete: true,
        }
    }

    #[test]
    fn skip_false_is_bit_identical() {
        let bars = vec![
            c(10.0, 11.0, 9.5, 10.5),
            c(10.5, 10.5, 10.5, 10.5), // zero-range
            c(10.5, 12.0, 10.0, 11.0),
            c(11.0, 11.5, 10.8, 11.2),
        ];
        assert_eq!(compute_atr_filtered(&bars, 14, false), compute_atr(&bars, 14));
        assert_eq!(compute_atr_filtered(&bars, 2, false), compute_atr(&bars, 2));
    }

    #[test]
    fn no_zero_range_bars_identical_either_way() {
        let bars = vec![
            c(10.0, 11.0, 9.5, 10.5),
            c(10.5, 12.0, 10.0, 11.0),
            c(11.0, 11.5, 10.8, 11.2),
            c(11.2, 11.9, 11.1, 11.6),
        ];
        let a = compute_atr(&bars, 3);
        let b = compute_atr_filtered(&bars, 3, true);
        assert!((a - b).abs() < 1e-12);
    }

    #[test]
    fn zero_range_bars_are_excluded() {
        // Dense subsequence: the same ATR as running the plain ATR over only
        // the ranged bars (prev-close chains across the skipped flats).
        let flat = c(10.5, 10.5, 10.5, 10.5);
        let ranged = vec![
            c(10.0, 11.0, 9.5, 10.5),
            c(10.5, 12.0, 10.0, 11.0),
            c(11.0, 11.5, 10.8, 11.2),
            c(11.2, 11.9, 11.1, 11.6),
        ];
        let mut interleaved = Vec::new();
        for r in &ranged {
            interleaved.push(flat.clone());
            interleaved.push(r.clone());
            interleaved.push(flat.clone());
        }
        let expect = compute_atr(&ranged, 3);
        let got = compute_atr_filtered(&interleaved, 3, true);
        assert!((expect - got).abs() < 1e-12, "expect {expect} got {got}");
    }

    #[test]
    fn all_zero_range_returns_zero() {
        let bars = vec![c(10.5, 10.5, 10.5, 10.5); 20];
        assert_eq!(compute_atr_filtered(&bars, 14, true), 0.0);
    }

    #[test]
    fn fewer_than_two_ranged_bars_returns_zero() {
        let mut bars = vec![c(10.5, 10.5, 10.5, 10.5); 10];
        bars.push(c(10.0, 11.0, 9.5, 10.5));
        assert_eq!(compute_atr_filtered(&bars, 14, true), 0.0);
    }
}
