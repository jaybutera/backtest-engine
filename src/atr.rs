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
