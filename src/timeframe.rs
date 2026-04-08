use crate::models::{Candle, tf_id, tf_name};
use chrono::{NaiveDateTime, TimeDelta};
use std::collections::HashMap;

/// Timeframe durations in minutes (accepts u16 ID).
pub fn tf_minutes_id(tf: u16) -> i64 {
    tf_minutes_str(&tf_name(tf))
}

/// Timeframe durations in minutes (accepts string).
pub fn tf_minutes_str(tf: &str) -> i64 {
    match tf {
        "1m" => 1,
        "5m" => 5,
        "15m" => 15,
        "1h" => 60,
        "4h" => 240,
        "1D" => 1440,
        _ => 1,
    }
}

/// Compute the start of the timeframe period that contains `ts`.
fn tf_start(ts: NaiveDateTime, tf_min: i64) -> NaiveDateTime {
    let midnight = ts.date().and_hms_opt(0, 0, 0).unwrap();
    let mins_since_midnight = (ts - midnight).num_seconds() as f64 / 60.0;
    let period_index = (mins_since_midnight / tf_min as f64) as i64;
    midnight + TimeDelta::minutes(period_index * tf_min)
}

pub struct TimeframeBuilder {
    timeframes: Vec<u16>,
    buffers: HashMap<(u16, u16), Vec<Candle>>,
    period_start: HashMap<(u16, u16), NaiveDateTime>,
}

impl TimeframeBuilder {
    pub fn new(timeframes: &[String]) -> Self {
        let tfs: Vec<u16> = timeframes
            .iter()
            .filter(|tf| tf.as_str() != "1m")
            .map(|tf| tf_id(tf))
            .collect();
        Self {
            timeframes: tfs,
            buffers: HashMap::new(),
            period_start: HashMap::new(),
        }
    }

    /// Process a 1m candle. Returns completed higher-TF candles.
    pub fn process(&mut self, candle: &Candle) -> Vec<Candle> {
        if candle.timeframe != tf_id("1m") {
            return Vec::new();
        }

        let mut completed = Vec::new();
        for &tf in &self.timeframes {
            let tf_min = tf_minutes_id(tf);
            let key = (candle.asset, tf);
            let period = tf_start(candle.timestamp, tf_min);

            let current_period = self.period_start.get(&key).copied();
            if let Some(cp) = current_period {
                if period != cp {
                    // New period — flush buffer
                    if let Some(buf) = self.buffers.get(&key) {
                        if !buf.is_empty() {
                            completed.push(aggregate(buf, candle.asset, tf, cp));
                        }
                    }
                    self.buffers.insert(key, vec![candle.clone()]);
                    self.period_start.insert(key, period);
                } else {
                    self.buffers.entry(key).or_default().push(candle.clone());
                }
            } else {
                self.buffers.entry(key).or_default().push(candle.clone());
                self.period_start.insert(key, period);
            }
        }
        completed
    }

    /// Flush all incomplete buffers (end of backtest).
    pub fn flush(&mut self) -> Vec<Candle> {
        let mut completed = Vec::new();
        for (&(asset, tf), buf) in &self.buffers {
            if !buf.is_empty() {
                let period = self.period_start[&(asset, tf)];
                completed.push(aggregate(buf, asset, tf, period));
            }
        }
        self.buffers.clear();
        self.period_start.clear();
        completed
    }
}

fn aggregate(candles: &[Candle], asset: u16, tf: u16, period_start: NaiveDateTime) -> Candle {
    Candle {
        asset,
        timeframe: tf,
        open: candles[0].open,
        high: candles.iter().map(|c| c.high).fold(f64::NEG_INFINITY, f64::max),
        low: candles.iter().map(|c| c.low).fold(f64::INFINITY, f64::min),
        close: candles.last().unwrap().close,
        volume: candles.iter().map(|c| c.volume).sum(),
        timestamp: period_start,
        complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::asset_id;

    fn make_1m_candle(asset: &str, minute: i64, open: f64, high: f64, low: f64, close: f64) -> Candle {
        let base = chrono::NaiveDate::from_ymd_opt(2026, 3, 11).unwrap()
            .and_hms_opt(2, 0, 0).unwrap();
        Candle {
            asset: asset_id(asset),
            timeframe: tf_id("1m"),
            open, high, low, close,
            volume: 1.0,
            timestamp: base + TimeDelta::minutes(minute),
            complete: true,
        }
    }

    fn make_series(n: usize) -> Vec<Candle> {
        (0..n as i64).map(|i| {
            let price = 50000.0 + (i as f64) * 10.0;
            make_1m_candle("BTC", i, price, price + 20.0, price - 10.0, price + 5.0)
        }).collect()
    }

    #[test]
    fn test_5m_from_1m_count() {
        // 100 1m candles should produce ~19-20 5m candles (matches Python test)
        let candles = make_series(100);
        let mut tfb = TimeframeBuilder::new(&["5m".to_string(), "15m".to_string()]);
        let mut all_5m = Vec::new();
        let tf_5m = tf_id("5m");
        for c in &candles {
            for htf in tfb.process(c) {
                if htf.timeframe == tf_5m {
                    all_5m.push(htf);
                }
            }
        }
        assert!(all_5m.len() >= 15 && all_5m.len() <= 20,
            "Expected 15-20 5m candles, got {}", all_5m.len());
    }

    #[test]
    fn test_15m_from_1m_count() {
        // 100 1m candles should produce ~6 15m candles (matches Python test)
        let candles = make_series(100);
        let mut tfb = TimeframeBuilder::new(&["15m".to_string()]);
        let mut all_15m = Vec::new();
        let tf_15m = tf_id("15m");
        for c in &candles {
            for htf in tfb.process(c) {
                if htf.timeframe == tf_15m {
                    all_15m.push(htf);
                }
            }
        }
        assert!(all_15m.len() >= 4 && all_15m.len() <= 7,
            "Expected 4-7 15m candles, got {}", all_15m.len());
    }

    #[test]
    fn test_ohlcv_aggregation() {
        // 5 candles should aggregate to: first open, max high, min low, last close, sum volume
        let candles = vec![
            make_1m_candle("BTC", 0, 100.0, 110.0, 90.0, 105.0),
            make_1m_candle("BTC", 1, 105.0, 115.0, 95.0, 100.0),
            make_1m_candle("BTC", 2, 100.0, 120.0, 85.0, 110.0),
            make_1m_candle("BTC", 3, 110.0, 112.0, 88.0, 108.0),
            make_1m_candle("BTC", 4, 108.0, 118.0, 92.0, 115.0),
        ];
        let mut tfb = TimeframeBuilder::new(&["5m".to_string()]);
        let mut result = Vec::new();
        for c in &candles {
            result.extend(tfb.process(c));
        }
        // Flush to get the incomplete period
        result.extend(tfb.flush());

        assert!(!result.is_empty(), "Should produce at least one 5m candle");
        let agg = &result[0];
        assert_eq!(agg.open, 100.0);
        assert_eq!(agg.high, 120.0);
        assert_eq!(agg.low, 85.0);
        assert!((agg.volume - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_htf_properties() {
        // All HTF candles must have: high >= low, high >= open, high >= close
        let candles = make_series(100);
        let mut tfb = TimeframeBuilder::new(&["5m".to_string(), "15m".to_string(), "1h".to_string()]);
        let mut all_htf = Vec::new();
        for c in &candles {
            all_htf.extend(tfb.process(c));
        }
        all_htf.extend(tfb.flush());

        for c in &all_htf {
            assert!(c.high >= c.low, "high must be >= low");
            assert!(c.high >= c.open, "high must be >= open");
            assert!(c.high >= c.close, "high must be >= close");
            assert!(c.low <= c.open, "low must be <= open");
            assert!(c.low <= c.close, "low must be <= close");
            assert!(c.complete, "HTF candles must be marked complete");
        }
    }

    #[test]
    fn test_non_1m_ignored() {
        let mut tfb = TimeframeBuilder::new(&["5m".to_string()]);
        let candle = Candle {
            asset: asset_id("BTC"),
            timeframe: tf_id("5m"), // not 1m
            open: 100.0, high: 110.0, low: 90.0, close: 105.0,
            volume: 1.0,
            timestamp: chrono::NaiveDate::from_ymd_opt(2026, 3, 11).unwrap()
                .and_hms_opt(2, 0, 0).unwrap(),
            complete: true,
        };
        let result = tfb.process(&candle);
        assert!(result.is_empty(), "Non-1m candles should be ignored");
    }
}
