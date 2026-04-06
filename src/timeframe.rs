use crate::models::{Candle, intern};
use chrono::{NaiveDateTime, TimeDelta};
use std::collections::HashMap;
use std::sync::Arc;

/// Timeframe durations in minutes.
pub fn tf_minutes(tf: &str) -> i64 {
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
    timeframes: Vec<Arc<str>>,
    buffers: HashMap<(Arc<str>, Arc<str>), Vec<Candle>>,
    period_start: HashMap<(Arc<str>, Arc<str>), NaiveDateTime>,
}

impl TimeframeBuilder {
    pub fn new(timeframes: &[String]) -> Self {
        let tfs: Vec<Arc<str>> = timeframes
            .iter()
            .filter(|tf| tf.as_str() != "1m")
            .map(|tf| intern(tf))
            .collect();
        Self {
            timeframes: tfs,
            buffers: HashMap::new(),
            period_start: HashMap::new(),
        }
    }

    /// Process a 1m candle. Returns completed higher-TF candles.
    pub fn process(&mut self, candle: &Candle) -> Vec<Candle> {
        if &*candle.timeframe != "1m" {
            return Vec::new();
        }

        let mut completed = Vec::new();
        for tf in &self.timeframes {
            let tf_min = tf_minutes(tf);
            let key = (candle.asset.clone(), tf.clone());
            let period = tf_start(candle.timestamp, tf_min);

            let current_period = self.period_start.get(&key).copied();
            if let Some(cp) = current_period {
                if period != cp {
                    // New period — flush buffer
                    if let Some(buf) = self.buffers.get(&key) {
                        if !buf.is_empty() {
                            completed.push(aggregate(buf, &candle.asset, tf, cp));
                        }
                    }
                    self.buffers.insert(key.clone(), vec![candle.clone()]);
                    self.period_start.insert(key, period);
                } else {
                    self.buffers.entry(key).or_default().push(candle.clone());
                }
            } else {
                self.buffers.entry(key.clone()).or_default().push(candle.clone());
                self.period_start.insert(key, period);
            }
        }
        completed
    }

    /// Flush all incomplete buffers (end of backtest).
    pub fn flush(&mut self) -> Vec<Candle> {
        let mut completed = Vec::new();
        for ((asset, tf), buf) in &self.buffers {
            if !buf.is_empty() {
                let period = self.period_start[&(asset.clone(), tf.clone())];
                completed.push(aggregate(buf, asset, tf, period));
            }
        }
        self.buffers.clear();
        self.period_start.clear();
        completed
    }
}

fn aggregate(candles: &[Candle], asset: &str, tf: &str, period_start: NaiveDateTime) -> Candle {
    Candle {
        asset: intern(asset),
        timeframe: intern(tf),
        open: candles[0].open,
        high: candles.iter().map(|c| c.high).fold(f64::NEG_INFINITY, f64::max),
        low: candles.iter().map(|c| c.low).fold(f64::INFINITY, f64::min),
        close: candles.last().unwrap().close,
        volume: candles.iter().map(|c| c.volume).sum(),
        timestamp: period_start,
        complete: true,
    }
}
