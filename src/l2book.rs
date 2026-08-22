//! Orderbook depth model: how much size a trade can carry before a taker sweep
//! of the book costs more than a slippage budget allows.
//!
//! The point of this module is that position size is not a free parameter. A
//! backtest that reports R-multiples implicitly assumes every trade could be
//! filled at the modelled price, which is true at small size and false at
//! large. Walking a real book tells you where that assumption breaks.
//!
//! `BookSnapshot` plus the walk/cap math is the whole model. Snapshots come
//! from hourly parquets under `{root}/{ASSET}/{DATE}/l2-{HH}.parquet` via
//! [`L2Store`], and a driver annotates each trade post-hoc.
//!
//! The cap is measured PER TAKER LEG, as sweep-VWAP against mid in basis
//! points (so it includes the half-spread), plus a flat `adverse_bps` offset
//! for the latency and adverse-selection tax a real taker pays over what a
//! pre-fill snapshot predicts. A trade must clear the budget on BOTH the entry
//! leg and the stop-exit leg of the same book: stops exit as market orders, so
//! size that can get in but not out is not size you can carry.

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::NaiveDateTime;

/// One side of the book: levels as (price, size-in-units), best first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

/// What the live sizing site does with the depth cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2Mode {
    /// Feature off: no l2Book subscription, sizing untouched.
    Off,
    /// Compute + journal the cap on every entry; size unchanged.
    Log,
    /// size = min(risk_usd sizing, depth cap).
    Cap,
    /// size = depth cap (risk_usd sizing ignored — every trade carries the
    /// largest size the book supports under the slippage budget).
    Max,
}

impl L2Mode {
    /// Unknown/empty strings mean Off — a misspelled config key can only ever
    /// disable the feature, never silently max out a live account.
    pub fn parse(s: &str) -> Self {
        match s {
            "log" => L2Mode::Log,
            "cap" => L2Mode::Cap,
            "max" => L2Mode::Max,
            _ => L2Mode::Off,
        }
    }

    pub fn active(self) -> bool {
        self != L2Mode::Off
    }
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    /// Exchange timestamp, ms.
    pub time_ms: i64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

/// Sizing verdict for one prospective trade.
#[derive(Debug, Clone)]
pub struct L2Cap {
    /// Max notional ($) whose worse taker leg stays within the budget.
    pub notional: f64,
    /// True when the cap ran off the visible 20-level book without exhausting
    /// the budget — the real cap is deeper than the snapshot can see, and
    /// `notional` is only the visible-book total (a floor, not the cap).
    pub censored: bool,
    /// Which leg bound: "entry" or "exit".
    pub binding: &'static str,
    /// Book age at decision time, seconds.
    pub staleness_s: f64,
}

impl BookSnapshot {
    pub fn mid(&self) -> Option<f64> {
        match (self.bids.first(), self.asks.first()) {
            (Some(b), Some(a)) => Some((b.0 + a.0) / 2.0),
            _ => None,
        }
    }

    fn levels(&self, side: Side) -> &[(f64, f64)] {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// VWAP of a taker sweep of `notional` dollars. Returns (vwap, censored):
    /// censored = the visible book couldn't absorb it; the remainder is priced
    /// at the worst visible level (an optimistic floor — callers should treat
    /// censored results as "at least this bad").
    pub fn walk_vwap(&self, side: Side, notional: f64) -> Option<(f64, bool)> {
        let levels = self.levels(side);
        if levels.is_empty() || notional <= 0.0 {
            return None;
        }
        let mut rem = notional;
        let mut cost = 0.0; // dollars
        let mut qty = 0.0; // units
        for &(px, sz) in levels {
            let lvl = px * sz;
            let take = rem.min(lvl);
            cost += take;
            qty += take / px;
            rem -= take;
            if rem <= 1e-9 {
                return Some((cost / qty, false));
            }
        }
        let worst = levels[levels.len() - 1].0;
        cost += rem;
        qty += rem / worst;
        Some((cost / qty, true))
    }

    /// Max notional whose sweep-VWAP slippage vs mid (bps, adverse-signed for
    /// this side) plus `adverse_bps` stays ≤ `cap_bps`. Returns (notional,
    /// censored): censored = the whole visible side fits in the budget, so the
    /// true cap is beyond what the snapshot can see.
    pub fn max_notional(&self, side: Side, cap_bps: f64, adverse_bps: f64) -> Option<(f64, bool)> {
        let mid = self.mid()?;
        let levels = self.levels(side);
        if levels.is_empty() {
            return None;
        }
        let budget = cap_bps - adverse_bps;
        if budget <= 0.0 {
            return Some((0.0, false));
        }
        // Slippage sign: paying up on asks, selling down on bids.
        let sgn = match side {
            Side::Ask => 1.0,
            Side::Bid => -1.0,
        };
        let slip_bps = |cost: f64, qty: f64| -> f64 {
            if qty <= 0.0 {
                return 0.0;
            }
            sgn * (cost / qty - mid) / mid * 1e4
        };
        let mut cost = 0.0;
        let mut qty = 0.0;
        let mut cap = 0.0;
        for &(px, sz) in levels {
            let lvl = px * sz;
            let (c_full, q_full) = (cost + lvl, qty + lvl / px);
            if slip_bps(c_full, q_full) > budget {
                // Partial take of this level: slippage is monotone in the take,
                // bisect the boundary.
                let (mut lo, mut hi) = (0.0f64, lvl);
                for _ in 0..50 {
                    let x = (lo + hi) / 2.0;
                    if slip_bps(cost + x, qty + x / px) > budget {
                        hi = x;
                    } else {
                        lo = x;
                    }
                }
                return Some((cap + lo, false));
            }
            cost = c_full;
            qty = q_full;
            cap += lvl;
        }
        Some((cap, true)) // whole visible side within budget
    }

    /// Sizing cap for one prospective trade: the smaller of the entry-taker
    /// leg and the stop-exit leg on this book. `is_long`: entry sweeps asks,
    /// stop-exit sweeps bids (short: mirrored).
    pub fn trade_cap(
        &self,
        is_long: bool,
        cap_bps: f64,
        adverse_bps: f64,
        staleness_s: f64,
    ) -> Option<L2Cap> {
        let (entry_side, exit_side) = if is_long {
            (Side::Ask, Side::Bid)
        } else {
            (Side::Bid, Side::Ask)
        };
        let (cap_in, cens_in) = self.max_notional(entry_side, cap_bps, adverse_bps)?;
        let (cap_out, cens_out) = self.max_notional(exit_side, cap_bps, adverse_bps)?;
        let (notional, censored, binding) = if cap_in <= cap_out {
            (cap_in, cens_in, "entry")
        } else {
            (cap_out, cens_out, "exit")
        };
        Some(L2Cap {
            notional,
            censored,
            binding,
            staleness_s,
        })
    }

    /// Size-premium of a `notional` sweep beyond the touch price, as a
    /// fraction of mid. This is the extra cost a BIG taker order pays relative
    /// to the minimal taker fill the fill model already prices. Positive =
    /// cost. Second return: censored (book edge hit; premium is a floor).
    pub fn size_premium_frac(&self, side: Side, notional: f64) -> Option<(f64, bool)> {
        let mid = self.mid()?;
        let touch = self.levels(side).first()?.0;
        let (vwap, censored) = self.walk_vwap(side, notional)?;
        let prem = match side {
            Side::Ask => (vwap - touch) / mid,
            Side::Bid => (touch - vwap) / mid,
        };
        Some((prem.max(0.0), censored))
    }
}

// ─── Snapshot store: hourly book parquets ────────────────────────────────────

/// Lazy per-hour-file loader over `root/{ASSET}/{YYYY-MM-DD}/l2-{HH}.parquet`,
/// where ASSET is the asset name with ':' replaced by '_'. Files load on first
/// touch and stay cached for the rest of the run.
/// Cache key for one hourly book file: (asset, date, hour).
type HourKey = (String, String, u32);

/// Snapshots for one hour, sorted by time. `None` records a missing file, so a
/// gap is looked up once rather than re-probed on every trade in that hour.
type HourEntry = Option<Vec<BookSnapshot>>;

pub struct L2Store {
    root: PathBuf,
    cache: std::cell::RefCell<HashMap<HourKey, HourEntry>>,
}

impl L2Store {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cache: std::cell::RefCell::new(HashMap::new()),
        }
    }

    fn load_hour(&self, asset: &str, date: &str, hour: u32) -> Option<Vec<BookSnapshot>> {
        let key = (asset.to_string(), date.to_string(), hour);
        if let Some(cached) = self.cache.borrow().get(&key) {
            return cached.clone();
        }
        let path = self
            .root
            .join(asset.replace(':', "_"))
            .join(date)
            .join(format!("l2-{hour:02}.parquet"));
        let loaded = read_l2_parquet(&path);
        self.cache.borrow_mut().insert(key, loaded.clone());
        loaded
    }

    /// Last snapshot at/before `ts` within `max_staleness_s`. Checks the
    /// enclosing hour file and the one before it.
    pub fn snapshot_before(
        &self,
        asset: &str,
        ts: NaiveDateTime,
        max_staleness_s: f64,
    ) -> Option<BookSnapshot> {
        let ts_ms = ts.and_utc().timestamp_millis();
        let mut best: Option<BookSnapshot> = None;
        for hours_back in 0..2u32 {
            let t = ts - chrono::Duration::hours(hours_back as i64);
            let date = t.format("%Y-%m-%d").to_string();
            let hour = chrono::Timelike::hour(&t);
            if let Some(snaps) = self.load_hour(asset, &date, hour) {
                // Files are time-sorted; binary search for last ≤ ts_ms.
                let idx = snaps.partition_point(|s| s.time_ms <= ts_ms);
                if idx > 0 {
                    let cand = &snaps[idx - 1];
                    if best.as_ref().is_none_or(|b| cand.time_ms > b.time_ms) {
                        best = Some(cand.clone());
                    }
                }
            }
            if best.is_some() {
                break; // enclosing hour had one; the previous hour can't beat it
            }
        }
        let b = best?;
        if (ts_ms - b.time_ms) as f64 / 1000.0 > max_staleness_s {
            return None;
        }
        Some(b)
    }
}

/// Read one collector hour file into time-sorted snapshots.
fn read_l2_parquet(path: &std::path::Path) -> Option<Vec<BookSnapshot>> {
    use arrow::array::{Array, Float64Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let reader = builder.build().ok()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.ok()?;
        let schema = batch.schema();
        let col =
            |name: &str| -> Option<usize> { schema.fields().iter().position(|f| f.name() == name) };
        let time = batch
            .column(col("time")?)
            .as_any()
            .downcast_ref::<Int64Array>()?
            .clone();
        let f64_col = |name: &str| -> Option<Float64Array> {
            batch
                .column(col(name)?)
                .as_any()
                .downcast_ref::<Float64Array>()
                .cloned()
        };
        let side_cols = |prefix: &str| -> Option<Vec<(Float64Array, Float64Array)>> {
            (0..20)
                .map(|i| {
                    Some((
                        f64_col(&format!("{prefix}_px_{i}"))?,
                        f64_col(&format!("{prefix}_sz_{i}"))?,
                    ))
                })
                .collect()
        };
        let bid_cols = side_cols("bid")?;
        let ask_cols = side_cols("ask")?;
        for row in 0..batch.num_rows() {
            let take_side = |cols: &[(Float64Array, Float64Array)]| -> Vec<(f64, f64)> {
                cols.iter()
                    .filter_map(|(px, sz)| {
                        if px.is_null(row) || sz.is_null(row) {
                            return None;
                        }
                        let (p, s) = (px.value(row), sz.value(row));
                        if s > 0.0 && p > 0.0 {
                            Some((p, s))
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            out.push(BookSnapshot {
                time_ms: time.value(row),
                bids: take_side(&bid_cols),
                asks: take_side(&ask_cols),
            });
        }
    }
    out.sort_by_key(|s| s.time_ms);
    Some(out)
}

// ─── Backtest per-trade annotation ───────────────────────────────────────────

/// Depth-sizing annotation for one closed backtest trade, emitted in the JSON
/// sidecar. All costs the R-ledger doesn't know about are in R units here.
#[derive(Debug, Clone)]
pub struct L2Annotation {
    /// Max notional under the slippage budget at the entry-time book.
    pub cap_notional: f64,
    /// Cap ran off the visible book (true cap deeper; notional is a floor).
    pub censored: bool,
    pub binding: &'static str,
    /// Book age at the sizing moment, seconds.
    pub staleness_s: f64,
    /// USD risk when carrying `cap_notional`: cap × |entry−stop|/entry.
    pub risk_usd: f64,
    /// Size-premium of the entry leg at cap size, R units (0 for maker fills —
    /// the fill model already prices the touch; this is only the extra depth
    /// cost of being big).
    pub entry_tax_r: f64,
    /// Size-premium of the exit leg at cap size, R units (0 for TP exits —
    /// those are maker).
    pub exit_tax_r: f64,
    /// Exit-time book missing → exit tax unknowable, reported as 0 but flagged.
    pub exit_book_missing: bool,
    /// PnL in dollars at cap size: (r_pnl − entry_tax_r − exit_tax_r) × risk_usd.
    pub pnl_usd: f64,
}

/// Annotate one closed trade against the store. Returns None when no
/// fresh-enough entry-time book exists (no coverage → trade can't be sized).
#[allow(clippy::too_many_arguments)]
pub fn annotate_trade(
    store: &L2Store,
    asset: &str,
    is_long: bool,
    entry: f64,
    stop: f64,
    fill: f64,
    filled_at: NaiveDateTime,
    closed_at: Option<NaiveDateTime>,
    exit_is_taker: bool,
    r_pnl: f64,
    cap_bps: f64,
    adverse_bps: f64,
    max_staleness_s: f64,
) -> Option<L2Annotation> {
    let stop_frac = (entry - stop).abs() / entry;
    if stop_frac <= 0.0 {
        return None;
    }
    let book = store.snapshot_before(asset, filled_at, max_staleness_s)?;
    let ts_ms = filled_at.and_utc().timestamp_millis();
    let staleness_s = (ts_ms - book.time_ms) as f64 / 1000.0;
    let cap = book.trade_cap(is_long, cap_bps, adverse_bps, staleness_s)?;

    let entry_side = if is_long { Side::Ask } else { Side::Bid };
    let exit_side = if is_long { Side::Bid } else { Side::Ask };

    // Entry leg: taker only when the fill model chased (fill ≠ entry). The
    // touch fill is already in r_pnl; tax is the size premium beyond it.
    let entry_is_taker = (fill - entry).abs() > 1e-12;
    let entry_tax_r = if entry_is_taker {
        book.size_premium_frac(entry_side, cap.notional)
            .map(|(p, _)| p / stop_frac)
            .unwrap_or(0.0)
    } else {
        0.0
    };

    let mut exit_book_missing = false;
    let exit_tax_r = if exit_is_taker {
        match closed_at.and_then(|t| store.snapshot_before(asset, t, max_staleness_s)) {
            Some(xb) => xb
                .size_premium_frac(exit_side, cap.notional)
                .map(|(p, _)| p / stop_frac)
                .unwrap_or(0.0),
            None => {
                exit_book_missing = true;
                0.0
            }
        }
    } else {
        0.0
    };

    let risk_usd = cap.notional * stop_frac;
    let pnl_usd = (r_pnl - entry_tax_r - exit_tax_r) * risk_usd;
    Some(L2Annotation {
        cap_notional: cap.notional,
        censored: cap.censored,
        binding: cap.binding,
        staleness_s,
        risk_usd,
        entry_tax_r,
        exit_tax_r,
        exit_book_missing,
        pnl_usd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> BookSnapshot {
        // mid = 100.0; each level $10_000-ish notional
        BookSnapshot {
            time_ms: 0,
            bids: vec![(99.9, 100.0), (99.8, 100.0), (99.7, 100.0)],
            asks: vec![(100.1, 100.0), (100.2, 100.0), (100.3, 100.0)],
        }
    }

    #[test]
    fn walk_vwap_first_level() {
        let b = book();
        let (vwap, cens) = b.walk_vwap(Side::Ask, 5_000.0).unwrap();
        assert!(!cens);
        assert!((vwap - 100.1).abs() < 1e-9);
    }

    #[test]
    fn walk_vwap_censored_prices_remainder_at_worst() {
        let b = book();
        let total: f64 = b.asks.iter().map(|(p, s)| p * s).sum();
        let (vwap, cens) = b.walk_vwap(Side::Ask, total + 10_000.0).unwrap();
        assert!(cens);
        assert!(vwap > 100.2 && vwap <= 100.3);
    }

    #[test]
    fn max_notional_respects_budget() {
        let b = book();
        // touch slip on ask = (100.1-100)/100 = 10 bps → budget below 10bps = 0
        let (cap, cens) = b.max_notional(Side::Ask, 5.0, 0.0).unwrap();
        assert_eq!(cap, 0.0);
        assert!(!cens);
        // generous budget swallows the whole visible side → censored
        let (cap, cens) = b.max_notional(Side::Ask, 100.0, 0.0).unwrap();
        let total: f64 = b.asks.iter().map(|(p, s)| p * s).sum();
        assert!((cap - total).abs() < 1e-6);
        assert!(cens);
        // budget binding mid-book: cap strictly between first level and total
        let (cap_mid, cens) = b.max_notional(Side::Ask, 12.0, 0.0).unwrap();
        assert!(!cens);
        assert!(cap_mid > 100.1 * 100.0 - 1e-6 && cap_mid < total);
        // adverse offset shrinks the cap
        let (cap_off, _) = b.max_notional(Side::Ask, 12.0, 1.5).unwrap();
        assert!(cap_off < cap_mid);
    }

    #[test]
    fn trade_cap_takes_worse_side() {
        let mut b = book();
        b.bids.truncate(1); // thin bid side → exit leg binds for longs
        let cap = b.trade_cap(true, 100.0, 0.0, 0.0).unwrap();
        assert_eq!(cap.binding, "exit");
        let total_bid: f64 = b.bids.iter().map(|(p, s)| p * s).sum();
        assert!((cap.notional - total_bid).abs() < 1e-6);
    }

    #[test]
    fn size_premium_zero_at_touch_size() {
        let b = book();
        let (prem, _) = b.size_premium_frac(Side::Ask, 1_000.0).unwrap();
        assert!(prem.abs() < 1e-12); // fits in first level → no premium
        let (prem2, _) = b.size_premium_frac(Side::Ask, 15_000.0).unwrap();
        assert!(prem2 > 0.0);
    }
}
