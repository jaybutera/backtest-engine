use chrono::NaiveDateTime;
use std::fmt;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;

static SIGNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_signal_id() -> String {
    let id = SIGNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:012x}", id)
}

use std::collections::HashMap as StdHashMap;
use std::sync::LazyLock;
use std::sync::Mutex;

// ─── String → u16 Interning ─────────────────────────────────────────────────
// Separate intern pools for sig_type, asset, and timeframe.
// Each maps a string to a stable u16 ID for O(1) comparison and Copy semantics.

macro_rules! define_intern_pool {
    ($map:ident, $rev:ident, $id_fn:ident, $name_fn:ident) => {
        static $map: LazyLock<Mutex<StdHashMap<String, u16>>> =
            LazyLock::new(|| Mutex::new(StdHashMap::new()));
        static $rev: LazyLock<Mutex<Vec<Arc<str>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

        pub fn $id_fn(name: &str) -> u16 {
            let mut map = $map.lock().unwrap();
            if let Some(&id) = map.get(name) {
                return id;
            }
            let mut rev = $rev.lock().unwrap();
            let id = rev.len() as u16;
            rev.push(Arc::from(name));
            map.insert(name.to_string(), id);
            id
        }

        pub fn $name_fn(id: u16) -> Arc<str> {
            $rev.lock().unwrap()[id as usize].clone()
        }
    };
}

define_intern_pool!(SIG_TYPE_MAP, SIG_TYPE_REV, sig_type_id, sig_type_name);
define_intern_pool!(ASSET_MAP, ASSET_REV, asset_id, asset_name);
define_intern_pool!(TF_MAP, TF_REV, tf_id, tf_name);

// ─── Hot-path timeframe identity ────────────────────────────────────────────
// `tf_name` takes a global mutex and clones an `Arc<str>` on EVERY call, which
// is far too expensive for a per-candle path that only wants to compare the
// result against a string literal. Interned IDs are already stable u16s, so
// those comparisons can be integer compares against a once-resolved ID.
//
// IMPORTANT — this is NOT the same thing as `base_tf_id()`. The base interval
// is overridable via `set_base_interval` (e.g. "1h" for a 1h-only backtest),
// so `base_tf_id() != tf_id("1m")` in that mode. `TF_1M` always means the
// literal 1-minute timeframe, which is what `tf_name(x) == "1m"` meant before.
// Every migrated call site documents which of the two it needs.
static TF_1M: LazyLock<u16> = LazyLock::new(|| tf_id("1m"));

/// The interned tf_id of the literal "1m" timeframe, resolved once.
///
/// Prefer this (or [`is_1m`]) over `tf_name(id).as_ref() == "1m"` anywhere in a
/// per-candle path. Note this is the *literal* 1-minute timeframe, which is
/// distinct from [`base_tf_id`] when `set_base_interval` was given a non-"1m"
/// interval.
#[inline]
pub fn tf_1m_id() -> u16 {
    *TF_1M
}

/// True when `tf` is the literal 1-minute timeframe.
///
/// Exact replacement for the old `tf_name(tf).as_ref() == "1m"` idiom, without
/// the mutex acquisition or `Arc` clone.
#[inline]
pub fn is_1m(tf: u16) -> bool {
    tf == *TF_1M
}

// Interning "5m" here (rather than only looking it up if already present)
// gives the ID an unconditional, order-independent meaning: `is_5m` is correct
// no matter when it is first called. The alternative — a lookup that caches a
// "not present" sentinel — would silently answer `false` forever if it ran
// before the first 5m candle was built, an invariant nothing enforces.
//
// Interning eagerly does shift which number each later timeframe receives, but
// that is not observable: no tf ID is serialized numerically, no code sorts or
// compares raw tf IDs (all ordering goes through `tf_minutes`), and the only
// tf-keyed map whose iteration order is ID-dependent is
// `TimeframeBuilder::flush`'s buffer map, which has no production caller.
static TF_5M: LazyLock<u16> = LazyLock::new(|| tf_id("5m"));

/// True when `tf` is the literal 5-minute timeframe.
///
/// Exact replacement for `tf_name(tf).as_ref() == "5m"`, minus the lock and the
/// allocation. See [`is_1m`] for why this is a literal test, not a base test.
#[inline]
pub fn is_5m(tf: u16) -> bool {
    tf == *TF_5M
}

// ─── Memoized timeframe→constant lookups ────────────────────────────────────
// Several pure functions map a timeframe ID to a constant by matching on
// `tf_name` (`tf_minutes`, `tf_multiplier`, `tf_rank`, `tf_minutes_id`). They
// sit in per-level-per-candle loops — `LiquidityRegistry::tick_decay` calls
// `tf_minutes` once for EVERY active level on EVERY candle — so each one was
// paying the intern pool's mutex and an `Arc` clone to rediscover a constant.
//
// `TfMemo` caches the answer per timeframe ID. The mapping is pure and the
// intern pool is append-only (an ID's name never changes), so a cached entry
// can never go stale; a miss simply recomputes via the original closure.

/// The number of distinct timeframe IDs a memo can cover. The pool holds a
/// handful of entries ("1m", "5m", "15m", "1h", "4h", "1D", "1W", "1M", plus
/// spelling variants); anything beyond this bound falls back to the uncached
/// path rather than being cached, so the bound is a performance limit, never a
/// correctness one.
const TF_MEMO_SLOTS: usize = 64;

/// A lock-free memo over `tf_id -> f64`, backed by one atomic per slot.
///
/// Empty slots hold `EMPTY` (a NaN bit pattern that no real result uses).
/// Races between two threads filling the same slot are benign: the mapping is
/// pure, so both write the identical bits.
pub struct TfMemo {
    slots: [AtomicU64; TF_MEMO_SLOTS],
    compute: fn(u16) -> f64,
}

impl TfMemo {
    /// Sentinel for "not yet computed". A signalling-NaN bit pattern, which
    /// `f64::to_bits` never produces for any of the finite constants stored.
    const EMPTY: u64 = 0x7ff0_0000_0000_0001;

    pub const fn new(compute: fn(u16) -> f64) -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const INIT: AtomicU64 = AtomicU64::new(TfMemo::EMPTY);
        Self {
            slots: [INIT; TF_MEMO_SLOTS],
            compute,
        }
    }

    #[inline]
    pub fn get(&self, tf: u16) -> f64 {
        let idx = tf as usize;
        if idx >= TF_MEMO_SLOTS {
            return (self.compute)(tf);
        }
        let cached = self.slots[idx].load(Ordering::Relaxed);
        if cached != Self::EMPTY {
            return f64::from_bits(cached);
        }
        let value = (self.compute)(tf);
        self.slots[idx].store(value.to_bits(), Ordering::Relaxed);
        value
    }
}

// ─── Base interval (smallest loaded timeframe) ──────────────────────────────
// The backtester/live engine processes a stream of base candles and aggregates
// higher timeframes from them. By default the base is "1m" (the collector's
// native grain). The replay path can override this at startup (e.g. "1h" for a
// 1h-only backtest where no sub-1h data exists). Set ONCE before any candles
// are loaded; never mutated concurrently. Stored as an interned tf_id.
//
// A separate "set" flag is used rather than overloading a 0 tf_id sentinel:
// the intern pool assigns IDs starting at 0, so a valid tf can legitimately
// have id 0 and must not be mistaken for "unset".
static BASE_TF_ID: AtomicU16 = AtomicU16::new(0);
static BASE_TF_SET: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Override the base (smallest) timeframe. Call once at startup, before loading
/// candles. `interval` is e.g. "1m" (default) or "1h".
pub fn set_base_interval(interval: &str) {
    BASE_TF_ID.store(tf_id(interval), Ordering::Relaxed);
    BASE_TF_SET.store(true, Ordering::Relaxed);
}

/// The interned tf_id of the base interval. Defaults to `tf_id("1m")` when unset.
pub fn base_tf_id() -> u16 {
    if BASE_TF_SET.load(Ordering::Relaxed) {
        BASE_TF_ID.load(Ordering::Relaxed)
    } else {
        tf_id("1m")
    }
}

/// The base interval as a string (e.g. "1m", "1h").
pub fn base_interval() -> String {
    tf_name(base_tf_id()).to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Direction {
    Bull,
    Bear,
}

impl Direction {
    pub fn opposite(self) -> Direction {
        match self {
            Direction::Bull => Direction::Bear,
            Direction::Bear => Direction::Bull,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Bull => "bull",
            Direction::Bear => "bear",
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct Candle {
    pub asset: u16,
    pub timeframe: u16,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub timestamp: NaiveDateTime,
    pub complete: bool,
}

/// A single raw trade print (millisecond resolution), the atom of the
/// tick-resolution fill mode. Loaded from a per-asset tick parquet by
/// `data::load_ticks`; one row per executed trade.
///
/// Unlike a `Candle` (which summarizes a whole minute and loses intrabar order),
/// a `Tick` carries the EXACT time and price a trade executed, so the fill
/// engine can act on the FIRST tick that crosses a stop/TP/entry level rather
/// than guessing the intrabar order from OHLC.
#[derive(Debug, Clone, Copy)]
pub struct Tick {
    /// Trade time, millisecond precision (naive UTC, matching Candle timestamps).
    pub timestamp: NaiveDateTime,
    /// Executed price.
    pub price: f64,
    /// Executed size (base units).
    pub size: f64,
    /// Aggressor side: 0 = buy (up), 1 = sell (down). Carried for completeness;
    /// the fill engine keys off `price`/`timestamp` only.
    pub side: u8,
}

impl Candle {
    pub fn is_bullish(&self) -> bool {
        self.close > self.open
    }
    pub fn is_bearish(&self) -> bool {
        self.close < self.open
    }
    pub fn body_high(&self) -> f64 {
        self.open.max(self.close)
    }
    pub fn body_low(&self) -> f64 {
        self.open.min(self.close)
    }
    pub fn range(&self) -> f64 {
        self.high - self.low
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub signal_type: u16,
    pub asset: u16,
    pub timeframe: u16,
    pub direction: Direction,
    /// The strategy's own ranking of this setup. The engine never interprets
    /// it beyond comparing it against the configured floor, so its scale is
    /// entirely the strategy's business.
    pub score: f64,
    /// When the setup completed — the signal time, which can precede the fill
    /// by many bars while an entry order rests.
    pub created_at: NaiveDateTime,
    pub invalidated: bool,
    pub invalidation_reason: Option<String>,
    pub id: String,
    /// The setup is not final yet — emitted for diagnostics only. The engine
    /// journals a developing opportunity but never places an order for one.
    pub developing: bool,
    /// Price the strategy wants to enter at. `Strategy::admit`'s default
    /// implementation reads this (with `stop`) to build the trade geometry; a
    /// strategy that computes geometry inside `admit` may leave it `None`.
    pub entry: Option<f64>,
    /// Price at which the setup is wrong. Paired with `entry`; see above.
    pub stop: Option<f64>,
    /// Optional explicit take-profit. When `Some`, `default_admit` uses it
    /// verbatim instead of projecting one from the reward:risk target.
    pub target: Option<f64>,
}

impl Opportunity {
    /// A bare opportunity at the given time: zero score, no geometry. Stamp
    /// `score` / `entry` / `stop` with the builders below, or override
    /// `Strategy::admit` and compute the geometry there instead.
    ///
    /// `signal_type` names the kind of setup and is interned; it appears in
    /// the per-signal-type breakdown of the report, so a strategy that emits
    /// several kinds gets them attributed separately.
    pub fn new(
        signal_type: &str,
        asset: &str,
        timeframe: &str,
        direction: Direction,
        created_at: NaiveDateTime,
    ) -> Self {
        Self {
            signal_type: sig_type_id(signal_type),
            asset: asset_id(asset),
            timeframe: tf_id(timeframe),
            direction,
            score: 0.0,
            created_at,
            invalidated: false,
            invalidation_reason: None,
            id: next_signal_id(),
            developing: false,
            entry: None,
            stop: None,
            target: None,
        }
    }

    /// Builder: stamp the entry and stop geometry.
    pub fn with_entry_stop(mut self, entry: f64, stop: f64) -> Self {
        self.entry = Some(entry);
        self.stop = Some(stop);
        self
    }

    /// Builder: stamp the score.
    pub fn with_score(mut self, score: f64) -> Self {
        self.score = score;
        self
    }

    /// The `(entry, stop)` pair, present only when the strategy stamped both.
    pub fn entry_stop(&self) -> Option<(f64, f64)> {
        match (self.entry, self.stop) {
            (Some(e), Some(s)) => Some((e, s)),
            _ => None,
        }
    }

    pub fn age_seconds(&self, now: NaiveDateTime) -> f64 {
        (now - self.created_at).num_seconds() as f64
    }
}

// ─── Paper Trading Models ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum TradeResult {
    Win,
    Loss,
    Inconclusive,
}

impl TradeResult {
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeResult::Win => "win",
            TradeResult::Loss => "loss",
            TradeResult::Inconclusive => "inconclusive",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaperTrade {
    pub opportunity_id: String,
    pub signal_type: u16,
    pub asset: u16,
    pub timeframe: u16,
    pub direction: Direction,
    pub entry: f64,
    pub stop: f64,
    pub tp: f64,
    /// Price the entry limit actually filled at. Equals `entry` when there is
    /// no slippage; with `entry_slippage_r > 0` it is shifted toward the losing
    /// side (bull +, bear −). P&L is still divided by `R_planned = |entry−stop|`,
    /// NOT by `|fill−stop|` — the slippage shows up as a uniform drag, not a
    /// rescale. See the honest fill model in `paper.rs`.
    pub fill: f64,
    pub score: f64,
    pub opened_at: NaiveDateTime,
    /// When the entry actually filled (the booking bar/tick). `opened_at` is
    /// the SIGNAL time and can precede this by many bars while the limit rests;
    /// None only while the trade is a provisional pending-fill template.
    pub filled_at: Option<NaiveDateTime>,
    pub closed_at: Option<NaiveDateTime>,
    pub result: TradeResult,
    pub r_pnl: f64,
    /// Fee cost of the round-trip in R units (0.0 when fees are disabled).
    pub fee_r: f64,
}

impl PaperTrade {
    pub fn risk(&self) -> f64 {
        (self.entry - self.stop).abs()
    }
}

#[cfg(test)]
mod tf_hot_path_tests {
    use super::*;

    /// The whole point of `is_1m` / `is_5m` is that they are drop-in
    /// replacements for the string compares they replaced. If these ever
    /// diverge, every migrated hot-path gate silently changes meaning.
    #[test]
    fn is_1m_and_is_5m_agree_with_the_string_compare() {
        for name in [
            "1m", "5m", "15m", "1h", "4h", "1D", "1d", "daily", "1W", "1M",
        ] {
            let id = tf_id(name);
            assert_eq!(
                is_1m(id),
                tf_name(id).as_ref() == "1m",
                "is_1m disagrees with the string compare for {name:?}"
            );
            assert_eq!(
                is_5m(id),
                tf_name(id).as_ref() == "5m",
                "is_5m disagrees with the string compare for {name:?}"
            );
        }
    }

    /// "1m" is one minute and "1M" is one month. They are separate pool
    /// entries, and the integer compare must keep them apart exactly as the
    /// case-sensitive string compare did.
    #[test]
    fn one_minute_is_not_one_month() {
        assert_ne!(tf_id("1m"), tf_id("1M"));
        assert!(is_1m(tf_id("1m")));
        assert!(!is_1m(tf_id("1M")));
    }

    /// `is_1m` asks "is this literally the 1-minute timeframe?" while
    /// `base_tf_id` asks "is this the smallest loaded timeframe?". They
    /// coincide under the default base and MUST NOT under a non-1m base —
    /// that difference is why the migrated call sites were audited one by one
    /// rather than swapped mechanically for `base_tf_id()`.
    ///
    /// `set_base_interval` writes a process-global, so this test asserts the
    /// distinction without mutating it (a mutation would leak into every other
    /// test in the binary). The 1h ID standing in for a hypothetical non-1m
    /// base is enough to show `is_1m` does not track the base.
    #[test]
    fn is_1m_is_not_a_base_interval_test() {
        let one_h = tf_id("1h");
        assert!(
            !is_1m(one_h),
            "is_1m must be false for 1h even if 1h were the base"
        );
        assert_eq!(base_tf_id(), tf_id("1m"), "default base is 1m in tests");
    }

    /// The memo must return exactly what its backing function returns, for
    /// hits, misses, and IDs past the table bound.
    #[test]
    fn tf_memo_matches_its_uncached_function() {
        fn compute(tf: u16) -> f64 {
            (tf as f64) * 1.5 + 0.25
        }
        static MEMO: TfMemo = TfMemo::new(compute);

        // First call fills the slot, second reads it back — both must agree.
        for tf in [0u16, 1, 7, 63] {
            assert_eq!(MEMO.get(tf), compute(tf), "cold miss wrong for {tf}");
            assert_eq!(MEMO.get(tf), compute(tf), "cached hit wrong for {tf}");
        }
        // Past the table bound the memo must fall through, not misindex.
        for tf in [64u16, 1000, u16::MAX] {
            assert_eq!(
                MEMO.get(tf),
                compute(tf),
                "unbounded fallthrough wrong for {tf}"
            );
        }
    }

    /// A memoized value of 0.0 must not be mistaken for an empty slot —
    /// `tf_rank` legitimately returns 0 for the 1m timeframe.
    #[test]
    fn tf_memo_caches_zero() {
        fn zero(_tf: u16) -> f64 {
            0.0
        }
        static MEMO: TfMemo = TfMemo::new(zero);
        assert_eq!(MEMO.get(3), 0.0);
        assert_eq!(MEMO.get(3), 0.0);
    }
}
