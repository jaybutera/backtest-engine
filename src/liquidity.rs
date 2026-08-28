//! Native market-structure services a script strategy can subscribe to
//! instead of scanning every bar itself.
//!
//! A [`Scanner`] owns the per-bar bookkeeping that is the same for every
//! liquidity-style strategy and is all parameterized by the strategy: a
//! registry of price levels (cluster-merged, decaying, retest-refreshed), a
//! sweep detector over that registry, a fair-value-gap tracker with
//! mitigation and inversion, session / previous-period levels, swing /
//! gap / structure-break primitives with de-duplication, equal-high /
//! equal-low clustering, a ranked "draw on liquidity" map and a UTC day
//! aggregator. Nothing here decides what to trade: the services produce
//! events (sweeps, structure breaks, day rolls) and answer queries
//! (nearest level, first gap after a time, draw-map targets), and the
//! strategy composes them.
//!
//! Every table a service uses (source significance, timeframe multipliers,
//! session hours, caps, cadences) comes from [`ScannerCfg`], which a script
//! builds as a map. The defaults in this file are only fallbacks; a
//! strategy that relies on a value states it.
//!
//! The per-candle order is fixed and documented on [`Scanner::process`];
//! the strategy's hooks run at the two points marked there.

use std::collections::{HashMap, HashSet, VecDeque};

use rustc_hash::FxHashMap;
use serde::Deserialize;

use crate::models::{tf_id, Candle, Direction};

/// Numbers as a script writes them: an integer where a float is expected
/// (or the reverse) is accepted, since Rhai literals and TOML values do
/// not distinguish `5` from `5.0` on purpose.
mod lenient {
    use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
    use std::collections::HashMap;
    use std::fmt;

    struct Num;
    impl<'de> Visitor<'de> for Num {
        type Value = f64;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a number")
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<f64, E> {
            Ok(v as f64)
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<f64, E> {
            Ok(v as f64)
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<f64, E> {
            Ok(v)
        }
    }
    pub fn f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        d.deserialize_any(Num)
    }
    pub fn i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        d.deserialize_any(Num).map(|v| v as i64)
    }
    struct MapF64;
    impl<'de> Visitor<'de> for MapF64 {
        type Value = HashMap<String, f64>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a map of numbers")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<Self::Value, A::Error> {
            let mut out = HashMap::new();
            while let Some(k) = m.next_key::<String>()? {
                let v: f64 = m.next_value_seed(F64Seed)?;
                out.insert(k, v);
            }
            Ok(out)
        }
    }
    struct F64Seed;
    impl<'de> de::DeserializeSeed<'de> for F64Seed {
        type Value = f64;
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<f64, D::Error> {
            d.deserialize_any(Num)
        }
    }
    pub fn map_f64<'de, D: Deserializer<'de>>(d: D) -> Result<HashMap<String, f64>, D::Error> {
        d.deserialize_map(MapF64)
    }
    struct PairsVisitor;
    impl<'de> Visitor<'de> for PairsVisitor {
        type Value = Vec<(i64, f64)>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a list of [threshold, value] pairs")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut out = Vec::new();
            while let Some(pair) = seq.next_element_seed(PairSeed)? {
                out.push(pair);
            }
            Ok(out)
        }
    }
    struct PairSeed;
    impl<'de> de::DeserializeSeed<'de> for PairSeed {
        type Value = (i64, f64);
        fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = (i64, f64);
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("[threshold, value]")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                    let a: f64 = seq
                        .next_element_seed(F64Seed)?
                        .ok_or_else(|| de::Error::custom("missing threshold"))?;
                    let b: f64 = seq
                        .next_element_seed(F64Seed)?
                        .ok_or_else(|| de::Error::custom("missing value"))?;
                    Ok((a as i64, b))
                }
            }
            d.deserialize_seq(V)
        }
    }
    pub fn pairs<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<(i64, f64)>, D::Error> {
        d.deserialize_seq(PairsVisitor)
    }
}

// ─── Configuration ──────────────────────────────────────────────────────────

fn d_one() -> f64 {
    1.0
}
fn d_true() -> bool {
    true
}
fn d_10() -> i64 {
    10
}
fn d_50() -> i64 {
    50
}
fn d_2000() -> i64 {
    2000
}
fn d_500() -> i64 {
    500
}
fn d_240() -> i64 {
    240
}
fn d_5000() -> i64 {
    5000
}
fn d_15() -> i64 {
    15
}
fn d_base_tf() -> String {
    "1m".to_string()
}

/// How a level's significance is derived from its source, timeframe and
/// touch count: `base[source] * tf_mult[tf] (if source in tf_scaled) +
/// touch_bonus`.
#[derive(Clone, Debug, Deserialize, Default)]
pub struct SignificanceCfg {
    /// Base score per source name; unknown sources take `default`.
    #[serde(default, deserialize_with = "lenient::map_f64")]
    pub base: HashMap<String, f64>,
    #[serde(default = "d_one", deserialize_with = "lenient::f64")]
    pub default_score: f64,
    /// Sources whose base score is multiplied by the timeframe multiplier.
    #[serde(default)]
    pub tf_scaled: Vec<String>,
    /// Multiplier per timeframe name for `tf_scaled` sources (default 1).
    #[serde(default, deserialize_with = "lenient::map_f64")]
    pub tf_mult: HashMap<String, f64>,
    /// `[[touch, bonus], ...]`: the bonus of the highest threshold ≤ touch.
    #[serde(default, deserialize_with = "lenient::pairs")]
    pub touch_bonus: Vec<(i64, f64)>,
}

/// Registry behavior.
#[derive(Clone, Debug, Deserialize)]
pub struct LevelsCfg {
    /// Levels of one side within `atr * cluster_atr_tolerance` merge.
    #[serde(deserialize_with = "lenient::f64")]
    pub cluster_atr_tolerance: f64,
    /// A level expires once its decay counter reaches this (kept in single
    /// precision, as a counter of base bars).
    #[serde(deserialize_with = "lenient::f64")]
    pub decay_candles: f64,
    /// Levels at or above `sig_decay_min_sig` decay this many times slower.
    #[serde(default = "d_one", deserialize_with = "lenient::f64")]
    pub sig_decay_mult: f64,
    #[serde(default, deserialize_with = "lenient::f64")]
    pub sig_decay_min_sig: f64,
    /// A bar closing within `atr * refresh_atr` of a level without crossing
    /// it resets the level's decay. 0 disables.
    #[serde(default, deserialize_with = "lenient::f64")]
    pub refresh_atr: f64,
    /// Decay ticks once every this many candles (all timeframes counted).
    #[serde(default = "d_10", deserialize_with = "lenient::i64")]
    pub decay_every: i64,
    /// Non-active levels are dropped every this many candles.
    #[serde(default = "d_50", deserialize_with = "lenient::i64")]
    pub prune_every: i64,
    /// Minutes per timeframe name, the decay divisor (default 1).
    #[serde(default, deserialize_with = "lenient::map_f64")]
    pub tf_minutes: HashMap<String, f64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SweepCfg {
    /// A bar must exceed the level by `atr * noise_atr` to count as crossing.
    #[serde(deserialize_with = "lenient::f64")]
    pub noise_atr: f64,
    /// A crossing that persists this many bars finalizes as a sweep.
    #[serde(deserialize_with = "lenient::i64")]
    pub max_multi_candle: i64,
    /// Levels below this significance are never swept.
    #[serde(default, deserialize_with = "lenient::f64")]
    pub min_level_sig: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PrimitivesCfg {
    #[serde(deserialize_with = "lenient::i64")]
    pub atr_period: i64,
    /// Bars on each side a swing must dominate.
    #[serde(deserialize_with = "lenient::i64")]
    pub swing_lookback: i64,
    /// Minimum gap as a fraction of the middle bar's range.
    #[serde(deserialize_with = "lenient::f64")]
    pub fvg_min_gap_pct: f64,
    /// A close must clear the latest swing by `atr * structure_min_displacement_atr`.
    #[serde(deserialize_with = "lenient::f64")]
    pub structure_min_displacement_atr: f64,
    /// Signals kept before the oldest are dropped and the de-dup indexes rebuilt.
    #[serde(default = "d_2000", deserialize_with = "lenient::i64")]
    pub signals_cap: i64,
    /// Source names given to swing-high / swing-low levels.
    #[serde(default = "d_swing_high")]
    pub swing_high_source: String,
    #[serde(default = "d_swing_low")]
    pub swing_low_source: String,
}
fn d_swing_high() -> String {
    "swing_high".into()
}
fn d_swing_low() -> String {
    "swing_low".into()
}

#[derive(Clone, Debug, Deserialize)]
pub struct SessionSpec {
    pub name: String,
    #[serde(deserialize_with = "lenient::i64")]
    pub start_hour: i64,
    #[serde(deserialize_with = "lenient::i64")]
    pub end_hour: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpenMark {
    #[serde(deserialize_with = "lenient::i64")]
    pub hour: i64,
    #[serde(deserialize_with = "lenient::i64")]
    pub minute: i64,
    pub source: String,
}

/// Session and previous-period levels, on a fixed-offset clock.
#[derive(Clone, Debug, Deserialize, Default)]
pub struct SessionsCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Seconds added to UTC to get the session clock.
    #[serde(default, deserialize_with = "lenient::i64")]
    pub tz_offset_secs: i64,
    #[serde(default)]
    pub sessions: Vec<SessionSpec>,
    /// The first bar at or after `hour:minute` of each day marks its open.
    #[serde(default)]
    pub open_marks: Vec<OpenMark>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EqualLevelsCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Highs/lows kept for clustering.
    #[serde(default = "d_500", deserialize_with = "lenient::i64")]
    pub window: i64,
    /// Scan cadence in base bars.
    #[serde(default = "d_240", deserialize_with = "lenient::i64")]
    pub every: i64,
    /// Touches a cluster needs.
    #[serde(default = "d_10", deserialize_with = "lenient::i64")]
    pub min_count: i64,
    /// Clusters closer than `atr * min_dist_atr` to the close are skipped.
    #[serde(default = "d_one", deserialize_with = "lenient::f64")]
    pub min_dist_atr: f64,
    #[serde(default = "d_5000", deserialize_with = "lenient::i64")]
    pub emitted_cap: i64,
    #[serde(default = "d_eqh")]
    pub high_source: String,
    #[serde(default = "d_eql")]
    pub low_source: String,
}
fn d_eqh() -> String {
    "equal_highs".into()
}
fn d_eql() -> String {
    "equal_lows".into()
}

#[derive(Clone, Debug, Deserialize)]
pub struct FvgCfg {
    /// Active gaps per kind (regular / inverted) before the oldest are filled.
    #[serde(default = "d_500", deserialize_with = "lenient::i64")]
    pub cap: i64,
    /// How many extra to fill past the cap so capping is not per-bar work.
    #[serde(default = "d_50", deserialize_with = "lenient::i64")]
    pub cap_overshoot: i64,
}

/// The ranked draw-on-liquidity map.
#[derive(Clone, Debug, Deserialize)]
pub struct DrawCfg {
    #[serde(default)]
    pub enabled: bool,
    /// Rebuild every this many candles on the base timeframe, and on any
    /// bar that finalizes a sweep.
    #[serde(default = "d_15", deserialize_with = "lenient::i64")]
    pub every: i64,
    /// Candidates farther than `atr * dist_cap_atr` are ignored.
    #[serde(deserialize_with = "lenient::f64")]
    pub dist_cap_atr: f64,
    /// Sweeps younger than this (seconds) with magnitude ≥ `boost_min_magnitude`
    /// boost targets in their direction by `boost`.
    #[serde(deserialize_with = "lenient::i64")]
    pub sweep_window_secs: i64,
    #[serde(deserialize_with = "lenient::f64")]
    pub boost_min_magnitude: f64,
    #[serde(deserialize_with = "lenient::f64")]
    pub boost: f64,
    /// Levels of one source within `atr * quantum_atr` are one candidate.
    #[serde(deserialize_with = "lenient::f64")]
    pub quantum_atr: f64,
    /// Sources eligible as targets, with their draw significance.
    #[serde(deserialize_with = "lenient::map_f64")]
    pub sources: HashMap<String, f64>,
    /// Gap midpoints eligible as targets, significance per timeframe name.
    #[serde(default, deserialize_with = "lenient::map_f64")]
    pub fvg_sig: HashMap<String, f64>,
    /// Only candidates at or above this significance compete for the top slot.
    #[serde(deserialize_with = "lenient::f64")]
    pub top_min_sig: f64,
    /// Two nearest candidates closer than this (in ATR) tie on weighted significance.
    #[serde(deserialize_with = "lenient::f64")]
    pub tie_atr: f64,
    /// The incumbent top keeps its slot unless a challenger beats it by this factor.
    #[serde(deserialize_with = "lenient::f64")]
    pub hysteresis: f64,
    #[serde(deserialize_with = "lenient::f64")]
    pub hysteresis_tol_atr: f64,
    #[serde(deserialize_with = "lenient::i64")]
    pub keep: i64,
    /// With the first `one_sided_n` targets all one direction, the first
    /// opposing target is spliced into slot `one_sided_n - 1`.
    #[serde(deserialize_with = "lenient::i64")]
    pub one_sided_n: i64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ScannerCfg {
    #[serde(default = "d_base_tf")]
    pub base_tf: String,
    pub primitives: PrimitivesCfg,
    pub significance: SignificanceCfg,
    pub levels: LevelsCfg,
    pub sweep: SweepCfg,
    #[serde(default)]
    pub sessions: SessionsCfg,
    pub equal_levels: Option<EqualLevelsCfg>,
    pub fvg: FvgCfg,
    pub draw: Option<DrawCfg>,
    /// Track UTC days and report each completed one.
    #[serde(default = "d_true")]
    pub day_tracker: bool,
}

// ─── Sources ────────────────────────────────────────────────────────────────

/// A level source name interned to a small id, with the per-source facts
/// every service needs resolved once.
#[derive(Clone, Debug)]
struct SourceInfo {
    name: String,
    base: f64,
    tf_scaled: bool,
    draw_sig: Option<f64>,
}

#[derive(Clone, Default)]
struct Sources {
    infos: Vec<SourceInfo>,
    by_name: HashMap<String, u16>,
}

impl Sources {
    fn intern(&mut self, name: &str, cfg: &ScannerCfg) -> u16 {
        if let Some(&i) = self.by_name.get(name) {
            return i;
        }
        let s = &cfg.significance;
        let info = SourceInfo {
            name: name.to_string(),
            base: s.base.get(name).copied().unwrap_or(s.default_score),
            tf_scaled: s.tf_scaled.iter().any(|x| x == name),
            draw_sig: cfg.draw.as_ref().and_then(|d| d.sources.get(name).copied()),
        };
        let id = self.infos.len() as u16;
        self.infos.push(info);
        self.by_name.insert(name.to_string(), id);
        id
    }
}

// ─── Levels ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// Buy-side liquidity: a level above price.
    Bsl,
    /// Sell-side liquidity: a level below price.
    Ssl,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Bsl => "BSL",
            Side::Ssl => "SSL",
        }
    }
    pub fn parse(s: &str) -> Option<Side> {
        match s {
            "BSL" | "bsl" => Some(Side::Bsl),
            "SSL" | "ssl" => Some(Side::Ssl),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Active,
    Swept,
    Expired,
}

#[derive(Clone, Debug)]
pub struct Level {
    pub id: i64,
    pub price: f64,
    pub side: Side,
    source: u16,
    pub tf: u16,
    pub ts: i64,
    pub touch: i64,
    status: Status,
    decay: f32,
    pub last_touch: i64,
    pub sig: f64,
    /// Developing sweeps (across timeframes) on this level.
    developing: i64,
}

/// A finalized liquidity sweep.
#[derive(Clone, Debug)]
pub struct Sweep {
    pub level_id: i64,
    pub level_price: f64,
    pub level_source: String,
    pub level_sig: f64,
    pub level_side: Side,
    /// The direction price is expected to go after the sweep.
    pub dir: Direction,
    pub start_ts: i64,
    pub extreme_ts: i64,
    pub extreme_price: f64,
    pub magnitude_atr: f64,
    pub level_formed_at: i64,
}

/// A structure break: a close past the latest swing on a timeframe.
#[derive(Clone, Debug)]
pub struct Break {
    pub tf: u16,
    pub ts: i64,
    pub level: f64,
    pub dir: Direction,
}

#[derive(Clone, Debug)]
struct Developing {
    start_ts: i64,
    extreme_price: f64,
    extreme_ts: i64,
    count: i64,
    dir: Direction,
}

// ─── Signals (primitives) ───────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignalKind {
    SwingHigh,
    SwingLow,
    Fvg,
    Break,
}

#[derive(Clone, Debug)]
struct Signal {
    kind: SignalKind,
    tf: u16,
    ts: i64,
    level: f64,
    level_end: f64,
    dir: Direction,
    gap_ts: i64,
    c1_stop: f64,
}

#[derive(Clone, Copy, Debug)]
struct LatestSwing {
    level: f64,
    ts: i64,
}

// ─── FVGs ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FvgStatus {
    Unmitigated,
    Mitigated,
    Filled,
    Inverted,
}

impl FvgStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            FvgStatus::Unmitigated => "unmit",
            FvgStatus::Mitigated => "mit",
            FvgStatus::Filled => "filled",
            FvgStatus::Inverted => "inverted",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Fvg {
    pub id: i64,
    pub dir: Direction,
    pub tf: u16,
    pub ts: i64,
    /// The edge nearest the direction of travel (the entry edge).
    pub near: f64,
    pub far: f64,
    pub ce: f64,
    pub status: FvgStatus,
    pub is_inverted_kind: bool,
    /// The first candle's opposite extreme, for stops; none for inverted gaps.
    pub c1_stop: Option<f64>,
}

// ─── Draw map ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct DrawTarget {
    pub level_id: i64,
    pub price: f64,
    pub dir: Direction,
    pub distance_atr: f64,
    pub significance: f64,
    pub draw_score: f64,
}

#[derive(Clone, Debug)]
pub struct DrawMap {
    pub ts: i64,
    pub price: f64,
    pub atr: f64,
    pub targets: Vec<DrawTarget>,
}

/// A completed UTC day's aggregate.
#[derive(Clone, Copy, Debug)]
pub struct Day {
    pub day_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// What one bar produced, for the strategy's hooks.
#[derive(Clone, Debug, Default)]
pub struct BarEvents {
    pub atr: f64,
    pub sweeps: Vec<Sweep>,
    pub breaks: Vec<Break>,
    /// The UTC day that just completed, when the bar opened a new one.
    pub day_closed: Option<Day>,
    /// Whether the draw map is due a rebuild after the strategy's first hook.
    pub draw_due: bool,
}

// ─── Session state ──────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct SessionState {
    current_date: Option<chrono::NaiveDate>,
    current_week: Option<u32>,
    current_month: Option<u32>,
    day_high: Option<f64>,
    day_low: Option<f64>,
    week_high: Option<f64>,
    week_low: Option<f64>,
    month_high: Option<f64>,
    month_low: Option<f64>,
    session_high: Vec<Option<f64>>,
    session_low: Vec<Option<f64>>,
    session_active: Vec<bool>,
    open_mark_date: Vec<Option<chrono::NaiveDate>>,
}

#[derive(Clone)]
struct EqState {
    count: i64,
    highs: VecDeque<f64>,
    lows: VecDeque<f64>,
    last_close: f64,
    emitted: HashSet<(bool, i64)>,
}

// ─── The scanner ────────────────────────────────────────────────────────────

/// All services, stepped once per candle. See the module docs.
#[derive(Clone)]
pub struct Scanner {
    cfg: ScannerCfg,
    base_tf: u16,
    sources: Sources,
    tf_mult: FxHashMap<u16, f64>,
    tf_minutes: FxHashMap<u16, f64>,
    fvg_sig: FxHashMap<u16, f64>,
    src_pdh: u16,
    src_pdl: u16,
    src_pwh: u16,
    src_pwl: u16,
    src_pmh: u16,
    src_pml: u16,
    src_session_high: u16,
    src_session_low: u16,
    src_swing_high: u16,
    src_swing_low: u16,
    src_eqh: u16,
    src_eql: u16,
    open_mark_src: Vec<u16>,
    tf_1d: u16,
    tf_1w: u16,
    tf_1mo: u16,

    rings: FxHashMap<u16, VecDeque<Candle>>,
    ring_cap: usize,
    atr_cache: FxHashMap<u16, f64>,
    candle_count: i64,

    levels: Vec<Level>,
    next_level_id: i64,
    developing: FxHashMap<(i64, u16), Developing>,

    signals: VecDeque<Signal>,
    swing_seen: HashSet<(u8, u16, i64)>,
    fvg_seen: HashSet<(u16, i64, bool)>,
    break_seen: HashSet<(u16, bool, u64)>,
    latest_sh: FxHashMap<u16, LatestSwing>,
    latest_sl: FxHashMap<u16, LatestSwing>,

    sess: SessionState,
    eq: Option<EqState>,

    fvgs: Vec<Fvg>,
    fvg_active: Vec<usize>,
    fvg_by_tf: FxHashMap<u16, Vec<usize>>,

    recent_sweeps: Vec<Sweep>,
    draw_map: Option<DrawMap>,
    draw_top1: Option<(f64, Direction, f64)>,

    day_forming: Option<Day>,
    last_atr: f64,

    /// Timeframes the strategy wants a per-bar hook on even without events.
    pub wake: HashSet<u16>,
    /// Whether the strategy wants the book hook on bars with nothing closed.
    pub wake_book: bool,
}

fn dir_of_side(side: Side) -> Direction {
    match side {
        Side::Bsl => Direction::Bear,
        Side::Ssl => Direction::Bull,
    }
}

fn atr_of(b: &VecDeque<Candle>, period: i64) -> f64 {
    let n = (period.max(0) as usize).min(b.len().saturating_sub(1));
    if n < 1 {
        return 0.0;
    }
    let start = b.len() - n;
    let mut sum = 0.0;
    for i in start..b.len() {
        let h = b[i].high;
        let l = b[i].low;
        let pc = b[i - 1].close;
        let tr = (h - l).max((h - pc).abs()).max((l - pc).abs());
        sum += tr;
    }
    sum / n as f64
}

fn secs(c: &Candle) -> i64 {
    c.timestamp.and_utc().timestamp()
}

impl Scanner {
    pub fn new(cfg: ScannerCfg) -> Self {
        let mut sources = Sources::default();
        let mut intern = |n: &str| sources.intern(n, &cfg);
        let src_pdh = intern("pdh");
        let src_pdl = intern("pdl");
        let src_pwh = intern("pwh");
        let src_pwl = intern("pwl");
        let src_pmh = intern("pmh");
        let src_pml = intern("pml");
        let src_session_high = intern("session_high");
        let src_session_low = intern("session_low");
        let src_swing_high = intern(&cfg.primitives.swing_high_source);
        let src_swing_low = intern(&cfg.primitives.swing_low_source);
        let (eqh, eql) = match &cfg.equal_levels {
            Some(e) => (e.high_source.clone(), e.low_source.clone()),
            None => (d_eqh(), d_eql()),
        };
        let src_eqh = intern(&eqh);
        let src_eql = intern(&eql);
        let open_mark_src: Vec<u16> = cfg
            .sessions
            .open_marks
            .iter()
            .map(|m| intern(&m.source))
            .collect();
        for name in cfg.significance.base.keys() {
            intern(name);
        }
        if let Some(d) = &cfg.draw {
            for name in d.sources.keys() {
                intern(name);
            }
        }

        let tf_mult = cfg
            .significance
            .tf_mult
            .iter()
            .map(|(k, v)| (tf_id(k), *v))
            .collect();
        let tf_minutes = cfg
            .levels
            .tf_minutes
            .iter()
            .map(|(k, v)| (tf_id(k), *v))
            .collect();
        let fvg_sig = cfg
            .draw
            .as_ref()
            .map(|d| d.fvg_sig.iter().map(|(k, v)| (tf_id(k), *v)).collect())
            .unwrap_or_default();
        let ring_cap = (cfg.primitives.atr_period.max(0) as usize + 2)
            .max(2 * cfg.primitives.swing_lookback.max(0) as usize + 2)
            .max(4);
        let n_sess = cfg.sessions.sessions.len();
        let n_marks = cfg.sessions.open_marks.len();
        let eq = cfg.equal_levels.as_ref().filter(|e| e.enabled).map(|_| EqState {
            count: 0,
            highs: VecDeque::new(),
            lows: VecDeque::new(),
            last_close: 0.0,
            emitted: HashSet::new(),
        });
        Self {
            base_tf: tf_id(&cfg.base_tf),
            sources,
            tf_mult,
            tf_minutes,
            fvg_sig,
            src_pdh,
            src_pdl,
            src_pwh,
            src_pwl,
            src_pmh,
            src_pml,
            src_session_high,
            src_session_low,
            src_swing_high,
            src_swing_low,
            src_eqh,
            src_eql,
            open_mark_src,
            tf_1d: tf_id("1D"),
            tf_1w: tf_id("1W"),
            tf_1mo: tf_id("1M"),
            rings: FxHashMap::default(),
            ring_cap,
            atr_cache: FxHashMap::default(),
            candle_count: 0,
            levels: Vec::new(),
            next_level_id: 0,
            developing: FxHashMap::default(),
            signals: VecDeque::new(),
            swing_seen: HashSet::new(),
            fvg_seen: HashSet::new(),
            break_seen: HashSet::new(),
            latest_sh: FxHashMap::default(),
            latest_sl: FxHashMap::default(),
            sess: SessionState {
                session_high: vec![None; n_sess],
                session_low: vec![None; n_sess],
                session_active: vec![false; n_sess],
                open_mark_date: vec![None; n_marks],
                ..Default::default()
            },
            eq,
            fvgs: Vec::new(),
            fvg_active: Vec::new(),
            fvg_by_tf: FxHashMap::default(),
            recent_sweeps: Vec::new(),
            draw_map: None,
            draw_top1: None,
            day_forming: None,
            last_atr: 0.0,
            wake: HashSet::new(),
            wake_book: false,
            cfg,
        }
    }

    pub fn cfg(&self) -> &ScannerCfg {
        &self.cfg
    }

    pub fn source_name(&self, id: u16) -> &str {
        &self.sources.infos[id as usize].name
    }

    pub fn source_name_owned(&self, l: &Level) -> String {
        self.source_name(l.source).to_string()
    }

    /// Intern a source name (for levels a strategy adds itself).
    pub fn source_id(&mut self, name: &str) -> u16 {
        self.sources.intern(name, &self.cfg)
    }

    /// The configured significance of a level.
    pub fn significance(&self, source: u16, tf: u16, touch: i64) -> f64 {
        let info = &self.sources.infos[source as usize];
        let mut s = info.base;
        if info.tf_scaled {
            s *= self.tf_mult.get(&tf).copied().unwrap_or(1.0);
        }
        let mut bonus = 0.0;
        let mut best = i64::MIN;
        for &(thr, b) in &self.cfg.significance.touch_bonus {
            if touch >= thr && thr > best {
                best = thr;
                bonus = b;
            }
        }
        s + bonus
    }

    pub fn significance_by_name(&mut self, source: &str, tf: u16, touch: i64) -> f64 {
        let id = self.source_id(source);
        self.significance(id, tf, touch)
    }

    // ── Registry ────────────────────────────────────────────────────────────

    /// Add a level, merging into an active same-side level within the
    /// cluster tolerance when `atr > 0`. Returns the level's id.
    #[allow(clippy::too_many_arguments)]
    pub fn add_level(
        &mut self,
        price: f64,
        side: Side,
        source: u16,
        tf: u16,
        ts: i64,
        touch: i64,
        atr: f64,
    ) -> i64 {
        if atr > 0.0 {
            let tol = atr * self.cfg.levels.cluster_atr_tolerance;
            for i in 0..self.levels.len() {
                let l = &self.levels[i];
                if l.status == Status::Active
                    && l.side == side
                    && (l.price - price).abs() <= tol
                {
                    let (src, ltf, new_touch) = (l.source, l.tf, l.touch + touch);
                    let sig = self.significance(src, ltf, new_touch);
                    let l = &mut self.levels[i];
                    l.touch = new_touch;
                    l.sig = sig;
                    l.decay = 0.0;
                    l.last_touch = ts;
                    return l.id;
                }
            }
        }
        let id = self.next_level_id;
        self.next_level_id += 1;
        let sig = self.significance(source, tf, touch);
        self.levels.push(Level {
            id,
            price,
            side,
            source,
            tf,
            ts,
            touch,
            status: Status::Active,
            decay: 0.0,
            last_touch: ts,
            sig,
            developing: 0,
        });
        id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_level_named(
        &mut self,
        price: f64,
        side: Side,
        source: &str,
        tf: u16,
        ts: i64,
        touch: i64,
        atr: f64,
    ) -> i64 {
        let s = self.source_id(source);
        self.add_level(price, side, s, tf, ts, touch, atr)
    }

    fn level_index(&self, id: i64) -> Option<usize> {
        // Ids are issued in index order and pruning keeps order, so a
        // binary search on id finds the slot.
        self.levels.binary_search_by_key(&id, |l| l.id).ok()
    }

    pub fn level(&self, id: i64) -> Option<&Level> {
        self.level_index(id).map(|i| &self.levels[i])
    }

    /// Active levels of one side beyond `price`, nearest first, at or above
    /// `min_sig`.
    pub fn levels_beyond(&self, side: Side, price: f64, min_sig: f64) -> Vec<&Level> {
        let mut v: Vec<&Level> = self
            .levels
            .iter()
            .filter(|l| {
                l.status == Status::Active
                    && l.side == side
                    && match side {
                        Side::Bsl => l.price > price,
                        Side::Ssl => l.price < price,
                    }
                    && l.sig >= min_sig
            })
            .collect();
        match side {
            Side::Bsl => v.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap()),
            Side::Ssl => v.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap()),
        }
        v
    }

    /// The nearest active level of one side beyond `price`, any significance.
    pub fn nearest_beyond(&self, side: Side, price: f64) -> Option<&Level> {
        let mut best: Option<&Level> = None;
        for l in &self.levels {
            if l.status != Status::Active || l.side != side {
                continue;
            }
            let ok = match side {
                Side::Bsl => l.price > price,
                Side::Ssl => l.price < price,
            };
            if !ok {
                continue;
            }
            let better = match best {
                None => true,
                Some(b) => match side {
                    Side::Bsl => l.price < b.price,
                    Side::Ssl => l.price > b.price,
                },
            };
            if better {
                best = Some(l);
            }
        }
        best
    }

    /// The nearest level in the trade direction at least `min_rr` risk
    /// units away, at or above `min_sig` (0 = no floor).
    pub fn find_target(
        &self,
        dir: Direction,
        entry: f64,
        stop: f64,
        min_rr: f64,
        min_sig: f64,
    ) -> Option<(f64, i64, f64)> {
        let risk = (entry - stop).abs();
        if risk <= 0.0 {
            return None;
        }
        let side = match dir {
            Direction::Bull => Side::Bsl,
            Direction::Bear => Side::Ssl,
        };
        for l in self.levels_beyond(side, entry, 0.0) {
            if min_sig > 0.0 && l.sig < min_sig {
                continue;
            }
            let reward = (l.price - entry).abs();
            let rr = reward / risk;
            if rr < min_rr {
                continue;
            }
            return Some((l.price, l.id, rr));
        }
        None
    }

    fn mark_swept(&mut self, idx: usize) {
        self.levels[idx].status = Status::Swept;
    }

    fn refresh_on_retest(&mut self, high: f64, low: f64, atr: f64, now: i64) {
        let thr = self.cfg.levels.refresh_atr;
        if atr <= 0.0 || thr <= 0.0 {
            return;
        }
        let dist = atr * thr;
        for l in self.levels.iter_mut() {
            if l.status != Status::Active {
                continue;
            }
            match l.side {
                Side::Bsl => {
                    if high < l.price && (l.price - high) <= dist {
                        l.decay = 0.0;
                        l.last_touch = now;
                    }
                }
                Side::Ssl => {
                    if low > l.price && (low - l.price) <= dist {
                        l.decay = 0.0;
                        l.last_touch = now;
                    }
                }
            }
        }
    }

    fn tick_decay(&mut self) {
        let max = self.cfg.levels.decay_candles as f32;
        let mult = self.cfg.levels.sig_decay_mult;
        let min_sig = self.cfg.levels.sig_decay_min_sig;
        for l in self.levels.iter_mut() {
            if l.status != Status::Active {
                continue;
            }
            let mut divisor = self.tf_minutes.get(&l.tf).copied().unwrap_or(1.0);
            if mult > 1.0 && l.sig >= min_sig {
                divisor *= mult;
            }
            l.decay += (1.0 / divisor) as f32;
            if l.decay >= max {
                l.status = Status::Expired;
            }
        }
    }

    fn prune_levels(&mut self) {
        self.levels.retain(|l| l.status == Status::Active);
    }

    // ── Sweeps ──────────────────────────────────────────────────────────────

    fn finalize_sweep(&self, dev: &Developing, idx: usize, atr: f64) -> Sweep {
        let l = &self.levels[idx];
        let magnitude = if atr > 0.0 {
            (dev.extreme_price - l.price).abs() / atr
        } else {
            0.0
        };
        Sweep {
            level_id: l.id,
            level_price: l.price,
            level_source: self.sources.infos[l.source as usize].name.clone(),
            level_sig: l.sig,
            level_side: l.side,
            dir: dev.dir,
            start_ts: dev.start_ts,
            extreme_ts: dev.extreme_ts,
            extreme_price: dev.extreme_price,
            magnitude_atr: magnitude,
            level_formed_at: l.ts,
        }
    }

    fn detect_sweeps(&mut self, c: &Candle, atr: f64) -> Vec<Sweep> {
        let mut sweeps = Vec::new();
        let min_cross = atr * self.cfg.sweep.noise_atr;
        let min_sig = self.cfg.sweep.min_level_sig;
        let max_multi = self.cfg.sweep.max_multi_candle;
        let ts = secs(c);
        for idx in 0..self.levels.len() {
            let (lid, side, lprice, sig, dev_count) = {
                let l = &self.levels[idx];
                if l.status != Status::Active {
                    continue;
                }
                (l.id, l.side, l.price, l.sig, l.developing)
            };
            if sig < min_sig {
                continue;
            }
            let (crossed, extreme) = match side {
                Side::Bsl => (c.high > lprice + min_cross, c.high),
                Side::Ssl => (c.low < lprice - min_cross, c.low),
            };
            let key = (lid, c.timeframe);
            if !crossed {
                if dev_count == 0 {
                    continue;
                }
                if let Some(dev) = self.developing.remove(&key) {
                    self.levels[idx].developing -= 1;
                    let sw = self.finalize_sweep(&dev, idx, atr);
                    self.mark_swept(idx);
                    sweeps.push(sw);
                }
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(e) = self.developing.entry(key) {
                e.insert(Developing {
                    start_ts: ts,
                    extreme_price: extreme,
                    extreme_ts: ts,
                    count: 0,
                    dir: dir_of_side(side),
                });
                self.levels[idx].developing += 1;
            }
            let done = {
                let d = self.developing.get_mut(&key).unwrap();
                d.count += 1;
                let more = match side {
                    Side::Bsl => extreme > d.extreme_price,
                    Side::Ssl => extreme < d.extreme_price,
                };
                if more {
                    d.extreme_price = extreme;
                    d.extreme_ts = ts;
                }
                d.count >= max_multi
            };
            if done {
                let dev = self.developing.remove(&key).unwrap();
                self.levels[idx].developing -= 1;
                let sw = self.finalize_sweep(&dev, idx, atr);
                self.mark_swept(idx);
                sweeps.push(sw);
            }
        }
        sweeps
    }

    // ── Sessions ────────────────────────────────────────────────────────────

    fn session_process(&mut self, c: &Candle) {
        use chrono::{Datelike, Timelike};
        let ts = secs(c);
        let et = chrono::DateTime::from_timestamp(ts + self.cfg.sessions.tz_offset_secs, 0)
            .map(|d| d.naive_utc())
            .unwrap_or(chrono::NaiveDateTime::MIN);
        let date = et.date();
        let week = et.iso_week().week();
        let month = et.month();
        let hour = et.hour() as i64;
        let minute = et.minute() as i64;

        if self.sess.current_date.is_some() && self.sess.current_date != Some(date) {
            self.emit_period(c, true, false, false);
        }
        if self.sess.current_date != Some(date) {
            self.sess.current_date = Some(date);
        }
        if self.sess.current_week.is_some() && self.sess.current_week != Some(week) {
            self.emit_period(c, false, true, false);
        }
        if self.sess.current_week != Some(week) {
            self.sess.current_week = Some(week);
        }
        if self.sess.current_month.is_some() && self.sess.current_month != Some(month) {
            self.emit_period(c, false, false, true);
        }
        if self.sess.current_month != Some(month) {
            self.sess.current_month = Some(month);
        }
        let s = &mut self.sess;
        if s.day_high.is_none_or(|v| c.high > v) {
            s.day_high = Some(c.high);
        }
        if s.day_low.is_none_or(|v| c.low < v) {
            s.day_low = Some(c.low);
        }
        if s.week_high.is_none_or(|v| c.high > v) {
            s.week_high = Some(c.high);
        }
        if s.week_low.is_none_or(|v| c.low < v) {
            s.week_low = Some(c.low);
        }
        if s.month_high.is_none_or(|v| c.high > v) {
            s.month_high = Some(c.high);
        }
        if s.month_low.is_none_or(|v| c.low < v) {
            s.month_low = Some(c.low);
        }

        for k in 0..self.cfg.sessions.open_marks.len() {
            let m = &self.cfg.sessions.open_marks[k];
            if hour == m.hour && minute >= m.minute && self.sess.open_mark_date[k] != Some(date)
            {
                let src = self.open_mark_src[k];
                self.add_level(c.open, Side::Bsl, src, c.timeframe, ts, 1, 0.0);
                self.sess.open_mark_date[k] = Some(date);
            }
        }
        for k in 0..self.cfg.sessions.sessions.len() {
            let (start, end) = {
                let sp = &self.cfg.sessions.sessions[k];
                (sp.start_hour, sp.end_hour)
            };
            let in_session = if start < end {
                hour >= start && hour < end
            } else {
                hour >= start || hour < end
            };
            let was_active = self.sess.session_active[k];
            if in_session {
                self.sess.session_active[k] = true;
                match self.sess.session_high[k] {
                    None => self.sess.session_high[k] = Some(c.high),
                    Some(v) if c.high > v => self.sess.session_high[k] = Some(c.high),
                    _ => {}
                }
                match self.sess.session_low[k] {
                    None => self.sess.session_low[k] = Some(c.low),
                    Some(v) if c.low < v => self.sess.session_low[k] = Some(c.low),
                    _ => {}
                }
            } else if was_active {
                self.sess.session_active[k] = false;
                if let Some(sh) = self.sess.session_high[k].take() {
                    let src = self.src_session_high;
                    self.add_level(sh, Side::Bsl, src, c.timeframe, ts, 1, 0.0);
                }
                if let Some(sl) = self.sess.session_low[k].take() {
                    let src = self.src_session_low;
                    self.add_level(sl, Side::Ssl, src, c.timeframe, ts, 1, 0.0);
                }
            }
        }
    }

    fn emit_period(&mut self, c: &Candle, day: bool, week: bool, month: bool) {
        let ts = secs(c);
        if day {
            if let Some(h) = self.sess.day_high.take() {
                let (s, tf) = (self.src_pdh, self.tf_1d);
                self.add_level(h, Side::Bsl, s, tf, ts, 1, 0.0);
            }
            if let Some(l) = self.sess.day_low.take() {
                let (s, tf) = (self.src_pdl, self.tf_1d);
                self.add_level(l, Side::Ssl, s, tf, ts, 1, 0.0);
            }
        }
        if week {
            if let Some(h) = self.sess.week_high.take() {
                let (s, tf) = (self.src_pwh, self.tf_1w);
                self.add_level(h, Side::Bsl, s, tf, ts, 1, 0.0);
            }
            if let Some(l) = self.sess.week_low.take() {
                let (s, tf) = (self.src_pwl, self.tf_1w);
                self.add_level(l, Side::Ssl, s, tf, ts, 1, 0.0);
            }
        }
        if month {
            if let Some(h) = self.sess.month_high.take() {
                let (s, tf) = (self.src_pmh, self.tf_1mo);
                self.add_level(h, Side::Bsl, s, tf, ts, 1, 0.0);
            }
            if let Some(l) = self.sess.month_low.take() {
                let (s, tf) = (self.src_pml, self.tf_1mo);
                self.add_level(l, Side::Ssl, s, tf, ts, 1, 0.0);
            }
        }
    }

    // ── Primitives ──────────────────────────────────────────────────────────

    fn index_signal(&mut self, s: &Signal) {
        match s.kind {
            SignalKind::SwingHigh => {
                let replace = match self.latest_sh.get(&s.tf) {
                    None => true,
                    Some(l) => s.ts >= l.ts,
                };
                if replace {
                    self.latest_sh.insert(
                        s.tf,
                        LatestSwing {
                            level: s.level,
                            ts: s.ts,
                        },
                    );
                }
                self.swing_seen.insert((0, s.tf, s.ts));
            }
            SignalKind::SwingLow => {
                let replace = match self.latest_sl.get(&s.tf) {
                    None => true,
                    Some(l) => s.ts >= l.ts,
                };
                if replace {
                    self.latest_sl.insert(
                        s.tf,
                        LatestSwing {
                            level: s.level,
                            ts: s.ts,
                        },
                    );
                }
                self.swing_seen.insert((1, s.tf, s.ts));
            }
            SignalKind::Fvg => {
                self.fvg_seen
                    .insert((s.tf, s.gap_ts, s.dir == Direction::Bull));
            }
            SignalKind::Break => {
                self.break_seen
                    .insert((s.tf, s.dir == Direction::Bull, s.level.to_bits()));
            }
        }
    }

    fn add_signal(&mut self, s: Signal) {
        self.index_signal(&s);
        self.signals.push_back(s);
    }

    fn detect_swings(&self, h: &VecDeque<Candle>, tf: u16) -> Vec<Signal> {
        let mut out = Vec::new();
        let lb = self.cfg.primitives.swing_lookback.max(0) as usize;
        let len = h.len();
        if len < 2 * lb + 1 {
            return out;
        }
        let at = |i: usize| &h[len - 1 - i];
        let cts = secs(at(lb));
        let mut max_high = f64::NEG_INFINITY;
        let mut min_low = f64::INFINITY;
        for j in 0..(2 * lb + 1) {
            max_high = max_high.max(at(j).high);
            min_low = min_low.min(at(j).low);
        }
        let ch = at(lb).high;
        if ch == max_high && !self.swing_seen.contains(&(0, tf, cts)) {
            out.push(Signal {
                kind: SignalKind::SwingHigh,
                tf,
                ts: cts,
                level: ch,
                level_end: 0.0,
                dir: Direction::Bear,
                gap_ts: 0,
                c1_stop: 0.0,
            });
        }
        let cl = at(lb).low;
        if cl == min_low && !self.swing_seen.contains(&(1, tf, cts)) {
            out.push(Signal {
                kind: SignalKind::SwingLow,
                tf,
                ts: cts,
                level: cl,
                level_end: 0.0,
                dir: Direction::Bull,
                gap_ts: 0,
                c1_stop: 0.0,
            });
        }
        out
    }

    fn detect_fvgs(&self, h: &VecDeque<Candle>, tf: u16) -> Vec<Signal> {
        let mut out = Vec::new();
        let len = h.len();
        if len < 3 {
            return out;
        }
        let c1 = &h[len - 3];
        let c2 = &h[len - 2];
        let c3 = &h[len - 1];
        let c2_range = c2.high - c2.low;
        let c2ts = secs(c2);
        let c3ts = secs(c3);
        let min_gap_pct = self.cfg.primitives.fvg_min_gap_pct;
        if c3.low > c1.high {
            let gap = c3.low - c1.high;
            let gap_pct = if c2_range > 0.0 { gap / c2_range } else { 0.0 };
            if gap_pct >= min_gap_pct
                && gap >= 0.0
                && !self.fvg_seen.contains(&(tf, c2ts, true))
            {
                out.push(Signal {
                    kind: SignalKind::Fvg,
                    tf,
                    ts: c3ts,
                    level: c1.high,
                    level_end: c3.low,
                    dir: Direction::Bull,
                    gap_ts: c2ts,
                    c1_stop: c1.low,
                });
            }
        }
        if c3.high < c1.low {
            let gap = c1.low - c3.high;
            let gap_pct = if c2_range > 0.0 { gap / c2_range } else { 0.0 };
            if gap_pct >= min_gap_pct
                && gap >= 0.0
                && !self.fvg_seen.contains(&(tf, c2ts, false))
            {
                out.push(Signal {
                    kind: SignalKind::Fvg,
                    tf,
                    ts: c3ts,
                    level: c3.high,
                    level_end: c1.low,
                    dir: Direction::Bear,
                    gap_ts: c2ts,
                    c1_stop: c1.high,
                });
            }
        }
        out
    }

    fn detect_structure(&self, h: &VecDeque<Candle>, tf: u16, atr: f64, c: &Candle) -> Vec<Signal> {
        let mut out = Vec::new();
        if h.len() < 3 {
            return out;
        }
        let min_disp = atr * self.cfg.primitives.structure_min_displacement_atr;
        let ts = secs(c);
        if let Some(sh) = self.latest_sh.get(&tf) {
            if c.close > sh.level + min_disp
                && !self.break_seen.contains(&(tf, true, sh.level.to_bits()))
            {
                out.push(Signal {
                    kind: SignalKind::Break,
                    tf,
                    ts,
                    level: sh.level,
                    level_end: 0.0,
                    dir: Direction::Bull,
                    gap_ts: 0,
                    c1_stop: 0.0,
                });
            }
        }
        if let Some(sl) = self.latest_sl.get(&tf) {
            if c.close < sl.level - min_disp
                && !self.break_seen.contains(&(tf, false, sl.level.to_bits()))
            {
                out.push(Signal {
                    kind: SignalKind::Break,
                    tf,
                    ts,
                    level: sl.level,
                    level_end: 0.0,
                    dir: Direction::Bear,
                    gap_ts: 0,
                    c1_stop: 0.0,
                });
            }
        }
        out
    }

    fn prune_signals(&mut self) {
        let cap = self.cfg.primitives.signals_cap.max(0) as usize;
        if self.signals.len() <= cap {
            return;
        }
        let drain = self.signals.len() - cap;
        self.signals.drain(0..drain);
        self.swing_seen.clear();
        self.fvg_seen.clear();
        self.break_seen.clear();
        self.latest_sh.clear();
        self.latest_sl.clear();
        let sigs: Vec<Signal> = self.signals.iter().cloned().collect();
        for s in &sigs {
            self.index_signal(s);
        }
    }

    // ── Equal highs / lows ──────────────────────────────────────────────────

    fn eq_cluster(prices: &VecDeque<f64>, tolerance: f64) -> Vec<(f64, i64)> {
        let mut clusters = Vec::new();
        if prices.is_empty() || tolerance <= 0.0 {
            return clusters;
        }
        let mut sorted: Vec<f64> = prices.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut cluster_start = 0usize;
        let n = sorted.len();
        for i in 1..n {
            if sorted[i] - sorted[cluster_start] > tolerance {
                let count = i - cluster_start;
                if count >= 2 {
                    let mut s = 0.0;
                    for v in &sorted[cluster_start..i] {
                        s += v;
                    }
                    clusters.push((s / count as f64, count as i64));
                }
                cluster_start = i;
            }
        }
        let count = n - cluster_start;
        if count >= 2 {
            let mut s = 0.0;
            for v in &sorted[cluster_start..n] {
                s += v;
            }
            clusters.push((s / count as f64, count as i64));
        }
        clusters
    }

    fn eq_process(&mut self, c: &Candle, atr: f64) {
        let Some(ecfg) = self.cfg.equal_levels.clone() else {
            return;
        };
        if self.eq.is_none() || c.timeframe != self.base_tf || atr <= 0.0 {
            return;
        }
        let window = ecfg.window.max(0) as usize;
        {
            let e = self.eq.as_mut().unwrap();
            e.highs.push_back(c.high);
            e.lows.push_back(c.low);
            e.last_close = c.close;
            while e.highs.len() > window {
                e.highs.pop_front();
            }
            while e.lows.len() > window {
                e.lows.pop_front();
            }
            e.count += 1;
            if ecfg.every <= 0 || e.count % ecfg.every != 0 {
                return;
            }
        }
        let tolerance = atr * self.cfg.levels.cluster_atr_tolerance;
        let min_dist = atr * ecfg.min_dist_atr;
        let ts = secs(c);
        let (current_price, hs, ls) = {
            let e = self.eq.as_ref().unwrap();
            (
                e.last_close,
                Self::eq_cluster(&e.highs, tolerance),
                Self::eq_cluster(&e.lows, tolerance),
            )
        };
        for (is_high, list) in [(true, hs), (false, ls)] {
            for (price, count) in list {
                if count < ecfg.min_count {
                    continue;
                }
                if current_price > 0.0 && (price - current_price).abs() < min_dist {
                    continue;
                }
                let rk = if tolerance > 0.0 {
                    (price / tolerance).round() as i64
                } else {
                    price as i64
                };
                if self.eq.as_ref().unwrap().emitted.contains(&(is_high, rk)) {
                    continue;
                }
                let (side, src) = if is_high {
                    (Side::Bsl, self.src_eqh)
                } else {
                    (Side::Ssl, self.src_eql)
                };
                self.add_level(price, side, src, c.timeframe, ts, count, atr);
                self.eq.as_mut().unwrap().emitted.insert((is_high, rk));
            }
        }
        let e = self.eq.as_mut().unwrap();
        if e.emitted.len() as i64 > ecfg.emitted_cap {
            e.emitted.clear();
        }
    }

    // ── FVG tracker ─────────────────────────────────────────────────────────

    fn fvg_push(&mut self, f: Fvg) -> usize {
        let idx = self.fvgs.len();
        self.fvgs.push(f);
        idx
    }

    fn fvg_add(&mut self, sig: &Signal) {
        let id = self.fvgs.len() as i64;
        let f = Fvg {
            id,
            dir: sig.dir,
            tf: sig.tf,
            ts: sig.ts,
            near: sig.level_end,
            far: sig.level,
            ce: (sig.level + sig.level_end) / 2.0,
            status: FvgStatus::Unmitigated,
            is_inverted_kind: false,
            c1_stop: Some(sig.c1_stop),
        };
        let idx = self.fvg_push(f);
        self.fvg_active.push(idx);
        self.fvg_by_tf.entry(sig.tf).or_default().push(idx);
        self.fvg_cap_kind(false);
    }

    fn fvg_cap_kind(&mut self, inverted: bool) {
        let cap = self.cfg.fvg.cap;
        let kind_active = self
            .fvg_active
            .iter()
            .filter(|&&i| self.fvgs[i].is_inverted_kind == inverted)
            .count() as i64;
        if kind_active <= cap {
            return;
        }
        let mut n_to_remove = kind_active - cap + self.cfg.fvg.cap_overshoot;
        for k in 0..self.fvg_active.len() {
            if n_to_remove == 0 {
                break;
            }
            let idx = self.fvg_active[k];
            let f = &mut self.fvgs[idx];
            if f.is_inverted_kind == inverted && f.status == FvgStatus::Unmitigated {
                f.status = FvgStatus::Filled;
                n_to_remove -= 1;
            }
        }
        self.fvg_rebuild_active();
    }

    fn fvg_rebuild_active(&mut self) {
        let mut keep = Vec::with_capacity(self.fvg_active.len());
        let mut by_tf: FxHashMap<u16, Vec<usize>> = FxHashMap::default();
        for &i in &self.fvg_active {
            let f = &self.fvgs[i];
            if matches!(f.status, FvgStatus::Unmitigated | FvgStatus::Mitigated) {
                keep.push(i);
                by_tf.entry(f.tf).or_default().push(i);
            }
        }
        self.fvg_active = keep;
        self.fvg_by_tf = by_tf;
    }

    fn fvg_process(&mut self, c: &Candle) {
        let Some(act) = self.fvg_by_tf.get(&c.timeframe).cloned() else {
            return;
        };
        let mut new_inverted = Vec::new();
        let mut any_removed = false;
        let body_low = c.open.min(c.close);
        let body_high = c.open.max(c.close);
        let ts = secs(c);
        for idx in act {
            let (dir, near, far, ce, status) = {
                let f = &self.fvgs[idx];
                (f.dir, f.near, f.far, f.ce, f.status)
            };
            match dir {
                Direction::Bull => {
                    if body_low < far {
                        self.fvgs[idx].status = FvgStatus::Inverted;
                        let id = self.fvgs.len() as i64;
                        let fi = self.fvg_push(Fvg {
                            id,
                            dir: Direction::Bear,
                            tf: c.timeframe,
                            ts,
                            near,
                            far,
                            ce: (near + far) / 2.0,
                            status: FvgStatus::Unmitigated,
                            is_inverted_kind: true,
                            c1_stop: None,
                        });
                        new_inverted.push(fi);
                        any_removed = true;
                        continue;
                    }
                    if status == FvgStatus::Mitigated && c.low <= far {
                        self.fvgs[idx].status = FvgStatus::Filled;
                        any_removed = true;
                        continue;
                    }
                    if status == FvgStatus::Unmitigated && c.low <= ce {
                        self.fvgs[idx].status = FvgStatus::Mitigated;
                    }
                }
                Direction::Bear => {
                    if body_high > far {
                        self.fvgs[idx].status = FvgStatus::Inverted;
                        let id = self.fvgs.len() as i64;
                        let fi = self.fvg_push(Fvg {
                            id,
                            dir: Direction::Bull,
                            tf: c.timeframe,
                            ts,
                            near: far,
                            far: near,
                            ce: (far + near) / 2.0,
                            status: FvgStatus::Unmitigated,
                            is_inverted_kind: true,
                            c1_stop: None,
                        });
                        new_inverted.push(fi);
                        any_removed = true;
                        continue;
                    }
                    if status == FvgStatus::Mitigated && c.high >= far {
                        self.fvgs[idx].status = FvgStatus::Filled;
                        any_removed = true;
                        continue;
                    }
                    if status == FvgStatus::Unmitigated && c.high >= ce {
                        self.fvgs[idx].status = FvgStatus::Mitigated;
                    }
                }
            }
        }
        if any_removed || !new_inverted.is_empty() {
            self.fvg_rebuild_active();
            for i in &new_inverted {
                self.fvg_active.push(*i);
                let tf = self.fvgs[*i].tf;
                self.fvg_by_tf.entry(tf).or_default().push(*i);
            }
            if !new_inverted.is_empty() {
                self.fvg_cap_kind(true);
            }
        }
    }

    pub fn fvg(&self, id: i64) -> Option<&Fvg> {
        self.fvgs.get(id as usize)
    }

    /// Unmitigated gaps on a timeframe, of one direction and kind, formed at
    /// or after `since_ts`, oldest first.
    pub fn fvgs_since(
        &self,
        tf: u16,
        dir: Direction,
        inverted_kind: bool,
        since_ts: i64,
    ) -> Vec<&Fvg> {
        let Some(act) = self.fvg_by_tf.get(&tf) else {
            return Vec::new();
        };
        act.iter()
            .map(|&i| &self.fvgs[i])
            .filter(|f| {
                f.is_inverted_kind == inverted_kind
                    && f.dir == dir
                    && f.ts >= since_ts
                    && f.status == FvgStatus::Unmitigated
            })
            .collect()
    }

    /// The first unmitigated gap on a timeframe matching every filter:
    /// direction and kind, formed at or after `since_ts`, `|near - far| >=
    /// min_gap`, overlapping `[zone_lo, zone_hi]`, and with its entry edge
    /// at least `min_clear` beyond `clear_from` in the trade direction.
    /// Pass infinities to disable a filter.
    #[allow(clippy::too_many_arguments)]
    pub fn fvg_first(
        &self,
        tf: u16,
        dir: Direction,
        inverted_kind: bool,
        since_ts: i64,
        min_gap: f64,
        zone_lo: f64,
        zone_hi: f64,
        clear_from: f64,
        min_clear: f64,
    ) -> Option<&Fvg> {
        let act = self.fvg_by_tf.get(&tf)?;
        act.iter().map(|&i| &self.fvgs[i]).find(|f| {
            if f.is_inverted_kind != inverted_kind
                || f.dir != dir
                || f.ts < since_ts
                || f.status != FvgStatus::Unmitigated
            {
                return false;
            }
            if (f.near - f.far).abs() < min_gap {
                return false;
            }
            let fh = f.near.max(f.far);
            let fl = f.near.min(f.far);
            if !(fl <= zone_hi && fh >= zone_lo) {
                return false;
            }
            let clearance = match dir {
                Direction::Bull => f.near - clear_from,
                Direction::Bear => clear_from - f.near,
            };
            clearance >= min_clear
        })
    }

    // ── Draw map ────────────────────────────────────────────────────────────

    pub fn draw_map(&self) -> Option<&DrawMap> {
        self.draw_map.as_ref()
    }

    /// The draw map's bias: the top target's direction, with confidence
    /// `top / (top + first opposing target)` (1 when nothing opposes).
    pub fn draw_bias(&self) -> Option<(Direction, f64)> {
        let dm = self.draw_map.as_ref()?;
        let top = dm.targets.first()?;
        let opp = dm.targets[1..].iter().find(|t| t.dir != top.dir);
        let confidence = match opp {
            None => 1.0,
            Some(o) => {
                let denom = top.draw_score + o.draw_score;
                if denom > 0.0 {
                    top.draw_score / denom
                } else {
                    0.5
                }
            }
        };
        Some((top.dir, confidence))
    }

    fn build_draw(&mut self, price: f64, atr: f64, ts: i64) {
        let Some(dcfg) = self.cfg.draw.clone() else {
            return;
        };
        if price <= 0.0 || atr <= 0.0 {
            self.draw_map = Some(DrawMap {
                ts,
                price: 0.0,
                atr: 0.0,
                targets: Vec::new(),
            });
            return;
        }
        let dist_cap = atr * dcfg.dist_cap_atr;
        let sweep_cutoff = ts - dcfg.sweep_window_secs;
        let mut boost_bull = false;
        let mut boost_bear = false;
        for s in &self.recent_sweeps {
            if s.start_ts < sweep_cutoff || s.magnitude_atr < dcfg.boost_min_magnitude {
                continue;
            }
            match s.dir {
                Direction::Bull => boost_bull = true,
                Direction::Bear => boost_bear = true,
            }
        }
        let quantum = (atr * dcfg.quantum_atr).max(1e-6);
        let mut seen: HashSet<(u16, i64)> = HashSet::new();
        let mut cands: Vec<DrawTarget> = Vec::new();
        for l in &self.levels {
            if l.status != Status::Active {
                continue;
            }
            let Some(sig) = self.sources.infos[l.source as usize].draw_sig else {
                continue;
            };
            let d = (l.price - price).abs();
            if d > dist_cap || d <= 0.0 {
                continue;
            }
            let key = (l.source, (l.price / quantum).round() as i64);
            if !seen.insert(key) {
                continue;
            }
            let dir = if l.price > price {
                Direction::Bull
            } else {
                Direction::Bear
            };
            cands.push(DrawTarget {
                level_id: l.id,
                price: l.price,
                dir,
                distance_atr: d / atr,
                significance: sig,
                draw_score: 0.0,
            });
        }
        for &fi in &self.fvg_active {
            let f = &self.fvgs[fi];
            if f.status != FvgStatus::Unmitigated {
                continue;
            }
            let Some(&sig) = self.fvg_sig.get(&f.tf) else {
                continue;
            };
            let d = (f.ce - price).abs();
            if d > dist_cap || d <= 0.0 {
                continue;
            }
            let dir = if f.ce > price {
                Direction::Bull
            } else {
                Direction::Bear
            };
            cands.push(DrawTarget {
                level_id: -1,
                price: f.ce,
                dir,
                distance_atr: d / atr,
                significance: sig,
                draw_score: 0.0,
            });
        }
        for c in cands.iter_mut() {
            let proximity = 1.0 / (c.distance_atr + 0.5).sqrt();
            let boosted = match c.dir {
                Direction::Bull => boost_bull,
                Direction::Bear => boost_bear,
            };
            let boost = if boosted { dcfg.boost } else { 1.0 };
            c.draw_score = c.significance * boost * proximity;
        }
        let mut nearest_bull: Option<usize> = None;
        let mut nearest_bear: Option<usize> = None;
        for (i, c) in cands.iter().enumerate() {
            if c.significance < dcfg.top_min_sig {
                continue;
            }
            let slot = match c.dir {
                Direction::Bull => &mut nearest_bull,
                Direction::Bear => &mut nearest_bear,
            };
            match *slot {
                None => *slot = Some(i),
                Some(j) => {
                    if c.distance_atr < cands[j].distance_atr {
                        *slot = Some(i);
                    }
                }
            }
        }
        let top1 = match (nearest_bull, nearest_bear) {
            (Some(b), Some(r)) => {
                let diff = cands[b].distance_atr - cands[r].distance_atr;
                if diff.abs() < dcfg.tie_atr {
                    let bw = cands[b].significance * if boost_bull { dcfg.boost } else { 1.0 };
                    let rw = cands[r].significance * if boost_bear { dcfg.boost } else { 1.0 };
                    Some(if bw >= rw { b } else { r })
                } else if diff < 0.0 {
                    Some(b)
                } else {
                    Some(r)
                }
            }
            (Some(b), None) => Some(b),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        };
        let cmp = |a: &DrawTarget, b: &DrawTarget| -> std::cmp::Ordering {
            use std::cmp::Ordering::*;
            if b.draw_score > a.draw_score {
                Greater
            } else if b.draw_score < a.draw_score {
                Less
            } else if b.significance > a.significance {
                Greater
            } else if b.significance < a.significance {
                Less
            } else {
                Equal
            }
        };
        if let Some(t) = top1 {
            let t1 = cands.remove(t);
            cands.sort_by(cmp);
            cands.insert(0, t1);
        } else {
            cands.sort_by(cmp);
        }
        if let Some((tp, td, _)) = self.draw_top1 {
            let tol = atr * dcfg.hysteresis_tol_atr;
            let inc = cands
                .iter()
                .position(|c| c.dir == td && (c.price - tp).abs() <= tol);
            if let Some(i) = inc {
                if i > 0 {
                    let challenger = cands[0].draw_score;
                    let incumbent = cands[i].draw_score;
                    if challenger < incumbent * dcfg.hysteresis {
                        let it = cands.remove(i);
                        cands.insert(0, it);
                    }
                }
            }
        }
        let keep = dcfg.keep.max(0) as usize;
        cands.truncate(keep);
        let n1 = dcfg.one_sided_n.max(1) as usize;
        if cands.len() > n1 {
            let d0 = cands[0].dir;
            let one_sided = cands[1..n1].iter().all(|c| c.dir == d0);
            if one_sided {
                let splice = (n1..cands.len()).find(|&i| cands[i].dir != d0);
                if let Some(s) = splice {
                    let opp = cands.remove(s);
                    let replaced = cands.remove(n1 - 1);
                    cands.insert(n1 - 1, opp);
                    cands.push(replaced);
                    cands.truncate(keep);
                }
            }
        }
        if let Some(c0) = cands.first() {
            self.draw_top1 = Some((c0.price, c0.dir, c0.draw_score));
        }
        self.draw_map = Some(DrawMap {
            ts,
            price,
            atr,
            targets: cands,
        });
    }

    // ── Day tracker ─────────────────────────────────────────────────────────

    /// The forming UTC day's running aggregate.
    pub fn day(&self) -> Option<&Day> {
        self.day_forming.as_ref()
    }

    fn day_process(&mut self, c: &Candle) -> Option<Day> {
        let day_ms = (secs(c) * 1000 / 86_400_000) * 86_400_000;
        match self.day_forming.as_mut() {
            Some(d) if d.day_ms == day_ms => {
                if c.high > d.high {
                    d.high = c.high;
                }
                if c.low < d.low {
                    d.low = c.low;
                }
                d.close = c.close;
                None
            }
            Some(d) => {
                let prev = *d;
                self.day_forming = Some(Day {
                    day_ms,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                });
                Some(prev)
            }
            None => {
                self.day_forming = Some(Day {
                    day_ms,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                });
                None
            }
        }
    }

    // ── Per-candle driver ───────────────────────────────────────────────────

    /// Step every service for one completed candle, up to the strategy's
    /// first hook. Order: history + ATR; day tracker (base bars); sessions;
    /// swing / gap / break detection; swings → levels; equal levels; gap
    /// lifecycle; sweeps. The strategy then sees the events; after its hook
    /// the caller runs [`Scanner::rebuild_draw`] and [`Scanner::after_hooks`].
    pub fn process(&mut self, c: &Candle) -> BarEvents {
        let tf = c.timeframe;
        let cap = self.ring_cap;
        let ring = self.rings.entry(tf).or_insert_with(|| VecDeque::with_capacity(cap + 1));
        ring.push_back(c.clone());
        while ring.len() > cap {
            ring.pop_front();
        }
        let atr = if ring.len() > 2 {
            let a = atr_of(ring, self.cfg.primitives.atr_period);
            self.atr_cache.insert(tf, a);
            a
        } else {
            self.atr_cache.get(&tf).copied().unwrap_or(0.0)
        };
        self.candle_count += 1;
        if atr > 0.0 {
            self.last_atr = atr;
        }
        let is_base = tf == self.base_tf;

        let day_closed = if self.cfg.day_tracker && is_base {
            self.day_process(c)
        } else {
            None
        };

        self.session_process(c);

        let ring = self.rings.get(&tf).unwrap();
        let swings = self.detect_swings(ring, tf);
        let fvgs = self.detect_fvgs(ring, tf);
        let breaks = self.detect_structure(ring, tf, atr, c);
        for s in &swings {
            self.add_signal(s.clone());
        }
        for s in &fvgs {
            self.add_signal(s.clone());
        }
        for s in &breaks {
            self.add_signal(s.clone());
        }
        let ts = secs(c);
        for s in &swings {
            let (side, src) = match s.kind {
                SignalKind::SwingHigh => (Side::Bsl, self.src_swing_high),
                _ => (Side::Ssl, self.src_swing_low),
            };
            self.add_level(s.level, side, src, s.tf, s.ts, 1, atr);
        }
        self.eq_process(c, atr);
        for s in &fvgs {
            self.fvg_add(s);
        }
        self.fvg_process(c);
        let sweeps = self.detect_sweeps(c, atr);
        let _ = ts;

        let draw_due = self
            .cfg
            .draw
            .as_ref()
            .map(|d| {
                d.enabled
                    && is_base
                    && atr > 0.0
                    && ((d.every > 0 && self.candle_count % d.every == 0) || !sweeps.is_empty())
            })
            .unwrap_or(false);

        BarEvents {
            atr,
            sweeps,
            breaks: breaks
                .iter()
                .map(|s| Break {
                    tf: s.tf,
                    ts: s.ts,
                    level: s.level,
                    dir: s.dir,
                })
                .collect(),
            day_closed,
            draw_due,
        }
    }

    /// Record this bar's sweeps for the recency boost and rebuild the draw
    /// map when due. Call between the strategy's detection hook and its
    /// scoring hook, once per bar.
    pub fn rebuild_draw(&mut self, c: &Candle, ev: &BarEvents) {
        let Some(window) = self.cfg.draw.as_ref().filter(|d| d.enabled).map(|d| d.sweep_window_secs) else {
            return;
        };
        self.recent_sweeps.extend(ev.sweeps.iter().cloned());
        if !ev.draw_due {
            return;
        }
        let ts = secs(c);
        let cutoff = ts - window;
        self.recent_sweeps.retain(|s| s.start_ts >= cutoff);
        self.build_draw(c.close, ev.atr, ts);
    }

    /// The registry maintenance that follows the strategy's hooks: retest
    /// refresh, decay, pruning.
    pub fn after_hooks(&mut self, c: &Candle, atr: f64) {
        let ts = secs(c);
        if self.cfg.levels.refresh_atr > 0.0 && atr > 0.0 {
            self.refresh_on_retest(c.high, c.low, atr, ts);
        }
        let de = self.cfg.levels.decay_every;
        if de > 0 && self.candle_count % de == 0 {
            self.tick_decay();
        }
        let pe = self.cfg.levels.prune_every;
        if pe > 0 && self.candle_count % pe == 0 {
            self.prune_levels();
            self.fvg_rebuild_active();
            self.prune_signals();
        }
    }

    /// Discard every derived detector state and re-derive it by replaying
    /// `tape` — the trailing window of raw candles, in ARRIVAL order — into a
    /// fresh scanner on the same config.
    ///
    /// Age horizons bound how OLD a surviving object may be; they do not make
    /// the surviving SET a function of recent data alone. A level born 80 days
    /// ago that accumulated six touches and absorbed two cluster neighbours
    /// sits inside a 90-day age bound and is still unreconstructible by a run
    /// that only saw the last 30 days. That is why two warmup depths take
    /// different trades. Re-deriving from raw candles closes the gap: after a
    /// rebuild the state is a pure function of (trailing K bars, config), so
    /// any two runs whose tapes agree hold identical state.
    ///
    /// Doing it through a real `Scanner` rather than a bespoke re-derivation is
    /// deliberate — it is correct by construction and cannot drift from the
    /// live pipeline, because it IS the live pipeline.
    ///
    /// Arrival order matters and a timestamp sort would be wrong: an HTF
    /// composite carries its PERIOD-START stamp, so sorting would feed a 4h bar
    /// before the 1m bars it was built from and the replay would not reproduce
    /// the live interleave.
    ///
    /// `wake` / `wake_book` are strategy-set arming bits, not bar-derived
    /// state, so they are carried across rather than re-derived — a fresh
    /// scanner starts with them empty, which would silently mute every
    /// `on_bar(wake)` hook the script had registered.
    pub fn rebuild_from(&mut self, tape: &[Candle]) {
        let mut fresh = Scanner::new(self.cfg.clone());
        for c in tape {
            let ev = fresh.process(c);
            fresh.rebuild_draw(c, &ev);
            fresh.after_hooks(c, ev.atr);
        }
        fresh.wake = std::mem::take(&mut self.wake);
        fresh.wake_book = self.wake_book;
        *self = fresh;
    }

    pub fn candle_count(&self) -> i64 {
        self.candle_count
    }

    /// The most recent positive ATR seen on any timeframe.
    pub fn last_atr(&self) -> f64 {
        self.last_atr
    }

    /// Sizes of the live structures: (levels kept, active levels, gaps kept,
    /// active gaps, signals kept). The per-bar cost scales with the active
    /// counts, so a registry that never expires shows up here.
    pub fn sizes(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.levels.len(),
            self.levels.iter().filter(|l| l.status == Status::Active).count(),
            self.fvgs.len(),
            self.fvg_active.len(),
            self.signals.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn cfg() -> ScannerCfg {
        let mut base = HashMap::new();
        base.insert("pdh".to_string(), 4.0);
        base.insert("pdl".to_string(), 4.0);
        base.insert("swing_high".to_string(), 1.0);
        base.insert("swing_low".to_string(), 1.0);
        base.insert("session_high".to_string(), 3.0);
        let mut tf_mult = HashMap::new();
        tf_mult.insert("1m".to_string(), 1.0);
        tf_mult.insert("1h".to_string(), 2.0);
        let mut draw_sources = HashMap::new();
        draw_sources.insert("pdh".to_string(), 6.0);
        draw_sources.insert("pdl".to_string(), 6.0);
        ScannerCfg {
            base_tf: "1m".into(),
            primitives: PrimitivesCfg {
                atr_period: 3,
                swing_lookback: 1,
                fvg_min_gap_pct: 0.0,
                structure_min_displacement_atr: 0.0,
                signals_cap: 2000,
                swing_high_source: "swing_high".into(),
                swing_low_source: "swing_low".into(),
            },
            significance: SignificanceCfg {
                base,
                default_score: 1.0,
                tf_scaled: vec!["swing_high".into(), "swing_low".into()],
                tf_mult,
                touch_bonus: vec![(2, 1.0), (3, 2.0)],
            },
            levels: LevelsCfg {
                cluster_atr_tolerance: 0.5,
                decay_candles: 2.0,
                sig_decay_mult: 1.0,
                sig_decay_min_sig: 0.0,
                refresh_atr: 0.0,
                decay_every: 1,
                prune_every: 1000,
                tf_minutes: HashMap::new(),
            },
            sweep: SweepCfg {
                noise_atr: 0.0,
                max_multi_candle: 3,
                min_level_sig: 0.0,
            },
            sessions: SessionsCfg::default(),
            equal_levels: None,
            fvg: FvgCfg {
                cap: 500,
                cap_overshoot: 50,
            },
            draw: Some(DrawCfg {
                enabled: true,
                every: 1,
                dist_cap_atr: 20.0,
                sweep_window_secs: 3600,
                boost_min_magnitude: 1.0,
                boost: 1.5,
                quantum_atr: 0.1,
                sources: draw_sources,
                fvg_sig: HashMap::new(),
                top_min_sig: 3.0,
                tie_atr: 0.3,
                hysteresis: 1.10,
                hysteresis_tol_atr: 0.1,
                keep: 10,
                one_sided_n: 5,
            }),
            day_tracker: true,
        }
    }

    fn candle(ts: i64, o: f64, h: f64, l: f64, c: f64) -> Candle {
        Candle {
            asset: 0,
            timeframe: tf_id("1m"),
            open: o,
            high: h,
            low: l,
            close: c,
            volume: 0.0,
            timestamp: DateTime::from_timestamp(ts, 0).unwrap().naive_utc(),
            complete: true,
        }
    }

    #[test]
    fn significance_scales_swings_by_timeframe_and_touch() {
        let sc = Scanner::new(cfg());
        let sh = sc.sources.by_name["swing_high"];
        let pdh = sc.sources.by_name["pdh"];
        assert_eq!(sc.significance(sh, tf_id("1m"), 1), 1.0);
        assert_eq!(sc.significance(sh, tf_id("1h"), 1), 2.0);
        assert_eq!(sc.significance(sh, tf_id("1h"), 2), 3.0);
        assert_eq!(sc.significance(sh, tf_id("1h"), 5), 4.0);
        assert_eq!(sc.significance(pdh, tf_id("1h"), 1), 4.0);
    }

    #[test]
    fn levels_merge_within_the_cluster_tolerance_only_with_an_atr() {
        let mut sc = Scanner::new(cfg());
        let tf = tf_id("1m");
        let a = sc.add_level_named(100.0, Side::Bsl, "pdh", tf, 0, 1, 1.0);
        let b = sc.add_level_named(100.4, Side::Bsl, "pdh", tf, 10, 1, 1.0);
        assert_eq!(a, b, "within 0.5 * atr merges");
        assert_eq!(sc.level(a).unwrap().touch, 2);
        assert_eq!(sc.level(a).unwrap().sig, 5.0, "touch bonus applied on merge");
        let c = sc.add_level_named(100.4, Side::Ssl, "pdl", tf, 10, 1, 1.0);
        assert_ne!(a, c, "other side never merges");
        let d = sc.add_level_named(100.1, Side::Bsl, "pdh", tf, 10, 1, 0.0);
        assert_ne!(a, d, "no atr, no merge");
    }

    #[test]
    fn a_crossed_level_becomes_a_sweep_when_price_comes_back() {
        let mut sc = Scanner::new(cfg());
        let tf = tf_id("1m");
        sc.add_level_named(100.0, Side::Bsl, "pdh", tf, 0, 1, 0.0);
        let ev = sc.process(&candle(60, 99.0, 101.0, 98.0, 100.5));
        assert!(ev.sweeps.is_empty(), "a developing sweep is not final");
        let ev = sc.process(&candle(120, 99.8, 99.8, 99.0, 99.5));
        assert_eq!(ev.sweeps.len(), 1);
        let s = &ev.sweeps[0];
        assert_eq!(s.dir, Direction::Bear);
        assert_eq!(s.extreme_price, 101.0);
        assert_eq!(s.start_ts, 60);
        assert_eq!(s.level_source, "pdh");
        assert!(sc.levels_beyond(Side::Bsl, 0.0, 0.0).is_empty(), "swept levels leave the registry");
    }

    #[test]
    fn a_persistent_crossing_finalizes_at_max_multi_candle() {
        let mut sc = Scanner::new(cfg());
        let tf = tf_id("1m");
        sc.add_level_named(100.0, Side::Ssl, "pdl", tf, 0, 1, 0.0);
        let mut n = 0;
        for i in 0..3 {
            let ev = sc.process(&candle(60 * (i + 1), 100.0, 100.0, 99.0 - i as f64, 99.5));
            n += ev.sweeps.len();
            if i < 2 {
                assert_eq!(n, 0);
            }
        }
        assert_eq!(n, 1);
    }

    #[test]
    fn swings_and_gaps_feed_the_registry_and_the_tracker() {
        let mut sc = Scanner::new(cfg());
        // A bullish gap: c1 high 101 < c3 low 103, centre bar is a swing low.
        sc.process(&candle(60, 100.0, 101.0, 99.0, 100.5));
        sc.process(&candle(120, 100.5, 101.5, 98.0, 101.0));
        sc.process(&candle(180, 103.0, 104.0, 103.0, 103.5));
        let f = sc.fvgs_since(tf_id("1m"), Direction::Bull, false, 0);
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].near, 103.0);
        assert_eq!(f[0].far, 101.0);
        assert_eq!(f[0].c1_stop, Some(99.0));
        // The swing low at 98 is now a sell-side level.
        let below = sc.levels_beyond(Side::Ssl, 100.0, 0.0);
        assert_eq!(below.len(), 1);
        assert_eq!(below[0].price, 98.0);
        // A body trading through the far edge inverts the gap.
        sc.process(&candle(240, 103.0, 103.0, 100.0, 100.5));
        assert_eq!(sc.fvg(0).unwrap().status, FvgStatus::Inverted);
        let inv = sc.fvgs_since(tf_id("1m"), Direction::Bear, true, 0);
        assert_eq!(inv.len(), 1);
        assert_eq!(inv[0].near, 103.0);
    }

    #[test]
    fn first_gap_query_honours_every_filter() {
        let mut sc = Scanner::new(cfg());
        sc.process(&candle(60, 100.0, 101.0, 99.0, 100.5));
        sc.process(&candle(120, 100.5, 101.5, 98.0, 101.0));
        sc.process(&candle(180, 103.0, 104.0, 103.0, 103.5));
        let tf = tf_id("1m");
        let inf = f64::INFINITY;
        assert!(sc.fvg_first(tf, Direction::Bull, false, 0, 0.0, -inf, inf, 0.0, -inf).is_some());
        assert!(sc.fvg_first(tf, Direction::Bull, false, 181, 0.0, -inf, inf, 0.0, -inf).is_none(), "since");
        assert!(sc.fvg_first(tf, Direction::Bull, false, 0, 5.0, -inf, inf, 0.0, -inf).is_none(), "min gap");
        assert!(sc.fvg_first(tf, Direction::Bull, false, 0, 0.0, 110.0, 120.0, 0.0, -inf).is_none(), "zone");
        assert!(sc.fvg_first(tf, Direction::Bull, false, 0, 0.0, -inf, inf, 98.0, 6.0).is_none(), "clearance");
        assert!(sc.fvg_first(tf, Direction::Bull, false, 0, 0.0, -inf, inf, 98.0, 5.0).is_some());
    }

    #[test]
    fn decay_expires_levels_in_single_precision() {
        let mut sc = Scanner::new(cfg());
        sc.add_level_named(100.0, Side::Bsl, "pdh", tf_id("1m"), 0, 1, 0.0);
        sc.after_hooks(&candle(60, 1.0, 1.0, 1.0, 1.0), 1.0);
        assert_eq!(sc.levels[0].status, Status::Active, "one tick of 1/1 per candle: 1 < 2");
        assert_eq!(sc.levels[0].decay, 1.0f32);
        sc.candle_count = 1; // keep the prune cadence out of it
        sc.after_hooks(&candle(120, 1.0, 1.0, 1.0, 1.0), 1.0);
        assert_eq!(sc.levels[0].status, Status::Expired);
        sc.candle_count = 1000;
        sc.after_hooks(&candle(180, 1.0, 1.0, 1.0, 1.0), 1.0);
        assert!(sc.levels.is_empty(), "pruned on the cadence");
    }

    /// `cfg()` decays a level in two bars, which is right for the decay tests
    /// and useless for an invariance test: nothing survives long enough to
    /// carry a warmup difference. This is the same config with the deployed
    /// preset's horizons (500-candle decay, maintenance on the real cadence).
    fn cfg_long() -> ScannerCfg {
        let mut c = cfg();
        c.levels.decay_candles = 500.0;
        c.levels.decay_every = 10;
        c.levels.prune_every = 50;
        c.levels.cluster_atr_tolerance = 0.3;
        c.primitives.atr_period = 14;
        c.primitives.swing_lookback = 5;
        if let Some(d) = c.draw.as_mut() {
            d.every = 15;
        }
        c
    }

    /// A synthetic tape with structure: a drifting sine with a widening range,
    /// so swings, gaps, levels and sweeps all actually fire.
    fn tape(from_min: i64, to_min: i64) -> Vec<Candle> {
        let mut out = Vec::new();
        for i in from_min..to_min {
            let t = i as f64;
            // Irrational-ratio periods, so the series never repeats: a
            // periodic fixture would leave the deep prefix indistinguishable
            // from the shallow one and the control test would have no teeth.
            let mid = 100.0
                + (t / 37.0).sin() * 5.0
                + (t / 311.7).sin() * 12.0
                + (t / 1013.3).sin() * 25.0;
            let w = 0.15 + ((t / 53.0).cos() * 0.1).abs();
            let o = mid + (t / 7.0).sin() * w;
            let c = mid + (t / 11.0).cos() * w;
            out.push(candle(i * 60, o, o.max(c) + w, o.min(c) - w, c));
        }
        out
    }

    /// Everything a rebuild is meant to make reproducible, as a comparable
    /// string: the level registry, the gap store and the draw map.
    fn fingerprint(sc: &Scanner) -> String {
        let mut s = String::new();
        let mut levels: Vec<&Level> = sc.levels.iter().filter(|l| l.status == Status::Active).collect();
        levels.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap().then(a.ts.cmp(&b.ts)));
        for l in levels {
            s.push_str(&format!(
                "L {:.6} {} {} {} {} {:.6}\n",
                l.price,
                l.side.as_str(),
                sc.source_name(l.source),
                l.tf,
                l.touch,
                l.sig
            ));
        }
        // The whole gap store, not just the active index: a script asks
        // `fvgs_since` / `fvg_status` about gaps by id, so a store that holds
        // 420 gaps in one run and 186 in the other is two different books.
        for f in &sc.fvgs {
            s.push_str(&format!(
                "F {} {:?} {} {:.6} {:.6} {:?}\n",
                f.ts, f.dir, f.tf, f.near, f.far, f.status
            ));
        }
        for g in &sc.signals {
            s.push_str(&format!("S {:?} {} {} {:.6}\n", g.kind, g.tf, g.ts, g.level));
        }
        if let Some(d) = &sc.draw_map {
            for t in &d.targets {
                s.push_str(&format!("D {:.6} {:?} {:.6}\n", t.price, t.dir, t.draw_score));
            }
        }
        s
    }

    /// The property the rebuild exists for: two scanners that start at
    /// different times, and so warm up to different depths, hold IDENTICAL
    /// derived state once both have rebuilt past the same horizon.
    ///
    /// Age horizons alone do not give this. A level born outside the shallow
    /// run's window that accumulated touches and absorbed cluster neighbours
    /// survives its age bound in the deep run and is unreconstructible by the
    /// shallow one, so the two disagree about which levels exist at all — and
    /// a level that exists in one book and not the other is a trade taken in
    /// one book and not the other.
    #[test]
    fn rebuild_makes_state_warmup_invariant() {
        let window = &tape(0, 60 * 24 * 40);
        let deep = &tape(-60 * 24 * 40, 0);

        let mut shallow = Scanner::new(cfg_long());
        shallow.rebuild_from(window);

        let mut warm = Scanner::new(cfg_long());
        for c in deep {
            let ev = warm.process(c);
            warm.rebuild_draw(c, &ev);
            warm.after_hooks(c, ev.atr);
        }
        // Twice the history at the moment of the rebuild, same trailing window.
        warm.rebuild_from(window);

        assert_eq!(
            fingerprint(&warm),
            fingerprint(&shallow),
            "a rebuild from the same trailing window must erase the warmup difference"
        );
        assert!(
            !fingerprint(&shallow).is_empty(),
            "the fixture must actually build state, or the comparison is vacuous"
        );
    }

    /// The control: without the rebuild the same two runs DISAGREE, so the
    /// test above is not passing on an empty or trivially equal state.
    #[test]
    fn without_rebuild_state_is_not_warmup_invariant() {
        let window = &tape(0, 60 * 24 * 40);
        let deep = &tape(-60 * 24 * 40, 0);

        let step = |sc: &mut Scanner, cs: &[Candle]| {
            for c in cs {
                let ev = sc.process(c);
                sc.rebuild_draw(c, &ev);
                sc.after_hooks(c, ev.atr);
            }
        };
        let mut shallow = Scanner::new(cfg_long());
        step(&mut shallow, window);
        let mut warm = Scanner::new(cfg_long());
        step(&mut warm, deep);
        step(&mut warm, window);

        assert_ne!(
            fingerprint(&warm),
            fingerprint(&shallow),
            "control: warmup depth must still move the state without a rebuild"
        );
    }

    /// The rebuild is a swap, and `wake` / `wake_book` are strategy-set arming
    /// bits rather than bar-derived state. A fresh scanner starts with them
    /// clear, which would silently park every timeframe the script had armed.
    #[test]
    fn rebuild_carries_the_wake_bits_across() {
        let mut sc = Scanner::new(cfg_long());
        sc.wake.insert(tf_id("1m"));
        sc.wake.insert(tf_id("1h"));
        sc.wake_book = true;
        sc.rebuild_from(&tape(0, 500));
        assert!(sc.wake.contains(&tf_id("1m")));
        assert!(sc.wake.contains(&tf_id("1h")));
        assert!(sc.wake_book);
    }

    #[test]
    fn find_target_wants_the_nearest_level_past_the_minimum_reward() {
        let mut sc = Scanner::new(cfg());
        let tf = tf_id("1m");
        sc.add_level_named(101.0, Side::Bsl, "pdh", tf, 0, 1, 0.0);
        sc.add_level_named(105.0, Side::Bsl, "pdh", tf, 0, 1, 0.0);
        let (price, _, rr) = sc
            .find_target(Direction::Bull, 100.0, 99.0, 2.0, 0.0)
            .unwrap();
        assert_eq!(price, 105.0);
        assert_eq!(rr, 5.0);
        assert!(sc.find_target(Direction::Bull, 100.0, 99.0, 6.0, 0.0).is_none());
        assert_eq!(sc.nearest_beyond(Side::Bsl, 100.0).unwrap().price, 101.0);
        assert!(sc.nearest_beyond(Side::Ssl, 100.0).is_none());
    }

    #[test]
    fn the_draw_map_ranks_by_significance_over_distance() {
        let mut sc = Scanner::new(cfg());
        let tf = tf_id("1m");
        sc.add_level_named(110.0, Side::Bsl, "pdh", tf, 0, 1, 0.0);
        sc.add_level_named(97.0, Side::Ssl, "pdl", tf, 0, 1, 0.0);
        sc.add_level_named(130.0, Side::Bsl, "swing_high", tf, 0, 1, 0.0);
        sc.build_draw(100.0, 1.0, 600);
        let dm = sc.draw_map().unwrap();
        assert_eq!(dm.targets.len(), 2, "swing sources are not draw candidates");
        assert_eq!(dm.targets[0].price, 97.0, "nearer of two equal-significance levels leads");
        assert_eq!(dm.targets[0].dir, Direction::Bear);
        let (dir, conf) = sc.draw_bias().unwrap();
        assert_eq!(dir, Direction::Bear);
        assert!(conf > 0.5 && conf < 1.0);
    }

    #[test]
    fn the_day_tracker_reports_each_completed_utc_day() {
        let mut sc = Scanner::new(cfg());
        assert!(sc.process(&candle(86_400 * 10 + 60, 1.0, 2.0, 0.5, 1.5)).day_closed.is_none());
        assert!(sc.process(&candle(86_400 * 10 + 120, 1.5, 3.0, 1.0, 2.0)).day_closed.is_none());
        let ev = sc.process(&candle(86_400 * 11 + 60, 2.0, 2.5, 1.5, 2.2));
        let d = ev.day_closed.expect("a new day closes the previous one");
        assert_eq!(d.day_ms, 86_400_000 * 10);
        assert_eq!((d.open, d.high, d.low, d.close), (1.0, 3.0, 0.5, 2.0));
        assert_eq!(sc.day().unwrap().high, 2.5);
    }

    #[test]
    fn config_accepts_integers_where_floats_are_expected() {
        let script = r#"#{
            primitives: #{ atr_period: 14, swing_lookback: 5, fvg_min_gap_pct: 0, structure_min_displacement_atr: 1 },
            significance: #{ base: #{ pdh: 4 }, tf_mult: #{ "1h": 2 }, touch_bonus: [[2, 1], [3, 2]] },
            levels: #{ cluster_atr_tolerance: 0.5, decay_candles: 300 },
            sweep: #{ noise_atr: 1, max_multi_candle: 3 },
            fvg: #{ cap: 500 },
        }"#;
        let engine = rhai::Engine::new();
        let m: rhai::Map = engine.eval(script).unwrap();
        let parsed: ScannerCfg = rhai::serde::from_dynamic(&rhai::Dynamic::from_map(m)).unwrap();
        assert_eq!(parsed.primitives.atr_period, 14);
        assert_eq!(parsed.primitives.fvg_min_gap_pct, 0.0);
        assert_eq!(parsed.significance.base["pdh"], 4.0);
        assert_eq!(parsed.significance.tf_mult["1h"], 2.0);
        assert_eq!(parsed.significance.touch_bonus, vec![(2, 1.0), (3, 2.0)]);
        assert_eq!(parsed.levels.decay_candles, 300.0);
        assert_eq!(parsed.levels.decay_every, 10, "defaults fill in");
        assert!(parsed.draw.is_none());
    }
}
