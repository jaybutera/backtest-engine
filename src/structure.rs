//! Market-structure tiers: short-, intermediate- and long-term highs and lows
//! (STH/STL, ITH/ITL, LTH/LTL), per timeframe, labelled bar by bar with no
//! lookahead.
//!
//! Definitions (MxM-style advanced market structure):
//!
//! * **ST** (tier 0): a swing — a high with a lower high on each side
//!   (`st_lookback` bars, 1 = the 3-bar swing). Confirmed when the right-hand
//!   bars have closed.
//! * **IT** (tier 1): an ST with a lower ST on each side, counting only STs of
//!   the same side. Confirmed when the right-hand ST is. A **rebalanced** IT is
//!   an ST whose bar traded into an open opposing fair value gap (on its own or
//!   a higher timeframe); it is an IT as soon as it is an ST.
//! * **LT** (tier 2), `lt_mode = "htf_reaction"` (the default): an ST whose bar
//!   reacted off a higher-timeframe level (touched an open HTF gap, or a level
//!   whose timeframe is above the bar's, or a period/session mark) and is
//!   followed by a lower ST. `lt_mode = "fractal"`: an IT with a lower IT on
//!   each side.
//! * **Tier k+1, k >= 2**: a tier-k point with a lower tier-k point on each
//!   side, up to `max_tier`. In `htf_reaction` mode the fractal ladder resumes
//!   above LT; the IT -> LT step is the HTF rule instead.
//!
//! "Lower"/"higher" is strict beyond `eq_tol_atr * atr`, so equal highs are
//! not a sweep. Lows mirror everything.
//!
//! A point is **taken** when a bar of its timeframe closes beyond it
//! (`take_by = "close"`) or trades beyond it (`"wick"`). Taking an IT-or-higher
//! is reported as a tier-tagged event, which is how a strategy asks for "the
//! ITH was taken" rather than "some swing broke".
//!
//! Memory is bounded: per timeframe and side, the last two points of each
//! tier, the last `keep` labelled points, and at most `max_open` untaken IT+
//! points. Nothing here feeds the level registry or any decision; strategies
//! read it through the scanner.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;
use serde::Deserialize;

fn d_true() -> bool {
    true
}
fn d_one() -> i64 {
    1
}
fn d_mode() -> String {
    "htf_reaction".into()
}
fn d_take() -> String {
    "close".into()
}
fn d_max_tier() -> i64 {
    8
}
fn d_max_open() -> i64 {
    64
}
fn d_keep() -> i64 {
    400
}
fn d_min_gap() -> f64 {
    0.1
}
fn d_htf_map() -> std::collections::HashMap<String, String> {
    [("1m", "15m"), ("5m", "1h"), ("15m", "4h"), ("1h", "1D"), ("4h", "1W"), ("1D", "1M")]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}
fn d_htf_sources() -> Vec<String> {
    ["pdh", "pdl", "pwh", "pwl", "pmh", "pml", "session_high", "session_low"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

#[derive(Clone, Debug, Deserialize)]
pub struct StructureCfg {
    #[serde(default = "d_true")]
    pub enabled: bool,
    /// Bars on each side an ST must beat (1 = the 3-bar swing).
    #[serde(default = "d_one")]
    pub st_lookback: i64,
    /// "htf_reaction" or "fractal".
    #[serde(default = "d_mode")]
    pub lt_mode: String,
    /// Two extremes within `eq_tol_atr * atr` are equal (neither is beyond).
    #[serde(default)]
    pub eq_tol_atr: f64,
    /// "close" or "wick".
    #[serde(default = "d_take")]
    pub take_by: String,
    /// Highest tier labelled (0 = ST, 1 = IT, 2 = LT, ...).
    #[serde(default = "d_max_tier")]
    pub max_tier: i64,
    /// Untaken IT+ points watched per timeframe and side.
    #[serde(default = "d_max_open")]
    pub max_open: i64,
    /// Labelled points kept per timeframe and side (queries, snapshots).
    #[serde(default = "d_keep")]
    pub keep: i64,
    /// Rebalanced ITs: an ST that traded into an opposing gap.
    #[serde(default = "d_true")]
    pub rebalance: bool,
    /// Level sources that count as higher-timeframe levels whatever their
    /// timeframe stamp.
    #[serde(default = "d_htf_sources")]
    pub htf_sources: Vec<String>,
    /// The higher timeframe each timeframe reacts off (ICT pairs: 1m -> 15m,
    /// 5m -> 1h, ...). Gaps and levels on that timeframe or above count.
    #[serde(default = "d_htf_map")]
    pub htf_map: std::collections::HashMap<String, String>,
    /// Gaps narrower than this many ATRs of the bar's timeframe are ignored.
    #[serde(default = "d_min_gap")]
    pub min_gap_atr: f64,
}

impl Default for StructureCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            st_lookback: 1,
            lt_mode: d_mode(),
            eq_tol_atr: 0.0,
            take_by: d_take(),
            max_tier: d_max_tier(),
            max_open: d_max_open(),
            keep: d_keep(),
            rebalance: true,
            htf_sources: d_htf_sources(),
            htf_map: d_htf_map(),
            min_gap_atr: d_min_gap(),
        }
    }
}

impl StructureCfg {
    fn fractal(&self) -> bool {
        self.lt_mode == "fractal"
    }
}

/// What a bar's extreme traded into, as the scanner saw it before that bar
/// updated the gap and level books.
#[derive(Clone, Debug, Default)]
pub struct BarZones {
    /// The high touched a higher-timeframe level/gap: its description.
    pub htf_high: Option<String>,
    pub htf_low: Option<String>,
    /// The high traded into an open bearish gap (same or higher timeframe).
    pub rebal_high: Option<String>,
    pub rebal_low: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointKind {
    /// Reached its tier through the swing ladder.
    Swing,
    /// IT by rebalancing a gap.
    Rebalanced,
    /// LT by reacting off a higher-timeframe level.
    HtfReaction,
}

impl PointKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PointKind::Swing => "swing",
            PointKind::Rebalanced => "rebalanced",
            PointKind::HtfReaction => "htf",
        }
    }
}

#[derive(Clone, Debug)]
pub struct MsPoint {
    pub tf: u16,
    pub high: bool,
    pub price: f64,
    /// Open time of the bar that made the extreme, unix seconds.
    pub ts: i64,
    /// Open time of the bar on which it became an ST.
    pub st_confirmed_ts: i64,
    /// Highest tier reached.
    pub tier: u8,
    /// How it reached that tier.
    pub kind: PointKind,
    /// Open time of the bar on which it reached `tier`.
    pub tier_ts: i64,
    /// The HTF level/gap its bar touched, if any.
    pub htf: Option<String>,
    /// The gap its bar rebalanced, if any.
    pub rebal: Option<String>,
    /// Open time of the bar that took it.
    pub taken_ts: Option<i64>,
}

impl MsPoint {
    pub fn tier_name(&self) -> String {
        tier_label(self.tier, self.high)
    }
}

/// "STH", "ITL", "LTH", then "T4H", "T5L", ... (1-based tier numbers above LT).
pub fn tier_label(tier: u8, high: bool) -> String {
    let s = if high { "H" } else { "L" };
    match tier {
        0 => format!("ST{s}"),
        1 => format!("IT{s}"),
        2 => format!("LT{s}"),
        t => format!("T{}{s}", t + 1),
    }
}

#[derive(Clone, Debug)]
pub enum MsEvent {
    /// A point reached a new tier (ST included). `from` is its previous tier,
    /// none for a new ST.
    Tier { point: MsPoint, from: Option<u8> },
    /// An IT-or-higher point was taken on this bar.
    Taken { point: MsPoint },
}

impl MsEvent {
    pub fn point(&self) -> &MsPoint {
        match self {
            MsEvent::Tier { point, .. } | MsEvent::Taken { point } => point,
        }
    }
}

/// One timeframe, one side.
#[derive(Clone, Debug, Default)]
struct Ladder {
    pts: FxHashMap<u64, MsPoint>,
    order: VecDeque<u64>,
    next: u64,
    /// The last two points of each tier, oldest first.
    tiers: Vec<VecDeque<u64>>,
    /// Points whose taking is still watched.
    open: VecDeque<u64>,
}

impl Ladder {
    fn referenced(&self, s: u64) -> bool {
        self.tiers.iter().any(|t| t.contains(&s)) || self.open.contains(&s)
    }
}

#[derive(Clone, Debug, Default)]
struct TfState {
    /// (bar open ts, high, low, zones) for the last 2*lb+1 bars.
    bars: VecDeque<(i64, f64, f64, BarZones)>,
    hi: Ladder,
    lo: Ladder,
}

#[derive(Clone, Debug)]
pub struct Structure {
    cfg: StructureCfg,
    tfs: FxHashMap<u16, TfState>,
}

impl Structure {
    pub fn new(cfg: StructureCfg) -> Self {
        Self { cfg, tfs: FxHashMap::default() }
    }

    pub fn cfg(&self) -> &StructureCfg {
        &self.cfg
    }

    /// Step one closed bar of timeframe `tf`. `zones` is what the bar's
    /// extremes traded into; `atr` the timeframe's ATR. Returns this bar's
    /// promotions and takings.
    #[allow(clippy::too_many_arguments)]
    pub fn process(
        &mut self,
        tf: u16,
        ts: i64,
        high: f64,
        low: f64,
        close: f64,
        atr: f64,
        zones: BarZones,
    ) -> Vec<MsEvent> {
        let mut out = Vec::new();
        if !self.cfg.enabled {
            return out;
        }
        let cfg = self.cfg.clone();
        let lb = cfg.st_lookback.max(1) as usize;
        let tol = (cfg.eq_tol_atr * atr).max(0.0);
        let st = self.tfs.entry(tf).or_default();

        // Takings first: this bar acts on what was known before it.
        let (hx, lx) = if cfg.take_by == "wick" { (high, low) } else { (close, close) };
        take(&mut st.hi, true, hx, ts, &mut out);
        take(&mut st.lo, false, lx, ts, &mut out);

        st.bars.push_back((ts, high, low, zones));
        while st.bars.len() > 2 * lb + 1 {
            st.bars.pop_front();
        }
        if st.bars.len() < 2 * lb + 1 {
            return out;
        }
        // Same tie-break as the scanner's swings: strictly beyond the left
        // bars, at least level with the right ones (a flat top is one ST, at
        // its first bar).
        let c = lb;
        let (cts, ch, cl) = (st.bars[c].0, st.bars[c].1, st.bars[c].2);
        let mut is_h = true;
        let mut is_l = true;
        for j in 0..st.bars.len() {
            if j == c {
                continue;
            }
            let (_, h, l, _) = &st.bars[j];
            if j < c {
                is_h &= *h < ch;
                is_l &= *l > cl;
            } else {
                is_h &= *h <= ch;
                is_l &= *l >= cl;
            }
        }
        let zones = st.bars[c].3.clone();
        if is_h {
            let p = MsPoint {
                tf,
                high: true,
                price: ch,
                ts: cts,
                st_confirmed_ts: ts,
                tier: 0,
                kind: PointKind::Swing,
                tier_ts: ts,
                htf: zones.htf_high.clone(),
                rebal: zones.rebal_high.clone(),
                taken_ts: None,
            };
            new_st(&mut st.hi, p, &cfg, tol, ts, &mut out);
        }
        if is_l {
            let p = MsPoint {
                tf,
                high: false,
                price: cl,
                ts: cts,
                st_confirmed_ts: ts,
                tier: 0,
                kind: PointKind::Swing,
                tier_ts: ts,
                htf: zones.htf_low.clone(),
                rebal: zones.rebal_low.clone(),
                taken_ts: None,
            };
            new_st(&mut st.lo, p, &cfg, tol, ts, &mut out);
        }
        out
    }

    /// Every kept point, oldest first per timeframe/side, for snapshots and
    /// queries.
    pub fn points(&self) -> Vec<&MsPoint> {
        let mut tfs: Vec<&u16> = self.tfs.keys().collect();
        tfs.sort();
        let mut out = Vec::new();
        for tf in tfs {
            let st = &self.tfs[tf];
            for lad in [&st.hi, &st.lo] {
                let mut seqs: Vec<&u64> = lad.pts.keys().collect();
                seqs.sort();
                out.extend(seqs.into_iter().map(|s| &lad.pts[s]));
            }
        }
        out
    }

    /// Kept points of one timeframe and side at `min_tier` or above, newest
    /// first.
    pub fn points_of(&self, tf: u16, high: bool, min_tier: u8) -> Vec<&MsPoint> {
        let Some(st) = self.tfs.get(&tf) else { return Vec::new() };
        let lad = if high { &st.hi } else { &st.lo };
        let mut seqs: Vec<&u64> = lad.pts.keys().collect();
        seqs.sort_by(|a, b| b.cmp(a));
        seqs.into_iter()
            .map(|s| &lad.pts[s])
            .filter(|p| p.tier >= min_tier)
            .collect()
    }
}

/// `a` is beyond `b` on this side by more than `tol`.
fn beyond(high: bool, a: f64, b: f64, tol: f64) -> bool {
    if high {
        a > b + tol
    } else {
        a < b - tol
    }
}

fn take(lad: &mut Ladder, high: bool, x: f64, ts: i64, out: &mut Vec<MsEvent>) {
    let mut i = 0;
    while i < lad.open.len() {
        let s = lad.open[i];
        let Some(p) = lad.pts.get_mut(&s) else {
            lad.open.remove(i);
            continue;
        };
        if p.ts < ts && beyond(high, x, p.price, 0.0) {
            p.taken_ts = Some(ts);
            if p.tier >= 1 {
                out.push(MsEvent::Taken { point: p.clone() });
            }
            lad.open.remove(i);
            continue;
        }
        i += 1;
    }
}

fn new_st(lad: &mut Ladder, p: MsPoint, cfg: &StructureCfg, tol: f64, ts: i64, out: &mut Vec<MsEvent>) {
    let high = p.high;
    let s = lad.next;
    lad.next += 1;
    let rebal = cfg.rebalance && p.rebal.is_some();
    out.push(MsEvent::Tier { point: p.clone(), from: None });
    lad.pts.insert(s, p);
    lad.order.push_back(s);
    lad.open.push_back(s);

    // HTF-reaction LT: the previous ST, if it reacted off an HTF level, is an
    // LT now that a lower ST printed to its right. Done before the ladder
    // pushes `s`, so the previous ST is still the newest of tier 0.
    if !cfg.fractal() && cfg.max_tier >= 2 {
        if let Some(&b) = lad.tiers.first().and_then(|t| t.back()) {
            let (bp, bh) = {
                let x = &lad.pts[&b];
                (x.price, x.htf.is_some())
            };
            // An intact LT already beyond it: this is a lower reaction inside
            // the same move, not a new LT.
            let inside = lad.open.iter().any(|o| {
                lad.pts.get(o).is_some_and(|q| q.tier >= 2 && q.taken_ts.is_none() && !beyond(high, bp, q.price, tol))
            });
            if bh && !inside && beyond(high, bp, lad.pts[&s].price, tol) {
                promote(lad, b, 2, PointKind::HtfReaction, cfg, tol, ts, out);
            }
        }
    }
    push_tier(lad, 0, s, cfg, tol, ts, out);
    if rebal && cfg.max_tier >= 1 {
        promote(lad, s, 1, PointKind::Rebalanced, cfg, tol, ts, out);
    }

    // Bound memory.
    let keep = cfg.keep.max(8) as usize;
    while lad.order.len() > keep {
        let old = lad.order.pop_front().unwrap();
        if !lad.referenced(old) {
            lad.pts.remove(&old);
        } else {
            lad.order.push_back(old);
            break;
        }
    }
    let max_open = cfg.max_open.max(1) as usize;
    let mut i = 0;
    while lad.open.len() > max_open && i < lad.open.len() {
        // Evict the oldest open ST first; IT+ points only when nothing else.
        let s0 = lad.open[i];
        if lad.pts.get(&s0).is_none_or(|p| p.tier == 0) {
            lad.open.remove(i);
        } else {
            i += 1;
        }
    }
    while lad.open.len() > max_open {
        lad.open.pop_front();
    }
    lad.open.retain(|x| lad.pts.contains_key(x));
}

/// Append `s` to tier `k`'s pair; if the pair's newer point has a lower
/// point on each side, it climbs to `k + 1`.
fn push_tier(lad: &mut Ladder, k: usize, s: u64, cfg: &StructureCfg, tol: f64, ts: i64, out: &mut Vec<MsEvent>) {
    while lad.tiers.len() <= k {
        lad.tiers.push(VecDeque::new());
    }
    if lad.tiers[k].contains(&s) {
        return;
    }
    let up = k + 1;
    let ladder_step = !(up == 2 && !cfg.fractal());
    if ladder_step && up as i64 <= cfg.max_tier && lad.tiers[k].len() == 2 {
        let (a, b) = (lad.tiers[k][0], lad.tiers[k][1]);
        let high = lad.pts[&s].high;
        let (pa, pb, ps) = (lad.pts[&a].price, lad.pts[&b].price, lad.pts[&s].price);
        if beyond(high, pb, pa, tol) && beyond(high, pb, ps, tol) {
            promote(lad, b, up as u8, PointKind::Swing, cfg, tol, ts, out);
        }
    }
    lad.tiers[k].push_back(s);
    while lad.tiers[k].len() > 2 {
        lad.tiers[k].pop_front();
    }
}

#[allow(clippy::too_many_arguments)]
fn promote(lad: &mut Ladder, s: u64, to: u8, kind: PointKind, cfg: &StructureCfg, tol: f64, ts: i64, out: &mut Vec<MsEvent>) {
    let from = {
        let p = lad.pts.get_mut(&s).unwrap();
        if p.tier >= to {
            return;
        }
        let from = p.tier;
        p.tier = to;
        p.kind = kind;
        p.tier_ts = ts;
        from
    };
    out.push(MsEvent::Tier { point: lad.pts[&s].clone(), from: Some(from) });
    if lad.pts[&s].taken_ts.is_none() && !lad.open.contains(&s) {
        lad.open.push_back(s);
    }
    // A point that skipped tiers (rebalanced, HTF) also stands in each tier
    // it passed, so the ladder above it sees it.
    for k in (from as usize + 1)..=(to as usize) {
        push_tier(lad, k, s, cfg, tol, ts, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed (high, low) bars; close = midpoint. Returns every event.
    fn run(cfg: StructureCfg, bars: &[(f64, f64)], zones: &[(usize, BarZones)]) -> Vec<MsEvent> {
        let mut s = Structure::new(cfg);
        let mut out = Vec::new();
        for (i, &(h, l)) in bars.iter().enumerate() {
            let z = zones.iter().find(|(j, _)| *j == i).map(|(_, z)| z.clone()).unwrap_or_default();
            out.extend(s.process(0, i as i64 * 60, h, l, (h + l) / 2.0, 1.0, z));
        }
        out
    }

    fn tiers(ev: &[MsEvent], high: bool) -> Vec<(i64, u8, &'static str)> {
        ev.iter()
            .filter_map(|e| match e {
                MsEvent::Tier { point, .. } if point.high == high => {
                    Some((point.ts / 60, point.tier, point.kind.as_str()))
                }
                _ => None,
            })
            .collect()
    }

    /// Highs: a zigzag whose peaks are given; troughs sit 5 below the lower
    /// neighbour. Peak k sits at bar 2k+1.
    fn zig(peaks: &[f64]) -> Vec<(f64, f64)> {
        let mut v = vec![(peaks[0] - 6.0, peaks[0] - 8.0)];
        for &p in peaks {
            v.push((p, p - 2.0));
            v.push((p - 4.0, p - 7.0));
        }
        v
    }

    #[test]
    fn three_bar_sth_and_its_confirmation_bar() {
        let ev = run(StructureCfg::default(), &[(10.0, 9.0), (12.0, 10.0), (11.0, 10.0)], &[]);
        let t = tiers(&ev, true);
        assert_eq!(t, vec![(1, 0, "swing")]);
        if let MsEvent::Tier { point, .. } = &ev[0] {
            assert_eq!(point.st_confirmed_ts, 120);
        }
    }

    #[test]
    fn ith_is_an_sth_with_lower_sths_each_side() {
        // peaks 10, 12, 11 -> the 12 is an ITH once the 11 is an STH.
        let ev = run(StructureCfg::default(), &zig(&[10.0, 12.0, 11.0]), &[]);
        let t = tiers(&ev, true);
        assert!(t.contains(&(3, 1, "swing")), "{t:?}");
        // Not before the right-hand STH is confirmed.
        let ev2 = run(StructureCfg::default(), &zig(&[10.0, 12.0])[..5], &[]);
        assert!(!tiers(&ev2, true).iter().any(|x| x.1 >= 1));
    }

    #[test]
    fn equal_highs_are_not_beyond() {
        let ev = run(StructureCfg::default(), &zig(&[10.0, 10.0, 9.0]), &[]);
        assert!(!tiers(&ev, true).iter().any(|x| x.1 >= 1));
        let cfg = StructureCfg { eq_tol_atr: 0.6, ..Default::default() };
        let ev = run(cfg, &zig(&[10.0, 10.5, 9.0]), &[]);
        assert!(!tiers(&ev, true).iter().any(|x| x.1 >= 1));
    }

    #[test]
    fn fractal_mode_keeps_climbing() {
        // IT peaks at 12, 14, 13 (each flanked by lower STs) -> 14 becomes LT,
        // and an LT pattern 14, 16, 15 at tier 2 makes the 16 a T4.
        let peaks = [
            10.0, 12.0, 11.0, // IT 12
            11.5, 14.0, 12.5, // IT 14
            12.0, 13.0, 12.2, // IT 13 -> LT 14
            12.1, 16.0, 13.0, // IT 16
            12.5, 14.5, 12.6, // IT 14.5 -> LT 16? needs LT pattern: LT 14, X, ...
            12.4, 15.5, 12.3, // IT 15.5 -> 16 is LT (14.5 < 16 > 15.5)
            12.0, 13.5, 11.0, // IT 13.5 -> 15.5 not LT (16 > 15.5)
        ];
        let cfg = StructureCfg { lt_mode: "fractal".into(), ..Default::default() };
        let ev = run(cfg, &zig(&peaks), &[]);
        let t = tiers(&ev, true);
        let max_tier_of = |price: f64| {
            ev.iter()
                .filter_map(|e| match e {
                    MsEvent::Tier { point, .. } if point.high && point.price == price => Some(point.tier),
                    _ => None,
                })
                .max()
                .unwrap_or(0)
        };
        assert_eq!(max_tier_of(12.0), 1, "{t:?}");
        assert_eq!(max_tier_of(14.0), 2, "{t:?}");
        assert_eq!(max_tier_of(16.0), 3, "LT 14 < LT 16 > LT 15.5 climbs to T4: {t:?}");
    }

    #[test]
    fn htf_mode_lt_needs_the_reaction_and_a_lower_st_to_its_right() {
        let bars = zig(&[10.0, 12.0, 11.0]);
        let z = BarZones { htf_high: Some("1h fvg".into()), ..Default::default() };
        let ev = run(StructureCfg::default(), &bars, &[(3, z.clone())]);
        let t = tiers(&ev, true);
        assert!(t.contains(&(3, 2, "htf")), "{t:?}");
        // A higher ST to its right: not an LT.
        let ev = run(StructureCfg::default(), &zig(&[10.0, 12.0, 13.0]), &[(3, z)]);
        assert!(!tiers(&ev, true).iter().any(|x| x.1 >= 2));
        // Without the reaction, IT at most.
        let ev = run(StructureCfg::default(), &bars, &[]);
        assert!(!tiers(&ev, true).iter().any(|x| x.1 >= 2));
    }

    #[test]
    fn rebalanced_it_is_immediate() {
        let bars = zig(&[10.0, 12.0]);
        let z = BarZones { rebal_high: Some("5m fvg".into()), ..Default::default() };
        let ev = run(StructureCfg::default(), &bars[..5], &[(3, z)]);
        let t = tiers(&ev, true);
        assert!(t.contains(&(3, 1, "rebalanced")), "{t:?}");
    }

    #[test]
    fn taking_an_ith_reports_once() {
        let mut bars = zig(&[10.0, 12.0, 11.0]);
        bars.push((11.0, 10.0));
        bars.push((13.5, 11.0)); // close 12.25 > 12
        bars.push((14.0, 12.0));
        let ev = run(StructureCfg::default(), &bars, &[]);
        let taken: Vec<_> = ev
            .iter()
            .filter_map(|e| match e {
                MsEvent::Taken { point } => Some((point.price, point.taken_ts)),
                _ => None,
            })
            .collect();
        assert_eq!(taken, vec![(12.0, Some(8 * 60))]);
    }

    #[test]
    fn lows_mirror_highs() {
        let bars: Vec<(f64, f64)> = zig(&[10.0, 12.0, 11.0]).iter().map(|&(h, l)| (-l, -h)).collect();
        let ev = run(StructureCfg::default(), &bars, &[]);
        let t = tiers(&ev, false);
        assert!(t.contains(&(3, 1, "swing")), "{t:?}");
    }

    #[test]
    fn no_lookahead_labels_are_a_prefix() {
        let peaks: Vec<f64> = (0..60).map(|i| 10.0 + ((i * 37) % 11) as f64).collect();
        let bars = zig(&peaks);
        let cfg = StructureCfg { lt_mode: "fractal".into(), ..Default::default() };
        let full = run(cfg.clone(), &bars, &[]);
        for n in [10, 33, 70, bars.len() - 1] {
            let part = run(cfg.clone(), &bars[..n], &[]);
            let f: Vec<String> = full.iter().map(|e| format!("{e:?}")).take(part.len()).collect();
            let p: Vec<String> = part.iter().map(|e| format!("{e:?}")).collect();
            assert_eq!(f, p, "events for the first {n} bars differ");
        }
    }

    #[test]
    fn memory_is_bounded() {
        let peaks: Vec<f64> = (0..5000).map(|i| 10.0 + ((i * 7919) % 101) as f64 / 10.0).collect();
        let cfg = StructureCfg { keep: 50, max_open: 10, lt_mode: "fractal".into(), ..Default::default() };
        let mut s = Structure::new(cfg);
        for (i, &(h, l)) in zig(&peaks).iter().enumerate() {
            s.process(0, i as i64 * 60, h, l, (h + l) / 2.0, 1.0, BarZones::default());
        }
        let st = &s.tfs[&0];
        assert!(st.hi.pts.len() <= 50 + 2 * 9, "{}", st.hi.pts.len());
        assert!(st.hi.open.len() <= 10);
    }
}
