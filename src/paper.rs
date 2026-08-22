use crate::fees::{self, EntryFeeSide};
use crate::hold::{self, HoldClock, HoldTick};
use crate::models::{
    asset_name, sig_type_name, tf_name, Candle, Direction, Opportunity, PaperTrade, Tick,
    TradeResult,
};
use crate::strategy::{Decision, TakeParams};
use chrono::{Duration as ChronoDuration, NaiveDateTime};
use std::collections::{HashMap, HashSet, VecDeque};

/// A per-asset, time-ordered raw trade stream plus a monotonic cursor, used by
/// the tick-resolution fill mode. `update_prices` runs once per COMPLETED 1m
/// candle in ascending time order, so the cursor only ever moves forward: for a
/// bar `[open, open+1m)` we advance from the cursor to the first tick ≥ the
/// bar's open, then collect the contiguous run of ticks strictly before the next
/// bar boundary. That slice — real prints in real time order — is what the fill
/// engine walks to resolve stops/TPs/entries at the exact crossing tick.
#[derive(Debug, Clone)]
pub struct TickStore {
    ticks: Vec<Tick>,
    cursor: usize,
}

impl TickStore {
    pub fn new(ticks: Vec<Tick>) -> Self {
        TickStore { ticks, cursor: 0 }
    }
    pub fn len(&self) -> usize {
        self.ticks.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ticks.is_empty()
    }

    /// Return the slice of ticks whose timestamp falls in `[bar_open,
    /// bar_open + 1m)`. Advances the internal cursor to (and past) that window,
    /// so subsequent calls for later bars are O(ticks in the bar). Callers MUST
    /// invoke this in ascending bar order (the backtest per-asset loop does).
    /// Returns an empty slice when the bar has no ticks (quiet minute / gap).
    fn ticks_for_bar(&mut self, bar_open: NaiveDateTime) -> &[Tick] {
        let bar_end = bar_open + ChronoDuration::minutes(1);
        // Advance cursor to the first tick at/after bar_open. (Ticks before
        // bar_open belong to an earlier bar already processed, or fall in a gap
        // — skip them.)
        while self.cursor < self.ticks.len() && self.ticks[self.cursor].timestamp < bar_open {
            self.cursor += 1;
        }
        let lo = self.cursor;
        let mut hi = lo;
        while hi < self.ticks.len() && self.ticks[hi].timestamp < bar_end {
            hi += 1;
        }
        // Leave the cursor at the window start; the next bar advances it forward
        // from here (it never needs to look behind lo again).
        self.cursor = hi;
        &self.ticks[lo..hi]
    }
}

/// The fill simulator and trade ledger: everything that happens to an order
/// after a strategy decides to place it.
///
/// One `PaperTrader` owns one asset's book in a backtest (the driver runs a
/// thread per asset). It holds no strategy state — `evaluate` takes a decided
/// verdict rather than computing one — so the same trader serves any
/// [`crate::strategy::Strategy`] unchanged.
///
/// `Clone` exists so a caller can fork a fully-configured, still-empty trader
/// and run a variant book beside the primary without the two drifting on a
/// knob. Cloning a mid-run trader copies its open positions too.
#[derive(Clone)]
pub struct PaperTrader {
    pub min_score: f64,
    pub rr_target: f64,
    pub max_hold_candles: usize,
    /// The validated strategy knob bag. This ONE field replaces the 25
    /// hand-threaded gate fields that used to live here purely to be copied
    /// into `Filters` — every one of them had exactly one use (the `filters()`
    /// builder below). Adding a gate knob now needs no field here at all.
    pub params: crate::params::Params,
    /// Charge fees. Kept as a real field (not read through `params`) because
    /// paper.rs's own exit/ledger code reads it ~10 times per trade on the
    /// fill path, not just when building `Filters`.
    pub use_fees: bool,
    /// Open profit in R at which the stop moves to lock in `trail_lock_r`
    /// (0 = off).
    pub breakeven_r: f64,
    /// R profit to lock in once breakeven_r threshold is reached (0 = breakeven, 0.5 = lock 0.5R)
    pub trail_lock_r: f64,
    /// Per-asset R:R target (asset_name -> rr)
    // ─── Stop-exit gap penalty (see fees::fee_in_r_side / close_trade) ─────────
    /// Flat, deterministic stop-exit gap penalty in bps of the stop price,
    /// applied to every asset without a `stop_gap_bps_asset` entry. On a
    /// genuine stop-loss exit (not TP, not a de-risk/timeout close), the fill
    /// price becomes `stop*(1 - bps*1e-4)` for longs / `stop*(1 + bps*1e-4)`
    /// for shorts, before r_pnl and the exit fee are computed. Models the
    /// trigger-cascade slippage a resting stop suffers in a real market versus
    /// a clean fill exactly at the stop price. 0 = off.
    pub stop_gap_bps_default: f64,
    /// Per-asset override (asset_name -> bps), substring key match like
    /// `rr_asset`. Unmatched assets use `stop_gap_bps_default`.
    pub stop_gap_bps_asset: HashMap<String, f64>,
    /// Take half position at this R level, let rest ride (0 = off)
    pub partial_tp_r: f64,
    /// Compounding-risk fraction for the reported equity curve (0 = off).
    pub risk_frac: f64,
    /// Starting balance for the compounding equity curve.
    pub account_size: f64,
    // ─── Resting-limit entry-fill model ───────────────────────────────────────
    /// When `true`, the entry limit may fill on the signal bar itself. When
    /// `false` (honest default), the signal bar is excluded — its high/low
    /// printed before the live order existed — and the fill search starts on the
    /// bar AFTER `opened_at`, mirroring live's ~1-candle placement lag.
    pub allow_signal_bar_fill: bool,
    /// Entry slippage in R-multiples of `R_planned`. The fill price is always
    /// `entry ± slip·R_planned` toward the LOSING side (bull +, bear −), on
    /// whichever bar fills. 0.0 = pristine maker fill at `entry`.
    pub entry_slippage_r: f64,
    /// Intrabar tie-break when a single candle spans both stop and TP. `true`
    /// (default, pessimistic) = stop resolves first; `false` = TP first.
    pub intrabar_stop_first: bool,
    // ─── Hybrid entry-fill model ──────────────────────────────────────────────
    /// `false` (default) = the pure resting-limit model (`fill_action`, honoring
    /// `allow_signal_bar_fill`/`entry_slippage_r`). `true` = the live-faithful
    /// hybrid model (`hybrid_fill_action`): decision-bar-open cases, taker when
    /// aggressing past the entry, seed+chase watchdog, abandon on TP/stop-gap/age.
    /// When `true`, `allow_signal_bar_fill` and `entry_slippage_r` are IGNORED.
    pub hybrid_fill: bool,
    // ─── Tick-resolution fill model (entry_fill_mode = "tick") ─────────────────
    /// When `true`, entry/stop/TP resolution walks the bar's real millisecond
    /// TRADE ticks in time order and acts on the FIRST tick that crosses each
    /// level — replacing the OHLC `hit_stop`/`hit_tp` + `intrabar_stop_first`
    /// guess with true intrabar ordering. The resolved fill/exit price and
    /// TIMESTAMP are the crossing tick's, not the bar boundary's. A bar with no
    /// ticks (quiet minute / gap in the tick archive) falls back to the OHLC
    /// candle logic (`tick_fallback_bars` counts how often). Entry fills use the
    /// existing resting-limit population (`decide()` → `pending`); only the fill
    /// timing/price is taken from ticks. See `update_prices_tick`.
    pub tick_fill: bool,
    /// Per-asset tick stream + monotonic cursor. `Some` only when `tick_fill` is
    /// on AND a tick file existed for this asset; `None` → tick mode silently
    /// degrades to the OHLC path for every bar.
    pub tick_store: Option<TickStore>,
    /// Diagnostic: bars for which tick mode fell back to OHLC because the bar had
    /// no ticks. Summed across per-asset threads and reported.
    pub tick_fallback_bars: usize,
    /// Diagnostic: bars for which tick mode used the tick stream. Together with
    /// `tick_fallback_bars` this shows tick coverage over the run.
    pub tick_resolved_bars: usize,
    /// Diagnostic: total ticks walked across the run.
    pub tick_walked: usize,
    /// Tick mode: model the live entry-chase on real tick order. A resting
    /// limit whose boundary B = E ± chase_r·R is reached (while armed) BEFORE
    /// any tick touches E fills TAKER at B instead of never filling — the same
    /// chase live performs. `true` (the live-faithful default) unless the fill
    /// lens sets `tick_chase = false`; env `ICT_TICK_CHASE=1|0` overrides both.
    pub tick_chase: bool,
    /// Hybrid: chase gate = fill cap in R-multiples of R=|entry−stop|.
    pub chase_r: f64,
    /// Hybrid: require a seed touch (T or T−1 traded to the entry) to arm chases.
    pub chase_requires_seed: bool,
    /// Hybrid: fill immediately at the decision-bar open when armed and the open
    /// sits in front of the entry but within the chase cap.
    pub immediate_chase_at_open: bool,
    /// Hybrid: when one bar reaches BOTH the entry (maker) and the chase
    /// boundary (taker), the intrabar order is unknowable and has to be
    /// assumed. `true` resolves the race to the maker fill at the entry;
    /// `false` (default) takes the pessimistic boundary chase.
    ///
    /// Which assumption is right depends on your execution: a fast resting
    /// order usually gets the maker fill, a slow one usually does not. Run
    /// both and treat the difference as the cost of not knowing.
    pub race_maker_first: bool,
    /// Hybrid: deferred open-chase. Only acts when `immediate_chase_at_open`
    /// is false.
    ///
    /// A decision bar opening in front of the entry, within the chase cap,
    /// carries its OPEN as this limit's chase price for the order's whole
    /// life. The limit still rests, and a maker touch at the entry still wins
    /// the race — but every other resolution (boundary cross, gap past the
    /// cap, TP-open, age-out) books taker at that carried open rather than at
    /// the full-cap boundary, and the order never abandons.
    ///
    /// The reading is that the watchdog's chase near the open did happen; this
    /// just recognizes it lazily, letting an earlier maker touch supersede it.
    /// So this config dominates the same one with `immediate_chase_at_open`
    /// set, trade for trade: the same entries, each filled at the open price
    /// or better. Default false.
    pub deferred_chase_at_open: bool,
    /// Hybrid: Case-A (open at/past entry) entry-fee side. Live's GTC limit is
    /// marketable when price is already past entry, so it aggresses → `Taker`
    /// (default). `Maker` is a counterfactual knob.
    pub past_entry_fee: EntryFeeSide,
    /// Fill-path breakdown counters for the hybrid model (all zero in limit mode).
    pub hybrid_counters: HybridFillCounters,
    /// Cancel watchdog (hybrid mode only): while an entry limit is pending, if
    /// a COMPLETED bar closes beyond the strategy's stamped `min_target`,
    /// cancel the order — the move ran without us, so chasing it is a worse
    /// bet than standing down. Acts only on bars STRICTLY BEFORE the fill bar
    /// (same-bar → the fill stands). Default false.
    pub cancel_on_target_consumed: bool,
    /// Cancel watchdog (hybrid mode only): while an entry limit is pending,
    /// cancel it if the strategy's setup is invalidated on a completed bar.
    /// The invalidation check is driven externally — a driver marks pendings
    /// via `cancel_pending_invalidated` BEFORE feeding the current bar to the
    /// strategy, so the state it sees is as of the previous bar's close and
    /// no lookahead is possible. Default false.
    pub cancel_on_setup_invalidated: bool,
    /// rest-on-Ready EXECUTION lens (`entry_fill_mode = "rest_on_ready"`,
    /// hybrid mode extension). The engine's trigger logic is UNTOUCHED (the
    /// signal still fires at the first touch, so the trade population is
    /// identical to any other lens on the same strategy); this only changes how
    /// the entry FILLS. Live places the resting limit when the setup goes
    /// `Ready` (entry/stop freeze) instead of after the touch bar closes, and
    /// cancels it if the engine invalidates the setup — for emitted signals the
    /// engine itself guarantees no invalidation fired between Ready and touch,
    /// so the order is still resting when the touch bar arrives. Semantics: if
    /// `created_at − ready_at ≥ rest_min_lead_secs` (the order was demonstrably
    /// on the book before the touch bar) and the signal bar T touched the
    /// entry, T fills MAKER at exactly the entry; otherwise (same-bar Ready →
    /// no lead time, or no `ready_at`) the standard live-faithful hybrid path
    /// takes over — which is what live does today for an order placed after
    /// the signal bar closes. Default false.
    pub rest_on_ready_fill: bool,
    /// Minimum Ready→touch lead for the rested-maker fill, in seconds. 60 (one
    /// 1m bar) = the order goes up ~9.5s into the bar before the touch bar —
    /// slightly optimistic if the touch printed in those first seconds; 120 =
    /// strictly honest (a full spare bar).
    pub rest_min_lead_secs: i64,
    // ─── De-risk exit (trade management, default OFF) ─────────────────────────
    /// After a trade has been open for more than this many 1m bars (≈ minutes),
    /// if its unrealized R at the bar close is below `derisk_below_r`, close it
    /// at market (the bar's close / next tick). Models the discretionary
    /// habit of cutting a stale flat-or-losing trade rather than waiting for
    /// the stop, which lands an average loss short of a full R at the cost of
    /// giving up the trades that would have recovered. 0 = feature OFF
    /// (default; existing strategies
    /// unaffected). Works in both the OHLC and tick fill paths.
    pub derisk_after_min: usize,
    /// Unrealized-R threshold for the de-risk exit (only meaningful when
    /// `derisk_after_min > 0`). E.g. 0.0 = close stale trades that are flat or
    /// under water.
    pub derisk_below_r: f64,
    pub trades: Vec<PaperTrade>,
    /// Depth-sizing annotations keyed by opportunity_id, attached post-run by
    /// a driver that has orderbook snapshots (see `l2book::annotate_trade`).
    /// Purely diagnostic: never read by trading logic.
    pub l2_annotations: HashMap<String, crate::l2book::L2Annotation>,
    pub open_trades: Vec<PaperTrade>,
    /// Entry limits placed but not yet filled. Each is checked against every
    /// subsequent candle until it fills (→ `open_trades`) or its 2h deadline
    /// lapses unfilled (→ dropped; NO TRADE — never counted anywhere).
    pending: Vec<PendingFill>,
    pub opportunities_seen: usize,
    pub opportunities_taken: usize,
    /// How many opportunities each skip reason rejected, keyed by
    /// `SkipReason::as_str`. Emitted in the report so a gate that swallows
    /// most of the flow is visible rather than silent.
    pub skips: HashMap<String, usize>,
    hold_counts: HashMap<String, HoldClock>,
    recent_candles: HashMap<u16, VecDeque<Candle>>,
    /// Best favorable price seen per open trade (in R multiples from entry)
    watermarks: HashMap<String, f64>,
    /// Trades that have taken partial profit
    partial_taken: HashSet<String>,
    /// Entry-leg fee side per booked trade (opportunity_id → side). Populated at
    /// fill time; read in `close_trade` to pick the maker/taker entry rate. Only
    /// the hybrid model varies this — limit-mode fills always insert `Maker`.
    entry_fee_side: HashMap<String, EntryFeeSide>,
    /// Diagnostic: opportunity_ids whose entry filled on a bar that also reached
    /// the stop (first-touch losers). Read in `close_trade` to accumulate their
    /// net R into `hybrid_counters.first_touch_stop_r_milli`. Attribution only.
    first_touch_ids: HashSet<String>,
    /// Concurrency diagnostic: (placed_at, resolved_at) for every resting entry
    /// limit — placed at `evaluate`, resolved when filled/abandoned/expired.
    /// Per-asset, since each asset runs on its own thread. Sweeping the merged
    /// intervals across assets gives the true GLOBAL count of simultaneously
    /// resting orders, which is what tells you whether a result depended on
    /// holding more positions at once than you could actually margin.
    pub resting_intervals: Vec<(NaiveDateTime, NaiveDateTime)>,
    /// Placement time per still-open pending limit (opportunity_id → placed_at),
    /// used to close out its interval when it resolves. Drained as limits resolve.
    pending_placed_at: HashMap<String, NaiveDateTime>,
    /// Diagnostic: `Opportunity::ready_at` per placed limit (opportunity_id →
    /// ready_at), kept for the JSON sidecar so the Ready→touch lead of every
    /// trade is analyzable offline. Never drained; merged across assets.
    pub trade_ready_at: HashMap<String, NaiveDateTime>,
    /// Trades that closed during the most recent [`Self::update_prices`]
    /// call, as `(index into trades, was_a_genuine_stop_exit)`. Cleared at
    /// the start of every update, so a strategy's
    /// [`crate::strategy::Strategy::on_bar_close`] hook sees exactly this
    /// bar's exits and nothing older.
    pub closed_this_bar: Vec<(usize, bool)>,
    /// Strategy-managed stop overrides, keyed by opportunity id (see
    /// [`Self::set_open_stop`]). An override replaces the planned stop for
    /// exit resolution only; the planned `|entry - stop|` stays the R unit.
    stop_overrides: HashMap<String, f64>,
}

/// A compounding equity curve: per-trade `(opportunity_id, balance_after,
/// dollar_pnl)` in the trades' own order, the final balance, and the maximum
/// drawdown as a fraction of the running peak.
pub type EquityCurve = (Vec<(String, f64, f64)>, f64, f64);

/// Fill-path breakdown for the hybrid entry-fill model. Every booked hybrid
/// trade increments exactly one of the fill counters; every dropped pending
/// limit increments exactly one abandon counter. Summed across per-asset threads
/// in `main.rs` and emitted in the JSON sidecar for the results table.
#[derive(Debug, Clone, Default)]
pub struct HybridFillCounters {
    /// Case A: decision bar opened at/past the entry → taker/maker fill at open.
    pub past_entry_fills: usize,
    /// Case B0: seeded immediate chase at the decision-bar open (taker).
    pub immediate_chases: usize,
    /// Deferred open-chase (`deferred_chase_at_open`): a B0-carried limit
    /// resolved without a maker touch — booked taker at the carried
    /// decision-bar open (boundary cross / gap past cap / TP-open / age-out).
    pub deferred_chases: usize,
    /// Boundary chase taken when price rose INTO the cap from below (b.high ≥ E+xR).
    pub boundary_chases_from_below: usize,
    /// Boundary chase taken when price fell back THROUGH the cap from above
    /// (b.open beyond the boundary, b.low ≤ E+xR).
    pub boundary_chases_from_above: usize,
    /// Maker fill at exactly the entry (limit rested and price came to it).
    pub maker_fills: usize,
    /// rest-on-Ready lens: maker fill at the entry ON the signal bar T itself,
    /// legitimate because the limit had been resting since `ready_at` (with
    /// ≥ `rest_min_lead_secs` of lead). Counted separately from `maker_fills`
    /// (post-signal-bar rests) so the lens's recovered edge is attributable.
    pub rested_maker_fills: usize,
    /// Abandoned: decision bar (or a gap) opened at/through the STOP (Case A).
    pub abandon_gap_stop: usize,
    /// Abandoned: a bar opened at/past the TP while still unfilled.
    pub abandon_tp_open: usize,
    /// Abandoned: TP was reached intrabar (range) while unfilled (pessimistic).
    pub abandon_tp_range: usize,
    /// Abandoned: rested past `ENTRY_MAX_AGE_SECS` (1800s) unfilled.
    pub abandon_age: usize,
    /// Abandoned: reached the fill deadline / data ran out with no fill.
    pub abandon_deadline: usize,
    /// Canceled by the rest-on-Ready v2 `cancel_on_target_consumed` watchdog:
    /// a prior completed bar closed beyond `min_target` while unfilled.
    pub abandon_target_consumed: usize,
    /// Canceled by the `cancel_on_setup_invalidated` watchdog: the strategy's
    /// setup was invalidated on a prior completed bar while unfilled.
    pub abandon_setup_invalidated: usize,
    /// Diagnostic (does NOT change P&L): of the booked fills, how many filled on
    /// a candle whose OWN range also reached the stop (a "first-touch loser" — the
    /// resting order got hit and price kept going to the stop within the same
    /// bar). The stop/TP race still starts next candle, so this is purely an
    /// attribution counter for the rest-on-Ready experiment (which intentionally
    /// inherits these), not a fill-path branch.
    pub first_touch_stop_fills: usize,
    /// Companion to `first_touch_stop_fills`: their summed net R (post-fee),
    /// resolved when each such trade closes. Scaled ×1000 and stored as an int so
    /// the counter stays `Copy`/mergeable without a float field; divide by 1000.
    pub first_touch_stop_r_milli: i64,
}

impl HybridFillCounters {
    pub fn merge(&mut self, o: &HybridFillCounters) {
        self.past_entry_fills += o.past_entry_fills;
        self.immediate_chases += o.immediate_chases;
        self.deferred_chases += o.deferred_chases;
        self.boundary_chases_from_below += o.boundary_chases_from_below;
        self.boundary_chases_from_above += o.boundary_chases_from_above;
        self.maker_fills += o.maker_fills;
        self.rested_maker_fills += o.rested_maker_fills;
        self.abandon_gap_stop += o.abandon_gap_stop;
        self.abandon_tp_open += o.abandon_tp_open;
        self.abandon_tp_range += o.abandon_tp_range;
        self.abandon_age += o.abandon_age;
        self.abandon_deadline += o.abandon_deadline;
        self.abandon_target_consumed += o.abandon_target_consumed;
        self.abandon_setup_invalidated += o.abandon_setup_invalidated;
        self.first_touch_stop_fills += o.first_touch_stop_fills;
        self.first_touch_stop_r_milli += o.first_touch_stop_r_milli;
    }
}

/// An entry limit that has been placed and is waiting for price to reach it.
/// `trade.entry`/`stop`/`tp` are the PLANNED levels; the fill price is computed
/// from `entry` + slippage on the candle that finally touches the entry.
#[derive(Debug, Clone)]
struct PendingFill {
    trade: PaperTrade,
    /// Hard deadline (from `opened_at`) after which an unfilled limit is
    /// cancelled, mirroring the finite lifetime a real resting order gets.
    deadline: NaiveDateTime,
    /// The signal bar is ineligible when `allow_signal_bar_fill` is false. We
    /// can't key off timestamps (HTF candles reuse the base bar's stamp), so we
    /// mark the FIRST candle offered to a pending fill as the signal bar — it IS
    /// the signal bar, because `evaluate` runs inside the same
    /// pipeline step that then invokes `update_prices` on that same bar.
    seen_first_candle: bool,
    /// Timestamp of T (the first candle offered to this pending fill) — the
    /// moment the order actually starts existing, and the anchor for the 30-min
    /// hybrid age abandon. For 1m signals T's stamp equals `opened_at` (the
    /// signal time), so the anchor is unchanged; for HTF signals `opened_at` is
    /// the bar LABEL, already ≥ one full timeframe in the past by the time the
    /// bar closes and the order can exist — aging from it gives HTF entries
    /// zero resting time (mirroring the live bug fixed alongside this field).
    /// `None` until T is processed.
    age_anchor: Option<NaiveDateTime>,
    /// Hybrid model only: did the signal bar's PREDECESSOR (T−1, the last closed
    /// candle before the signal) already trade to the entry? Captured at
    /// placement from `recent_candles`; OR-ed with T's own touch when T is seen to
    /// compute `seeded` (mirrors live's `entry_crossed` two-candle seed).
    hybrid_pred_touch: bool,
    /// Hybrid model only: resolved once T is seen — whether the chase is armed
    /// (`armed = chase_requires_seed ? (T.touch || T−1.touch) : true`). `None`
    /// until T is processed. Ignored entirely by the limit model.
    hybrid_armed: Option<bool>,
    /// Hybrid model only: set true once the decision bar D (first candle after T)
    /// has been processed, so subsequent bars pass `is_decision_bar = false`.
    hybrid_seen_decision_bar: bool,
    /// Deferred open-chase (`deferred_chase_at_open`): the decision-bar OPEN,
    /// carried as this limit's chase fill price when D opened in front of the
    /// entry within the chase cap (armed). None everywhere else.
    hybrid_b0_open: Option<f64>,
    /// Price beyond which the move is considered already spent: a completed
    /// bar closing past it cancels this unfilled limit when
    /// `cancel_on_target_consumed` is set. Stamped by the strategy via
    /// `annotate_last_pending`; None when it stamped none.
    min_target: Option<f64>,
    /// When the strategy's setup became final, i.e. when a real order could
    /// have gone on the book. Drives the signal-bar rested-maker fill under
    /// the rest-on-ready lens; None → the standard hybrid path only.
    ready_at: Option<NaiveDateTime>,
    /// rest-on-Ready v2: a cancel confirmed by a PRIOR completed bar, to be
    /// applied at the START of the next processed bar (before any fill attempt
    /// on it). This encodes the honesty rule: a condition confirmed by bar
    /// N−1's close cancels at bar N; a condition arising on the fill bar itself
    /// arrives too late — the fill stands.
    cancel_pending: Option<HybridAbandon>,
}

/// Resting-limit cancel horizon for the LIMIT fill model: an entry that has
/// not filled 2h after the signal is cancelled unfilled and books no trade.
const FILL_DEADLINE_SECS: i64 = 7200;

/// Entry-abandon horizon for the HYBRID fill model: a resting entry limit is
/// cancelled 30 min after it starts existing, reclaiming the margin it
/// reserves. Deliberately shorter than the limit model's 2h deadline, because
/// a real order-management watchdog does not leave stale entries on the book.
const HYBRID_ENTRY_MAX_AGE_SECS: i64 = 1800;

/// Pure fill decision for one candle against one resting entry limit.
///
/// Returns the fill PRICE (planned `entry` shifted by `slip·r_planned` toward
/// the losing side) when the candle's range touches the entry and the candle is
/// eligible, else `None`. `is_signal_bar` is true only for the bar the signal
/// formed on; it is ineligible unless `allow_signal_bar_fill` is set. One rule
/// everywhere — no special case for the signal bar's fill price.
///
/// Side-effect-free and exhaustively unit-tested: given a bar and a resting
/// order it returns what happens, with no state to get wrong.
fn fill_action(
    dir: Direction,
    entry: f64,
    r_planned: f64,
    candle: &Candle,
    is_signal_bar: bool,
    allow_signal_bar_fill: bool,
    slip: f64,
) -> Option<f64> {
    if is_signal_bar && !allow_signal_bar_fill {
        return None;
    }
    let touched = match dir {
        Direction::Bull => candle.low <= entry,
        Direction::Bear => candle.high >= entry,
    };
    if !touched {
        return None;
    }
    let fill_px = match dir {
        Direction::Bull => entry + slip * r_planned,
        Direction::Bear => entry - slip * r_planned,
    };
    Some(fill_px)
}

/// Outcome of the hybrid fill decision for ONE candle. `fill_px` variants also
/// carry the entry-fee side (maker vs taker) and which fill-path counter to bump;
/// abandon variants carry the reason. `Wait` keeps the resting limit for the
/// next candle. See `hybrid_fill_action` for the full case analysis.
#[derive(Debug, Clone, Copy, PartialEq)]
enum HybridAction {
    /// Fill at `px` on this bar; `side` is the entry-fee leg, `path` names the
    /// fill route (for the breakdown counters). The stop/TP race starts NEXT bar.
    Fill {
        px: f64,
        side: EntryFeeSide,
        path: HybridPath,
    },
    /// The resting limit stays; re-evaluate on the next candle.
    Wait,
    /// Abandon the pending limit unfilled (NO TRADE), with the reason.
    Abandon(HybridAbandon),
}

/// Which route a hybrid fill took (drives the breakdown counters).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HybridPath {
    PastEntry,
    ImmediateChase,
    /// Deferred open-chase resolution of a B0-carried limit (taker at the
    /// carried decision-bar open). See `deferred_chase_at_open`.
    DeferredChase,
    BoundaryFromBelow,
    BoundaryFromAbove,
    Maker,
}

/// Why a hybrid pending limit was abandoned (NO TRADE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HybridAbandon {
    GapStop,
    TpOpen,
    TpRange,
    Age,
    /// Cancel watchdog: a COMPLETED bar strictly before the would-be fill bar
    /// closed beyond the stamped `min_target` while the limit was unfilled
    /// (`cancel_on_target_consumed`).
    TargetConsumed,
    /// Cancel watchdog: the strategy's setup was invalidated on a completed
    /// bar strictly before the would-be fill bar, while the limit was still
    /// unfilled (`cancel_on_setup_invalidated`).
    SetupInvalidated,
}

/// Pure hybrid entry-fill decision for one candle against one resting entry limit.
///
/// Models a resting GTC limit watched by a seed/chase watchdog, where
/// `chase_r` serves as both the gate on whether a chase fires and the cap on
/// how far past the entry the resulting fill may land. A chase that would
/// fill worse than the cap abandons instead.
///
/// Everything below is written for Bull; Bear is the exact mirror (entry above,
/// "past entry" = above entry, boundary = `entry − x·R`, TP below, etc.). Let
/// E=entry, S=stop, TP=tp, R=`r_planned`, x=`chase_r`, boundary B = E ± x·R.
///
/// `is_decision_bar` marks bar D (the first candle AFTER the signal bar T). The
/// signal bar T itself is filtered out by the caller (the order did not exist on
/// it — same honesty rule as `allow_signal_bar_fill=false`); this fn is only ever
/// called for D and later bars. `armed` = whether the seed+chase is armed.
/// `past_age` = the bar is strictly past `age_anchor + ENTRY_MAX_AGE_SECS`,
/// where `age_anchor` is T's stamp (when the order starts existing; equals
/// `opened_at` for 1m signals, the HTF bar's close for HTF signals).
///
/// `b0_carry` = the deferred open-chase price (`deferred_chase_at_open`; the
/// caller stamps the decision-bar open when D opened in front of the entry
/// within the cap, armed, with the immediate chase off). When `Some(px)`, the
/// carried chase supersedes every non-maker resolution: chases fill taker at
/// `px` instead of the boundary, and the TP-open / TP-range / age abandons
/// become taker fills at `px` (the counterfactual watchdog was already in the
/// trade at that price) — a maker touch at the entry still wins first.
///
/// Side-effect-free and exhaustively unit-tested, like `fill_action` above.
///
/// The long argument list is deliberate: this is a PURE function of the fill
/// state, so every input it consults is visible in its signature rather than
/// reached through `&self`. That is what makes the case analysis testable one
/// branch at a time.
#[allow(clippy::too_many_arguments)]
fn hybrid_fill_action(
    dir: Direction,
    entry: f64,
    stop: f64,
    tp: f64,
    r_planned: f64,
    chase_r: f64,
    armed: bool,
    immediate_chase_at_open: bool,
    race_maker_first: bool,
    b0_carry: Option<f64>,
    past_entry_fee: EntryFeeSide,
    candle: &Candle,
    is_decision_bar: bool,
    past_age: bool,
) -> HybridAction {
    let (o, hi, lo) = (candle.open, candle.high, candle.low);
    // Boundary B = the chase gate / fill cap, x·R past the entry toward the loss.
    let boundary = match dir {
        Direction::Bull => entry + chase_r * r_planned,
        Direction::Bear => entry - chase_r * r_planned,
    };
    // Direction-aware primitive predicates (all written for Bull, mirrored Bear).
    let open_past_entry = match dir {
        Direction::Bull => o <= entry,
        Direction::Bear => o >= entry,
    };
    let open_through_stop = match dir {
        Direction::Bull => o <= stop,
        Direction::Bear => o >= stop,
    };
    // `< boundary` (strict): open sits between the entry and the cap (still chaseable
    // as a maker-or-boundary fill), vs `>= boundary` = already beyond the cap.
    let open_before_boundary = match dir {
        Direction::Bull => o < boundary,
        Direction::Bear => o > boundary,
    };
    let maker_touch = match dir {
        Direction::Bull => lo <= entry,
        Direction::Bear => hi >= entry,
    };
    let boundary_from_below = match dir {
        Direction::Bull => hi >= boundary,
        Direction::Bear => lo <= boundary,
    };
    let boundary_from_above = match dir {
        Direction::Bull => lo <= boundary,
        Direction::Bear => hi >= boundary,
    };
    let tp_open = match dir {
        Direction::Bull => o >= tp,
        Direction::Bear => o <= tp,
    };
    let tp_range = match dir {
        Direction::Bull => hi >= tp,
        Direction::Bear => lo <= tp,
    };

    // ─── Case A — decision bar opens at/past the entry ────────────────────────
    // Only the decision bar D can be in Case A: a later bar reaching here means
    // the limit rested through D unfilled, i.e. D opened in front (Case B); the
    // limit then fills on the first re-approach handled by the Case-B branches,
    // never as a fresh "open past entry" gap. (Path-sound; see the B branches.)
    if is_decision_bar && open_past_entry {
        // Gapped at/through the stop → live teardown kills a breached-stop setup;
        // entering would corrupt R accounting. NO TRADE.
        if open_through_stop {
            return HybridAction::Abandon(HybridAbandon::GapStop);
        }
        // A live GTC limit already past the entry is marketable → it aggresses and
        // pays taker (default); the counterfactual `maker` knob keeps it passive.
        // Filling BETTER than planned (o past E toward the stop) is legitimate
        // positive drift — the divisor stays R_planned, never rescaled.
        return HybridAction::Fill {
            px: o,
            side: past_entry_fee,
            path: HybridPath::PastEntry,
        };
    }

    // ─── Case B — the bar opens in front of the entry ─────────────────────────
    // The B0 test is "O ≤ E + x·R" i.e. open at-or-inside the cap on the front
    // side. `open_before_boundary` is strict `<`; the cap itself (o == B) must
    // also qualify, so test `open_at_or_before_boundary` = open at-or-before B.
    let open_at_or_before_boundary = match dir {
        Direction::Bull => o <= boundary,
        Direction::Bear => o >= boundary,
    };
    // B0 immediate chase: a seeded watchdog fires on its first poll and the IOC
    // fills at ~current price. Only on the decision bar, only if armed & enabled,
    // only when the open is in front of the entry but within the cap.
    if is_decision_bar
        && armed
        && immediate_chase_at_open
        && !open_past_entry
        && open_at_or_before_boundary
    {
        return HybridAction::Fill {
            px: o,
            side: EntryFeeSide::Taker,
            path: HybridPath::ImmediateChase,
        };
    }

    // Per-bar walk (D, D+1, …). Order matters — evaluate exactly as specified.

    // 1) Age: past ENTRY_MAX_AGE_SECS → abandon (reclaim margin). Checked before
    //    any fill so a stale limit never books. A B0-carried limit books its
    //    deferred chase instead — the counterfactual watchdog entered at D.
    if past_age {
        if let Some(px) = b0_carry {
            return HybridAction::Fill {
                px,
                side: EntryFeeSide::Taker,
                path: HybridPath::DeferredChase,
            };
        }
        return HybridAction::Abandon(HybridAbandon::Age);
    }

    // 2) Open already at/past TP → the move completed without us (live AbandonTpHit).
    //    B0-carried: it did NOT complete without us — the deferred chase from D
    //    was in the move; book it (the race then resolves at TP like mh03's).
    if tp_open {
        if let Some(px) = b0_carry {
            return HybridAction::Fill {
                px,
                side: EntryFeeSide::Taker,
                path: HybridPath::DeferredChase,
            };
        }
        return HybridAction::Abandon(HybridAbandon::TpOpen);
    }

    // Split branch 3 vs 4 by where the open sits relative to the cap:
    //   armed:   `open < B` (strict) → branch 3 (in-cap rest band); else branch 4.
    //   unarmed: no cap at all → always branch 3.
    let in_rest_band = if armed { open_before_boundary } else { true };

    // 3) Open in front but strictly inside the cap (or unarmed) — resting-limit zone.
    if in_rest_band {
        let can_maker = maker_touch;
        let can_chase = armed && boundary_from_below;
        // Both reachable in one bar (open between E and B): intrabar order is
        // unknowable → PESSIMISTIC default, take the chase (worse price + taker
        // fee). `race_maker_first` resolves the same unknowable race to the
        // maker fill at the entry — the live-census calibration (maker beat the
        // chase 17:1 in observed fills; live's watchdog polls give the resting
        // limit a real window before any chase fires).
        if can_chase && !(race_maker_first && can_maker) {
            // B0-carried: the chase books at the carried decision-bar open, not
            // the full-cap boundary — the deferred watchdog IOC fired at ~open.
            if let Some(px) = b0_carry {
                return HybridAction::Fill {
                    px,
                    side: EntryFeeSide::Taker,
                    path: HybridPath::DeferredChase,
                };
            }
            return HybridAction::Fill {
                px: boundary,
                side: EntryFeeSide::Taker,
                path: HybridPath::BoundaryFromBelow,
            };
        }
        if can_maker {
            return HybridAction::Fill {
                px: entry,
                side: EntryFeeSide::Maker,
                path: HybridPath::Maker,
            };
        }
        // Neither filled. If unarmed and TP was reached intrabar while we sat
        // unfilled → abandon (pessimistic no-trade). Note this DEVIATES from
        // limit_only, which would have filled the resting maker here; the hybrid
        // model refuses the trade because, unarmed, we can't know the sub-minute
        // order and price reached TP without a confirmed fill. (If the bar spans
        // both E and TP we still abandon — unknowable order, pessimistic.)
        if !armed && tp_range {
            return HybridAction::Abandon(HybridAbandon::TpRange);
        }
        return HybridAction::Wait;
    }

    // 4) Open beyond the boundary (armed) — price is past the cap on the loss side.
    //    A maker fill at E can NEVER happen from here: price falling from beyond B
    //    crosses B before E, so the live chase intercepts at the gate first
    //    (path-sound). We can only chase back at the boundary, or abandon on TP.
    {
        // B0-carried: price already beyond the cap means the deferred chase has
        // certainly resolved by now — book it at the carried open, whatever
        // this bar does next (the mh03 counterfactual has been in the trade
        // since D at this exact price).
        if let Some(px) = b0_carry {
            return HybridAction::Fill {
                px,
                side: EntryFeeSide::Taker,
                path: HybridPath::DeferredChase,
            };
        }
        // TP reached AND boundary re-approached in the same bar → unknowable
        // order → abandon (pessimistic).
        if tp_range && boundary_from_above {
            return HybridAction::Abandon(HybridAbandon::TpRange);
        }
        // Price fell back to the cap → chase-intercept at the boundary (taker).
        if boundary_from_above {
            return HybridAction::Fill {
                px: boundary,
                side: EntryFeeSide::Taker,
                path: HybridPath::BoundaryFromAbove,
            };
        }
        // TP reached without a re-approach → move gone.
        if tp_range {
            return HybridAction::Abandon(HybridAbandon::TpRange);
        }
    }

    HybridAction::Wait
}

impl PaperTrader {
    pub fn new(min_score: f64, rr_target: f64, max_hold_candles: usize) -> Self {
        Self {
            min_score,
            rr_target,
            max_hold_candles,
            params: crate::params::Params::new(),
            use_fees: false,
            stop_gap_bps_default: 0.0,
            stop_gap_bps_asset: HashMap::new(),
            breakeven_r: 0.0,
            trail_lock_r: 0.0,
            partial_tp_r: 0.0,
            risk_frac: 0.0,
            account_size: 10_000.0,
            // Honest defaults: no signal-bar fill, no slippage, stop-first.
            allow_signal_bar_fill: false,
            entry_slippage_r: 0.0,
            intrabar_stop_first: true,
            // Hybrid fill model OFF by default → limit model behavior unchanged.
            hybrid_fill: false,
            // Tick fill model OFF by default → OHLC behavior unchanged.
            tick_fill: false,
            tick_store: None,
            tick_fallback_bars: 0,
            tick_resolved_bars: 0,
            tick_walked: 0,
            tick_chase: true,
            chase_r: 0.1,
            chase_requires_seed: true,
            immediate_chase_at_open: true,
            race_maker_first: false,
            deferred_chase_at_open: false,
            past_entry_fee: EntryFeeSide::Taker,
            hybrid_counters: HybridFillCounters::default(),
            cancel_on_target_consumed: false,
            cancel_on_setup_invalidated: false,
            trades: Vec::new(),
            l2_annotations: HashMap::new(),
            open_trades: Vec::new(),
            pending: Vec::new(),
            rest_on_ready_fill: false,
            rest_min_lead_secs: 60,
            // De-risk exit OFF by default.
            derisk_after_min: 0,
            derisk_below_r: 0.0,
            trade_ready_at: HashMap::new(),
            opportunities_seen: 0,
            opportunities_taken: 0,
            skips: HashMap::new(),
            hold_counts: HashMap::new(),
            recent_candles: HashMap::new(),
            watermarks: HashMap::new(),
            partial_taken: HashSet::new(),
            entry_fee_side: HashMap::new(),
            first_touch_ids: HashSet::new(),
            resting_intervals: Vec::new(),
            pending_placed_at: HashMap::new(),
            closed_this_bar: Vec::new(),
            stop_overrides: HashMap::new(),
        }
    }

    /// Place a resting entry order for an admitted opportunity.
    ///
    /// This is the seam between a strategy and the fill simulator. The
    /// take/skip verdict is DECIDED BY THE CALLER — normally
    /// [`crate::strategy::Strategy::admit`] — and passed in already made, so
    /// the trader carries no strategy state and no gate logic of its own.
    /// Everything after this point is pure execution modelling.
    ///
    /// A `Take` means a real resting entry LIMIT is PLACED, not that the
    /// position is open. The trade is only booked when a later candle's range
    /// actually reaches the entry (`update_prices` → `fill_action`). If price
    /// never gets there before the deadline, the limit is cancelled and NO
    /// TRADE is recorded: `opportunities_taken`, `trades`, `open_trades`,
    /// win/loss counts and R are all deferred to fill time.
    ///
    /// Returns the provisional trade template on a placement, `None` on a
    /// skip. The return value signals "order placed" for callers that track
    /// live orders; it does NOT mean the trade was taken.
    pub fn evaluate(&mut self, opp: &Opportunity, decision: &Decision) -> Option<PaperTrade> {
        self.opportunities_seen += 1;

        let take: &TakeParams = match decision {
            Decision::Take(t) => t,
            Decision::Skip(reason) => {
                *self.skips.entry(reason.as_str().to_string()).or_insert(0) += 1;
                return None;
            }
        };

        self.place_order(opp, take)
    }

    /// Convenience wrapper: admit `opp` with `strategy` and place the order if
    /// it says take. Equivalent to calling [`Self::admit_context`],
    /// [`crate::strategy::Strategy::admit`] and [`Self::evaluate`] in sequence.
    pub fn evaluate_with<S: crate::strategy::Strategy + ?Sized>(
        &mut self,
        opp: &Opportunity,
        strategy: &S,
    ) -> Option<PaperTrade> {
        let recent: Option<Vec<Candle>> = self
            .recent_candles
            .get(&opp.asset)
            .map(|buf| buf.iter().cloned().collect());
        let ctx = crate::strategy::AdmitContext::new(self.min_score, self.rr_target)
            .with_params(self.params.clone())
            .with_recent(recent.as_deref());
        let decision = strategy.admit(opp, &ctx);
        self.evaluate(opp, &decision)
    }

    /// Build the admission context this trader would hand a strategy: the
    /// effective score floor and R:R target, the knob bag, and the recent
    /// candles for `asset`.
    ///
    /// Exposed for callers that want to admit an opportunity themselves (to
    /// inspect or journal the verdict) before passing it to [`Self::evaluate`].
    pub fn admit_context<'a>(
        &'a self,
        asset: u16,
        scratch: &'a mut Vec<Candle>,
    ) -> crate::strategy::AdmitContext<'a> {
        scratch.clear();
        if let Some(buf) = self.recent_candles.get(&asset) {
            scratch.extend(buf.iter().cloned());
        }
        crate::strategy::AdmitContext::new(self.min_score, self.rr_target)
            .with_params(self.params.clone())
            .with_recent(Some(scratch.as_slice()))
    }

    /// Place the resting entry limit for an admitted opportunity. Split out of
    /// `evaluate` so the skip bookkeeping and the placement mechanics stay
    /// separable.
    fn place_order(&mut self, opp: &Opportunity, take: &TakeParams) -> Option<PaperTrade> {
        let trade = PaperTrade {
            opportunity_id: opp.id.clone(),
            signal_type: opp.signal_type,
            asset: opp.asset,
            timeframe: opp.timeframe,
            direction: opp.direction,
            entry: take.entry,
            stop: take.stop,
            tp: take.tp,
            fill: take.entry, // provisional; overwritten with the real fill price
            score: opp.score,
            opened_at: opp.created_at,
            filled_at: None, // stamped by book_fill / tick booking
            closed_at: None,
            result: TradeResult::Inconclusive,
            r_pnl: 0.0,
            fee_r: 0.0,
        };

        let deadline = opp.created_at + chrono::Duration::seconds(FILL_DEADLINE_SECS);

        // Hybrid seed: did T−1 — the last closed candle before the signal bar —
        // already trade to the entry? At placement time `recent_candles.back()`
        // IS T−1 (the signal bar T has not been pushed yet; it arrives via the
        // next `update_prices`). T's own touch is OR-ed in when T is first seen
        // in `process_pending_fills`. If T−1 is absent (no history yet), only T
        // can contribute.
        let hybrid_pred_touch = self
            .recent_candles
            .get(&opp.asset)
            .and_then(|buf| buf.back())
            .map(|k| match opp.direction {
                Direction::Bull => k.low <= take.entry,
                Direction::Bear => k.high >= take.entry,
            })
            .unwrap_or(false);

        // Concurrency diagnostic: remember when this limit was placed so we can
        // close out its resting interval when it resolves.
        self.pending_placed_at
            .insert(opp.id.clone(), opp.created_at);

        self.pending.push(PendingFill {
            trade,
            deadline,
            seen_first_candle: false,
            age_anchor: None,
            hybrid_pred_touch,
            hybrid_armed: None,
            hybrid_seen_decision_bar: false,
            hybrid_b0_open: None,
            min_target: None,
            ready_at: None,
            cancel_pending: None,
        });

        Some(self.pending.last().unwrap().trade.clone())
    }

    /// Stamp a rest-on-ready placement time and a target-consumed threshold on
    /// the most recently placed pending limit.
    ///
    /// `ready_at` is when the strategy's setup became final and a real order
    /// could have gone on the book, which may precede the signal bar; the
    /// rest-on-ready fill lens uses the lead time to decide whether the signal
    /// bar itself may fill maker. `min_target` is the price beyond which the
    /// move is considered already spent, cancelling an unfilled limit when
    /// `cancel_on_target_consumed` is set. Both are optional strategy hints
    /// with no effect unless the corresponding lens knob is on.
    pub fn annotate_last_pending(
        &mut self,
        ready_at: Option<NaiveDateTime>,
        min_target: Option<f64>,
    ) {
        if let Some(pf) = self.pending.last_mut() {
            pf.ready_at = ready_at;
            pf.min_target = min_target;
        }
        if let (Some(ra), Some(pf)) = (ready_at, self.pending.last()) {
            self.trade_ready_at
                .insert(pf.trade.opportunity_id.clone(), ra);
        }
    }

    pub fn update_prices(&mut self, candle: &Candle) {
        self.closed_this_bar.clear();
        // Tick-resolution fill mode: if enabled AND this bar has real trade
        // ticks, resolve entries/stops/TPs by walking those ticks in time order
        // (exact crossing timestamps). A bar with no ticks falls through to the
        // OHLC path below (counted). All other modes are unaffected.
        if self.tick_fill {
            let bar_ticks: Vec<Tick> = self
                .tick_store
                .as_mut()
                .map(|s| s.ticks_for_bar(candle.timestamp).to_vec())
                .unwrap_or_default();
            if !bar_ticks.is_empty() {
                self.tick_resolved_bars += 1;
                self.tick_walked += bar_ticks.len();
                self.update_prices_tick(candle, &bar_ticks);
                return;
            }
            // No ticks for this bar → OHLC fallback (below). Count only when a
            // tick store actually exists (else tick mode was requested with no
            // data and every bar "falls back" trivially).
            if self.tick_store.is_some() {
                self.tick_fallback_bars += 1;
            }
        }
        self.update_prices_ohlc(candle);
    }

    /// The original OHLC (candle-based) price-update path. Renamed from
    /// `update_prices` so the tick path can fall back to it per-bar. Behavior is
    /// byte-identical to before for every non-tick mode.
    fn update_prices_ohlc(&mut self, candle: &Candle) {
        // Track rolling 2h
        let buf = self
            .recent_candles
            .entry(candle.asset)
            .or_insert_with(|| VecDeque::with_capacity(120));
        buf.push_back(candle.clone());
        if buf.len() > 120 {
            buf.pop_front();
        }

        // ─── 1) Resting entry limits: fill, expire, or keep waiting ──────────
        // Snapshot the trades already open BEFORE this candle: only those are
        // raced now. A trade FILLED on this candle joins `open_trades` but is not
        // raced until the next candle — matching live (SL/TP are placed only
        // after the entry confirms) and the pre-honest model (race began the
        // candle after opened_at). `process_pending_fills` appends any freshly
        // filled trade to `self.open_trades`, which we preserve past the race.
        let open_trades = std::mem::take(&mut self.open_trades);
        self.process_pending_fills(candle);

        let mut still_open = Vec::new();
        // 5th field: is_stop_exit — true only for a genuine stop-loss hit (not
        // TP, not a timeout/de-risk close). Feeds the stop-exit gap penalty in
        // `close_trade`.
        let mut closes: Vec<(String, TradeResult, f64, NaiveDateTime, bool)> = Vec::new();
        for trade in open_trades {
            if trade.asset != candle.asset {
                still_open.push(trade);
                continue;
            }

            let clock = self
                .hold_counts
                .entry(trade.opportunity_id.clone())
                .or_default();

            // R_planned is the ONLY divisor for P&L. Slippage moved `fill`, not
            // this risk unit, so a worse entry shows up as a uniform drag
            // (losses read −(1+slip)R, wins read (planned_RR − slip)R).
            let risk = (trade.entry - trade.stop).abs();

            if clock.tick(self.max_hold_candles) == HoldTick::Timeout {
                // Timeout close at candle close, measured from the actual fill.
                let timeout_r = hold::timeout_r(trade.direction, trade.fill, candle.close, risk);
                closes.push((
                    trade.opportunity_id.clone(),
                    TradeResult::Inconclusive,
                    timeout_r,
                    candle.timestamp,
                    false,
                ));
                continue;
            }

            // Update watermark (always, needed for partial TP and breakeven),
            // measured as actual favorable R from the fill price.
            if risk > 0.0 {
                let favorable_r = if trade.direction == Direction::Bull {
                    (candle.high - trade.fill) / risk
                } else {
                    (trade.fill - candle.low) / risk
                };
                let wm = self
                    .watermarks
                    .entry(trade.opportunity_id.clone())
                    .or_insert(0.0);
                if favorable_r > *wm {
                    *wm = favorable_r;
                }

                // Track partial TP taken
                if self.partial_tp_r > 0.0 && *wm >= self.partial_tp_r {
                    self.partial_taken.insert(trade.opportunity_id.clone());
                }
            }

            // Apply trailing stop (locks R relative to the actual fill).
            let mut effective_stop = trade.stop;
            let mut trail_active = false;
            if self.breakeven_r > 0.0 && risk > 0.0 {
                let wm = self
                    .watermarks
                    .get(&trade.opportunity_id)
                    .copied()
                    .unwrap_or(0.0);
                if wm >= self.breakeven_r {
                    trail_active = true;
                    effective_stop = if trade.direction == Direction::Bull {
                        trade.fill + self.trail_lock_r * risk
                    } else {
                        trade.fill - self.trail_lock_r * risk
                    };
                }
            }
            // A strategy-managed stop (on_bar_close hook) wins over both the
            // planned stop and the trailing lock.
            if let Some(&s) = self.stop_overrides.get(&trade.opportunity_id) {
                effective_stop = s;
                trail_active = false;
            }

            // Loss/win R measured from `fill`, always ÷ R_planned (`risk`).
            let loss_r = |px_stop: f64| -> f64 {
                if risk <= 0.0 {
                    return -1.0;
                }
                if trade.direction == Direction::Bull {
                    (px_stop - trade.fill) / risk
                } else {
                    (trade.fill - px_stop) / risk
                }
            };
            let win_r = if risk > 0.0 {
                (trade.tp - trade.fill).abs() / risk
            } else {
                self.rr_target
            };

            let hit_stop = if trade.direction == Direction::Bull {
                candle.low <= effective_stop
            } else {
                candle.high >= effective_stop
            };
            let hit_tp = if trade.direction == Direction::Bull {
                candle.high >= trade.tp
            } else {
                candle.low <= trade.tp
            };

            // Intrabar tie-break: if one candle spans both, resolve per flag
            // (default stop-first, pessimistic).
            let resolve_stop = hit_stop && (!hit_tp || self.intrabar_stop_first);
            let resolve_tp = hit_tp && (!hit_stop || !self.intrabar_stop_first);

            if resolve_stop {
                let (result, r_pnl) = if trail_active {
                    (TradeResult::Inconclusive, self.trail_lock_r)
                } else {
                    (TradeResult::Loss, loss_r(effective_stop))
                };
                // Gap penalty applies to a genuine stop hit only, not a
                // trailing-stop lock-in: that exits at a profit-lock level, not
                // the raw stop, so it never suffers the trigger cascade the gap
                // models.
                closes.push((
                    trade.opportunity_id.clone(),
                    result,
                    r_pnl,
                    candle.timestamp,
                    !trail_active,
                ));
            } else if resolve_tp {
                closes.push((
                    trade.opportunity_id.clone(),
                    TradeResult::Win,
                    win_r,
                    candle.timestamp,
                    false,
                ));
            } else if self.derisk_after_min > 0
                && risk > 0.0
                && self
                    .hold_counts
                    .get(&trade.opportunity_id)
                    .map(|c| c.candles_held)
                    .unwrap_or(0)
                    > self.derisk_after_min
            {
                // De-risk exit: the trade survived the bar's stop/TP race but is
                // stale (> derisk_after_min bars ≈ minutes since fill) and its
                // unrealized R at this bar's CLOSE is below the threshold →
                // close at market (the close). A losing de-risk counts as a
                // Loss; a flat/positive one as Inconclusive (like a timeout).
                let unreal_r = if trade.direction == Direction::Bull {
                    (candle.close - trade.fill) / risk
                } else {
                    (trade.fill - candle.close) / risk
                };
                if unreal_r < self.derisk_below_r {
                    let result = if unreal_r < 0.0 {
                        TradeResult::Loss
                    } else {
                        TradeResult::Inconclusive
                    };
                    // De-risk is a management exit at the candle CLOSE, not a
                    // stop-price exit — no gap.
                    closes.push((
                        trade.opportunity_id.clone(),
                        result,
                        unreal_r,
                        candle.timestamp,
                        false,
                    ));
                } else {
                    still_open.push(trade);
                }
            } else {
                still_open.push(trade);
            }
        }
        for (opp_id, result, r_pnl, closed_at, is_stop_exit) in closes {
            self.close_trade(&opp_id, result, r_pnl, closed_at, is_stop_exit);
            self.watermarks.remove(&opp_id);
            self.partial_taken.remove(&opp_id);
            self.entry_fee_side.remove(&opp_id);
            self.stop_overrides.remove(&opp_id);
        }

        // Survivors of the race + any freshly-filled trades appended by
        // `process_pending_fills`, which — being fresh fills — are not raced
        // until the next candle.
        still_open.append(&mut self.open_trades);
        self.open_trades = still_open;
    }

    /// Tick-resolution price update for ONE completed 1m bar, given the bar's
    /// real trade `ticks` in ascending time order (non-empty; the caller handles
    /// the empty-bar OHLC fallback). This replaces the OHLC intrabar GUESS
    /// (`hit_stop`/`hit_tp` + `intrabar_stop_first`) with true ordering:
    ///
    ///   • Resting entry limits fill at the FIRST tick that reaches the entry
    ///     price (maker at `entry`), stamped with that tick's timestamp. The
    ///     signal bar is ineligible unless `allow_signal_bar_fill` (same honesty
    ///     rule as the limit model); the 2h deadline still cancels.
    ///   • Open trades resolve at the FIRST tick that crosses the (trail-adjusted)
    ///     stop OR the TP — whichever comes first in TICK TIME wins — closing at
    ///     that tick's timestamp and level. No stop-first/TP-first heuristic.
    ///   • A trade FILLED on this bar is raced against the LATER ticks of the
    ///     SAME bar (true intrabar precision), but its `hold_counts` clock still
    ///     starts next bar (consistent with the OHLC model and live: SL/TP go on
    ///     only after the entry confirms).
    ///
    /// max_hold timeout, watermark/trailing stop, and fees all match
    /// `update_prices_ohlc`; only the intrabar resolution differs. The exit
    /// R is measured from `fill` ÷ `R_planned`, identical to the OHLC path.
    fn update_prices_tick(&mut self, candle: &Candle, ticks: &[Tick]) {
        // Rolling 2h candle buffer (feeds the chase seed) — same as OHLC path.
        let buf = self
            .recent_candles
            .entry(candle.asset)
            .or_insert_with(|| VecDeque::with_capacity(120));
        buf.push_back(candle.clone());
        if buf.len() > 120 {
            buf.pop_front();
        }

        // ─── 1) Pending entry limits: mark the signal bar, honor the deadline,
        // and pre-compute the first tick index at which each still-eligible limit
        // fills. We do NOT book yet — booking is interleaved with stop/TP in the
        // single tick walk below so a same-bar fill can be raced by later ticks.
        let pending = std::mem::take(&mut self.pending);
        let mut still_pending = Vec::new();
        // (pending_index_into_fills, fill_tick_index, fill_px, fee_side) for limits
        // that will fill. fill_px/fee_side = maker-at-entry normally; under
        // tick_chase (fill-lens field, default ON) a limit that reaches the
        // boundary B before E resolves to a taker chase at B (faithful
        // entry-chase on real tick order). ICT_TICK_CHASE=1|0 force-overrides
        // the config for quick A/B without editing the lens file.
        let tick_chase = match std::env::var("ICT_TICK_CHASE").as_deref() {
            Ok("1") => true,
            Ok("0") => false,
            _ => self.tick_chase,
        };
        let mut armed_fills: Vec<(usize, usize, f64, EntryFeeSide)> = Vec::new();
        let mut fills: Vec<PendingFill> = Vec::new();
        for mut pf in pending {
            if pf.trade.asset != candle.asset {
                still_pending.push(pf);
                continue;
            }
            let is_signal_bar = !pf.seen_first_candle;
            pf.seen_first_candle = true;

            // Deadline lapsed unfilled → cancel (never counted). Bar-open clock.
            if candle.timestamp > pf.deadline {
                self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                continue;
            }
            if is_signal_bar && !self.allow_signal_bar_fill {
                // Arm the chase seed the same way the hybrid path does (line ~1749):
                // seeded = T touched entry (any tick this bar) OR T−1 touched it.
                // Needed by the tick_chase branch on the decision bar.
                if pf.hybrid_armed.is_none() {
                    let d = pf.trade.direction;
                    let e = pf.trade.entry;
                    let t_touch = ticks.iter().any(|t| match d {
                        Direction::Bull => t.price <= e,
                        Direction::Bear => t.price >= e,
                    });
                    let seeded = t_touch || pf.hybrid_pred_touch;
                    pf.hybrid_armed = Some(if self.chase_requires_seed {
                        seeded
                    } else {
                        true
                    });
                }
                still_pending.push(pf);
                continue;
            }
            // First tick reaching the entry price → fill index (maker at entry).
            let dir = pf.trade.direction;
            let entry = pf.trade.entry;
            let maker_hit = ticks.iter().position(|t| match dir {
                Direction::Bull => t.price <= entry,
                Direction::Bear => t.price >= entry,
            });
            // tick_chase: faithful entry-chase on real tick order. When the
            // limit is armed, a tick reaching the boundary B = E ± chase_r·R
            // BEFORE any tick touches E is a chase — fill taker at B. Whichever
            // level a tick reaches first (in true tick time) wins.
            let armed = pf.hybrid_armed.unwrap_or(false);
            let resolved: Option<(usize, f64, EntryFeeSide)> =
                if tick_chase && armed && self.chase_r > 0.0 {
                    let risk = (pf.trade.entry - pf.trade.stop).abs();
                    let boundary = match dir {
                        Direction::Bull => entry + self.chase_r * risk,
                        Direction::Bear => entry - self.chase_r * risk,
                    };
                    let chase_hit = ticks.iter().position(|t| match dir {
                        Direction::Bull => t.price >= boundary,
                        Direction::Bear => t.price <= boundary,
                    });
                    match (maker_hit, chase_hit) {
                        (Some(mi), Some(ci)) if ci < mi => {
                            self.hybrid_counters.boundary_chases_from_below += 1;
                            Some((ci, boundary, EntryFeeSide::Taker))
                        }
                        (Some(mi), _) => {
                            self.hybrid_counters.maker_fills += 1;
                            Some((mi, entry, EntryFeeSide::Maker))
                        }
                        (None, Some(ci)) => {
                            self.hybrid_counters.boundary_chases_from_below += 1;
                            Some((ci, boundary, EntryFeeSide::Taker))
                        }
                        (None, None) => None,
                    }
                } else {
                    maker_hit.map(|idx| (idx, entry, EntryFeeSide::Maker))
                };
            match resolved {
                Some((idx, px, side)) => {
                    armed_fills.push((fills.len(), idx, px, side));
                    fills.push(pf);
                }
                None => still_pending.push(pf),
            }
        }
        self.pending = still_pending;

        // ─── 2) Single tick walk. At each tick, in time order: (a) book any
        // entry that fills at this tick, then (b) resolve stop/TP for every
        // currently-open trade for this asset. A trade booked at tick i is raced
        // only from tick i+1 onward (its own fill tick doesn't stop it).
        let open_trades = std::mem::take(&mut self.open_trades);
        // Per-open-trade running state for this bar. `filled_at_tick` = the tick
        // index this trade became open on (usize::MAX = was already open at bar
        // start, so it's raced from tick 0).
        struct Live {
            trade: PaperTrade,
            filled_at_tick: usize,
            counted_hold: bool,
        }
        let mut live: Vec<Live> = open_trades
            .into_iter()
            .map(|t| Live {
                trade: t,
                filled_at_tick: usize::MAX,
                counted_hold: false,
            })
            .collect();

        // Bump hold_counts once per open (pre-existing) trade for this bar, and
        // apply max_hold timeout at the bar close (measured, like OHLC, at the
        // candle close for trades that survive the whole bar). We defer timeout
        // to after the tick walk so an intrabar stop/TP wins over a same-bar
        // timeout (matches OHLC, where the race is checked before the count gate
        // only for the CURRENT bar — see note below).
        // 5th field: is_stop_exit (see `update_prices_ohlc`).
        let mut closes: Vec<(String, TradeResult, f64, NaiveDateTime, bool)> = Vec::new();

        // Increment hold for pre-existing open trades and short-circuit any that
        // exceed max_hold BEFORE racing (mirrors OHLC ordering: the count gate is
        // evaluated first; a timed-out trade closes at this bar's close).
        let mut timed_out_ids: HashSet<String> = HashSet::new();
        for lv in live.iter_mut() {
            if lv.trade.asset != candle.asset {
                continue;
            }
            let clock = self
                .hold_counts
                .entry(lv.trade.opportunity_id.clone())
                .or_default();
            lv.counted_hold = true;
            if clock.tick(self.max_hold_candles) == HoldTick::Timeout {
                let risk = (lv.trade.entry - lv.trade.stop).abs();
                let timeout_r =
                    hold::timeout_r(lv.trade.direction, lv.trade.fill, candle.close, risk);
                closes.push((
                    lv.trade.opportunity_id.clone(),
                    TradeResult::Inconclusive,
                    timeout_r,
                    candle.timestamp,
                    false,
                ));
                timed_out_ids.insert(lv.trade.opportunity_id.clone());
            }
        }
        // Drop timed-out trades from the live race set.
        live.retain(|lv| !timed_out_ids.contains(&lv.trade.opportunity_id));

        let mut resolved_ids: HashSet<String> = HashSet::new();
        for (ti, tick) in ticks.iter().enumerate() {
            // (a) Book entries that fill at this tick (maker at entry). Their
            // race starts at the NEXT tick.
            for (fi, fidx, fill_px, fee_side) in armed_fills.iter() {
                if *fidx != ti {
                    continue;
                }
                let pf = &fills[*fi];
                self.end_resting(&pf.trade.opportunity_id, tick.timestamp);
                let mut trade = pf.trade.clone();
                // Book at the resolved fill price / fee side. Default = maker at
                // entry (a resting limit fills at its price when a trade prints
                // through it). Under tick_chase a boundary-first tick books a
                // taker chase at B. Stamp the fill time to this tick — the whole
                // point of tick mode.
                trade.fill = *fill_px;
                trade.opened_at = pf.trade.opened_at; // signal time (unchanged)
                trade.filled_at = Some(tick.timestamp);
                self.entry_fee_side
                    .insert(trade.opportunity_id.clone(), *fee_side);
                self.trades.push(trade.clone());
                self.opportunities_taken += 1;
                self.hold_counts
                    .insert(trade.opportunity_id.clone(), HoldClock::default());
                self.watermarks.insert(trade.opportunity_id.clone(), 0.0);
                live.push(Live {
                    trade,
                    filled_at_tick: ti,
                    counted_hold: true,
                });
            }

            // (b) Resolve stop/TP for every open trade against this tick. First
            // crossing wins; a trade filled at tick i is not raced until i+1.
            for lv in live.iter_mut() {
                if lv.trade.asset != candle.asset {
                    continue;
                }
                if resolved_ids.contains(&lv.trade.opportunity_id) {
                    continue;
                }
                if ti <= lv.filled_at_tick && lv.filled_at_tick != usize::MAX {
                    continue; // own fill tick or earlier — not yet racing
                }
                let trade = &lv.trade;
                let risk = (trade.entry - trade.stop).abs();
                if risk <= 0.0 {
                    continue;
                }
                // Watermark (favorable excursion) from the fill, updated per tick.
                let favorable_r = if trade.direction == Direction::Bull {
                    (tick.price - trade.fill) / risk
                } else {
                    (trade.fill - tick.price) / risk
                };
                let wm = self
                    .watermarks
                    .entry(trade.opportunity_id.clone())
                    .or_insert(0.0);
                if favorable_r > *wm {
                    *wm = favorable_r;
                }
                if self.partial_tp_r > 0.0 && *wm >= self.partial_tp_r {
                    self.partial_taken.insert(trade.opportunity_id.clone());
                }

                // Trailing stop (locks R relative to the fill) once the watermark
                // reaches breakeven_r — identical rule to OHLC.
                let mut effective_stop = trade.stop;
                let mut trail_active = false;
                if self.breakeven_r > 0.0 {
                    let wmv = self
                        .watermarks
                        .get(&trade.opportunity_id)
                        .copied()
                        .unwrap_or(0.0);
                    if wmv >= self.breakeven_r {
                        trail_active = true;
                        effective_stop = if trade.direction == Direction::Bull {
                            trade.fill + self.trail_lock_r * risk
                        } else {
                            trade.fill - self.trail_lock_r * risk
                        };
                    }
                }
                if let Some(&s) = self.stop_overrides.get(&trade.opportunity_id) {
                    effective_stop = s;
                    trail_active = false;
                }

                let hit_stop = match trade.direction {
                    Direction::Bull => tick.price <= effective_stop,
                    Direction::Bear => tick.price >= effective_stop,
                };
                let hit_tp = match trade.direction {
                    Direction::Bull => tick.price >= trade.tp,
                    Direction::Bear => tick.price <= trade.tp,
                };
                // True intrabar order: this single tick can't be both above TP
                // and below stop for a valid geometry, so at most one fires. If
                // (pathologically) both, stop wins (pessimistic), matching the
                // OHLC default.
                if hit_stop {
                    let (result, r_pnl) = if trail_active {
                        (TradeResult::Inconclusive, self.trail_lock_r)
                    } else {
                        let r = if trade.direction == Direction::Bull {
                            (effective_stop - trade.fill) / risk
                        } else {
                            (trade.fill - effective_stop) / risk
                        };
                        (TradeResult::Loss, r)
                    };
                    closes.push((
                        trade.opportunity_id.clone(),
                        result,
                        r_pnl,
                        tick.timestamp,
                        !trail_active,
                    ));
                    resolved_ids.insert(trade.opportunity_id.clone());
                } else if hit_tp {
                    let win_r = (trade.tp - trade.fill).abs() / risk;
                    closes.push((
                        trade.opportunity_id.clone(),
                        TradeResult::Win,
                        win_r,
                        tick.timestamp,
                        false,
                    ));
                    resolved_ids.insert(trade.opportunity_id.clone());
                }
            }
        }

        // De-risk exit (management): trades that survived the tick race but are
        // stale (> derisk_after_min bars since fill) and below the unrealized-R
        // threshold at this bar's CLOSE exit at market. Mirrors the OHLC path;
        // evaluated bar-close granular (a de-risk is a management decision, not
        // a tick event). No-op unless `derisk_after_min > 0`.
        if self.derisk_after_min > 0 {
            for lv in live.iter() {
                let t = &lv.trade;
                if t.asset != candle.asset
                    || resolved_ids.contains(&t.opportunity_id)
                    || timed_out_ids.contains(&t.opportunity_id)
                {
                    continue;
                }
                let held = self
                    .hold_counts
                    .get(&t.opportunity_id)
                    .map(|c| c.candles_held)
                    .unwrap_or(0);
                if held <= self.derisk_after_min {
                    continue;
                }
                let risk = (t.entry - t.stop).abs();
                if risk <= 0.0 {
                    continue;
                }
                let unreal_r = if t.direction == Direction::Bull {
                    (candle.close - t.fill) / risk
                } else {
                    (t.fill - candle.close) / risk
                };
                if unreal_r < self.derisk_below_r {
                    let result = if unreal_r < 0.0 {
                        TradeResult::Loss
                    } else {
                        TradeResult::Inconclusive
                    };
                    closes.push((
                        t.opportunity_id.clone(),
                        result,
                        unreal_r,
                        candle.timestamp,
                        false,
                    ));
                    resolved_ids.insert(t.opportunity_id.clone());
                }
            }
        }

        // Apply all closes (fees, partial TP, closed_at) via the shared path.
        for (opp_id, result, r_pnl, closed_at, is_stop_exit) in closes {
            self.close_trade(&opp_id, result, r_pnl, closed_at, is_stop_exit);
            self.watermarks.remove(&opp_id);
            self.partial_taken.remove(&opp_id);
            self.entry_fee_side.remove(&opp_id);
            self.stop_overrides.remove(&opp_id);
        }

        // Survivors of the tick race stay open for the next bar.
        let survivors: Vec<PaperTrade> = live
            .into_iter()
            .filter(|lv| {
                !resolved_ids.contains(&lv.trade.opportunity_id)
                    && !timed_out_ids.contains(&lv.trade.opportunity_id)
            })
            .map(|lv| lv.trade)
            .collect();

        let mut survivors = survivors;
        survivors.append(&mut self.open_trades);
        self.open_trades = survivors;
    }

    /// Advance every resting entry limit for this candle's asset: fill it (→
    /// `open_trades`, counted as taken), keep it waiting, or cancel it unfilled
    /// (dropped; NO TRADE). Dispatches to the limit model (`fill_action`) or the
    /// hybrid model (`hybrid_fill_action`) per `hybrid_fill`.
    fn process_pending_fills(&mut self, candle: &Candle) {
        if self.hybrid_fill {
            self.process_pending_fills_hybrid(candle);
        } else {
            self.process_pending_fills_limit(candle);
        }
    }

    /// Pure resting-limit fill model (the original, unchanged behavior). The
    /// signal bar (the first candle a pending fill sees) is ineligible unless
    /// `allow_signal_bar_fill` is set; the 2h `FILL_DEADLINE_SECS` cancels it.
    fn process_pending_fills_limit(&mut self, candle: &Candle) {
        let pending = std::mem::take(&mut self.pending);
        let mut still_pending = Vec::new();
        for mut pf in pending {
            if pf.trade.asset != candle.asset {
                still_pending.push(pf);
                continue;
            }

            let is_signal_bar = !pf.seen_first_candle;
            pf.seen_first_candle = true;

            // Deadline lapsed unfilled → cancel (mirror live "opportunity
            // invalidated"): drop it, never counted anywhere.
            if candle.timestamp > pf.deadline {
                self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                continue;
            }

            let r_planned = (pf.trade.entry - pf.trade.stop).abs();
            match fill_action(
                pf.trade.direction,
                pf.trade.entry,
                r_planned,
                candle,
                is_signal_bar,
                self.allow_signal_bar_fill,
                self.entry_slippage_r,
            ) {
                Some(fill_px) => {
                    self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                    self.book_fill(pf.trade, fill_px, EntryFeeSide::Maker, candle.timestamp);
                }
                None => still_pending.push(pf),
            }
        }
        self.pending = still_pending;
    }

    /// Live-faithful hybrid fill model. The signal bar T (first candle seen) only
    /// finalizes the seed/arm state and is otherwise ineligible; the decision bar
    /// D (second candle seen) and later bars drive `hybrid_fill_action`. Abandons
    /// (NO TRADE) increment the matching breakdown counter; the 30-min
    /// `HYBRID_ENTRY_MAX_AGE_SECS` is the abandon horizon (via `past_age`), not
    /// the 2h `FILL_DEADLINE_SECS` (which still bounds a data-runs-out drop).
    fn process_pending_fills_hybrid(&mut self, candle: &Candle) {
        let pending = std::mem::take(&mut self.pending);
        let mut still_pending = Vec::new();
        for mut pf in pending {
            if pf.trade.asset != candle.asset {
                still_pending.push(pf);
                continue;
            }

            // Cancel watchdogs: a cancel confirmed by a PRIOR completed bar
            // (target consumed at its close, or the setup invalidated as of it
            // — marked externally via `cancel_pending_invalidated`) kills the resting
            // limit NOW, before any fill attempt on this bar. A condition that
            // only becomes true on THIS bar is handled at the bottom (flag set
            // after a failed fill attempt → applied next bar), so a same-bar
            // touch+condition always resolves to "fill stands" (pessimistic).
            if let Some(reason) = pf.cancel_pending {
                match reason {
                    HybridAbandon::TargetConsumed => {
                        self.hybrid_counters.abandon_target_consumed += 1
                    }
                    HybridAbandon::SetupInvalidated => {
                        self.hybrid_counters.abandon_setup_invalidated += 1
                    }
                    _ => unreachable!("cancel_pending only carries watchdog reasons"),
                }
                self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                // Dropped: NO TRADE, never counted in trades/W/L/R.
                continue;
            }

            // First candle for this pending fill IS the signal bar T. Finalize the
            // seed: `seeded = T.touch || T−1.touch`, then `armed = requires_seed ?
            // seeded : true`. T itself is never eligible to fill (order didn't
            // exist yet) — just record arming and wait for D.
            if !pf.seen_first_candle {
                pf.seen_first_candle = true;
                // T's stamp = the moment the order starts existing → the 30-min
                // age-abandon anchor. Equals `opened_at` for 1m signals; later
                // than it for HTF (the label predates the bar's close).
                pf.age_anchor = Some(candle.timestamp);
                let t_touch = match pf.trade.direction {
                    Direction::Bull => candle.low <= pf.trade.entry,
                    Direction::Bear => candle.high >= pf.trade.entry,
                };
                // rest-on-Ready execution lens: the limit was placed at Ready,
                // not after T — so when Ready precedes T by at least
                // `rest_min_lead_secs`, the order was resting on the book when
                // T opened and T's touch of the entry fills it MAKER at exactly
                // the entry. This is the honest version of the phantom signal-
                // bar fill: eligibility is earned by the measured Ready→touch
                // lead, not assumed. Same-bar Ready (lead 0) or a missing
                // ready_at falls through to the standard T handling below —
                // live places that order after T closes, exactly today's path.
                if self.rest_on_ready_fill && t_touch {
                    let lead_ok = pf.ready_at.is_some_and(|ra| {
                        (pf.trade.opened_at - ra).num_seconds() >= self.rest_min_lead_secs
                    });
                    if lead_ok {
                        self.hybrid_counters.rested_maker_fills += 1;
                        let fill_bar_hit_stop = match pf.trade.direction {
                            Direction::Bull => candle.low <= pf.trade.stop,
                            Direction::Bear => candle.high >= pf.trade.stop,
                        };
                        if fill_bar_hit_stop {
                            self.hybrid_counters.first_touch_stop_fills += 1;
                            self.first_touch_ids.insert(pf.trade.opportunity_id.clone());
                        }
                        self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                        let entry_px = pf.trade.entry;
                        self.book_fill(pf.trade, entry_px, EntryFeeSide::Maker, candle.timestamp);
                        continue;
                    }
                }
                let seeded = t_touch || pf.hybrid_pred_touch;
                pf.hybrid_armed = Some(if self.chase_requires_seed {
                    seeded
                } else {
                    true
                });
                // T is a completed bar the live watchdog would see before D:
                // its close consuming the target cancels the order at D.
                self.flag_target_consumed(&mut pf, candle);
                still_pending.push(pf);
                continue;
            }

            // The decision bar D is the first candle after T (the first to reach
            // here past the T-continue above); later bars pass false.
            let is_decision_bar = !pf.hybrid_seen_decision_bar;
            pf.hybrid_seen_decision_bar = true;

            // Hard data-deadline: 2h opportunity expiry. The 30-min age abandon is
            // handled inside `hybrid_fill_action`; this only catches the case where
            // the loop somehow rests past 2h (defensive) → drop as deadline.
            if candle.timestamp > pf.deadline {
                self.hybrid_counters.abandon_deadline += 1;
                self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                continue;
            }

            let r_planned = (pf.trade.entry - pf.trade.stop).abs();
            let past_age = (candle.timestamp - pf.age_anchor.unwrap_or(pf.trade.opened_at))
                .num_seconds()
                > HYBRID_ENTRY_MAX_AGE_SECS;
            let armed = pf.hybrid_armed.unwrap_or(false);

            // Deferred open-chase (`deferred_chase_at_open`): a decision bar
            // that opens in front of the entry within the chase cap (armed,
            // immediate chase off) stamps its open as this limit's carried
            // chase price — the exact fill the immediate-chase lens would have
            // booked here. Every later non-maker resolution books at it.
            if is_decision_bar
                && armed
                && self.deferred_chase_at_open
                && !self.immediate_chase_at_open
            {
                let (open_front, in_cap) = match pf.trade.direction {
                    Direction::Bull => (
                        candle.open > pf.trade.entry,
                        candle.open <= pf.trade.entry + self.chase_r * r_planned,
                    ),
                    Direction::Bear => (
                        candle.open < pf.trade.entry,
                        candle.open >= pf.trade.entry - self.chase_r * r_planned,
                    ),
                };
                if open_front && in_cap {
                    pf.hybrid_b0_open = Some(candle.open);
                }
            }

            let action = hybrid_fill_action(
                pf.trade.direction,
                pf.trade.entry,
                pf.trade.stop,
                pf.trade.tp,
                r_planned,
                self.chase_r,
                armed,
                self.immediate_chase_at_open,
                self.race_maker_first,
                pf.hybrid_b0_open,
                self.past_entry_fee,
                candle,
                is_decision_bar,
                past_age,
            );

            match action {
                HybridAction::Fill { px, side, path } => {
                    match path {
                        HybridPath::PastEntry => self.hybrid_counters.past_entry_fills += 1,
                        HybridPath::ImmediateChase => self.hybrid_counters.immediate_chases += 1,
                        HybridPath::DeferredChase => self.hybrid_counters.deferred_chases += 1,
                        HybridPath::BoundaryFromBelow => {
                            self.hybrid_counters.boundary_chases_from_below += 1
                        }
                        HybridPath::BoundaryFromAbove => {
                            self.hybrid_counters.boundary_chases_from_above += 1
                        }
                        HybridPath::Maker => self.hybrid_counters.maker_fills += 1,
                    }
                    // First-touch-loser diagnostic: did the FILLING bar's own range
                    // also reach the stop? Purely attribution — the P&L race still
                    // begins next candle in book_fill.
                    let fill_bar_hit_stop = match pf.trade.direction {
                        Direction::Bull => candle.low <= pf.trade.stop,
                        Direction::Bear => candle.high >= pf.trade.stop,
                    };
                    if fill_bar_hit_stop {
                        self.hybrid_counters.first_touch_stop_fills += 1;
                        self.first_touch_ids.insert(pf.trade.opportunity_id.clone());
                    }
                    self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                    self.book_fill(pf.trade, px, side, candle.timestamp);
                }
                HybridAction::Wait => {
                    // Still unfilled after this bar's fill attempt: if this bar's
                    // CLOSE consumed the target, flag the cancel for the next bar
                    // (never this one — the fill attempt above already had its
                    // chance, preserving the same-bar "fill stands" rule).
                    self.flag_target_consumed(&mut pf, candle);
                    still_pending.push(pf)
                }
                HybridAction::Abandon(reason) => {
                    match reason {
                        HybridAbandon::GapStop => self.hybrid_counters.abandon_gap_stop += 1,
                        HybridAbandon::TpOpen => self.hybrid_counters.abandon_tp_open += 1,
                        HybridAbandon::TpRange => self.hybrid_counters.abandon_tp_range += 1,
                        HybridAbandon::Age => self.hybrid_counters.abandon_age += 1,
                        // Watchdog cancels are applied at the top of the loop
                        // (from `cancel_pending`), never returned by
                        // `hybrid_fill_action`.
                        HybridAbandon::TargetConsumed | HybridAbandon::SetupInvalidated => {
                            unreachable!("watchdog cancels don't come from hybrid_fill_action")
                        }
                    }
                    self.end_resting(&pf.trade.opportunity_id, candle.timestamp);
                    // Dropped: NO TRADE, never counted in trades/W/L/R.
                }
            }
        }
        self.pending = still_pending;
    }

    /// If `cancel_on_target_consumed` is on and this COMPLETED bar's close is
    /// at or beyond the pending's stamped `min_target`, flag the cancel to be
    /// applied at the start of the NEXT processed bar. Evaluated after the
    /// fill attempt, so a same-bar touch always wins: the move being spent is
    /// only actionable once a bar has closed on it.
    fn flag_target_consumed(&self, pf: &mut PendingFill, candle: &Candle) {
        if !self.cancel_on_target_consumed || pf.cancel_pending.is_some() {
            return;
        }
        let Some(mt) = pf.min_target else { return };
        let consumed = match pf.trade.direction {
            Direction::Bull => candle.close >= mt,
            Direction::Bear => candle.close <= mt,
        };
        if consumed {
            pf.cancel_pending = Some(HybridAbandon::TargetConsumed);
        }
    }

    /// Opportunity ids of the pending entry limits still eligible to be
    /// invalidated by the setup-invalidation watchdog.
    ///
    /// A driver polls this BEFORE it feeds the current bar to the strategy, so
    /// whatever it then checks reflects state as of the PREVIOUS bar's close,
    /// and calls [`Self::cancel_pending_invalidated`] for any whose setup no
    /// longer holds. Polling afterwards would leak this bar's information into
    /// this bar's fill decision. Empty unless `cancel_on_setup_invalidated` is
    /// set.
    pub fn pending_watchdog_ids(&self) -> Vec<String> {
        if !self.cancel_on_setup_invalidated {
            return Vec::new();
        }
        self.pending
            .iter()
            .filter(|pf| pf.cancel_pending.is_none())
            .map(|pf| pf.trade.opportunity_id.clone())
            .collect()
    }

    /// Mark a pending entry limit for cancellation because the strategy's
    /// setup was invalidated (as of the PREVIOUS bar's close — see
    /// [`Self::pending_watchdog_ids`]).
    ///
    /// The cancel is applied at the start of the next `update_prices` bar,
    /// before any fill attempt on it. A condition arising on the fill bar
    /// itself therefore arrives too late and the fill stands, which is the
    /// pessimistic reading: we never cancel with information the order
    /// could not have had.
    pub fn cancel_pending_invalidated(&mut self, opp_id: &str) {
        if !self.cancel_on_setup_invalidated {
            return;
        }
        for pf in self.pending.iter_mut() {
            if pf.trade.opportunity_id == opp_id && pf.cancel_pending.is_none() {
                pf.cancel_pending = Some(HybridAbandon::SetupInvalidated);
            }
        }
    }

    /// Opportunity ids of all still-resting (unfilled, uncancelled) entry
    /// limits, for a driver keeping its own order bookkeeping in sync.
    pub fn pending_ids(&self) -> Vec<String> {
        self.pending
            .iter()
            .map(|pf| pf.trade.opportunity_id.clone())
            .collect()
    }

    /// Cancel a still-resting entry limit outright — for a driver whose entry
    /// TTL lapsed, or whose setup was superseded. Removes it from `pending`; NO TRADE
    /// is recorded, exactly like the 2h deadline expiry. A no-op when the id is
    /// not resting (already filled, expired, or never existed); it never
    /// touches open trades.
    pub fn cancel_pending_by_id(&mut self, opp_id: &str, at: NaiveDateTime) {
        let before = self.pending.len();
        self.pending.retain(|pf| pf.trade.opportunity_id != opp_id);
        if self.pending.len() != before {
            self.end_resting(opp_id, at);
        }
    }

    /// Concurrency diagnostic: close out a resting entry limit's interval when it
    /// resolves (fill / abandon / expiry), from placement to `resolved_at`.
    fn end_resting(&mut self, opp_id: &str, resolved_at: NaiveDateTime) {
        if let Some(placed) = self.pending_placed_at.remove(opp_id) {
            self.resting_intervals.push((placed, resolved_at));
        }
    }

    /// Book a filled entry limit: record the fill price + entry-fee side, push to
    /// `trades`/`open_trades`, count it taken, and start its race NEXT candle
    /// (hold_counts starts at 0; this candle isn't raced). Shared by both models.
    fn book_fill(
        &mut self,
        mut trade: PaperTrade,
        fill_px: f64,
        entry_side: EntryFeeSide,
        filled_at: NaiveDateTime,
    ) {
        trade.fill = fill_px;
        trade.filled_at = Some(filled_at);
        self.entry_fee_side
            .insert(trade.opportunity_id.clone(), entry_side);
        self.trades.push(trade.clone());
        self.open_trades.push(trade.clone());
        self.opportunities_taken += 1;
        self.hold_counts
            .insert(trade.opportunity_id.clone(), HoldClock::default());
        self.watermarks.insert(trade.opportunity_id.clone(), 0.0);
    }

    /// Gapped stop-exit price for a genuine stop-loss exit: `stop*(1 -
    /// bps*1e-4)` for longs, `stop*(1 + bps*1e-4)` for shorts — always a WORSE
    /// (more adverse) fill than the raw stop, matching the live trigger-
    /// cascade cost. `bps <= 0` (the default) returns `stop` unchanged.
    fn gapped_stop_price(stop: f64, direction: Direction, bps: f64) -> f64 {
        if bps <= 0.0 {
            return stop;
        }
        let factor = bps * 1e-4;
        if direction == Direction::Bull {
            stop * (1.0 - factor)
        } else {
            stop * (1.0 + factor)
        }
    }

    fn close_trade(
        &mut self,
        opp_id: &str,
        result: TradeResult,
        r_pnl: f64,
        closed_at: NaiveDateTime,
        is_stop_exit: bool,
    ) {
        let had_partial = self.partial_taken.remove(opp_id);
        let mut closed_idx: Option<usize> = None;
        for (idx, trade) in self.trades.iter_mut().enumerate() {
            if trade.opportunity_id == opp_id && trade.result == TradeResult::Inconclusive {
                closed_idx = Some(idx);
                trade.result = result;
                // Apply partial TP adjustment: half banked at partial_tp_r, half gets final r_pnl
                let adjusted_r = if had_partial && self.partial_tp_r > 0.0 {
                    0.5 * self.partial_tp_r + 0.5 * r_pnl
                } else {
                    r_pnl
                };
                // Stop-exit gap penalty: ONLY a genuine stop-loss exit (not TP,
                // not max-hold/de-risk). Widens the effective exit price toward
                // the losing side and re-derives r_pnl/fee off it, in R units
                // of the ORIGINAL R_planned (|entry-stop|) — never rescaled.
                let risk = (trade.entry - trade.stop).abs();
                let gap_bps = if !is_stop_exit {
                    0.0
                } else if self.stop_gap_bps_asset.is_empty() {
                    // Skip the asset_name() intern-pool lookup entirely in the
                    // common case (no per-asset overrides configured) — avoids
                    // requiring every trade's asset id to have been interned
                    // via asset_id() first (many unit tests build trades with
                    // a bare asset id and never touch the intern pool).
                    self.stop_gap_bps_default
                } else {
                    let asset = asset_name(trade.asset);
                    self.stop_gap_bps_asset
                        .iter()
                        .find(|(k, _)| asset.contains(k.as_str()))
                        .map(|(_, v)| *v)
                        .unwrap_or(self.stop_gap_bps_default)
                };
                let (adjusted_r, exit_price_for_fee) = if gap_bps > 0.0 && risk > 0.0 {
                    let gapped = Self::gapped_stop_price(trade.stop, trade.direction, gap_bps);
                    let gap_r = if trade.direction == Direction::Bull {
                        (gapped - trade.stop) / risk
                    } else {
                        (trade.stop - gapped) / risk
                    };
                    (adjusted_r + gap_r, gapped)
                } else {
                    (adjusted_r, trade.stop)
                };
                if self.use_fees {
                    // Entry-fee side recorded at fill time (limit model always
                    // maker; hybrid varies). Exit is always taker.
                    let entry_side = self
                        .entry_fee_side
                        .get(opp_id)
                        .copied()
                        .unwrap_or(EntryFeeSide::Maker);
                    let fee = fees::fee_in_r_side(
                        trade.asset,
                        trade.entry,
                        exit_price_for_fee,
                        entry_side,
                    );
                    trade.fee_r = fee;
                    trade.r_pnl = adjusted_r - fee;
                } else {
                    trade.r_pnl = adjusted_r;
                }
                // First-touch-loser attribution: accumulate this trade's net R if
                // it filled on a bar that also reached the stop (diagnostic only).
                if self.first_touch_ids.remove(opp_id) {
                    self.hybrid_counters.first_touch_stop_r_milli +=
                        (trade.r_pnl * 1000.0).round() as i64;
                }
                trade.closed_at = Some(closed_at);
                break;
            }
        }
        if let Some(idx) = closed_idx {
            self.closed_this_bar.push((idx, is_stop_exit));
        }
    }

    /// Book a market entry at this bar's close, on behalf of a strategy's
    /// [`crate::strategy::Strategy::on_bar_close`] hook.
    ///
    /// The position is opened immediately at `candle.close` as an aggressing
    /// (taker) fill and joins the book like any other fresh fill: it is not
    /// raced until the next candle, and its hold clock starts then. `stop`
    /// must sit on the losing side of the close and `tp` on the winning side,
    /// else nothing is booked and `false` is returned. `opportunity_id` must
    /// be unique across the run.
    #[allow(clippy::too_many_arguments)]
    pub fn book_market_entry(
        &mut self,
        opportunity_id: &str,
        signal_type: u16,
        direction: Direction,
        stop: f64,
        tp: f64,
        score: f64,
        candle: &Candle,
    ) -> bool {
        let fill = candle.close;
        let take = TakeParams {
            entry: fill,
            stop,
            tp,
        };
        if !take.is_valid(direction) {
            return false;
        }
        if self
            .trades
            .iter()
            .any(|t| t.opportunity_id == opportunity_id)
        {
            return false;
        }
        let trade = PaperTrade {
            opportunity_id: opportunity_id.to_string(),
            signal_type,
            asset: candle.asset,
            timeframe: candle.timeframe,
            direction,
            entry: fill,
            stop,
            tp,
            fill,
            score,
            opened_at: candle.timestamp,
            filled_at: None,
            closed_at: None,
            result: TradeResult::Inconclusive,
            r_pnl: 0.0,
            fee_r: 0.0,
        };
        self.book_fill(trade, fill, EntryFeeSide::Taker, candle.timestamp);
        true
    }

    /// Move an open trade's stop. Applies from the next candle on; the planned
    /// `|entry - stop|` stays the R unit, so a stop moved to breakeven books a
    /// 0R exit rather than rescaling the trade. Returns `false` when no open
    /// trade has this id.
    pub fn set_open_stop(&mut self, opportunity_id: &str, stop: f64) -> bool {
        if !self
            .open_trades
            .iter()
            .any(|t| t.opportunity_id == opportunity_id)
        {
            return false;
        }
        self.stop_overrides
            .insert(opportunity_id.to_string(), stop);
        true
    }

    /// Move an open trade's take-profit. Applies from the next candle on.
    pub fn set_open_tp(&mut self, opportunity_id: &str, tp: f64) -> bool {
        match self
            .open_trades
            .iter_mut()
            .find(|t| t.opportunity_id == opportunity_id)
        {
            Some(t) => {
                t.tp = tp;
                true
            }
            None => false,
        }
    }

    /// The effective stop of an open trade: the strategy override if one is
    /// set, else the planned stop.
    pub fn effective_stop(&self, trade: &PaperTrade) -> f64 {
        self.stop_overrides
            .get(&trade.opportunity_id)
            .copied()
            .unwrap_or(trade.stop)
    }

    /// Close an open trade at this bar's close (a market exit). The R is
    /// measured from the fill over the planned risk; a negative result books
    /// as a loss, a flat or positive one as inconclusive — a management exit
    /// is neither a stop-out nor a target hit.
    pub fn close_open_at_market(&mut self, opportunity_id: &str, candle: &Candle) -> bool {
        let Some(pos) = self
            .open_trades
            .iter()
            .position(|t| t.opportunity_id == opportunity_id)
        else {
            return false;
        };
        let trade = self.open_trades.remove(pos);
        let risk = (trade.entry - trade.stop).abs();
        let r = hold::timeout_r(trade.direction, trade.fill, candle.close, risk);
        let result = if r < 0.0 {
            TradeResult::Loss
        } else {
            TradeResult::Inconclusive
        };
        self.close_trade(&trade.opportunity_id, result, r, candle.timestamp, false);
        self.watermarks.remove(&trade.opportunity_id);
        self.partial_taken.remove(&trade.opportunity_id);
        self.entry_fee_side.remove(&trade.opportunity_id);
        self.hold_counts.remove(&trade.opportunity_id);
        self.stop_overrides.remove(&trade.opportunity_id);
        true
    }

    pub fn close_remaining(&mut self) {
        for trade in &mut self.open_trades {
            for t in self.trades.iter_mut() {
                if t.opportunity_id == trade.opportunity_id && t.result == TradeResult::Inconclusive
                {
                    t.result = TradeResult::Inconclusive;
                    break;
                }
            }
        }
        self.open_trades.clear();
    }

    // ─── Stats ───────────────────────────────────────────────────────────────

    pub fn wins(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.result == TradeResult::Win)
            .count()
    }

    pub fn losses(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.result == TradeResult::Loss)
            .count()
    }

    pub fn win_rate(&self) -> f64 {
        let decided = self.wins() + self.losses();
        if decided == 0 {
            0.0
        } else {
            self.wins() as f64 / decided as f64 * 100.0
        }
    }

    pub fn expectancy(&self) -> f64 {
        let decided = self.wins() + self.losses();
        if decided == 0 {
            return 0.0;
        }
        let wr = self.wins() as f64 / decided as f64;
        wr * self.rr_target - (1.0 - wr)
    }

    pub fn total_r_pnl(&self) -> f64 {
        self.trades.iter().map(|t| t.r_pnl).sum()
    }

    /// Compounding equity curve. Each trade risks `risk_frac` of the account
    /// balance KNOWN AT ITS OPEN — only trades that have already closed by then
    /// contribute (what a live trader can actually size off, given concurrent
    /// positions). Balance updates by `risk_dollars * r_pnl` at each close.
    /// Returns (per-trade `(opportunity_id, equity_after, dollar_pnl)` in the
    /// trades' own order, final_balance, max_drawdown_fraction). Empty if
    /// `risk_frac <= 0`.
    pub fn compound_equity(&self) -> Option<EquityCurve> {
        if self.risk_frac <= 0.0 {
            return None;
        }
        let f = self.risk_frac;
        let start = self.account_size;
        // Event-driven over open/close times; closes win ties so freed capital
        // is available to a same-instant open. Index into self.trades.
        let n = self.trades.len();
        let mut events: Vec<(chrono::NaiveDateTime, u8, usize)> = Vec::with_capacity(n * 2);
        for (i, t) in self.trades.iter().enumerate() {
            let close = t.closed_at.unwrap_or(t.opened_at);
            events.push((t.opened_at, 0, i)); // open
            events.push((close, 1, i)); // close
        }
        // Sort by time; at equal time, closes (kind 1) before opens (kind 0).
        events.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

        let mut realized = start;
        let mut risk_at_open = vec![0.0_f64; n];
        let mut dollar_pnl = vec![0.0_f64; n];
        let mut equity_after = vec![start; n];
        let mut peak = start;
        let mut max_dd = 0.0_f64;
        for (_, kind, i) in events {
            if kind == 0 {
                risk_at_open[i] = if realized > 0.0 { f * realized } else { 0.0 };
            } else {
                let pnl = risk_at_open[i] * self.trades[i].r_pnl;
                realized += pnl;
                dollar_pnl[i] = pnl;
                equity_after[i] = realized;
                if realized > peak {
                    peak = realized;
                }
                if peak > 0.0 {
                    max_dd = max_dd.max((peak - realized) / peak);
                }
            }
        }
        let per_trade: Vec<(String, f64, f64)> = self
            .trades
            .iter()
            .enumerate()
            .map(|(i, t)| (t.opportunity_id.clone(), equity_after[i], dollar_pnl[i]))
            .collect();
        Some((per_trade, realized, max_dd))
    }

    pub fn total_fees(&self) -> f64 {
        self.trades.iter().map(|t| t.fee_r).sum()
    }

    pub fn gross_r_pnl(&self) -> f64 {
        self.trades.iter().map(|t| t.r_pnl + t.fee_r).sum()
    }

    pub fn inconclusive(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.result == TradeResult::Inconclusive)
            .count()
    }

    // ─── Reporting ───────────────────────────────────────────────────────────

    pub fn render_text(&self, label: &str) {
        use comfy_table::Table;

        println!();
        println!("\x1b[1;36m═══ Paper Trading Simulation ═══\x1b[0m");
        println!();
        println!("\x1b[1mSim:\x1b[0m {}", label);
        println!("\x1b[1mR:R target:\x1b[0m 1:{}", self.rr_target);
        println!("\x1b[1mMin score:\x1b[0m {}", self.min_score);
        println!();
        println!(
            "\x1b[1mOpportunities seen:\x1b[0m {}",
            self.opportunities_seen
        );
        println!("\x1b[1mTrades taken:\x1b[0m {}", self.opportunities_taken);
        println!();

        let decided = self.wins() + self.losses();
        if decided > 0 {
            let wr = self.win_rate();
            let wr_color = if wr >= 50.0 { "\x1b[32m" } else { "\x1b[31m" };
            println!("\x1b[32mWins:\x1b[0m {}  \x1b[31mLosses:\x1b[0m {}  \x1b[2mInconclusive:\x1b[0m {}",
                     self.wins(), self.losses(), self.inconclusive());
            if self.use_fees {
                println!(
                    "{}Win rate: {:.1}%\x1b[0m  Expectancy: {:.2}R",
                    wr_color,
                    wr,
                    self.expectancy()
                );
                println!(
                    "Gross P&L: {:.1}R  Fees: {:.1}R  \x1b[1mNet P&L: {:.1}R\x1b[0m",
                    self.gross_r_pnl(),
                    self.total_fees(),
                    self.total_r_pnl()
                );
            } else {
                println!(
                    "{}Win rate: {:.1}%\x1b[0m  Expectancy: {:.2}R  Total P&L: {:.1}R",
                    wr_color,
                    wr,
                    self.expectancy(),
                    self.total_r_pnl()
                );
            }
        } else {
            println!("\x1b[33mNo decided trades.\x1b[0m");
        }
        println!();

        // By signal type
        if !self.trades.is_empty() {
            let mut by_type: HashMap<u16, Vec<&PaperTrade>> = HashMap::new();
            for t in &self.trades {
                by_type.entry(t.signal_type).or_default().push(t);
            }

            let mut table = Table::new();
            if self.use_fees {
                table.set_header(vec![
                    "Signal Type",
                    "Taken",
                    "W",
                    "L",
                    "WR%",
                    "Gross (R)",
                    "Fees (R)",
                    "Net (R)",
                ]);
            } else {
                table.set_header(vec!["Signal Type", "Taken", "W", "L", "WR%", "P&L (R)"]);
            }
            let mut entries: Vec<_> = by_type.iter().collect();
            entries.sort_by_key(|(k, _)| **k);
            for (sig_type, trades) in entries {
                let sig_type = sig_type_name(*sig_type);
                let w = trades
                    .iter()
                    .filter(|t| t.result == TradeResult::Win)
                    .count();
                let l = trades
                    .iter()
                    .filter(|t| t.result == TradeResult::Loss)
                    .count();
                let d = w + l;
                let wr = if d > 0 {
                    format!("{:.0}", w as f64 / d as f64 * 100.0)
                } else {
                    "-".to_string()
                };
                let pnl: f64 = trades.iter().map(|t| t.r_pnl).sum();
                if self.use_fees {
                    let fees: f64 = trades.iter().map(|t| t.fee_r).sum();
                    let gross = pnl + fees;
                    table.add_row(vec![
                        sig_type.to_string(),
                        trades.len().to_string(),
                        w.to_string(),
                        l.to_string(),
                        wr,
                        format!("{:.1}", gross),
                        format!("{:.1}", fees),
                        format!("{:.1}", pnl),
                    ]);
                } else {
                    table.add_row(vec![
                        sig_type.to_string(),
                        trades.len().to_string(),
                        w.to_string(),
                        l.to_string(),
                        wr,
                        format!("{:.1}", pnl),
                    ]);
                }
            }
            println!("By Signal Type:");
            println!("{}", table);
            println!();
        }

        // By asset
        if !self.trades.is_empty() {
            let mut by_asset: HashMap<u16, Vec<&PaperTrade>> = HashMap::new();
            for t in &self.trades {
                by_asset.entry(t.asset).or_default().push(t);
            }

            let mut table = Table::new();
            if self.use_fees {
                table.set_header(vec![
                    "Asset",
                    "Taken",
                    "W",
                    "L",
                    "WR%",
                    "Gross (R)",
                    "Fees (R)",
                    "Net (R)",
                ]);
            } else {
                table.set_header(vec!["Asset", "Taken", "W", "L", "WR%", "P&L (R)"]);
            }
            let mut entries: Vec<_> = by_asset.iter().collect();
            entries.sort_by(|a, b| {
                let pnl_a: f64 = a.1.iter().map(|t| t.r_pnl).sum();
                let pnl_b: f64 = b.1.iter().map(|t| t.r_pnl).sum();
                pnl_b.partial_cmp(&pnl_a).unwrap()
            });
            for (asset_id_val, trades) in entries {
                let w = trades
                    .iter()
                    .filter(|t| t.result == TradeResult::Win)
                    .count();
                let l = trades
                    .iter()
                    .filter(|t| t.result == TradeResult::Loss)
                    .count();
                let d = w + l;
                let wr = if d > 0 {
                    format!("{:.0}", w as f64 / d as f64 * 100.0)
                } else {
                    "-".to_string()
                };
                let pnl: f64 = trades.iter().map(|t| t.r_pnl).sum();
                if self.use_fees {
                    let fees: f64 = trades.iter().map(|t| t.fee_r).sum();
                    let gross = pnl + fees;
                    table.add_row(vec![
                        asset_name(*asset_id_val).to_string(),
                        trades.len().to_string(),
                        w.to_string(),
                        l.to_string(),
                        wr,
                        format!("{:.1}", gross),
                        format!("{:.1}", fees),
                        format!("{:.1}", pnl),
                    ]);
                } else {
                    table.add_row(vec![
                        asset_name(*asset_id_val).to_string(),
                        trades.len().to_string(),
                        w.to_string(),
                        l.to_string(),
                        wr,
                        format!("{:.1}", pnl),
                    ]);
                }
            }
            println!("By Asset:");
            println!("{}", table);
            println!();
        }

        // Trade details
        if !self.trades.is_empty() {
            let mut table = Table::new();
            if self.use_fees {
                table.set_header(vec![
                    "Time", "Asset", "Dir", "Type", "Score", "Entry", "Stop", "TP", "Result",
                    "Gross", "Fee", "Net",
                ]);
            } else {
                table.set_header(vec![
                    "Time", "Asset", "Dir", "Type", "Score", "Entry", "Stop", "TP", "Result", "P&L",
                ]);
            }
            let mut sorted: Vec<&PaperTrade> = self.trades.iter().collect();
            sorted.sort_by_key(|t| t.opened_at);
            for t in sorted {
                let arrow = if t.direction == Direction::Bull {
                    "▲"
                } else {
                    "▼"
                };
                let ts = t.opened_at.format("%m-%d %H:%M").to_string();
                let result = match t.result {
                    TradeResult::Win => "WIN",
                    TradeResult::Loss => "LOSS",
                    TradeResult::Inconclusive => "---",
                };
                let mut row = vec![
                    ts,
                    asset_name(t.asset).to_string(),
                    arrow.to_string(),
                    sig_type_name(t.signal_type).to_string(),
                    format!("{:.1}", t.score),
                    format!("{:.2}", t.entry),
                    format!("{:.2}", t.stop),
                    format!("{:.2}", t.tp),
                    result.to_string(),
                ];
                if self.use_fees {
                    let gross = t.r_pnl + t.fee_r;
                    let gross_s = if gross != 0.0 {
                        format!("{:+.2}R", gross)
                    } else {
                        "-".to_string()
                    };
                    let fee_s = if t.fee_r != 0.0 {
                        format!("{:.2}R", t.fee_r)
                    } else {
                        "-".to_string()
                    };
                    let net_s = if t.r_pnl != 0.0 || t.fee_r != 0.0 {
                        format!("{:+.2}R", t.r_pnl)
                    } else {
                        "-".to_string()
                    };
                    row.push(gross_s);
                    row.push(fee_s);
                    row.push(net_s);
                } else {
                    let pnl = if t.r_pnl != 0.0 {
                        format!("{:+.1}R", t.r_pnl)
                    } else {
                        "-".to_string()
                    };
                    row.push(pnl);
                }
                table.add_row(row);
            }
            println!("Trades:");
            println!("{}", table);
        }
    }

    pub fn render_json(&self, label: &str) -> String {
        let mut by_type: HashMap<String, serde_json::Value> = HashMap::new();
        for t in &self.trades {
            let st_name = sig_type_name(t.signal_type).to_string();
            let entry = by_type
                .entry(st_name)
                .or_insert_with(|| serde_json::json!({"wins": 0, "losses": 0, "inconclusive": 0}));
            match t.result {
                TradeResult::Win => {
                    entry["wins"] = serde_json::json!(entry["wins"].as_i64().unwrap_or(0) + 1);
                }
                TradeResult::Loss => {
                    entry["losses"] = serde_json::json!(entry["losses"].as_i64().unwrap_or(0) + 1);
                }
                TradeResult::Inconclusive => {
                    entry["inconclusive"] =
                        serde_json::json!(entry["inconclusive"].as_i64().unwrap_or(0) + 1);
                }
            }
        }

        let mut by_asset: HashMap<String, serde_json::Value> = HashMap::new();
        for t in &self.trades {
            let entry = by_asset
                .entry(asset_name(t.asset).to_string())
                .or_insert_with(|| serde_json::json!({"win": 0, "loss": 0, "inconclusive": 0}));
            match t.result {
                TradeResult::Win => {
                    entry["win"] = serde_json::json!(entry["win"].as_i64().unwrap_or(0) + 1);
                }
                TradeResult::Loss => {
                    entry["loss"] = serde_json::json!(entry["loss"].as_i64().unwrap_or(0) + 1);
                }
                TradeResult::Inconclusive => {
                    entry["inconclusive"] =
                        serde_json::json!(entry["inconclusive"].as_i64().unwrap_or(0) + 1);
                }
            }
        }

        // Optional compounding equity curve (when risk_frac > 0). Keyed by
        // opportunity_id so it attaches to the right trade regardless of order.
        let compound = self.compound_equity();
        let comp_by_id: HashMap<String, (f64, f64)> = compound
            .as_ref()
            .map(|(per, _, _)| {
                per.iter()
                    .map(|(id, eq, pnl)| (id.clone(), (*eq, *pnl)))
                    .collect()
            })
            .unwrap_or_default();

        let trades_json: Vec<serde_json::Value> = self
            .trades
            .iter()
            .map(|t| {
                let (equity, pnl_dollars) = comp_by_id
                    .get(&t.opportunity_id)
                    .map(|(e, p)| (Some(*e), Some(*p)))
                    .unwrap_or((None, None));
                serde_json::json!({
                    "opportunity_id": t.opportunity_id,
                    "signal_type": &*sig_type_name(t.signal_type),
                    "asset": &*asset_name(t.asset),
                    "timeframe": &*tf_name(t.timeframe),
                    "direction": t.direction.as_str(),
                    "entry": t.entry,
                    "fill": t.fill,
                    "stop": t.stop,
                    "tp": t.tp,
                    "score": t.score,
                    "opened_at": t.opened_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
                    // When the entry limit actually filled; `opened_at` is the
                    // signal time and can precede this by many bars. Trades from
                    // pre-field journals lack the key entirely.
                    "filled_at": t.filled_at.map(|f| f.format("%Y-%m-%dT%H:%M:%S").to_string()),
                    "closed_at": t.closed_at.map(|c| c.format("%Y-%m-%dT%H:%M:%S").to_string()),
                    "result": t.result.as_str(),
                    "r_pnl": t.r_pnl,
                    "fee_r": t.fee_r,
                    "gross_r_pnl": t.r_pnl + t.fee_r,
                    // Compounding (null unless risk_frac > 0): account balance after
                    // this trade closes, and its dollar P&L at the size it was given.
                    "equity": equity,
                    "pnl_dollars": pnl_dollars,
                    // When the strategy's setup became final and a real order could
                    // have gone on the book. Null unless the strategy stamped it.
                    "ready_at": self.trade_ready_at.get(&t.opportunity_id)
                        .map(|r| r.format("%Y-%m-%dT%H:%M:%S").to_string()),
                })
            })
            .collect();

        let compound_json = compound.as_ref().map(|(_, final_bal, max_dd)| {
            serde_json::json!({
                "risk_frac": self.risk_frac,
                "account_size": self.account_size,
                "final_balance": final_bal,
                "max_dd_frac": max_dd,
                "multiple": if self.account_size > 0.0 { final_bal / self.account_size } else { 0.0 },
            })
        });

        let report = serde_json::json!({
            "label": label,
            "rr_target": self.rr_target,
            "min_score": self.min_score,
            "opportunities_seen": self.opportunities_seen,
            "opportunities_taken": self.opportunities_taken,
            "trades_decided": self.wins() + self.losses(),
            "wins": self.wins(),
            "losses": self.losses(),
            "inconclusive": self.inconclusive(),
            "win_rate": self.win_rate(),
            "expectancy": self.expectancy(),
            "total_r_pnl": self.total_r_pnl(),
            "gross_r_pnl": self.gross_r_pnl(),
            "total_fees": self.total_fees(),
            "use_fees": self.use_fees,
            "by_signal_type": by_type,
            "by_asset": by_asset,
            // Skip tally by reason: how much flow each gate rejected. A gate
            // that silently drops most of the book is visible here.
            "skips": self.skips,
            "compound": compound_json,
            // Hybrid fill-path breakdown (all zero unless entry_fill_mode=hybrid).
            "hybrid_fill": self.hybrid_fill,
            "hybrid_fill_paths": {
                "past_entry_fills": self.hybrid_counters.past_entry_fills,
                "immediate_chases": self.hybrid_counters.immediate_chases,
                "boundary_chases_from_below": self.hybrid_counters.boundary_chases_from_below,
                "boundary_chases_from_above": self.hybrid_counters.boundary_chases_from_above,
                "maker_fills": self.hybrid_counters.maker_fills,
                "rested_maker_fills": self.hybrid_counters.rested_maker_fills,
                "abandon_gap_stop": self.hybrid_counters.abandon_gap_stop,
                "abandon_tp_open": self.hybrid_counters.abandon_tp_open,
                "abandon_tp_range": self.hybrid_counters.abandon_tp_range,
                "abandon_age": self.hybrid_counters.abandon_age,
                "abandon_deadline": self.hybrid_counters.abandon_deadline,
                "abandon_target_consumed": self.hybrid_counters.abandon_target_consumed,
                "abandon_setup_invalidated": self.hybrid_counters.abandon_setup_invalidated,
                "first_touch_stop_fills": self.hybrid_counters.first_touch_stop_fills,
                "first_touch_stop_r": self.hybrid_counters.first_touch_stop_r_milli as f64 / 1000.0,
            },
            // Resting entry-limit lifetimes (placed_at, resolved_at) for the
            // concurrency diagnostic. Global concurrency = merge across assets.
            "resting_intervals": self.resting_intervals.iter().map(|(a, b)| {
                serde_json::json!([a.format("%Y-%m-%dT%H:%M:%S").to_string(),
                                   b.format("%Y-%m-%dT%H:%M:%S").to_string()])
            }).collect::<Vec<_>>(),
            "trades": trades_json,
        });

        serde_json::to_string_pretty(&report).unwrap()
    }
}

#[cfg(test)]
mod fill_model_tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(min: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 6, 25)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            + chrono::Duration::minutes(min)
    }

    /// A 1m candle for asset 0 with the given OHLC at minute `min`.
    fn candle(min: i64, o: f64, h: f64, l: f64, c: f64) -> Candle {
        candle_a(0, min, o, h, l, c)
    }

    /// A 1m candle for the given asset id.
    fn candle_a(asset: u16, min: i64, o: f64, h: f64, l: f64, c: f64) -> Candle {
        Candle {
            asset,
            timeframe: 0,
            open: o,
            high: h,
            low: l,
            close: c,
            volume: 0.0,
            timestamp: ts(min),
            complete: true,
        }
    }

    fn base_trade(dir: Direction, entry: f64, stop: f64, tp: f64) -> PaperTrade {
        PaperTrade {
            opportunity_id: "t1".to_string(),
            signal_type: 0,
            asset: 0,
            timeframe: 0,
            direction: dir,
            entry,
            stop,
            tp,
            fill: entry,
            score: 10.0,
            opened_at: ts(0),
            filled_at: None,
            closed_at: None,
            result: TradeResult::Inconclusive,
            r_pnl: 0.0,
            fee_r: 0.0,
        }
    }

    /// Inject a resting entry limit directly (bypassing admission), so we can
    /// drive the fill/race deterministically from candles. `pred_touch` seeds the
    /// hybrid T−1 touch (irrelevant to the limit model).
    fn place_pred(pt: &mut PaperTrader, trade: PaperTrade, pred_touch: bool) {
        let deadline = trade.opened_at + chrono::Duration::seconds(FILL_DEADLINE_SECS);
        pt.pending.push(PendingFill {
            trade,
            deadline,
            seen_first_candle: false,
            age_anchor: None,
            hybrid_pred_touch: pred_touch,
            hybrid_armed: None,
            hybrid_seen_decision_bar: false,
            hybrid_b0_open: None,
            min_target: None,
            ready_at: None,
            cancel_pending: None,
        });
    }

    /// `place_pred` with a stamped `min_target` (rest-on-Ready v2 cancel tests).
    fn place_with_min_target(pt: &mut PaperTrader, trade: PaperTrade, min_target: f64) {
        place_pred(pt, trade, false);
        pt.pending.last_mut().unwrap().min_target = Some(min_target);
    }

    fn place(pt: &mut PaperTrader, trade: PaperTrade) {
        place_pred(pt, trade, false);
    }

    // ─── Pure fill_action ────────────────────────────────────────────────────

    #[test]
    fn fill_action_bull_touches_below_entry() {
        // low 99 <= entry 100 → fill at entry (no slip).
        let c = candle(1, 100.5, 101.0, 99.0, 100.2);
        assert_eq!(
            fill_action(Direction::Bull, 100.0, 10.0, &c, false, false, 0.0),
            Some(100.0)
        );
    }

    #[test]
    fn fill_action_bull_no_touch_when_low_above_entry() {
        let c = candle(1, 102.0, 103.0, 101.0, 102.5); // low 101 > entry 100
        assert_eq!(
            fill_action(Direction::Bull, 100.0, 10.0, &c, false, false, 0.0),
            None
        );
    }

    #[test]
    fn fill_action_signal_bar_excluded_then_allowed() {
        let c = candle(0, 100.5, 101.0, 99.0, 100.2); // touches entry
                                                      // Signal bar, not allowed → no fill.
        assert_eq!(
            fill_action(Direction::Bull, 100.0, 10.0, &c, true, false, 0.0),
            None
        );
        // Signal bar, allowed → fill.
        assert_eq!(
            fill_action(Direction::Bull, 100.0, 10.0, &c, true, true, 0.0),
            Some(100.0)
        );
    }

    #[test]
    fn fill_action_slippage_price_bull_and_bear() {
        let cb = candle(1, 100.5, 101.0, 99.0, 100.2);
        // Bull, R=10, slip 0.1 → 100 + 1 = 101.
        assert_eq!(
            fill_action(Direction::Bull, 100.0, 10.0, &cb, false, false, 0.1),
            Some(101.0)
        );
        // Bear: entry 100, high 101 >= entry → 100 − 1 = 99.
        let cs = candle(1, 99.5, 101.0, 99.0, 100.2);
        assert_eq!(
            fill_action(Direction::Bear, 100.0, 10.0, &cs, false, false, 0.1),
            Some(99.0)
        );
    }

    // ─── Integrated: placement → fill → race, and R accounting ────────────────

    #[test]
    fn signal_bar_does_not_fill_when_disallowed() {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.allow_signal_bar_fill = false;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        // Signal bar (first candle) dips to the entry — must NOT fill.
        pt.update_prices(&candle(0, 100.0, 101.0, 100.0, 100.5));
        assert_eq!(pt.opportunities_taken, 0, "signal bar should not fill");
        assert!(pt.open_trades.is_empty());
        // Next candle also touches → now it fills.
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.2));
        assert_eq!(
            pt.opportunities_taken, 1,
            "fills on first eligible bar after signal"
        );
    }

    #[test]
    fn signal_bar_fills_when_allowed() {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.allow_signal_bar_fill = true;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 100.0, 101.0, 100.0, 100.5));
        assert_eq!(pt.opportunities_taken, 1, "signal bar fills when allowed");
    }

    #[test]
    fn fills_on_first_touch_after_placement() {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch anyway
        pt.update_prices(&candle(1, 104.0, 104.0, 103.0, 103.5)); // no touch (low 103 > 100)
        assert_eq!(pt.opportunities_taken, 0);
        pt.update_prices(&candle(2, 101.0, 101.0, 99.0, 100.0)); // low 99 <= 100 → fill
        assert_eq!(pt.opportunities_taken, 1);
        assert_eq!(pt.open_trades[0].fill, 100.0);
    }

    #[test]
    fn no_touch_before_deadline_is_no_trade() {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        // Price stays above entry for the whole 2h window, then a candle after
        // the deadline finally dips — too late, cancelled.
        for m in 0..=125 {
            pt.update_prices(&candle(m, 105.0, 106.0, 104.0, 105.0));
        }
        // 2h = 120min; a dip at minute 121 (> deadline) must not fill.
        pt.update_prices(&candle(121, 100.0, 100.0, 99.0, 99.5));
        assert_eq!(pt.opportunities_taken, 0, "expired limit never trades");
        assert!(pt.pending.is_empty(), "expired limit is dropped");
        assert!(pt.trades.is_empty());
    }

    #[test]
    fn loss_reads_minus_one_plus_slip_r() {
        // Bull entry 100, stop 90 → R_planned 10; slip 0.1 → fill 101.
        // Stop at 90 loses 101−90 = 11 pts = −1.1R.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.entry_slippage_r = 0.1;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // fill at 101
        assert_eq!(pt.open_trades[0].fill, 101.0);
        pt.update_prices(&candle(2, 95.0, 95.0, 89.0, 90.0)); // hits stop 90
        assert_eq!(pt.losses(), 1);
        let r = pt.trades[0].r_pnl;
        assert!((r - (-1.1)).abs() < 1e-9, "loss should be −1.1R, got {r}");
    }

    #[test]
    fn win_reads_planned_rr_minus_slip() {
        // Bull entry 100, stop 90, tp 130 → planned +3R; slip 0.1 → fill 101.
        // Win = (130−101)/10 = 2.9R.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.entry_slippage_r = 0.1;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // fill at 101
        pt.update_prices(&candle(2, 120.0, 131.0, 119.0, 130.0)); // hits tp 130
        assert_eq!(pt.wins(), 1);
        let r = pt.trades[0].r_pnl;
        assert!((r - 2.9).abs() < 1e-9, "win should be 2.9R, got {r}");
    }

    // ─── Stop-exit gap penalty ───────────────────────────────────────────

    #[test]
    fn stop_gap_bps_widens_long_loss() {
        // Bull entry 100, stop 90 → R_planned 10, clean stop-out = −1.0R.
        // 20 bps gap → exit at 90*(1 - 20*1e-4) = 90*0.998 = 89.82.
        // Extra loss = (89.82 - 90) / 10 = -0.018R → total -1.018R. // leak-check: ok test arithmetic
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.stop_gap_bps_default = 20.0;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // fill at 100
        pt.update_prices(&candle(2, 95.0, 95.0, 89.0, 90.0)); // hits stop 90
        assert_eq!(pt.losses(), 1);
        let r = pt.trades[0].r_pnl;
        assert!((r - (-1.018)).abs() < 1e-9, "expected -1.018R, got {r}");
    }

    #[test]
    fn stop_gap_bps_widens_short_loss() {
        // Bear entry 100, stop 110 → R_planned 10, clean stop-out = −1.0R.
        // 20 bps gap → exit at 110*(1 + 20*1e-4) = 110*1.002 = 110.22.
        // Extra loss = (110 - 110.22) / 10 = -0.022R → total -1.022R. // leak-check: ok test arithmetic
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.stop_gap_bps_default = 20.0;
        place(&mut pt, base_trade(Direction::Bear, 100.0, 110.0, 70.0));
        pt.update_prices(&candle(0, 95.0, 96.0, 94.0, 95.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 99.5, 100.5, 99.0, 100.0)); // fill at 100
        pt.update_prices(&candle(2, 105.0, 111.0, 105.0, 110.0)); // hits stop 110
        assert_eq!(pt.losses(), 1);
        let r = pt.trades[0].r_pnl;
        assert!((r - (-1.022)).abs() < 1e-9, "expected -1.022R, got {r}");
    }

    #[test]
    fn stop_gap_bps_zero_is_unchanged_behavior() {
        // stop_gap_bps_default = 0 (the field's own default) must reproduce the
        // pre-gap −1.0R exactly — every existing lens is unaffected.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        assert_eq!(pt.stop_gap_bps_default, 0.0);
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0));
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0));
        pt.update_prices(&candle(2, 95.0, 95.0, 89.0, 90.0));
        assert_eq!(pt.losses(), 1);
        assert!((pt.trades[0].r_pnl - (-1.0)).abs() < 1e-9);
    }

    #[test]
    fn stop_gap_bps_does_not_apply_to_tp_exit() {
        // A TP win must be completely unaffected by a nonzero stop gap.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.stop_gap_bps_default = 20.0;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0));
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0));
        pt.update_prices(&candle(2, 120.0, 131.0, 119.0, 130.0)); // hits tp 130
        assert_eq!(pt.wins(), 1);
        assert!((pt.trades[0].r_pnl - 3.0).abs() < 1e-9);
    }

    #[test]
    fn stop_gap_bps_per_asset_override_wins_over_default() {
        // Intern a real asset name so asset_name(aid) resolves (same pattern
        // as hybrid_taker_entry_fee_applied), then override it by substring
        // match — the shared convention used by rr_asset/risk_usd_asset.
        let aid = crate::models::asset_id("ASSET_A_stopgap_test");
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.stop_gap_bps_default = 5.0;
        pt.stop_gap_bps_asset = HashMap::from([("ASSET_A".to_string(), 20.0)]);
        let mut t = base_trade(Direction::Bull, 100.0, 90.0, 130.0);
        t.asset = aid;
        place(&mut pt, t);
        pt.update_prices(&candle_a(aid, 0, 105.0, 106.0, 104.0, 105.0));
        pt.update_prices(&candle_a(aid, 1, 100.5, 101.0, 99.5, 100.0));
        pt.update_prices(&candle_a(aid, 2, 95.0, 95.0, 89.0, 90.0));
        assert_eq!(pt.losses(), 1);
        // Override (20 bps) must win, giving the same -1.018R as the direct test.
        let r = pt.trades[0].r_pnl;
        assert!(
            (r - (-1.018)).abs() < 1e-9,
            "expected override applied (-1.018R), got {r}"
        );
    }

    #[test]
    fn zero_slip_win_reads_full_planned_rr() {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // fill at 100
        pt.update_prices(&candle(2, 120.0, 131.0, 119.0, 130.0)); // tp
        assert!((pt.trades[0].r_pnl - 3.0).abs() < 1e-9);
    }

    #[test]
    fn same_bar_entry_and_stop_resolves_stop_first() {
        // A single post-fill candle spans both stop and tp → stop-first default.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.intrabar_stop_first = true;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // fill at 100
        pt.update_prices(&candle(2, 100.0, 131.0, 89.0, 120.0)); // spans stop 90 AND tp 130
        assert_eq!(
            pt.losses(),
            1,
            "spanning candle resolves as loss (stop-first)"
        );
        assert_eq!(pt.wins(), 0);
        assert!((pt.trades[0].r_pnl - (-1.0)).abs() < 1e-9);
    }

    #[test]
    fn same_bar_entry_and_stop_can_resolve_tp_first() {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.intrabar_stop_first = false;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // fill
        pt.update_prices(&candle(2, 100.0, 131.0, 89.0, 120.0)); // spans both
        assert_eq!(
            pt.wins(),
            1,
            "tp-first flag resolves spanning candle as win"
        );
    }

    #[test]
    fn fill_candle_is_not_raced() {
        // The candle that fills the entry must NOT also close the trade, even if
        // it would touch the stop — the race starts next candle.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        place(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0));
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // signal bar, no touch
                                                                  // Fill candle also dips to the stop at 90 within the same bar.
        pt.update_prices(&candle(1, 100.0, 101.0, 89.0, 95.0));
        assert_eq!(pt.opportunities_taken, 1, "trade filled");
        assert_eq!(pt.losses(), 0, "fill bar is not raced");
        assert_eq!(pt.open_trades.len(), 1);
    }

    #[test]
    fn bear_slippage_and_loss() {
        // Bear entry 100, stop 110 → R 10; slip 0.1 → fill 99.
        // Stop at 110 loses 110−99 = 11 = −1.1R.
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.entry_slippage_r = 0.1;
        place(&mut pt, base_trade(Direction::Bear, 100.0, 110.0, 70.0));
        pt.update_prices(&candle(0, 95.0, 96.0, 94.0, 95.0)); // signal bar, no touch (high 96 < 100)
        pt.update_prices(&candle(1, 99.5, 101.0, 99.0, 100.0)); // high 101 >= 100 → fill 99
        assert_eq!(pt.open_trades[0].fill, 99.0);
        pt.update_prices(&candle(2, 105.0, 111.0, 104.0, 110.0)); // hits stop 110
        assert!((pt.trades[0].r_pnl - (-1.1)).abs() < 1e-9);
    }

    // ─── Pure hybrid_fill_action ─────────────────────────────────────────────
    //
    // Canonical Bull setup: E=100, S=90, TP=130, R=10, x=0.1 → boundary B=101.
    // Canonical Bear setup: E=100, S=110, TP=70, R=10, x=0.1 → boundary B=99.
    // Every Bull case below has an exact Bear mirror.

    /// Call the pure hybrid decision with the canonical knobs (armed, chase enabled,
    /// taker past-entry). `dec` = is_decision_bar, `age` = past_age.
    #[allow(clippy::too_many_arguments)]
    fn hyb(
        dir: Direction,
        entry: f64,
        stop: f64,
        tp: f64,
        c: &Candle,
        armed: bool,
        immediate: bool,
        dec: bool,
        age: bool,
    ) -> HybridAction {
        hybrid_fill_action(
            dir,
            entry,
            stop,
            tp,
            (entry - stop).abs(),
            0.1,
            armed,
            immediate,
            false,
            None,
            EntryFeeSide::Taker,
            c,
            dec,
            age,
        )
    }

    fn is_fill(a: HybridAction) -> Option<(f64, EntryFeeSide, HybridPath)> {
        if let HybridAction::Fill { px, side, path } = a {
            Some((px, side, path))
        } else {
            None
        }
    }

    // ── Case A: open at/past entry ──
    #[test]
    fn hybrid_case_a_open_past_entry_taker_fill_at_open() {
        // Bull: D opens 99.5 (< E=100, past entry, above stop) → taker fill @ open.
        let c = candle(1, 99.5, 100.5, 99.0, 100.0);
        let (px, side, path) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            true,
            true,
            true,
            false,
        ))
        .unwrap();
        assert_eq!(px, 99.5);
        assert_eq!(side, EntryFeeSide::Taker);
        assert_eq!(path, HybridPath::PastEntry);
        // Bear mirror: D opens 100.5 (> E=100) → taker fill @ open.
        let c = candle(1, 100.5, 101.0, 99.5, 100.0);
        let (px, side, path) = is_fill(hyb(
            Direction::Bear,
            100.0,
            110.0,
            70.0,
            &c,
            true,
            true,
            true,
            false,
        ))
        .unwrap();
        assert_eq!(px, 100.5);
        assert_eq!(side, EntryFeeSide::Taker);
        assert_eq!(path, HybridPath::PastEntry);
    }

    #[test]
    fn hybrid_case_a_past_entry_fee_maker_knob() {
        // past_entry_fee = maker → maker fee side on a Case-A fill.
        let c = candle(1, 99.5, 100.5, 99.0, 100.0);
        let a = hybrid_fill_action(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            10.0,
            0.1,
            true,
            true,
            false,
            None,
            EntryFeeSide::Maker,
            &c,
            true,
            false,
        );
        assert_eq!(is_fill(a).unwrap().1, EntryFeeSide::Maker);
    }

    /// `race_maker_first`: a bar reaching BOTH the entry and the chase boundary
    /// resolves to the maker fill at the entry (live-census race), while the
    /// default chase-first race books the boundary taker fill. Chase-only and
    /// maker-only bars are unaffected by the knob.
    #[test]
    fn hybrid_race_maker_first_resolves_both_reachable_to_maker() {
        let race = |maker_first: bool, c: &Candle| {
            hybrid_fill_action(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                10.0,
                0.1,
                true,
                false,
                maker_first,
                None,
                EntryFeeSide::Taker,
                c,
                false,
                false,
            )
        };
        // Opens between E=100 and B=101, spans both (lo 99.8 ≤ E, hi 101.2 ≥ B).
        let both = candle(2, 100.5, 101.2, 99.8, 100.9);
        let (px, side, path) = is_fill(race(false, &both)).unwrap();
        assert_eq!(
            (px, side, path),
            (101.0, EntryFeeSide::Taker, HybridPath::BoundaryFromBelow)
        );
        let (px, side, path) = is_fill(race(true, &both)).unwrap();
        assert_eq!(
            (px, side, path),
            (100.0, EntryFeeSide::Maker, HybridPath::Maker)
        );
        // Boundary-only bar (never touches E): the knob must NOT suppress the chase.
        let chase_only = candle(2, 100.5, 101.2, 100.2, 100.9);
        let (px, _, path) = is_fill(race(true, &chase_only)).unwrap();
        assert_eq!((px, path), (101.0, HybridPath::BoundaryFromBelow));
        // Bear mirror of the both-reachable bar: E=100, S=110 → B=99.
        let both_bear = candle(2, 99.5, 100.2, 98.8, 99.1);
        let a = hybrid_fill_action(
            Direction::Bear,
            100.0,
            110.0,
            70.0,
            10.0,
            0.1,
            true,
            false,
            true,
            None,
            EntryFeeSide::Taker,
            &both_bear,
            false,
            false,
        );
        let (px, side, path) = is_fill(a).unwrap();
        assert_eq!(
            (px, side, path),
            (100.0, EntryFeeSide::Maker, HybridPath::Maker)
        );
    }

    #[test]
    fn hybrid_case_a_gap_through_stop_abandons() {
        // Bull: D opens 89 (≤ stop 90) → GapStop abandon, NO TRADE.
        let c = candle(1, 89.0, 90.0, 88.0, 89.5);
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                true,
                true,
                true,
                false
            ),
            HybridAction::Abandon(HybridAbandon::GapStop)
        );
        // Bear mirror: D opens 111 (≥ stop 110) → GapStop.
        let c = candle(1, 111.0, 112.0, 110.0, 111.5);
        assert_eq!(
            hyb(
                Direction::Bear,
                100.0,
                110.0,
                70.0,
                &c,
                true,
                true,
                true,
                false
            ),
            HybridAction::Abandon(HybridAbandon::GapStop)
        );
    }

    // ── Case B0: immediate chase at open ──
    #[test]
    fn hybrid_b0_immediate_chase_on() {
        // Bull: D opens 100.5 (front of E, within cap B=101) → taker @ open.
        let c = candle(1, 100.5, 100.8, 100.4, 100.6);
        let (px, side, path) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            true,
            true,
            true,
            false,
        ))
        .unwrap();
        assert_eq!(px, 100.5);
        assert_eq!(side, EntryFeeSide::Taker);
        assert_eq!(path, HybridPath::ImmediateChase);
        // Bear mirror: D opens 99.5 (front, within cap B=99) → taker @ open.
        let c = candle(1, 99.5, 99.6, 99.2, 99.4);
        let (px, _, path) = is_fill(hyb(
            Direction::Bear,
            100.0,
            110.0,
            70.0,
            &c,
            true,
            true,
            true,
            false,
        ))
        .unwrap();
        assert_eq!(px, 99.5);
        assert_eq!(path, HybridPath::ImmediateChase);
    }

    #[test]
    fn hybrid_b0_immediate_chase_off_rests() {
        // immediate_chase disabled → D opens in front, within cap, low doesn't
        // reach E, high doesn't reach B → Wait (rest the limit).
        let c = candle(1, 100.5, 100.9, 100.3, 100.6); // low 100.3 > E, high 100.9 < B=101
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                true,
                false,
                true,
                false
            ),
            HybridAction::Wait
        );
    }

    // ── Deferred open-chase (b0_carry / deferred_chase_at_open) ──
    //
    // Canonical carried setup: Bull E=100, S=90, TP=130, R=10, x=0.1 → B=101;
    // decision bar opened at 100.5 (front, in-cap) → carry = 100.5. The carry
    // must make every resolution at-or-better than the immediate-chase fill at
    // the open: maker at E when the entry trades, taker at the CARRIED OPEN
    // (never the boundary, never an abandon) otherwise.

    /// Carried-limit call: `hyb` with `race_maker_first = true` and `b0_carry`.
    fn hyb_carry(c: &Candle, carry: Option<f64>, dec: bool, age: bool) -> HybridAction {
        hybrid_fill_action(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            10.0,
            0.1,
            true,
            false,
            true,
            carry,
            EntryFeeSide::Taker,
            c,
            dec,
            age,
        )
    }

    #[test]
    fn hybrid_deferred_chase_books_carried_open_not_boundary() {
        // Boundary crossed (hi 101.4 ≥ B=101), entry never touched (lo 100.2):
        // without carry the chase books the full-cap boundary 101; with carry it
        // books the carried decision-bar open 100.5 — the mh03 B0 price.
        let c = candle(1, 100.5, 101.4, 100.2, 101.2);
        let (px, _, path) = is_fill(hyb_carry(&c, None, true, false)).unwrap();
        assert_eq!((px, path), (101.0, HybridPath::BoundaryFromBelow));
        let (px, side, path) = is_fill(hyb_carry(&c, Some(100.5), true, false)).unwrap();
        assert_eq!(
            (px, side, path),
            (100.5, EntryFeeSide::Taker, HybridPath::DeferredChase)
        );
    }

    #[test]
    fn hybrid_deferred_chase_maker_touch_still_wins() {
        // Later bar spans both E and B: the maker-first race beats the carried
        // chase — maker at exactly the entry.
        let c = candle(2, 100.5, 101.2, 99.8, 100.9);
        let (px, side, path) = is_fill(hyb_carry(&c, Some(100.5), false, false)).unwrap();
        assert_eq!(
            (px, side, path),
            (100.0, EntryFeeSide::Maker, HybridPath::Maker)
        );
    }

    #[test]
    fn hybrid_deferred_chase_tp_open_fills_instead_of_abandon() {
        // Later bar opens at/past TP: uncarried limits abandon (move gone);
        // a carried limit was in the move since D — fill at the carried open.
        let c = candle(3, 130.5, 131.0, 130.0, 130.8);
        assert_eq!(
            hyb_carry(&c, None, false, false),
            HybridAction::Abandon(HybridAbandon::TpOpen)
        );
        let (px, side, path) = is_fill(hyb_carry(&c, Some(100.5), false, false)).unwrap();
        assert_eq!(
            (px, side, path),
            (100.5, EntryFeeSide::Taker, HybridPath::DeferredChase)
        );
    }

    #[test]
    fn hybrid_deferred_chase_age_fills_instead_of_abandon() {
        // Aged out while resting in front: carried limits book the deferred
        // chase instead of abandoning.
        let c = candle(31, 100.5, 100.9, 100.3, 100.6);
        assert_eq!(
            hyb_carry(&c, None, false, true),
            HybridAction::Abandon(HybridAbandon::Age)
        );
        let (px, _, path) = is_fill(hyb_carry(&c, Some(100.5), false, true)).unwrap();
        assert_eq!((px, path), (100.5, HybridPath::DeferredChase));
    }

    #[test]
    fn hybrid_deferred_chase_open_beyond_boundary_fills_carry() {
        // Later bar gaps beyond the cap (open 101.5 > B=101): price past the
        // boundary means the deferred chase certainly resolved — fill at carry
        // (uncarried: waits for a re-approach / books the boundary from above).
        let c = candle(2, 101.5, 101.8, 101.3, 101.6);
        assert_eq!(hyb_carry(&c, None, false, false), HybridAction::Wait);
        let (px, _, path) = is_fill(hyb_carry(&c, Some(100.5), false, false)).unwrap();
        assert_eq!((px, path), (100.5, HybridPath::DeferredChase));
    }

    #[test]
    fn hybrid_deferred_chase_no_carry_when_untriggered_waits() {
        // In-cap decision bar touching neither E nor B with carry stamped:
        // still Wait (the limit legitimately rests; the carry only prices a
        // LATER non-maker resolution).
        let c = candle(1, 100.5, 100.9, 100.3, 100.6);
        assert_eq!(hyb_carry(&c, Some(100.5), true, false), HybridAction::Wait);
    }

    /// End-to-end through the PaperTrader: deferred lens (immediate off, race
    /// on, deferred on) must book the SAME trade as the immediate-chase lens at
    /// the same-or-better price on a breakaway B0 bar.
    #[test]
    fn hybrid_deferred_lens_dominates_immediate_on_breakaway() {
        let run = |immediate: bool, deferred: bool| {
            let mut pt = PaperTrader::new(0.0, 3.0, 300);
            pt.hybrid_fill = true;
            pt.chase_r = 0.3; // B = 103
            pt.immediate_chase_at_open = immediate;
            pt.race_maker_first = !immediate;
            pt.deferred_chase_at_open = deferred;
            place_pred(
                &mut pt,
                base_trade(Direction::Bull, 100.0, 90.0, 130.0),
                true,
            );
            // T: seeds (touches entry), no fill eligibility.
            pt.update_prices(&candle(0, 100.2, 100.4, 99.9, 100.3));
            // D: opens 100.8 (front, in-cap), runs away through B=103 without
            // re-touching E — a breakaway.
            pt.update_prices(&candle(1, 100.8, 103.5, 100.7, 103.2));
            assert_eq!(
                pt.open_trades.len(),
                1,
                "trade must book (imm={immediate} def={deferred})"
            );
            pt.open_trades[0].fill
        };
        let mh03_fill = run(true, false); // immediate chase at open
        let census_fill = run(false, true); // deferred chase
        assert_eq!(mh03_fill, 100.8);
        assert_eq!(
            census_fill, 100.8,
            "deferred chase books the carried open, not the boundary"
        );
    }

    #[test]
    fn hybrid_b0_unarmed_does_not_immediate_chase() {
        // Unarmed: no immediate chase even if enabled; open 100.5 in front, no
        // maker touch (low>E), no cap → Wait.
        let c = candle(1, 100.5, 100.9, 100.3, 100.6);
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                false,
                true,
                true,
                false
            ),
            HybridAction::Wait
        );
    }

    // ── Branch 3: boundary from below & maker, both-possible pessimism ──
    #[test]
    fn hybrid_boundary_touch_from_below() {
        // Later bar (not D), open in front within cap, high reaches B, low above E
        // → chase @ boundary (from below), no maker possible.
        let c = candle(2, 100.5, 101.2, 100.3, 100.8); // high 101.2 ≥ B=101, low 100.3 > E
        let (px, side, path) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 101.0);
        assert_eq!(side, EntryFeeSide::Taker);
        assert_eq!(path, HybridPath::BoundaryFromBelow);
        // Bear mirror: B=99, low reaches 98.8 ≤ B, high below E.
        let c = candle(2, 99.5, 99.7, 98.8, 99.2);
        let (px, _, path) = is_fill(hyb(
            Direction::Bear,
            100.0,
            110.0,
            70.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 99.0);
        assert_eq!(path, HybridPath::BoundaryFromBelow);
    }

    #[test]
    fn hybrid_maker_fill_at_entry() {
        // In-cap open, low reaches E, high does NOT reach B → maker fill @ entry.
        let c = candle(2, 100.5, 100.9, 99.8, 100.2); // low 99.8 ≤ E, high 100.9 < B=101
        let (px, side, path) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 100.0);
        assert_eq!(side, EntryFeeSide::Maker);
        assert_eq!(path, HybridPath::Maker);
    }

    #[test]
    fn hybrid_both_possible_pessimistic_chase() {
        // Open between E and B, low reaches E AND high reaches B in one bar →
        // PESSIMISTIC: take the chase (worse price + taker), not the maker.
        let c = candle(2, 100.5, 101.5, 99.5, 100.8); // low 99.5 ≤ E, high 101.5 ≥ B=101
        let (px, side, path) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(
            px, 101.0,
            "pessimistic: chase at boundary, not maker at entry"
        );
        assert_eq!(side, EntryFeeSide::Taker);
        assert_eq!(path, HybridPath::BoundaryFromBelow);
        // Bear mirror.
        let c = candle(2, 99.5, 100.5, 98.5, 99.2); // high 100.5 ≥ E, low 98.5 ≤ B=99
        let (px, _, path) = is_fill(hyb(
            Direction::Bear,
            100.0,
            110.0,
            70.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 99.0);
        assert_eq!(path, HybridPath::BoundaryFromBelow);
    }

    // ── Branch 4: boundary from above (re-approach) ──
    #[test]
    fn hybrid_boundary_touch_from_above() {
        // Open beyond the cap (o=101.5 ≥ B=101), price falls back to B (low ≤ B),
        // no TP reached → chase-intercept @ boundary from above.
        let c = candle(3, 101.5, 101.6, 100.9, 101.2); // o 101.5 > B, low 100.9 ≤ B=101
        let (px, side, path) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 101.0);
        assert_eq!(side, EntryFeeSide::Taker);
        assert_eq!(path, HybridPath::BoundaryFromAbove);
        // Bear mirror: open 98.5 beyond B=99, high rises back to ≥ B.
        let c = candle(3, 98.5, 99.1, 98.4, 98.8);
        let (px, _, path) = is_fill(hyb(
            Direction::Bear,
            100.0,
            110.0,
            70.0,
            &c,
            true,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 99.0);
        assert_eq!(path, HybridPath::BoundaryFromAbove);
    }

    // ── TP abandons ──
    #[test]
    fn hybrid_tp_open_abandon() {
        // Any unfilled bar opening at/past TP → move gone, abandon.
        let c = candle(2, 130.0, 131.0, 129.0, 130.5);
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                true,
                true,
                false,
                false
            ),
            HybridAction::Abandon(HybridAbandon::TpOpen)
        );
        // Bear mirror (TP=70, open ≤ 70).
        let c = candle(2, 70.0, 71.0, 69.0, 69.5);
        assert_eq!(
            hyb(
                Direction::Bear,
                100.0,
                110.0,
                70.0,
                &c,
                true,
                true,
                false,
                false
            ),
            HybridAction::Abandon(HybridAbandon::TpOpen)
        );
    }

    #[test]
    fn hybrid_tp_range_abandon_armed_from_above() {
        // Armed, open beyond boundary, bar spans both B (re-approach) and TP →
        // unknowable order → abandon (pessimistic).
        let c = candle(3, 101.5, 130.5, 100.9, 120.0); // o>B, high≥TP=130, low≤B=101
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                true,
                true,
                false,
                false
            ),
            HybridAction::Abandon(HybridAbandon::TpRange)
        );
    }

    #[test]
    fn hybrid_tp_range_abandon_unarmed() {
        // Unarmed: in-front open (no cap), neither maker touch nor chase, but TP
        // reached intrabar → abandon (deviates from limit_only, which would fill).
        // Open in front (100.5 > E), low does not reach E, high reaches TP.
        let c = candle(2, 100.5, 130.5, 100.4, 120.0); // low 100.4 > E, high ≥ TP
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                false,
                true,
                false,
                false
            ),
            HybridAction::Abandon(HybridAbandon::TpRange)
        );
    }

    #[test]
    fn hybrid_spans_e_and_tp_unarmed_abandons() {
        // Unarmed, bar spans BOTH E (maker touch) and TP → we still abandon
        // (unknowable order, pessimistic no-trade — documented deviation).
        let c = candle(2, 100.5, 130.5, 99.5, 120.0); // low 99.5 ≤ E AND high ≥ TP
                                                      // maker touch is possible, but unarmed branch: can_maker true → maker fill.
                                                      // WAIT: spec says unarmed with maker touch fills maker. The spans-both
                                                      // abandon applies only when NEITHER maker nor chase filled. Here low ≤ E
                                                      // so maker fills. Assert the maker fill (this is the honest reading).
        let (px, side, _) = is_fill(hyb(
            Direction::Bull,
            100.0,
            90.0,
            130.0,
            &c,
            false,
            true,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(px, 100.0);
        assert_eq!(side, EntryFeeSide::Maker);
    }

    // ── Age abandon ──
    #[test]
    fn hybrid_age_abandon() {
        // past_age true → abandon regardless of price (checked before fills).
        let c = candle(30, 100.5, 100.9, 100.3, 100.6);
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                true,
                true,
                false,
                true
            ),
            HybridAction::Abandon(HybridAbandon::Age)
        );
    }

    // ── Wait when nothing happens ──
    #[test]
    fn hybrid_wait_when_price_stays_in_front() {
        // In-cap, no maker touch, no boundary, no TP → Wait.
        let c = candle(2, 100.5, 100.9, 100.3, 100.6);
        assert_eq!(
            hyb(
                Direction::Bull,
                100.0,
                90.0,
                130.0,
                &c,
                true,
                false,
                false,
                false
            ),
            HybridAction::Wait
        );
    }

    // ─── Integrated hybrid path: placement → seed → decision bar ───────────────

    /// Build a hybrid PaperTrader with canonical knobs.
    fn hybrid_pt() -> PaperTrader {
        let mut pt = PaperTrader::new(0.0, 3.0, 300);
        pt.hybrid_fill = true;
        pt.chase_r = 0.1;
        pt.chase_requires_seed = true;
        pt.immediate_chase_at_open = true;
        pt.past_entry_fee = EntryFeeSide::Taker;
        pt
    }

    #[test]
    fn hybrid_seed_from_t_touch_arms_chase() {
        // No T−1 seed, but the signal bar T itself touches the entry → armed.
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            false,
        );
        // T: touches entry (low ≤ 100). Never fills (signal bar).
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2));
        assert_eq!(pt.opportunities_taken, 0, "T never fills");
        // D opens 100.5 in front within cap → seeded immediate chase.
        pt.update_prices(&candle(1, 100.5, 100.8, 100.4, 100.6));
        assert_eq!(pt.opportunities_taken, 1);
        assert_eq!(pt.hybrid_counters.immediate_chases, 1);
    }

    #[test]
    fn hybrid_seed_from_t_minus_1_touch_arms_chase() {
        // T does NOT touch, but T−1 did (pred_touch=true) → armed.
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        // T: no touch (low 100.4 > entry).
        pt.update_prices(&candle(0, 100.6, 100.9, 100.4, 100.7));
        // D opens in front within cap → immediate chase because pred-seeded.
        pt.update_prices(&candle(1, 100.5, 100.8, 100.4, 100.6));
        assert_eq!(pt.hybrid_counters.immediate_chases, 1);
    }

    #[test]
    fn hybrid_no_seed_does_not_arm_when_required() {
        // Neither T nor T−1 touch; chase_requires_seed=true → unarmed. D opens in
        // front within cap → NO immediate chase; low never reaches E → Wait forever.
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            false,
        );
        pt.update_prices(&candle(0, 100.6, 100.9, 100.4, 100.7)); // T no touch
        pt.update_prices(&candle(1, 100.5, 100.9, 100.4, 100.6)); // D no immediate chase (unarmed)
        assert_eq!(pt.opportunities_taken, 0, "unarmed: no chase");
        // A later bar that touches E fills MAKER (resting limit).
        pt.update_prices(&candle(2, 100.2, 100.4, 99.8, 100.0));
        assert_eq!(pt.hybrid_counters.maker_fills, 1);
    }

    #[test]
    fn hybrid_unseeded_chase_when_seed_not_required() {
        // chase_requires_seed=false → armed even with no touch.
        let mut pt = hybrid_pt();
        pt.chase_requires_seed = false;
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            false,
        );
        pt.update_prices(&candle(0, 100.6, 100.9, 100.4, 100.7)); // T no touch
        pt.update_prices(&candle(1, 100.5, 100.8, 100.4, 100.6)); // D immediate chase
        assert_eq!(pt.hybrid_counters.immediate_chases, 1);
    }

    #[test]
    fn hybrid_case_a_integrated_fill_and_race() {
        // D opens past entry → taker fill @ open; race starts next bar.
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2)); // T (seeds; touches)
        pt.update_prices(&candle(1, 99.5, 100.5, 99.0, 100.0)); // D opens 99.5 past E → fill
        assert_eq!(pt.opportunities_taken, 1);
        assert_eq!(pt.hybrid_counters.past_entry_fills, 1);
        assert_eq!(pt.open_trades[0].fill, 99.5);
        // Fill bar not raced; next bar hits TP → win, R measured from fill 99.5.
        pt.update_prices(&candle(2, 120.0, 131.0, 119.0, 130.0));
        assert_eq!(pt.wins(), 1);
        // Win = (130 − 99.5)/10 = 3.05R (better-than-planned entry → drift up).
        assert!(
            (pt.trades[0].r_pnl - 3.05).abs() < 1e-9,
            "got {}",
            pt.trades[0].r_pnl
        );
    }

    #[test]
    fn hybrid_taker_entry_fee_applied() {
        // Case-A taker fill with fees on → entry leg pays taker (not maker).
        let aid = crate::models::asset_id("ASSET_B"); // intern so asset_name works
        let mut pt = hybrid_pt();
        pt.use_fees = true;
        let mut t = base_trade(Direction::Bull, 100.0, 90.0, 130.0);
        t.asset = aid;
        place_pred(&mut pt, t, true);
        pt.update_prices(&candle_a(aid, 0, 100.5, 101.0, 99.5, 100.2)); // T
        pt.update_prices(&candle_a(aid, 1, 99.5, 100.5, 99.0, 100.0)); // D fill @ 99.5
        pt.update_prices(&candle_a(aid, 2, 120.0, 131.0, 119.0, 130.0)); // TP
        let expected = fees::fee_in_r_side(aid, 100.0, 90.0, EntryFeeSide::Taker);
        assert!(
            (pt.trades[0].fee_r - expected).abs() < 1e-12,
            "got {}",
            pt.trades[0].fee_r
        );
        // And it differs from (exceeds) the maker-entry fee.
        let maker = fees::fee_in_r_side(aid, 100.0, 90.0, EntryFeeSide::Maker);
        assert!(pt.trades[0].fee_r > maker);
    }

    #[test]
    fn hybrid_gap_through_stop_is_no_trade() {
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2)); // T seeds
        pt.update_prices(&candle(1, 89.0, 90.0, 88.0, 89.5)); // D gaps through stop
        assert_eq!(pt.opportunities_taken, 0, "gap-through-stop = NO TRADE");
        assert_eq!(pt.hybrid_counters.abandon_gap_stop, 1);
        assert!(pt.trades.is_empty());
    }

    #[test]
    fn hybrid_age_abandon_at_1800s() {
        // A limit that never fills within 30 min is abandoned by age. 1-min
        // candles: minute 30 is the first strictly past 1800s from opened_at (T@0).
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        pt.update_prices(&candle(0, 100.6, 100.9, 100.4, 100.7)); // T (no touch → but pred seeds)
                                                                  // Bars 1..=30 sit in front, never reaching E or B (open 100.5 within cap,
                                                                  // low 100.3 > E, high 100.9 < B) → immediate chase only fires on D=bar1.
                                                                  // Disable immediate chase so it rests.
        pt.immediate_chase_at_open = false;
        for m in 1..30 {
            pt.update_prices(&candle(m, 100.5, 100.9, 100.3, 100.6));
        }
        assert_eq!(pt.opportunities_taken, 0);
        assert!(!pt.pending.is_empty(), "still resting before 1800s");
        // Minute 30 = 1800s exactly is NOT strictly greater; minute 31 is.
        pt.update_prices(&candle(30, 100.5, 100.9, 100.3, 100.6)); // 1800s, not > → rest
        assert!(!pt.pending.is_empty(), "1800s is not strictly past age");
        pt.update_prices(&candle(31, 100.5, 100.9, 100.3, 100.6)); // 1860s > 1800 → age abandon
        assert!(pt.pending.is_empty(), "past 1800s → abandoned");
        assert_eq!(pt.hybrid_counters.abandon_age, 1);
        assert_eq!(pt.opportunities_taken, 0);
    }

    #[test]
    fn hybrid_age_anchors_at_first_candle_not_signal_label() {
        // HTF signal: `opened_at` is the bar LABEL (ts 0), but the bar only
        // closes — and the order can only start existing — an hour later, so
        // the first candle offered (T) is minute 61. The 30-min age abandon
        // must count from T, not the label: a maker touch at minute 80 (19 min
        // after placement, 80 min after the label) fills. Before the anchor
        // fix this abandoned by age on the first post-T bar (SILVER_0029a5).
        let mut pt = hybrid_pt();
        pt.immediate_chase_at_open = false;
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        pt.update_prices(&candle(61, 100.6, 100.9, 100.4, 100.7)); // T, an hour past the label
        for m in 62..80 {
            pt.update_prices(&candle(m, 100.5, 100.9, 100.3, 100.6)); // resting in front
        }
        assert!(!pt.pending.is_empty(), "resting within 30 min of placement");
        pt.update_prices(&candle(80, 100.5, 100.9, 99.8, 100.6)); // touches E → maker fill
        assert_eq!(pt.opportunities_taken, 1, "filled 19 min after placement");
        assert_eq!(pt.hybrid_counters.abandon_age, 0);
        assert_eq!(pt.hybrid_counters.maker_fills, 1);
    }

    #[test]
    fn hybrid_age_abandon_still_fires_30m_after_late_placement() {
        // Same late-T setup, but price never comes back: the abandon fires 30
        // minutes after T (minute 61+31), not 30 minutes after the label.
        let mut pt = hybrid_pt();
        pt.immediate_chase_at_open = false;
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        pt.update_prices(&candle(61, 100.6, 100.9, 100.4, 100.7)); // T
        for m in 62..=91 {
            pt.update_prices(&candle(m, 100.5, 100.9, 100.3, 100.6));
        }
        assert!(
            !pt.pending.is_empty(),
            "61+30 min = 1800s exactly, not strictly past"
        );
        pt.update_prices(&candle(92, 100.5, 100.9, 100.3, 100.6)); // 1860s past T
        assert!(pt.pending.is_empty(), "abandoned 30 min after placement");
        assert_eq!(pt.hybrid_counters.abandon_age, 1);
        assert_eq!(pt.opportunities_taken, 0);
    }

    // ─── rest-on-Ready EXECUTION lens ───────────────────────────────────────
    //
    // The lens changes only WHO fills the signal bar T: with enough Ready→touch
    // lead the limit was already resting, so T's own touch fills maker at the
    // entry. Everything else is the standard hybrid path.

    /// `place_pred` with `ready_at` stamped `lead_secs` before `opened_at`.
    fn place_with_ready(pt: &mut PaperTrader, trade: PaperTrade, lead_secs: i64) {
        let ready = trade.opened_at - chrono::Duration::seconds(lead_secs);
        place_pred(pt, trade, false);
        pt.pending.last_mut().unwrap().ready_at = Some(ready);
    }

    #[test]
    fn rest_on_ready_signal_bar_fills_maker_at_entry() {
        // Ready one bar before the touch → the order was resting when T opened;
        // T's touch fills maker at exactly the entry, on T itself.
        let mut pt = hybrid_pt();
        pt.rest_on_ready_fill = true;
        place_with_ready(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0), 60);
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2)); // T touches E
        assert_eq!(pt.opportunities_taken, 1, "rested limit fills on T");
        assert_eq!(pt.hybrid_counters.rested_maker_fills, 1);
        assert_eq!(pt.hybrid_counters.maker_fills, 0);
        assert_eq!(
            pt.open_trades[0].fill, 100.0,
            "maker fill at exactly the entry"
        );
        // Race starts next bar; TP bar books the win from the entry fill.
        pt.update_prices(&candle(1, 120.0, 131.0, 119.0, 130.0));
        assert_eq!(pt.wins(), 1);
        assert!(
            (pt.trades[0].r_pnl - 3.0).abs() < 1e-9,
            "got {}",
            pt.trades[0].r_pnl
        );
    }

    #[test]
    fn rest_on_ready_same_bar_ready_falls_back_to_hybrid() {
        // Ready on the touch bar itself (lead 0 < 60) → no rested fill; the
        // standard hybrid path takes over (T seeds, D immediate-chases).
        let mut pt = hybrid_pt();
        pt.rest_on_ready_fill = true;
        place_with_ready(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0), 0);
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2)); // T touches; no lead
        assert_eq!(pt.opportunities_taken, 0, "no lead → T ineligible");
        assert_eq!(pt.hybrid_counters.rested_maker_fills, 0);
        pt.update_prices(&candle(1, 100.5, 100.8, 100.4, 100.6)); // D → immediate chase
        assert_eq!(pt.hybrid_counters.immediate_chases, 1);
    }

    #[test]
    fn rest_on_ready_respects_min_lead_knob() {
        // Lead 60 but the strict knob demands 120 → fallback, not a rested fill.
        let mut pt = hybrid_pt();
        pt.rest_on_ready_fill = true;
        pt.rest_min_lead_secs = 120;
        place_with_ready(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0), 60);
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2));
        assert_eq!(pt.hybrid_counters.rested_maker_fills, 0);
        assert_eq!(pt.opportunities_taken, 0);
    }

    #[test]
    fn rest_on_ready_no_touch_on_t_rests_on() {
        // Lead OK but T never touches the entry → nothing to fill on T; the
        // limit keeps resting and a later touch fills as a normal hybrid maker.
        let mut pt = hybrid_pt();
        pt.rest_on_ready_fill = true;
        pt.immediate_chase_at_open = false;
        place_with_ready(&mut pt, base_trade(Direction::Bull, 100.0, 90.0, 130.0), 60);
        pt.update_prices(&candle(0, 100.6, 100.9, 100.4, 100.7)); // T no touch
        assert_eq!(pt.opportunities_taken, 0);
        pt.update_prices(&candle(1, 100.2, 100.4, 99.8, 100.0)); // later touch
        assert_eq!(
            pt.hybrid_counters.maker_fills, 1,
            "normal hybrid maker path"
        );
        assert_eq!(pt.hybrid_counters.rested_maker_fills, 0);
    }

    #[test]
    fn rest_on_ready_off_is_regression_safe() {
        // Flag off: a stamped ready_at changes nothing — T never fills.
        let mut pt = hybrid_pt();
        place_with_ready(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            600,
        );
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2));
        assert_eq!(pt.opportunities_taken, 0, "flag off: signal bar ineligible");
        assert_eq!(pt.hybrid_counters.rested_maker_fills, 0);
    }

    #[test]
    fn rest_on_ready_missing_ready_at_falls_back() {
        // v1-engine / Model-2 opportunities carry no ready_at → hybrid only.
        let mut pt = hybrid_pt();
        pt.rest_on_ready_fill = true;
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            false,
        );
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2));
        assert_eq!(pt.opportunities_taken, 0);
        assert_eq!(pt.hybrid_counters.rested_maker_fills, 0);
    }

    #[test]
    fn hybrid_ignores_allow_signal_bar_fill_and_slippage() {
        // Both limit-model knobs are set, but hybrid mode ignores them: the signal
        // bar T never fills (despite allow_signal_bar_fill=true), and the fill
        // price is the hybrid open/entry, NOT entry+slip.
        let mut pt = hybrid_pt();
        pt.allow_signal_bar_fill = true; // ignored in hybrid
        pt.entry_slippage_r = 0.5; // ignored in hybrid
        place_pred(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            true,
        );
        // T touches entry; with allow_signal_bar_fill it WOULD fill in limit mode.
        pt.update_prices(&candle(0, 100.5, 101.0, 99.5, 100.2));
        assert_eq!(pt.opportunities_taken, 0, "hybrid: signal bar never fills");
        // D touches E exactly (maker), immediate chase off so we get maker @ entry
        // (NOT entry + 0.5·R = 105).
        pt.immediate_chase_at_open = false;
        pt.update_prices(&candle(1, 100.2, 100.4, 99.8, 100.0)); // low ≤ E → maker @ 100
        assert_eq!(pt.hybrid_counters.maker_fills, 1);
        assert_eq!(
            pt.open_trades[0].fill, 100.0,
            "no slippage applied in hybrid"
        );
    }

    #[test]
    fn hybrid_bear_case_a_integrated() {
        // Bear mirror of the Case-A integrated fill.
        let mut pt = hybrid_pt();
        place_pred(
            &mut pt,
            base_trade(Direction::Bear, 100.0, 110.0, 70.0),
            true,
        );
        pt.update_prices(&candle(0, 99.5, 100.5, 99.0, 100.0)); // T seeds (high ≥ E)
        pt.update_prices(&candle(1, 100.5, 101.0, 99.5, 100.0)); // D opens 100.5 past E → fill
        assert_eq!(pt.hybrid_counters.past_entry_fills, 1);
        assert_eq!(pt.open_trades[0].fill, 100.5);
        pt.update_prices(&candle(2, 80.0, 81.0, 69.0, 70.0)); // TP=70
        assert_eq!(pt.wins(), 1);
        // Win = (100.5 − 70)/10 = 3.05R.
        assert!(
            (pt.trades[0].r_pnl - 3.05).abs() < 1e-9,
            "got {}",
            pt.trades[0].r_pnl
        );
    }

    #[test]
    fn hybrid_boundary_from_below_integrated_bear() {
        // Bear: rest, then a later bar chases at the boundary from below.
        let mut pt = hybrid_pt();
        pt.immediate_chase_at_open = false;
        place_pred(
            &mut pt,
            base_trade(Direction::Bear, 100.0, 110.0, 70.0),
            true,
        );
        pt.update_prices(&candle(0, 99.5, 100.5, 99.0, 100.0)); // T seeds
        pt.update_prices(&candle(1, 99.5, 99.6, 99.2, 99.4)); // D: in front, no fill (rest)
        assert_eq!(pt.opportunities_taken, 0);
        // Later bar: low reaches B=99, high below E → chase from below @ 99.
        pt.update_prices(&candle(2, 99.4, 99.5, 98.8, 99.1));
        assert_eq!(pt.hybrid_counters.boundary_chases_from_below, 1);
        assert_eq!(pt.open_trades[0].fill, 99.0);
    }

    // ─── rest-on-Ready v2 cancel watchdogs ───────────────────────────────────
    //
    // Bull setup throughout: entry 100, stop 90 (risk 10), tp 130. With the
    // tracker's min_rr = 2.0 the stamped min_target is 120: a completed bar
    // CLOSING ≥ 120 while the limit is unfilled cancels the order — but only
    // from the NEXT bar (same-bar touch+condition → the fill stands).

    #[test]
    fn cancel_target_consumed_prior_bar_close_cancels_before_fill() {
        let mut pt = hybrid_pt();
        pt.cancel_on_target_consumed = true;
        place_with_min_target(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            120.0,
        );
        // T: no touch, close 105 < 120 → still pending, no flag.
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0));
        // D: no touch (low 104), but CLOSES 121 ≥ 120 → flag set for next bar.
        pt.update_prices(&candle(1, 105.0, 121.5, 104.0, 121.0));
        assert_eq!(pt.opportunities_taken, 0);
        assert_eq!(
            pt.hybrid_counters.abandon_target_consumed, 0,
            "flag, not yet canceled"
        );
        // Next bar wicks the entry — but the prior-bar cancel wins: NO fill.
        pt.update_prices(&candle(2, 105.0, 105.5, 99.5, 100.2));
        assert_eq!(
            pt.opportunities_taken, 0,
            "canceled before the fill attempt"
        );
        assert_eq!(pt.hybrid_counters.abandon_target_consumed, 1);
        assert_eq!(pt.hybrid_counters.maker_fills, 0);
    }

    #[test]
    fn cancel_target_same_bar_touch_fill_stands() {
        // The SAME bar wicks the entry AND closes beyond min_target: the fill
        // is attempted first → maker fill stands; the cancel never fires.
        let mut pt = hybrid_pt();
        pt.cancel_on_target_consumed = true;
        place_with_min_target(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            120.0,
        );
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0)); // T: no touch
                                                                  // D: unarmed rest band; low 99.5 touches entry → maker fill at 100,
                                                                  // even though the close (121) consumed the target.
        pt.update_prices(&candle(1, 105.0, 121.5, 99.5, 121.0));
        assert_eq!(pt.opportunities_taken, 1, "same-bar: fill stands");
        assert_eq!(pt.hybrid_counters.maker_fills, 1);
        assert_eq!(pt.hybrid_counters.abandon_target_consumed, 0);
        assert_eq!(pt.open_trades[0].fill, 100.0);
    }

    #[test]
    fn cancel_target_signal_bar_close_counts() {
        // T itself closes beyond min_target: the order (placed just after T
        // closes) is canceled at D — even though D touches the entry.
        let mut pt = hybrid_pt();
        pt.cancel_on_target_consumed = true;
        place_with_min_target(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            120.0,
        );
        pt.update_prices(&candle(0, 105.0, 121.5, 104.0, 121.0)); // T closes 121 ≥ 120
        pt.update_prices(&candle(1, 105.0, 105.5, 99.0, 100.0)); // D touches entry
        assert_eq!(pt.opportunities_taken, 0, "T-close cancel beats the D fill");
        assert_eq!(pt.hybrid_counters.abandon_target_consumed, 1);
    }

    #[test]
    fn cancel_target_flag_off_is_inert() {
        // Same price path as the prior-bar-cancel test, but the knob is off:
        // the limit survives and fills maker.
        let mut pt = hybrid_pt();
        place_with_min_target(
            &mut pt,
            base_trade(Direction::Bull, 100.0, 90.0, 130.0),
            120.0,
        );
        pt.update_prices(&candle(0, 105.0, 106.0, 104.0, 105.0));
        pt.update_prices(&candle(1, 105.0, 121.5, 104.0, 121.0)); // close ≥ 120, ignored
        pt.update_prices(&candle(2, 105.0, 105.5, 99.5, 100.2));
        assert_eq!(pt.opportunities_taken, 1);
        assert_eq!(pt.hybrid_counters.maker_fills, 1);
        assert_eq!(pt.hybrid_counters.abandon_target_consumed, 0);
    }

    #[test]
    fn cancel_target_bear_mirror() {
        // Bear: entry 100, stop 110, tp 70; min_target 80 (entry − 2·risk).
        let mut pt = hybrid_pt();
        pt.cancel_on_target_consumed = true;
        place_with_min_target(
            &mut pt,
            base_trade(Direction::Bear, 100.0, 110.0, 70.0),
            80.0,
        );
        pt.update_prices(&candle(0, 95.0, 96.0, 94.0, 95.0)); // T: no touch
        pt.update_prices(&candle(1, 95.0, 96.0, 79.0, 79.5)); // closes 79.5 ≤ 80 → flag
        pt.update_prices(&candle(2, 95.0, 100.5, 94.0, 95.0)); // wicks entry — canceled first
        assert_eq!(pt.opportunities_taken, 0);
        assert_eq!(pt.hybrid_counters.abandon_target_consumed, 1);
    }

    #[test]
    fn setup_invalidated_marks_then_cancels_before_fill() {
        // The invalidation mark arrives externally (a driver polls the
        // strategy's own state as of the PREVIOUS bar's close) — same honesty
        // rule as the target-consumed watchdog.
        let mut pt = hybrid_pt();
        pt.cancel_on_setup_invalidated = true;
        place(&mut pt, base_trade(Direction::Bull, 100.0, 99.0, 103.0));
        // T: no touch, the limit rests.
        pt.update_prices(&candle(0, 101.0, 101.5, 100.6, 101.0));
        // Between bars the driver sees the setup invalidated and marks it.
        let ids = pt.pending_watchdog_ids();
        assert_eq!(ids, vec!["t1".to_string()]);
        pt.cancel_pending_invalidated("t1");
        // D would have filled maker at 100 — but the cancel lands first.
        pt.update_prices(&candle(1, 100.5, 100.8, 99.8, 100.2));
        assert!(pt.trades.is_empty(), "cancelled before the fill attempt");
        assert_eq!(pt.hybrid_counters.abandon_setup_invalidated, 1);
    }

    #[test]
    fn setup_invalidated_flag_off_is_inert() {
        let mut pt = hybrid_pt();
        // Flag left off: the watchdog exposes nothing and marks nothing.
        place(&mut pt, base_trade(Direction::Bull, 100.0, 99.0, 103.0));
        pt.update_prices(&candle(0, 101.0, 101.5, 100.6, 101.0));
        assert!(
            pt.pending_watchdog_ids().is_empty(),
            "flag off → no ids exposed"
        );
        pt.cancel_pending_invalidated("t1"); // no-op
        pt.update_prices(&candle(1, 100.5, 100.8, 99.8, 100.2));
        assert_eq!(pt.trades.len(), 1, "fill stands with the watchdog off");
        assert_eq!(pt.hybrid_counters.abandon_setup_invalidated, 0);
    }
}
