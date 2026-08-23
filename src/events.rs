//! Book events: a stream of everything that happens to orders and positions,
//! as it happens.
//!
//! The trade ledger ([`crate::paper::PaperTrader::trades`]) is a *summary* —
//! it says what each trade ended up being. A live consumer (an order
//! executor mirroring the simulated book onto a real venue, a monitoring
//! journal, a state snapshotter) needs the *transitions*: an entry limit was
//! placed, it filled or was cancelled, a stop moved, the position closed and
//! why. This module is that vocabulary.
//!
//! Events are recorded by [`crate::paper::PaperTrader`] only when its
//! `record_events` flag is on (a backtest leaves it off and pays nothing),
//! and drained with [`crate::paper::PaperTrader::take_events`] — typically
//! once per pushed candle by a [`crate::session::Session`].
//!
//! Recording events changes nothing about how the book behaves: the same
//! run with recording on and off books identical trades. That property is
//! what lets one code path serve both batch replay and live streaming.

use chrono::NaiveDateTime;

use crate::models::{asset_name, sig_type_name, Direction, TradeResult};

/// Why a position closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    /// A genuine stop-loss exit at the (possibly overridden) stop.
    Stop,
    /// A trailing-lock exit: the stop had been ratcheted to a profit-lock
    /// level, so this is not a stop-out in the loss sense.
    TrailLock,
    /// The take-profit was reached.
    Target,
    /// The max-hold clock expired; closed at the bar's close.
    Timeout,
    /// The de-risk rule closed a stale under-threshold trade at the close.
    Derisk,
    /// The strategy closed it via a management action
    /// ([`crate::strategy::Book::close`]).
    Management,
}

impl ExitReason {
    /// Short, stable, lowercase name for journals.
    pub fn as_str(&self) -> &'static str {
        match self {
            ExitReason::Stop => "stop",
            ExitReason::TrailLock => "trail_lock",
            ExitReason::Target => "target",
            ExitReason::Timeout => "timeout",
            ExitReason::Derisk => "derisk",
            ExitReason::Management => "management",
        }
    }
}

/// Why a resting entry limit was cancelled unfilled. Mirrors the fill
/// models' abandon vocabulary; no trade is ever recorded for these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// The hard fill deadline lapsed with no fill.
    Deadline,
    /// Hybrid: a bar (or gap) opened at/through the stop while unfilled.
    GapStop,
    /// Hybrid: a bar opened at/past the take-profit while unfilled.
    TpOpen,
    /// Hybrid: the take-profit was reached intrabar while unfilled.
    TpRange,
    /// Hybrid: the order rested past its maximum age.
    Age,
    /// Watchdog: a completed bar closed beyond the stamped `min_target`.
    TargetConsumed,
    /// Watchdog: the strategy's setup was invalidated on a completed bar.
    SetupInvalidated,
    /// The driver or strategy cancelled it outright
    /// ([`crate::paper::PaperTrader::cancel_pending_by_id`]).
    Driver,
}

impl CancelReason {
    /// Short, stable, lowercase name for journals.
    pub fn as_str(&self) -> &'static str {
        match self {
            CancelReason::Deadline => "deadline",
            CancelReason::GapStop => "gap_stop",
            CancelReason::TpOpen => "tp_open",
            CancelReason::TpRange => "tp_range",
            CancelReason::Age => "age",
            CancelReason::TargetConsumed => "target_consumed",
            CancelReason::SetupInvalidated => "setup_invalidated",
            CancelReason::Driver => "driver",
        }
    }
}

/// One transition of the simulated book. Every variant carries the
/// opportunity id (the stable key across a trade's whole life) and the
/// asset, so a consumer can route without extra lookups.
#[derive(Debug, Clone)]
pub enum BookEvent {
    /// An emitted opportunity was offered and skipped, with the strategy's
    /// (or the engine's) reason. No order exists.
    Skipped {
        opportunity_id: String,
        asset: u16,
        signal_type: u16,
        reason: String,
        at: NaiveDateTime,
    },
    /// An entry limit is now resting at `entry`, with its planned geometry.
    EntryPlaced {
        opportunity_id: String,
        asset: u16,
        signal_type: u16,
        direction: Direction,
        entry: f64,
        stop: f64,
        tp: f64,
        score: f64,
        at: NaiveDateTime,
    },
    /// An entry filled and the position is open. Carries the full geometry
    /// so a consumer can act on this event alone — market entries booked by
    /// a management hook never had an `EntryPlaced`.
    EntryFilled {
        opportunity_id: String,
        asset: u16,
        signal_type: u16,
        direction: Direction,
        price: f64,
        entry: f64,
        stop: f64,
        tp: f64,
        /// True for a maker fill at the limit price, false for an
        /// aggressing (taker) fill — a chase, a past-entry open, a market
        /// entry.
        maker: bool,
        at: NaiveDateTime,
    },
    /// A resting entry limit was cancelled unfilled. No trade exists.
    EntryCancelled {
        opportunity_id: String,
        asset: u16,
        reason: CancelReason,
        at: NaiveDateTime,
    },
    /// A strategy management action moved an open trade's stop.
    StopMoved {
        opportunity_id: String,
        asset: u16,
        stop: f64,
        at: NaiveDateTime,
    },
    /// A strategy management action moved an open trade's take-profit.
    TargetMoved {
        opportunity_id: String,
        asset: u16,
        tp: f64,
        at: NaiveDateTime,
    },
    /// The partial take-profit level was first reached on an open trade
    /// (only under `partial_tp_r > 0`).
    PartialBanked {
        opportunity_id: String,
        asset: u16,
        at: NaiveDateTime,
    },
    /// The position closed. `r_pnl`/`fee_r` are the final booked values.
    Closed {
        opportunity_id: String,
        asset: u16,
        reason: ExitReason,
        result: TradeResult,
        r_pnl: f64,
        fee_r: f64,
        at: NaiveDateTime,
    },
}

impl BookEvent {
    /// The opportunity id this event belongs to.
    pub fn opportunity_id(&self) -> &str {
        match self {
            BookEvent::Skipped { opportunity_id, .. }
            | BookEvent::EntryPlaced { opportunity_id, .. }
            | BookEvent::EntryFilled { opportunity_id, .. }
            | BookEvent::EntryCancelled { opportunity_id, .. }
            | BookEvent::StopMoved { opportunity_id, .. }
            | BookEvent::TargetMoved { opportunity_id, .. }
            | BookEvent::PartialBanked { opportunity_id, .. }
            | BookEvent::Closed { opportunity_id, .. } => opportunity_id,
        }
    }

    /// The asset this event belongs to (interned id).
    pub fn asset(&self) -> u16 {
        match self {
            BookEvent::Skipped { asset, .. }
            | BookEvent::EntryPlaced { asset, .. }
            | BookEvent::EntryFilled { asset, .. }
            | BookEvent::EntryCancelled { asset, .. }
            | BookEvent::StopMoved { asset, .. }
            | BookEvent::TargetMoved { asset, .. }
            | BookEvent::PartialBanked { asset, .. }
            | BookEvent::Closed { asset, .. } => *asset,
        }
    }

    /// The event's timestamp (the simulated time it happened at).
    pub fn at(&self) -> NaiveDateTime {
        match self {
            BookEvent::Skipped { at, .. }
            | BookEvent::EntryPlaced { at, .. }
            | BookEvent::EntryFilled { at, .. }
            | BookEvent::EntryCancelled { at, .. }
            | BookEvent::StopMoved { at, .. }
            | BookEvent::TargetMoved { at, .. }
            | BookEvent::PartialBanked { at, .. }
            | BookEvent::Closed { at, .. } => *at,
        }
    }

    /// Short, stable, lowercase event name for journals.
    pub fn kind(&self) -> &'static str {
        match self {
            BookEvent::Skipped { .. } => "skipped",
            BookEvent::EntryPlaced { .. } => "entry_placed",
            BookEvent::EntryFilled { .. } => "entry_filled",
            BookEvent::EntryCancelled { .. } => "entry_cancelled",
            BookEvent::StopMoved { .. } => "stop_moved",
            BookEvent::TargetMoved { .. } => "target_moved",
            BookEvent::PartialBanked { .. } => "partial_banked",
            BookEvent::Closed { .. } => "closed",
        }
    }

    /// A JSON object for one journal line, with interned ids resolved to
    /// names and timestamps in `%Y-%m-%dT%H:%M:%S`. Key order is sorted by
    /// serde_json, so identical events serialize identically.
    pub fn to_json(&self) -> serde_json::Value {
        const T: &str = "%Y-%m-%dT%H:%M:%S";
        match self {
            BookEvent::Skipped {
                opportunity_id,
                asset,
                signal_type,
                reason,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "signal_type": &*sig_type_name(*signal_type),
                "reason": reason,
                "at": at.format(T).to_string(),
            }),
            BookEvent::EntryPlaced {
                opportunity_id,
                asset,
                signal_type,
                direction,
                entry,
                stop,
                tp,
                score,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "signal_type": &*sig_type_name(*signal_type),
                "direction": direction.as_str(),
                "entry": entry,
                "stop": stop,
                "tp": tp,
                "score": score,
                "at": at.format(T).to_string(),
            }),
            BookEvent::EntryFilled {
                opportunity_id,
                asset,
                signal_type,
                direction,
                price,
                entry,
                stop,
                tp,
                maker,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "signal_type": &*sig_type_name(*signal_type),
                "direction": direction.as_str(),
                "price": price,
                "entry": entry,
                "stop": stop,
                "tp": tp,
                "maker": maker,
                "at": at.format(T).to_string(),
            }),
            BookEvent::EntryCancelled {
                opportunity_id,
                asset,
                reason,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "reason": reason.as_str(),
                "at": at.format(T).to_string(),
            }),
            BookEvent::StopMoved {
                opportunity_id,
                asset,
                stop,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "stop": stop,
                "at": at.format(T).to_string(),
            }),
            BookEvent::TargetMoved {
                opportunity_id,
                asset,
                tp,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "tp": tp,
                "at": at.format(T).to_string(),
            }),
            BookEvent::PartialBanked {
                opportunity_id,
                asset,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "at": at.format(T).to_string(),
            }),
            BookEvent::Closed {
                opportunity_id,
                asset,
                reason,
                result,
                r_pnl,
                fee_r,
                at,
            } => serde_json::json!({
                "event": self.kind(),
                "opportunity_id": opportunity_id,
                "asset": &*asset_name(*asset),
                "reason": reason.as_str(),
                "result": result.as_str(),
                "r_pnl": r_pnl,
                "fee_r": fee_r,
                "at": at.format(T).to_string(),
            }),
        }
    }
}
