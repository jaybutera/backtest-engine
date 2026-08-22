//! A backtest engine: everything a trading strategy needs around it, and
//! nothing that decides what to trade.
//!
//! # What this crate is
//!
//! You write a [`Strategy`](strategy::Strategy) — one method that turns
//! candles into [`Opportunity`](models::Opportunity) values. This crate does
//! the rest: loads parquet candles and tick data, aggregates higher
//! timeframes, models what would have happened to a real order at your prices,
//! charges fees, keeps the books in R-multiples, and reports.
//!
//! The split matters because the two halves fail differently. A strategy is
//! wrong when its idea about the market is wrong. An engine is wrong when it
//! quietly flatters every strategy that runs on it — by filling orders that
//! would have been missed, by resolving unknowable intrabar races in your
//! favor, by pricing fees off the wrong side of the book. Those errors are
//! invisible in the output and they inflate every result equally, which is why
//! the fill model here is deliberately pessimistic wherever the truth is
//! unknowable.
//!
//! # The fill model is the point
//!
//! Most of the code in [`paper`] exists to answer one question honestly: given
//! a decision to buy at a price, what actually happens? The naive answer — "it
//! fills at that price" — is wrong in both directions at once. It fills orders
//! that price never reached, and it fills at the limit price orders that in
//! practice had to chase. Several lenses are provided:
//!
//! - **limit** — a pure resting order that fills only when price reaches it.
//! - **hybrid** — models an order-management watchdog: pays taker when it
//!   aggresses past the entry, chases within a cap, abandons on age or when
//!   the move has gone.
//! - **tick** — resolves entries and exits against real trade prints in true
//!   time order, replacing the intrabar guess with intrabar fact.
//!
//! Running one strategy under several lenses tells you how much of a result is
//! the idea and how much is the assumption about execution. That difference is
//! usually larger than people expect, which is the reason this is configurable
//! rather than fixed.
//!
//! # Getting started
//!
//! ```no_run
//! use backtest_engine::example_strategy::MaCrossover;
//! use backtest_engine::paper::PaperTrader;
//! use backtest_engine::pipeline::Pipeline;
//! use backtest_engine::timeframe::TimeframeBuilder;
//! use backtest_engine::models::asset_id;
//!
//! let asset = asset_id("EXAMPLE");
//! let mut pipeline = Pipeline::new();
//! pipeline.insert_asset(
//!     asset,
//!     MaCrossover::default(),
//!     TimeframeBuilder::new(&[]),
//!     PaperTrader::new(0.0, 2.0, 300),
//! );
//!
//! // for candle in candles { pipeline.process_candle(&candle); }
//!
//! let trader = pipeline.finish(asset).unwrap();
//! trader.render_text("example run");
//! ```
//!
//! [`example_strategy::MaCrossover`] is a naive moving-average crossover,
//! included so the engine runs end to end out of the box. It is a control, not
//! an edge — see its module docs.
//!
//! # A warning about fees
//!
//! [`fees`] ships with placeholder rates. Fees are not a rounding error on a
//! high-turnover strategy; they routinely decide the sign of the result, and a
//! fee-aware admission rule means changing them changes which trades are
//! TAKEN, not just what those trades earn. Register your venue's published
//! numbers before believing any net-of-fee figure, and re-run rather than
//! rescaling an existing total.

// The report's JSON literal nests deeply enough to exceed the default macro
// recursion limit.
#![recursion_limit = "256"]

pub mod atr;
pub mod data;
pub mod driver;
pub mod example_strategy;
pub mod fees;
pub mod hold;
pub mod l2book;
pub mod leverage;
pub mod models;
pub mod paper;
pub mod params;
pub mod pipeline;
pub mod strategy;
pub mod strategy_config;
pub mod timeframe;

pub use models::{Candle, Direction, Opportunity, PaperTrade, Tick, TradeResult};
pub use paper::PaperTrader;
pub use pipeline::Pipeline;
pub use strategy::{AdmitContext, Decision, SkipReason, Strategy, TakeParams};
