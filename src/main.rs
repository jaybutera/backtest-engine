//! `backtest` — replay historical candles through a strategy and report.
//!
//! The whole driver lives in [`backtest_engine::driver`]; this binary only
//! says which strategy factories it knows. To backtest your own strategy,
//! write a binary like this one in your own crate and register your own
//! factories — the driver, config loading, data, fills and report come from
//! the library unchanged.

use backtest_engine::driver;
use backtest_engine::example_strategy::MaCrossoverFactory;

fn main() {
    driver::main(&[&MaCrossoverFactory]);
}
