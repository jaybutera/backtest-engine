use crate::models::asset_name;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{LazyLock, Mutex};

/// A proportional fee rate pair, in decimal form (not basis points).
/// maker = a resting limit that got hit; taker = an aggressing or triggered
/// fill.
#[derive(Debug, Clone, Copy)]
pub struct FeeRate {
    pub maker: f64,
    pub taker: f64,
}

/// How a venue charges for a round trip.
///
/// Two structurally different models, and the difference matters more than the
/// rates do:
///
///   - **Proportional** (`Perp`, `Spot`) — a fraction of notional, quoted in
///     basis points, with separate maker and taker rates. Fee cost in R scales
///     with `entry / risk`, so a tight stop on an expensive instrument is
///     punished hard.
///   - **Flat per contract** (`Futures`, `FuturesFullSize`) — a fixed dollar
///     amount per contract per side, independent of notional and identical for
///     limit and market fills. Fee cost in R is `round_turn / (risk_points ×
///     point_value)`: no entry-price term at all, and maker/taker is
///     irrelevant.
///
/// Rates below are placeholder defaults. Set your venue's real numbers before
/// trusting any net-of-fee result: fees gate which trades clear a
/// fee-sensitive admission rule, so changing them changes which trades are
/// TAKEN, not just what they earn. Never rescale a fee total to estimate a
/// change — re-run the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeSchedule {
    /// Perpetual futures on a proportional maker/taker schedule. The default.
    Perp,
    /// Spot on a proportional maker/taker schedule, typically with a wider
    /// taker rate and no maker rebate.
    Spot,
    /// Micro futures contracts on a flat per-contract schedule (commission +
    /// exchange + clearing fees, summed over both legs). Priced off the
    /// contract's point value rather than bps of notional; see
    /// `futures_contract`.
    Futures,
    /// Full-size futures contracts: the same flat per-contract structure as
    /// `Futures`, with full-size point values and round turns.
    FuturesFullSize,
}

/// Process-global selected schedule. Set once at startup (from the
/// `--fee-schedule` flag or inferred from the data); `fee_rate_for` reads it
/// on every lookup. Encoded as a u8 so it fits in an `AtomicU8`.
static ACTIVE_SCHEDULE: AtomicU8 = AtomicU8::new(0);

/// Set the active fee schedule. Call once at startup before any backtest runs.
pub fn set_schedule(schedule: FeeSchedule) {
    let v = match schedule {
        FeeSchedule::Perp => 0,
        FeeSchedule::Spot => 1,
        FeeSchedule::Futures => 2,
        FeeSchedule::FuturesFullSize => 3,
    };
    ACTIVE_SCHEDULE.store(v, Ordering::Relaxed);
}

/// The currently active fee schedule (defaults to `Perp`).
pub fn active_schedule() -> FeeSchedule {
    match ACTIVE_SCHEDULE.load(Ordering::Relaxed) {
        1 => FeeSchedule::Spot,
        2 => FeeSchedule::Futures,
        3 => FeeSchedule::FuturesFullSize,
        _ => FeeSchedule::Perp,
    }
}

/// Parse a schedule name, for the CLI. Case-insensitive.
pub fn parse_schedule(s: &str) -> Result<FeeSchedule, String> {
    match s.to_ascii_lowercase().as_str() {
        "perp" => Ok(FeeSchedule::Perp),
        "spot" => Ok(FeeSchedule::Spot),
        "futures" => Ok(FeeSchedule::Futures),
        "futures_full" | "futures_fullsize" => Ok(FeeSchedule::FuturesFullSize),
        other => Err(format!(
            "unknown fee schedule '{other}' (expected perp|spot|futures|futures_full)"
        )),
    }
}

/// Fee rate for any asset with no explicit entry in the schedule. Placeholder
/// values — replace with your venue's published rates.
const DEFAULT_FEE: FeeRate = FeeRate {
    maker: 0.29e-4, // 0.29 bp
    taker: 0.86e-4, // 0.86 bp
};

/// Placeholder spot rate: no maker rebate, wider taker.
const SPOT_FEE: FeeRate = FeeRate {
    maker: 0.0,
    taker: 3.2e-4,
};

/// A flat per-contract futures spec: `(point_value, round_turn)`.
///
/// `point_value` is dollars per one point of price movement for one contract.
/// `round_turn` is the all-in fee for one contract in and out — commission,
/// exchange, clearing, everything — summed over both legs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ContractSpec {
    /// Dollars per point of price movement, per contract.
    pub point_value: f64,
    /// All-in fee per contract for the round trip, in dollars.
    pub round_turn: f64,
}

/// Registered per-contract futures specs, keyed by asset name.
///
/// Empty by default, so a `Futures` schedule with nothing registered falls
/// through to the proportional table and behaves like `Perp`. Register your
/// contracts at startup with [`register_contract`] before running a futures
/// backtest, or the fee model will silently price futures as bps of notional.
static CONTRACTS: LazyLock<Mutex<HashMap<String, ContractSpec>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Same, for the full-size variants of the registered contracts.
static CONTRACTS_FULL: LazyLock<Mutex<HashMap<String, ContractSpec>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register a contract spec for an asset under the `Futures` schedule.
///
/// Look the numbers up in your broker's published rate card; a guessed fee is
/// worse than no fee, because it moves which trades a fee-sensitive admission
/// rule lets through.
///
/// ```
/// use backtest_engine::fees::{self, ContractSpec, FeeSchedule};
/// use backtest_engine::models::asset_id;
///
/// // A contract worth $5 per point, costing $1.90 in and out.
/// fees::register_contract("EXAMPLE", ContractSpec { point_value: 5.0, round_turn: 1.90 });
/// fees::set_schedule(FeeSchedule::Futures);
///
/// // A 4-point stop: 1.90 / (4 * 5) = 0.095 R of fee, whatever the price is.
/// let fee = fees::fee_in_r(asset_id("EXAMPLE"), 6400.0, 6396.0);
/// assert!((fee - 0.095).abs() < 1e-9);
/// # fees::set_schedule(FeeSchedule::Perp);
/// ```
pub fn register_contract(asset: &str, spec: ContractSpec) {
    CONTRACTS.lock().unwrap().insert(asset.to_string(), spec);
}

/// Register a contract spec for an asset under the `FuturesFullSize` schedule.
pub fn register_contract_full(asset: &str, spec: ContractSpec) {
    CONTRACTS_FULL
        .lock()
        .unwrap()
        .insert(asset.to_string(), spec);
}

/// Register a proportional maker/taker rate for one asset, overriding
/// `DEFAULT_FEE` under the `Perp` and `Spot` schedules.
pub fn register_rate(asset: &str, rate: FeeRate) {
    RATES.lock().unwrap().insert(asset.to_string(), rate);
}

/// Drop every registered contract and rate. Mainly for tests that need a
/// known-empty table.
pub fn clear_registrations() {
    CONTRACTS.lock().unwrap().clear();
    CONTRACTS_FULL.lock().unwrap().clear();
    RATES.lock().unwrap().clear();
}

/// Per-asset proportional rate overrides. Empty = every asset pays
/// `DEFAULT_FEE` (or `SPOT_FEE` under the `Spot` schedule).
static RATES: LazyLock<Mutex<HashMap<String, FeeRate>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The registered contract spec for an asset under the active schedule, or
/// `None` when the schedule is proportional or the asset has no spec.
fn contract_spec(name: &str) -> Option<ContractSpec> {
    let table = match active_schedule() {
        FeeSchedule::Futures => &CONTRACTS,
        FeeSchedule::FuturesFullSize => &CONTRACTS_FULL,
        _ => return None,
    };
    table.lock().unwrap().get(name).copied()
}

/// The (maker, taker) proportional rate for an interned asset id under the
/// active schedule.
///
/// Consults the per-asset overrides registered via [`register_rate`] first,
/// then falls back to the schedule's default. Under the flat per-contract
/// schedules this is only reached for assets with NO registered contract spec
/// — mapped assets are priced per-contract in [`fee_in_r_side`] and never
/// touch this table.
pub fn fee_rate_for(asset_id: u16) -> FeeRate {
    let name = asset_name(asset_id);
    if let Some(rate) = RATES.lock().unwrap().get(&*name) {
        return *rate;
    }
    match active_schedule() {
        FeeSchedule::Spot => SPOT_FEE,
        _ => DEFAULT_FEE,
    }
}

/// Which side (maker/taker) the ENTRY leg paid. The exit is always taker (a
/// trigger/market flat), so only the entry side varies:
///   - `Maker`  — a passive GTC limit that rested and got hit at its price.
///   - `Taker`  — an aggressing fill: a marketable limit (price was already
///     past the entry when the order landed), or a market chase. A resting
///     limit is marketable once price has crossed it, so it aggresses and pays
///     taker; the hybrid fill model books those as `Taker`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryFeeSide {
    Maker,
    Taker,
}

/// Round-trip fee in R units for a trade with a MAKER entry.
///
/// `fee_r = (entry / risk) * (maker_rate + taker_rate)`
///
/// Independent of position size. Call [`fee_in_r_side`] when the entry
/// aggressed and paid taker.
pub fn fee_in_r(asset_id: u16, entry: f64, stop: f64) -> f64 {
    fee_in_r_side(asset_id, entry, stop, EntryFeeSide::Maker)
}

/// Compute the round-trip fee in R units, choosing the ENTRY-leg rate.
///
/// - `EntryFeeSide::Maker` ⇒ `(entry/risk)·(maker + taker)` — passive limit entry.
/// - `EntryFeeSide::Taker` ⇒ `(entry/risk)·(taker + taker)` — aggressing entry
///   (marketable limit or market chase). The exit is taker in BOTH cases.
pub fn fee_in_r_side(asset_id: u16, entry: f64, stop: f64, entry_side: EntryFeeSide) -> f64 {
    let risk = (entry - stop).abs();
    if risk <= 0.0 {
        return 0.0;
    }
    if let Some(spec) = contract_spec(&asset_name(asset_id)) {
        // Flat per-contract fees are identical for limit and market fills, so
        // `entry_side` is irrelevant here. A given dollar risk buys
        // `risk_usd / (risk_points · point_value)` contracts, which makes the
        // round-trip fee `round_turn / (risk_points · point_value)` of one R —
        // independent of the dollar risk AND of the entry price. There is no
        // notional term at all, unlike the proportional schedules.
        return spec.round_turn / (risk * spec.point_value);
    }
    // No registered contract spec: fall through to the proportional rates.
    let rate = fee_rate_for(asset_id);
    let entry_rate = match entry_side {
        EntryFeeSide::Maker => rate.maker,
        EntryFeeSide::Taker => rate.taker,
    };
    (entry / risk) * (entry_rate + rate.taker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::asset_id;

    /// Every test drives the process-global schedule and registration tables,
    /// so they run under one lock and restore the defaults when done.
    static GUARD: Mutex<()> = Mutex::new(());

    fn fresh() -> std::sync::MutexGuard<'static, ()> {
        // A poisoned guard means an earlier test panicked; the state is reset
        // below either way, so recover rather than cascade the failure.
        let g = GUARD.lock().unwrap_or_else(|e| e.into_inner());
        clear_registrations();
        set_schedule(FeeSchedule::Perp);
        g
    }

    #[test]
    fn proportional_fee_scales_with_entry_over_risk() {
        let _g = fresh();
        let id = asset_id("PROP_A");
        register_rate(
            "PROP_A",
            FeeRate {
                maker: 1.44e-4,
                taker: 4.32e-4,
            },
        );
        // Entry 50000, stop 49900 → risk 100.
        // (50000/100) * (1.44e-4 + 4.32e-4) = 500 * 5.76e-4 = 0.288 R.
        let fee = fee_in_r(id, 50000.0, 49900.0);
        assert!((fee - 0.288).abs() < 1e-9, "got {fee}");
        // Halving the stop distance doubles the fee in R — the whole point of
        // the proportional model: a tighter stop pays the same dollars over a
        // smaller R.
        let tight = fee_in_r(id, 50000.0, 49950.0);
        assert!((tight - 0.576).abs() < 1e-9, "got {tight}");
    }

    #[test]
    fn unregistered_assets_fall_back_to_the_schedule_default() {
        let _g = fresh();
        let id = asset_id("PROP_UNKNOWN");
        // DEFAULT_FEE: (5000/10) * (0.29e-4 + 0.86e-4) = 500 * 1.15e-4 = 0.0575.
        let fee = fee_in_r(id, 5000.0, 4990.0);
        assert!((fee - 0.0575).abs() < 1e-9, "got {fee}");
    }

    #[test]
    fn spot_schedule_uses_its_own_default() {
        let _g = fresh();
        let id = asset_id("PROP_UNKNOWN");
        let perp = fee_in_r(id, 2000.0, 1990.0);
        set_schedule(FeeSchedule::Spot);
        // SPOT_FEE: (2000/10) * (0 + 3.2e-4) = 200 * 3.2e-4 = 0.064.
        let spot = fee_in_r(id, 2000.0, 1990.0);
        assert!((spot - 0.064).abs() < 1e-9, "got {spot}");
        assert!(
            spot > perp,
            "the placeholder spot taker is the wider of the two"
        );
    }

    #[test]
    fn zero_risk_charges_nothing() {
        let _g = fresh();
        let id = asset_id("PROP_A");
        assert_eq!(fee_in_r(id, 50000.0, 50000.0), 0.0);
        assert_eq!(
            fee_in_r_side(id, 50000.0, 50000.0, EntryFeeSide::Taker),
            0.0
        );
    }

    #[test]
    fn taker_entry_costs_more_than_maker_entry() {
        let _g = fresh();
        let id = asset_id("PROP_B");
        register_rate(
            "PROP_B",
            FeeRate {
                maker: 0.29e-4,
                taker: 0.86e-4,
            },
        );
        // maker entry: (5000/10)*(0.29e-4 + 0.86e-4) = 500*1.15e-4 = 0.0575.
        // taker entry: (5000/10)*(0.86e-4 + 0.86e-4) = 500*1.72e-4 = 0.086.
        let maker = fee_in_r_side(id, 5000.0, 4990.0, EntryFeeSide::Maker);
        let taker = fee_in_r_side(id, 5000.0, 4990.0, EntryFeeSide::Taker);
        assert!((maker - 0.0575).abs() < 1e-9, "maker got {maker}");
        assert!((taker - 0.086).abs() < 1e-9, "taker got {taker}");
        // The convenience wrapper is exactly the maker-entry variant.
        assert_eq!(maker, fee_in_r(id, 5000.0, 4990.0));
        assert!(taker > maker);
    }

    #[test]
    fn parse_schedule_round_trips_and_rejects_junk() {
        assert_eq!(parse_schedule("perp").unwrap(), FeeSchedule::Perp);
        assert_eq!(parse_schedule("PERP").unwrap(), FeeSchedule::Perp);
        assert_eq!(parse_schedule("Spot").unwrap(), FeeSchedule::Spot);
        assert_eq!(parse_schedule("futures").unwrap(), FeeSchedule::Futures);
        assert_eq!(
            parse_schedule("futures_full").unwrap(),
            FeeSchedule::FuturesFullSize
        );
        assert!(parse_schedule("not-a-venue").is_err());
    }

    #[test]
    fn futures_fee_is_flat_per_contract_and_price_independent() {
        let _g = fresh();
        let id = asset_id("FUT_A");
        register_contract(
            "FUT_A",
            ContractSpec {
                point_value: 5.0,
                round_turn: 1.90,
            },
        );
        set_schedule(FeeSchedule::Futures);
        // 4-point stop → 1.90 / (4 * 5) = 0.095 R.
        let fee = fee_in_r(id, 6400.0, 6396.0);
        assert!((fee - 0.095).abs() < 1e-9, "got {fee}");
        // The entry PRICE must not matter: flat dollars, no notional term.
        let low = fee_in_r(id, 5000.0, 4996.0);
        assert!((low - 0.095).abs() < 1e-9, "got {low}");
        // And maker vs taker must not matter either — a futures commission
        // does not distinguish a limit fill from a market fill.
        assert_eq!(fee, fee_in_r_side(id, 6400.0, 6396.0, EntryFeeSide::Taker));
    }

    #[test]
    fn futures_fee_scales_with_the_contract_point_value() {
        let _g = fresh();
        let id = asset_id("FUT_B");
        register_contract(
            "FUT_B",
            ContractSpec {
                point_value: 100.0,
                round_turn: 2.20,
            },
        );
        set_schedule(FeeSchedule::Futures);
        // $0.50 stop on a $100/pt contract → 2.20 / (0.5 * 100) = 0.044 R.
        let fee = fee_in_r(id, 65.0, 64.5);
        assert!((fee - 0.044).abs() < 1e-9, "got {fee}");
    }

    #[test]
    fn full_size_contracts_are_cheaper_per_r_than_micros() {
        let _g = fresh();
        let id = asset_id("FUT_A");
        // A micro at $5/pt and its full-size sibling at $50/pt: ten times the
        // point value for roughly three times the round turn, so the same stop
        // costs materially less R on the full-size contract.
        register_contract(
            "FUT_A",
            ContractSpec {
                point_value: 5.0,
                round_turn: 1.90,
            },
        );
        register_contract_full(
            "FUT_A",
            ContractSpec {
                point_value: 50.0,
                round_turn: 5.76,
            },
        );

        set_schedule(FeeSchedule::Futures);
        let micro = fee_in_r(id, 6400.0, 6396.0); // 1.90 / 20  = 0.095
        set_schedule(FeeSchedule::FuturesFullSize);
        let full = fee_in_r(id, 6400.0, 6396.0); // 5.76 / 200 = 0.0288
        assert!((full - 0.0288).abs() < 1e-9, "got {full}");
        assert!(
            full < micro / 3.0,
            "full {full} should be >3x cheaper than micro {micro}"
        );
    }

    #[test]
    fn a_futures_schedule_falls_back_for_unregistered_assets() {
        let _g = fresh();
        let id = asset_id("PROP_A");
        register_rate(
            "PROP_A",
            FeeRate {
                maker: 1.44e-4,
                taker: 4.32e-4,
            },
        );
        // Deliberately register NO contract spec for this asset: a mixed
        // basket can hold instruments the futures venue does not list.
        let perp = fee_in_r(id, 50000.0, 49900.0);
        set_schedule(FeeSchedule::Futures);
        let futures = fee_in_r(id, 50000.0, 49900.0);
        assert_eq!(
            perp, futures,
            "no contract spec → keep the proportional rate"
        );
    }

    #[test]
    fn an_empty_contract_table_never_silently_zeroes_a_fee() {
        let _g = fresh();
        // The trap this guards: selecting a futures schedule and registering
        // nothing. Every asset must still be charged something, via the
        // proportional fallback, rather than trading fee-free.
        set_schedule(FeeSchedule::Futures);
        let fee = fee_in_r(asset_id("PROP_UNKNOWN"), 5000.0, 4990.0);
        assert!(
            fee > 0.0,
            "an unregistered futures asset must still pay a fee"
        );
    }
}
