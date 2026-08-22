//! Liq-safe maximum leverage: the highest leverage at which liquidation can
//! never affect a trade's outcome, because the liquidation price is guaranteed
//! to sit at least β stop-distances beyond the stop. Leverage is then purely a
//! margin-efficiency knob, never a risk knob. See specs/liq-safe-leverage.md.
//!
//! HL's maintenance margin is fixed per asset regardless of the selected
//! leverage: `mm = 1/(2·maxLev)`. For an ISOLATED position at leverage L the
//! liquidation distance (adverse move as a fraction of entry) is ~`1/L − mm`,
//! independent of account equity or other positions — which is why isolated
//! mode makes the guarantee structural per position. Solving
//! `1/L − mm ≥ β·s` for the largest L gives the clamp below.

/// Liq-safe leverage for a trade with stop distance `stop_frac` (|entry−stop|
/// as a fraction of entry) on an asset capped at `max_lev`:
///
/// ```text
/// L*(s) = min( maxLev , 1 / (β·s + mm) ),   mm = 1/(2·maxLev)
/// ```
///
/// Floored to a whole number (HL leverage settings are integers); flooring
/// only widens the liq distance, so the β guarantee survives the rounding.
/// Tight stops (`s ≤ mm/β`) return `max_lev` — the clamp is inactive.
pub fn liq_safe_leverage(stop_frac: f64, max_lev: u32, beta: f64) -> u32 {
    if max_lev == 0 {
        return 1;
    }
    // Written as explicit finite-and-positive tests rather than negated
    // comparisons: a NaN input must land here too, and NaN fails every
    // comparison, so `stop_frac <= 0.0` alone would let it through.
    if !stop_frac.is_finite() || stop_frac <= 0.0 || !beta.is_finite() || beta <= 0.0 {
        // Degenerate input: fall back to the cap (a zero-width stop has no
        // meaningful liq constraint; the caller rejects risk<=0 trades anyway).
        return max_lev;
    }
    let mm = 1.0 / (2.0 * max_lev as f64);
    let l_star = 1.0 / (beta * stop_frac + mm);
    (l_star.floor() as u32).clamp(1, max_lev)
}

/// Isolated-mode liquidation distance (fraction of entry) at leverage `lev`
/// for an asset capped at `max_lev`. Negative means the position is born
/// past maintenance (cannot happen for lev ≤ maxLev).
pub fn liq_distance(lev: u32, max_lev: u32) -> f64 {
    let mm = 1.0 / (2.0 * max_lev as f64);
    1.0 / lev as f64 - mm
}

/// The β invariant for a placed entry: the implied liquidation price must sit
/// at least β stop-widths beyond the stop, i.e. `liq_distance ≥ β·s`. Checked
/// on the logging path for every placed entry (never blocks placement).
pub fn liq_invariant_ok(lev: u32, stop_frac: f64, max_lev: u32, beta: f64) -> bool {
    // Tiny epsilon absorbs float noise at the exact boundary.
    liq_distance(lev, max_lev) + 1e-12 >= beta * stop_frac
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tight_stop_returns_max_lev() {
        // Index trade: SP500 at 50x cap, mm = 1%. s = 0.06% → β·s = 0.09%,
        // well under mm → clamp inactive.
        assert_eq!(liq_safe_leverage(0.0006, 50, 1.5), 50);
        // Exactly at the regime boundary s = mm/β the formula gives
        // 1/(2mm) = maxLev.
        let mm = 1.0 / (2.0 * 50.0);
        assert_eq!(liq_safe_leverage(mm / 1.5, 50, 1.5), 50);
    }

    #[test]
    fn wide_stop_clamps() {
        // CL's widest observed stop 4.64% at 20x cap (mm = 2.5%): naked 20x
        // has a 2.5% liq distance — inside the stop. The clamp must cut it.
        let l = liq_safe_leverage(0.0464, 20, 1.5);
        assert!(l < 20, "wide stop must clamp below max: got {l}");
        // 1/(1.5·0.0464 + 0.025) = 10.63 → floor 10
        assert_eq!(l, 10);
        assert!(liq_invariant_ok(l, 0.0464, 20, 1.5));
        // One notch higher would violate the buffer.
        assert!(!liq_invariant_ok(l + 1, 0.0464, 20, 1.5));
    }

    #[test]
    fn invariant_holds_for_all_stop_widths() {
        // Sweep stop widths across both regimes on every deployed asset cap:
        // the floored L* must always satisfy the β invariant.
        for &ml in &[20u32, 25, 30, 50] {
            for i in 1..=2000 {
                let s = i as f64 * 5e-5; // 0.005% .. 10%
                for &beta in &[1.0, 1.5, 2.0, 3.0] {
                    let l = liq_safe_leverage(s, ml, beta);
                    assert!(
                        liq_invariant_ok(l, s, ml, beta),
                        "violated at s={s} ml={ml} beta={beta} → L={l}"
                    );
                }
            }
        }
    }

    #[test]
    fn margin_cost_of_clamp_is_bounded() {
        // Spec: margin-per-$-risk never exceeds 2β for clamped (wide-stop)
        // trades. margin/risk$ = 1/(s·L). Use the real-valued L* (the integer
        // floor adds at most one notch of margin at tiny L, so allow the
        // bound on the un-floored value).
        let beta = 1.5;
        for &ml in &[20u32, 25] {
            let mm = 1.0 / (2.0 * ml as f64);
            for i in 1..=1000 {
                let s = mm / beta + i as f64 * 1e-4; // wide-stop regime only
                let l_star = 1.0 / (beta * s + mm);
                let margin_per_risk = 1.0 / (s * l_star);
                assert!(
                    margin_per_risk <= 2.0 * beta + 1e-9,
                    "margin/risk {margin_per_risk} > 2β at s={s} ml={ml}"
                );
            }
        }
    }

    #[test]
    fn degenerate_inputs_are_safe() {
        assert_eq!(liq_safe_leverage(0.0, 20, 1.5), 20);
        assert_eq!(liq_safe_leverage(f64::NAN, 20, 1.5), 20);
        assert_eq!(liq_safe_leverage(0.5, 20, 1.5), 1); // huge stop → floor at 1x
        assert_eq!(liq_safe_leverage(0.01, 0, 1.5), 1);
    }
}
