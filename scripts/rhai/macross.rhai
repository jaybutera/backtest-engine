// A Rhai port of the bundled moving-average crossover (example_strategy.rs),
// kept operation-for-operation identical so the two can be compared trade
// for trade. A control for the scripting layer, not an edge.

fn init(cfg) {
    #{
        fast: cfg.script.ma_fast,
        slow: cfg.script.ma_slow,
        stop_mult: cfg.script.ma_stop_atr_mult,
        fast_above: (),
    }
}

fn mean_closes(h, n) {
    let s = 0.0;
    for i in 0..n { s += h.close(i); }
    s / n.to_float()
}

fn mean_ranges(h, n) {
    let s = 0.0;
    for i in 0..n { s += h.high(i) - h.low(i); }
    s / n.to_float()
}

fn on_candle(c) {
    if c.tf != "1m" { return []; }
    let h = hist("1m");
    if h.len < this.slow { return []; }
    // The native version sums oldest-first; summation order matters for
    // bit-exact parity, so sum from the oldest of the window too.
    let slow = this.slow;
    let fast = this.fast;
    let s_slow = 0.0;
    for i in 0..slow { s_slow += h.close(slow - 1 - i); }
    let slow_ma = s_slow / slow.to_float();
    let s_fast = 0.0;
    for i in 0..fast { s_fast += h.close(fast - 1 - i); }
    let fast_ma = s_fast / fast.to_float();
    let now_above = fast_ma > slow_ma;
    let was = this.fast_above;
    this.fast_above = now_above;
    if was == () { return []; }
    if was == now_above { return []; }
    let s_rng = 0.0;
    for i in 0..slow { s_rng += h.high(slow - 1 - i) - h.low(slow - 1 - i); }
    let atr = s_rng / slow.to_float();
    let risk = fmax(this.stop_mult * atr, abs(c.close) * 1e-6);
    if !risk.is_finite() || risk <= 0.0 { return []; }
    let dir = if now_above { "bull" } else { "bear" };
    let entry = c.close;
    let stop = if now_above { entry - risk } else { entry + risk };
    let o = opp(if now_above { "ma_cross_up" } else { "ma_cross_down" }, "1m", dir, c.ts);
    o.score = 1.0;
    o.entry = entry;
    o.stop = stop;
    [o]
}
