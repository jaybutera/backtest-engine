// RSI-ATR pullback: the strategy behind the bundled demo.
//
// It began as a MetaTrader expert, was rewritten as a batch backtest binary,
// then as a streaming core for the live trader, and finally as this script;
// each rewrite was checked trade for trade against the previous one, and
// the engine features it needed (market-on-open entries, gap-at-open exits,
// native windows) are what commit 7a580b7 added. The rules:
//
//   - working-TF bars aggregate the 1m stream by (ts + anchor) div tf (the
//     engine's native `window`); a bar's ts is its window start; sub-H4 rides
//     the UTC grid, H4+ anchors 22:00 UTC.
//   - at the first 1m candle of a new window: BUY when close[1] > EMA[1], RSI
//     re-crossed up through the buy level (RSI[2] < buy && RSI[1] >= buy),
//     close[1] bullish and above high[2]; SELL mirrored. Entry at that candle's
//     open (market-on-open, raced on the same candle); SL = close[1] -/+
//     slMult*ATR[1]; TP = entry +/- slMult*ATR[1]*rr.
//   - one open position; a window start t <= last exit ts cannot re-enter;
//     optional consecutive-loss cooldown, net of fees.
//   - SL/TP resolve on the 1m stream: gap at the open first (exit at the open,
//     fill lens `exit_gap_at_open`), then SL-first inside an ambiguous bar.
//
// Indicators are streaming Wilder ATR, Wilder RSI and EMA, accumulated in
// the same association order as the batch binary (parity is bit-equality,
// not closeness).
//
// Nothing here is trading advice. The parameters in config/strategy/rsi_atr.toml
// are the ones that were traded; run the ensemble before believing a number.

fn init(cfg) {
    let s = cfg.script;
    // The preset lists every leg in one file: [script] carries the shared
    // keys and [script.per_asset.<ASSET>] the ones this asset changes. The
    // engine builds one instance per asset, so merge this asset's block over
    // the shared keys. A preset without the table runs on [script] alone.
    if "per_asset" in s && cfg.asset in s.per_asset {
        let o = s.per_asset[cfg.asset];
        for k in o.keys() { s[k] = o[k]; }
    }
    let tf = s.tf_secs;
    let ema_p = s.ema_period;
    let rsi_p = s.rsi_period;
    let atr_p = s.atr_period;
    let warm = ema_p;
    if rsi_p + 1 > warm { warm = rsi_p + 1; }
    if atr_p + 1 > warm { warm = atr_p + 1; }
    #{
        tf: tf,
        anchor: if tf >= 14400 { 7200 } else { 0 },
        ema_p: ema_p, rsi_p: rsi_p, atr_p: atr_p,
        rsi_buy: s.rsi_buy, rsi_sell: s.rsi_sell,
        sl_mult: s.sl_atr_mult, rr: s.rr,
        cd_on: s.cooldown_losses > 0 && s.cooldown_hours > 0.0,
        cd_losses: s.cooldown_losses,
        cd_secs: to_int_trunc(s.cooldown_hours * 3600.0),
        warmup: warm,
        // the working-TF aggregator; the engine steps it natively and calls
        // on_bar_close only when it rolls or a trade closes
        win: window(tf, if tf >= 14400 { 7200 } else { 0 }, 2),
        // Wilder ATR
        atr_has_prev: false, atr_prev_close: 0.0, atr_seed: 0.0, atr_n: 0, atr: 0.0, atr_seeded: false,
        // Wilder RSI
        rsi_has_prev: false, rsi_prev_close: 0.0, rsi_n: 0,
        sum_gain: 0.0, sum_loss: 0.0, avg_gain: 0.0, avg_loss: 0.0, rsi: 0.0, rsi_seeded: false,
        // EMA
        ema_has: false, ema: 0.0, alpha: 2.0 / (ema_p.to_float() + 1.0),
        // last two closed bars: s1 (newest) and s2
        closed_count: 0,
        c1o: 0.0, c1h: 0.0, c1l: 0.0, c1c: 0.0, rsi1: 0.0,
        c2h: 0.0, c2l: 0.0, rsi2: 0.0,
        ema_s1: 0.0, atr_s1: 0.0,
        // position / cooldown
        in_pos: false, last_exit_ts: -9223372036854775807, losses: 0, stop_until: -9223372036854775807,
    }
}

fn on_exit(t, ts) {
    this.in_pos = false;
    this.last_exit_ts = ts;
    let r_net = t.r_pnl;
    if r_net < 0.0 {
        this.losses += 1;
        if this.cd_on && this.losses >= this.cd_losses {
            this.stop_until = ts + this.cd_secs;
            this.losses = 0;
        }
    } else if r_net > 0.0 {
        this.losses = 0;
    }
}

fn note_closed_bar(o, h, l, c) {
    // ATR
    let tr = if this.atr_has_prev {
        let pc = this.atr_prev_close;
        let a = h - l;
        let b = (h - pc).abs();
        let d = (l - pc).abs();
        if b > a { a = b; }
        if d > a { a = d; }
        a
    } else {
        h - l
    };
    this.atr_has_prev = true;
    this.atr_prev_close = c;
    if !this.atr_seeded {
        this.atr_seed += tr;
        this.atr_n += 1;
        if this.atr_n == this.atr_p {
            this.atr = this.atr_seed / this.atr_p.to_float();
            this.atr_seeded = true;
        }
    } else {
        let p = this.atr_p.to_float();
        this.atr = (this.atr * (p - 1.0) + tr) / p;
    }
    // RSI
    if !this.rsi_has_prev {
        this.rsi_has_prev = true;
        this.rsi_prev_close = c;
    } else {
        let d = c - this.rsi_prev_close;
        this.rsi_prev_close = c;
        this.rsi_n += 1;
        let p = this.rsi_p.to_float();
        if !this.rsi_seeded {
            if d > 0.0 { this.sum_gain += d; } else { this.sum_loss += -d; }
            if this.rsi_n == this.rsi_p {
                this.avg_gain = this.sum_gain / p;
                this.avg_loss = this.sum_loss / p;
                this.rsi = rsi_calc(this.avg_gain, this.avg_loss);
                this.rsi_seeded = true;
            }
        } else {
            let g = 0.0; let lo = 0.0;
            if d > 0.0 { g = d; } else { lo = -d; }
            this.avg_gain = (this.avg_gain * (p - 1.0) + g) / p;
            this.avg_loss = (this.avg_loss * (p - 1.0) + lo) / p;
            this.rsi = rsi_calc(this.avg_gain, this.avg_loss);
        }
    }
    // EMA
    if this.ema_has {
        this.ema = this.alpha * c + (1.0 - this.alpha) * this.ema;
    } else {
        this.ema_has = true;
        this.ema = c;
    }
    // shift s1 -> s2
    this.c2h = this.c1h; this.c2l = this.c1l; this.rsi2 = this.rsi1;
    this.c1o = o; this.c1h = h; this.c1l = l; this.c1c = c;
    this.rsi1 = if this.rsi_seeded { this.rsi } else { 0.0 };
    this.ema_s1 = this.ema;
    this.atr_s1 = if this.atr_seeded { this.atr } else { 0.0 };
    this.closed_count += 1;
}

fn rsi_calc(g, l) {
    if l == 0.0 {
        if g == 0.0 { 50.0 } else { 100.0 }
    } else {
        100.0 - 100.0 / (1.0 + g / l)
    }
}

fn decide(t, c, book) {
    if this.closed_count < 2 { return; }
    if this.closed_count - 1 < this.warmup { return; }
    if this.in_pos || t <= this.last_exit_ts { return; }
    if this.cd_on && t < this.stop_until { return; }
    let atr1 = this.atr_s1;
    if atr1 <= 0.0 { return; }
    let c1c = this.c1c;
    let buy = c1c > this.ema_s1 && this.rsi2 < this.rsi_buy && this.rsi1 >= this.rsi_buy
        && c1c > this.c1o && c1c > this.c2h;
    let sell = c1c < this.ema_s1 && this.rsi2 > this.rsi_sell && this.rsi1 <= this.rsi_sell
        && c1c < this.c1o && c1c < this.c2l;
    if !buy && !sell { return; }
    let entry = c.open;
    let dist = this.sl_mult * atr1;
    let sl = if buy { c1c - dist } else { c1c + dist };
    let tp = if buy { entry + dist * this.rr } else { entry - dist * this.rr };
    let risk = if buy { entry - sl } else { sl - entry };
    if risk <= 0.0 { return; }   // gapped through the stop
    let id = "rsi_" + t;
    let ok = book.market_entry(#{
        id: id, signal_type: if buy { "rsi_atr_buy" } else { "rsi_atr_sell" },
        direction: if buy { "bull" } else { "bear" },
        stop: sl, tp: tp, score: 1.0, at: "open",
    });
    if !ok { return; }
    if book.has_open() {
        this.in_pos = true;
    } else {
        // resolved on its own entry candle
        for x in book.closed() { if x.id == id { this.on_exit(x, c.ts); } }
    }
}

fn on_bar_close(c, book) {
    if this.in_pos {
        for x in book.closed() { this.on_exit(x, c.ts); }
    }
    let w = this.win;
    if !w.rolled { return; }
    let b = w.closed;
    if b != () {
        this.note_closed_bar(b.open, b.high, b.low, b.close);
    }
    this.decide(w.start, c, book);
}
