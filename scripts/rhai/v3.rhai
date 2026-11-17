// v3: setups stated ahead of price.
//
// Every setup here ends in a resting plan: "if price comes to this entry,
// take it, with this stop and this target". The plan is placed as soon as
// the setup is complete, not when price has already come back, and it is
// cancelled when the idea dies.
//
// Setup 1, sweep / break / gap:
//   1. A level is swept (the scanner's sweep; `dir` is the reversal side).
//   2. Structure shifts: a bar of the sweep's timeframe closes beyond the
//      swing that launched the run into the sweep. That swing is the most
//      recent short-term swing on that timeframe, on the reversal side (a
//      low for a bear setup), whose pivot bar is before the sweep's extreme,
//      which had not been closed through before that extreme, and which lies
//      beyond the swept level. A swing that forms after the extreme can
//      never be the level. The engine's break events (a close past the
//      LATEST swing) are not used.
//   3. The entry is a gap made by the move that broke structure: the first
//      gap whose third candle is after the sweep's extreme and no later than
//      the candle after the break candle, and whose height is at least
//      `gap_min_risk_frac` of the entry-to-stop distance. Limit at its near
//      edge, stop at the sweep's extreme, target at the nearest opposing
//      level at least `min_target_rr` away. Once the candle after the break
//      has closed, a setup with no such gap is dead: the leg is over.
//
// The plan dies, and its resting order is cancelled, when a bar closes back
// through the sweep's extreme, when the gap inverts, or after
// `max_bars_after_sweep` bars of the sweep's timeframe.

fn init(cfg) {
    let s = cfg.script;
    // Decision trace for the replay debugger; off unless the run is traced.
    let tr = false;
    try { tr = tracing(); } catch { }
    #{
        asset: cfg.asset,
        tr: tr,
        scan: scanner(#{
            primitives: #{
                atr_period: 14, swing_lookback: 5,
                fvg_min_gap_pct: 0.0, structure_min_displacement_atr: s.break_displacement_atr,
            },
            significance: #{
                base: #{ pdh: 4.0, pdl: 4.0, pwh: 5.0, pwl: 5.0, session_high: 3.0, session_low: 3.0,
                         swing_high: 1.0, swing_low: 1.0 },
                tf_scaled: ["swing_high", "swing_low"],
                tf_mult: #{ "15m": 1.5, "1h": 2.0, "4h": 3.0 },
                touch_bonus: [[2, 1.0], [3, 2.0]],
            },
            levels: #{
                cluster_atr_tolerance: 0.5, decay_candles: 300, refresh_atr: 0.5,
                tf_minutes: #{ "5m": 5.0, "15m": 15.0, "1h": 60.0, "4h": 240.0, "1D": 1440.0, "1W": 10080.0 },
            },
            sweep: #{ noise_atr: s.noise_atr, max_multi_candle: 3, min_level_sig: s.min_level_sig },
            sessions: #{
                enabled: true, tz_offset_secs: -18000,
                sessions: [#{ name: "asia", start_hour: 20, end_hour: 0 },
                           #{ name: "london", start_hour: 2, end_hour: 5 },
                           #{ name: "ny_am", start_hour: 7, end_hour: 10 }],
            },
            fvg: #{ cap: 500 },
            day_tracker: false,
        }),
        min_sig: s.min_level_sig,
        max_bars: s.max_bars_after_sweep,
        min_rr: s.min_target_rr,
        min_clear: s.gap_min_clear_atr,
        gap_min_frac: s.gap_min_risk_frac + 0.0,
        next_key: 0,
        setups: [],        // one per sweep being worked
        placed: #{},       // setup key -> resting opportunity id
        used: #{},         // gaps that already carry a plan: one plan per gap
        dead: [],          // opportunity ids to cancel at the bar close
    }
}

// True when the broken swing lies on the reversal side of the swept level.
fn break_beyond_sweep(dir, break_level, swept_level) {
    if dir == "bull" { break_level > swept_level } else { break_level < swept_level }
}

// A timeframe name's length in seconds ("15m" -> 900).
fn tf_secs(tf) {
    let n = tf.len();
    let unit = tf.sub_string(n - 1, 1);
    let k = parse_int(tf.sub_string(0, n - 1));
    if unit == "m" { k * 60 } else if unit == "h" { k * 3600 } else if unit == "d" || unit == "D" { k * 86400 } else { k * 60 }
}

// The swing that launched the run into a sweep: the most recent short-term
// swing on the reversal side with its pivot before the sweep's extreme, not
// closed through before that extreme, and lying beyond the swept level (a
// swing between the swept level and the extreme is a pause inside the sweep).
// () when the structure layer holds none. The point's `taken` says whether a
// bar has closed through it since.
fn launch_swing(scan, tf, dir, extreme_ts, swept_level) {
    let side = if dir == "bear" { "low" } else { "high" };
    for p in scan.structure(tf, side, 0) {      // newest first
        if p.ts >= extreme_ts { continue; }
        if p.taken && p.taken_ts < extreme_ts { continue; }
        if !break_beyond_sweep(dir, p.price, swept_level) { continue; }
        return p;
    }
    ()
}

fn on_bar(c, atr, sweeps, breaks) {
    let tf = c.tf;
    for sw in sweeps {
        if sw.tf == tf && sw.level_sig >= this.min_sig {
            this.next_key += 1;
            let w = #{ key: `${this.next_key}`, sweep: sw, tf: tf, bars: 0,
                       brk: (), fvg: (), entry: (), stop: (), placed: false,
                       mss: (), mss_swing_ts: (), brk_ts: () };
            let p = launch_swing(this.scan, tf, sw.dir, sw.extreme_ts, sw.level_price);
            if p != () { w.mss = p.price; w.mss_swing_ts = p.ts; }
            this.setups.push(w);
            if this.tr {
                let rec = #{ dir: sw.dir, tf: tf,
                    level_id: sw.level_id, level_price: sw.level_price, level_source: sw.level_source,
                    level_sig: sw.level_sig, extreme_price: sw.extreme_price, start_ts: sw.start_ts,
                    extreme_ts: sw.extreme_ts, o: "watching" };
                if w.mss != () { rec.mss_level = w.mss; rec.mss_ts = w.mss_swing_ts; }
                trace("v3.sweep", "sweep", `D${this.next_key}`, rec);
            }
        }
    }
    let out = [];
    let keep = [];
    let awake = false;
    for w in this.setups {
        if w.tf != tf { keep.push(w); continue; }
        w.bars += 1;
        let dir = w.sweep.dir;
        let ext = w.sweep.extreme_price;
        let over = w.bars > this.max_bars
            || (dir == "bull" && c.close < ext) || (dir == "bear" && c.close > ext)
            || (w.fvg != () && this.scan.fvg_status(w.fvg) == "inverted");
        if over {
            if this.tr {
                let why = if w.bars > this.max_bars { "timed_out" }
                    else if w.fvg != () && this.scan.fvg_status(w.fvg) == "inverted" { "gap_inverted" }
                    else { "closed_through_sweep_extreme" };
                trace("v3.dead", "dead", `D${w.key}`, #{ o: why, bars: w.bars, close: c.close, extreme_price: ext,
                    cancels_resting_order: w.placed });
            }
            if w.placed && w.key in this.placed { this.dead.push(this.placed.remove(w.key)); }
            continue;
        }
        if w.brk == () {
            // The level is fixed by the sweep: the swing that launched the run.
            // (Looked up again while there is none: a pivot is only labelled
            // once the bar after it has closed.)
            if w.mss == () {
                let p = launch_swing(this.scan, tf, dir, w.sweep.extreme_ts, w.sweep.level_price);
                if p != () { w.mss = p.price; w.mss_swing_ts = p.ts; }
            }
            if w.mss != () {
                // Broken by this bar's close, or already closed through
                // between the sweep's extreme and the bar the sweep finished on.
                let fired = (dir == "bear" && c.close < w.mss) || (dir == "bull" && c.close > w.mss);
                let at = c.ts;
                if !fired {
                    let p = launch_swing(this.scan, tf, dir, w.sweep.extreme_ts, w.sweep.level_price);
                    if p != () && p.ts == w.mss_swing_ts && p.taken { fired = true; at = p.taken_ts; }
                }
                if fired {
                    w.brk = w.mss; w.brk_ts = at;
                    if this.tr {
                        trace("v3.break", "break", `D${w.key}`, #{ o: "accepted",
                            break_level: w.mss, swing_ts: w.mss_swing_ts, break_ts: at,
                            swept_level: w.sweep.level_price, close: c.close,
                            cmp: [#{ l: "bar close", lv: c.close, op: if dir == "bull" { ">" } else { "<" },
                                     r: "launch swing", rv: w.mss, ok: true }] });
                    }
                }
            }
        }
        if w.brk != () && !w.placed {
            // The first gap (oldest) made by the leg that broke structure:
            // third candle after the sweep's extreme and no later than the
            // candle after the break candle, and tall enough for its risk.
            let f = ();
            let last_ts = w.brk_ts + tf_secs(tf);
            let small = (); let leg_done = c.ts >= last_ts;
            for g in this.scan.fvgs_since(tf, dir, false, w.sweep.extreme_ts + 1) {
                if g.ts > last_ts { continue; }
                let clearance = if dir == "bull" { g.near - ext } else { ext - g.near };
                if clearance < atr * this.min_clear { continue; }
                let risk = (g.near - ext).abs();
                let height = (g.near - g.far).abs();
                if this.gap_min_frac > 0.0 && (risk <= 0.0 || height / risk < this.gap_min_frac) {
                    if small == () { small = #{ id: g.id, near: g.near, far: g.far, height: height, risk: risk }; }
                    continue;
                }
                f = g; break;
            }
            if f == () && small != () && this.tr && !("small_traced" in w) {
                w.small_traced = true;
                trace("v3.gap", "gap", `D${w.key}`, #{ o: "gap_too_small", gap_id: small.id,
                    gap_near: small.near, gap_far: small.far, height: small.height, risk: small.risk,
                    frac: small.height / small.risk, min_frac: this.gap_min_frac,
                    cmp: [#{ l: "gap height / risk", lv: small.height / small.risk, op: ">=",
                             r: "gap_min_risk_frac", rv: this.gap_min_frac, ok: false }] });
            }
            if f == () && leg_done {
                // The leg is over: no later gap can belong to it.
                let why = if small != () { "gap_too_small" } else { "no_gap_in_leg" };
                if this.tr {
                    if small == () {
                        trace("v3.gap", "gap", `D${w.key}`, #{ o: "no_gap_in_leg", extreme_ts: w.sweep.extreme_ts,
                            break_ts: w.brk_ts, last_gap_ts: last_ts });
                    }
                    trace("v3.dead", "dead", `D${w.key}`, #{ o: why, bars: w.bars, close: c.close,
                        extreme_price: ext, cancels_resting_order: false });
                }
                continue;
            }
            let gap_key = if f != () { `${tf}:${f.id}` } else { "" };
            if f != () && gap_key in this.used {
                if this.tr {
                    trace("v3.gap", "gap", `D${w.key}`, #{ o: "same_gap_as_earlier_plan", gap_id: f.id,
                        near: f.near, far: f.far });
                }
                // Another sweep on this bar or an earlier one already placed
                // this gap's plan; this setup is the same trade.
                continue;
            }
            if f != () {
                let t = this.scan.find_target(dir, f.near, ext, this.min_rr, 0.0);
                if t != () {
                    w.fvg = f.id; w.entry = f.near; w.stop = ext; w.placed = true;
                    this.used[gap_key] = true;
                    out.push(#{ key: w.key, dir: dir, tf: tf, entry: w.entry, stop: w.stop, target: t.price,
                                sig: w.sweep.level_sig });
                }
                if this.tr {
                    trace("v3.gap", "gap", `D${w.key}`, #{ o: if t != () { "plan_placed" } else { "no_target" },
                        gap_id: f.id, near: f.near, far: f.far, entry: f.near, stop: ext,
                        target: if t != () { t.price } else { () }, min_rr: this.min_rr,
                        opp_id: `v3_${this.asset}_${w.key}` });
                }
            }
        }
        awake = true;
        keep.push(w);
    }
    this.setups = keep;
    this.scan.wake(tf, awake);
    this.scan.wake_book(this.setups.len() > 0 || this.dead.len() > 0);
    out
}

fn emit(c, atr, cand) {
    let o = opp("v3_" + cand.dir, cand.tf, cand.dir, c.ts);
    o.entry = cand.entry;
    o.stop = cand.stop;
    o.target = cand.target;
    o.score = cand.sig;
    // The engine's own ids count across assets in thread order; an id built
    // from this asset's setup number is the same in every run.
    o.id = `v3_${this.asset}_${cand.key}`;
    this.placed[cand.key] = o.id;
    o
}

fn admit(o, ctx) {
    if o.score < ctx.min_score { return skip("below_min_score"); }
    take(o.entry, o.stop, o.target)
}

// Cancel the resting orders of dead setups, and forget setups whose order
// is no longer resting (filled, or dropped by the engine).
fn on_bar_close(c, book) {
    for id in this.dead { book.cancel(id); }
    this.dead = [];
    let resting = book.pending();
    let keep = [];
    for w in this.setups {
        if w.placed && w.key in this.placed && !(this.placed[w.key] in resting) {
            this.placed.remove(w.key);
            continue;
        }
        keep.push(w);
    }
    this.setups = keep;
}
