// v3: setups stated ahead of price.
//
// Every setup here ends in a resting plan: "if price comes to this entry,
// take it, with this stop and this target". The plan is placed as soon as
// the setup is complete, not when price has already come back, and it is
// cancelled when the idea dies.
//
// Setup 1, sweep / break / gap:
//   1. A level is swept (the scanner's sweep; `dir` is the reversal side).
//   2. Structure breaks in the reversal direction on the sweep's timeframe,
//      and the broken swing lies BEYOND the swept level: above it for a
//      bull setup, below it for a bear setup. A break of a swing that sits
//      between the sweep's extreme and the swept level is only a bounce
//      inside the sweep and is ignored; the setup keeps waiting for a real one.
//   3. The first unmitigated gap in the reversal direction formed since the
//      sweep is the entry: limit at its near edge, stop at the sweep's
//      extreme, target at the nearest opposing level at least `min_target_rr`
//      away.
//
// The plan dies, and its resting order is cancelled, when a bar closes back
// through the sweep's extreme, when the gap inverts, or after
// `max_bars_after_sweep` bars of the sweep's timeframe.

fn init(cfg) {
    let s = cfg.script;
    #{
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
        next_key: 0,
        setups: [],        // one per sweep being worked
        placed: #{},       // setup key -> resting opportunity id
        dead: [],          // opportunity ids to cancel at the bar close
    }
}

// True when the broken swing lies on the reversal side of the swept level.
fn break_beyond_sweep(dir, break_level, swept_level) {
    if dir == "bull" { break_level > swept_level } else { break_level < swept_level }
}

fn on_bar(c, atr, sweeps, breaks) {
    let tf = c.tf;
    for sw in sweeps {
        if sw.tf == tf && sw.level_sig >= this.min_sig {
            this.next_key += 1;
            this.setups.push(#{ key: `${this.next_key}`, sweep: sw, tf: tf, bars: 0,
                                brk: (), fvg: (), entry: (), stop: (), placed: false });
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
            if w.placed && w.key in this.placed { this.dead.push(this.placed.remove(w.key)); }
            continue;
        }
        if w.brk == () {
            for b in breaks {
                if b.dir == dir && b.tf == tf && b.ts >= w.sweep.start_ts
                    && break_beyond_sweep(dir, b.level, w.sweep.level_price) {
                    w.brk = b.level;
                    break;
                }
            }
        }
        if w.brk != () && !w.placed {
            let f = this.scan.fvg_first(tf, dir, false, w.sweep.start_ts, 0.0, -inf(), inf(), ext, atr * this.min_clear);
            if f != () {
                let t = this.scan.find_target(dir, f.near, ext, this.min_rr, 0.0);
                if t != () {
                    w.fvg = f.id; w.entry = f.near; w.stop = ext; w.placed = true;
                    out.push(#{ key: w.key, dir: dir, tf: tf, entry: w.entry, stop: w.stop, target: t.price,
                                sig: w.sweep.level_sig });
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
    let o = opp("v3_sweep_break_gap_" + cand.dir, cand.tf, cand.dir, c.ts);
    o.entry = cand.entry;
    o.stop = cand.stop;
    o.target = cand.target;
    o.score = cand.sig;
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
