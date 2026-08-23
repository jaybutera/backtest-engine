// A liquidity sweep, then the first fair-value gap, entered on the retrace.
//
// A demonstration of the native scanner services, not an edge. The engine
// keeps the level registry, detects sweeps and gaps and answers the
// queries; this script only says which sweeps matter, when a gap becomes
// an entry, and where the target is. It runs on bars that carry a sweep or
// a structure break, and on the bars it asks to be woken for.

fn init(cfg) {
    let s = cfg.script;
    #{
        scan: scanner(#{
            primitives: #{
                atr_period: 14, swing_lookback: 5,
                fvg_min_gap_pct: 0.0, structure_min_displacement_atr: 1.0,
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
        watching: [],           // sweeps waiting for a gap, then a retrace
    }
}

// Detection: runs before the draw map rebuild. Returns candidates for emit().
fn on_bar(c, atr, sweeps, breaks) {
    let tf = c.tf;
    for sw in sweeps {
        if sw.level_sig >= this.min_sig {
            this.watching.push(#{ sweep: sw, tf: tf, bars: 0, fvg: (), entry: (), stop: () });
        }
    }
    let out = [];
    let keep = [];
    let awake = false;
    for w in this.watching {
        if w.tf != tf { keep.push(w); continue; }
        w.bars += 1;
        if w.bars > this.max_bars { continue; }
        let dir = w.sweep.dir;
        let ext = w.sweep.extreme_price;
        // A close back through the sweep's extreme ends the idea.
        if (dir == "bull" && c.close < ext) || (dir == "bear" && c.close > ext) { continue; }
        if w.fvg == () {
            // The first unmitigated gap in the sweep direction formed since
            // the sweep, at least half an ATR clear of its extreme.
            let f = this.scan.fvg_first(tf, dir, false, w.sweep.start_ts, 0.0, -inf(), inf(), ext, atr * 0.5);
            if f != () { w.fvg = f.id; w.entry = f.near; w.stop = ext; }
        } else if this.scan.fvg_status(w.fvg) == "inverted" {
            continue;
        } else {
            let retraced = if dir == "bull" { c.low <= w.entry } else { c.high >= w.entry };
            if retraced {
                let t = this.scan.find_target(dir, w.entry, w.stop, this.min_rr, 0.0);
                if t != () {
                    out.push(#{ dir: dir, tf: tf, entry: w.entry, stop: w.stop, target: t.price,
                                sig: w.sweep.level_sig });
                }
                continue;
            }
        }
        awake = true;
        keep.push(w);
    }
    this.watching = keep;
    this.scan.wake(tf, awake);
    out
}

// Scoring: runs after the draw map rebuild, one call per candidate.
fn emit(c, atr, cand) {
    let o = opp("sweep_fvg_" + cand.dir, cand.tf, cand.dir, c.ts);
    o.entry = cand.entry;
    o.stop = cand.stop;
    o.target = cand.target;
    o.score = cand.sig;
    o
}

fn admit(o, ctx) {
    if o.score < ctx.min_score { return skip("below_min_score"); }
    let risk = (o.entry - o.stop).abs();
    let min_tp = if o.direction == "bull" { o.entry + ctx.rr_target * risk } else { o.entry - ctx.rr_target * risk };
    let tp = if o.direction == "bull" { fmax(o.target, min_tp) } else { fmin(o.target, min_tp) };
    take(o.entry, o.stop, tp)
}
