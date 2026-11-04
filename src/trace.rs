//! Decision traces: a per-asset JSONL record of what the strategy saw and
//! decided, bar by bar, for a replay viewer and a decision debugger.
//!
//! # Contract
//!
//! A traced run writes one file per asset, `<asset>.trace.jsonl` (`:` in the
//! asset name becomes `_`), plus `manifest.json`, into the directory given by
//! `replay --trace <dir>`. Every line is one JSON object with the envelope
//! `k n b t tf ph q run p o v cmp` (see [`TraceSink::record`] and the README
//! section "The trace contract").
//!
//! # Tracing reads, it never writes back
//!
//! Nothing in the engine or a script reads the sink. Every hook sits inside a
//! branch that already represents a state change, behind
//! `if let Some(t) = &self.trace { if t.on() { ... } }`, and only reads the
//! values the decision code already computed. A traced run therefore books
//! byte-identical trades to an untraced one; the tests in this module and in
//! `rhai_strategy.rs` pin that.
//!
//! # Failure
//!
//! Tracing never aborts a run. An I/O error prints one
//! `warning: trace <asset>: <err>` line, marks the sink failed and stops
//! writing. Passing the byte budget (`--trace-max-mb`) stops writing and
//! records the base-bar time it stopped at as `truncated_at` in the manifest.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::models::{base_tf_id, tf_name, Candle};

/// How much the trace records. `Full` adds the per-bar-per-level records
/// (`lv.refresh`), gap births and status changes (`fvg.*`) and a `hook`
/// record on every wake-only bar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Detail {
    Basic,
    Full,
}

impl Detail {
    pub fn parse(s: &str) -> Result<Detail, String> {
        match s {
            "basic" => Ok(Detail::Basic),
            "full" => Ok(Detail::Full),
            other => Err(format!("--trace-detail must be basic or full, got {other:?}")),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Detail::Basic => "basic",
            Detail::Full => "full",
        }
    }
}

/// Which part of the bar a record was written from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Scanner step and `on_bar` (or `on_candle`).
    Bar,
    /// The `emit` loop after the draw-map rebuild.
    Emit,
    /// The strategy's `admit`.
    Admit,
    /// `on_bar_close`.
    Close,
    /// Book transitions drained from the fill simulator.
    Book,
    /// Snapshot records.
    Snap,
    /// A scanner rebuild.
    Rebuild,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Bar => "bar",
            Phase::Emit => "emit",
            Phase::Admit => "admit",
            Phase::Close => "close",
            Phase::Book => "book",
            Phase::Snap => "snap",
            Phase::Rebuild => "rebuild",
        }
    }
}

/// What a run traces: plain data, resolved once by the driver and shared by
/// every asset thread.
#[derive(Clone, Debug)]
pub struct TraceConfig {
    /// Directory the per-asset files and the manifest go into.
    pub dir: PathBuf,
    /// First traced base-bar time, unix seconds (inclusive).
    pub from: i64,
    /// End of the traced window, unix seconds (exclusive).
    pub to: i64,
    pub detail: Detail,
    /// Byte budget per asset file.
    pub max_bytes: u64,
}

impl TraceConfig {
    /// The file name an asset's trace is written to.
    pub fn file_name(asset: &str) -> String {
        format!("{}.trace.jsonl", asset.replace(':', "_"))
    }
}

/// What one asset's sink wrote, for the manifest.
#[derive(Clone, Debug, Default)]
pub struct AssetStats {
    pub file: String,
    pub records: u64,
    pub bytes: u64,
    /// Base-bar time (unix s) at which the byte budget stopped the trace.
    pub truncated_at: Option<i64>,
    /// True when an I/O error stopped the trace.
    pub failed: bool,
}

/// One asset's trace writer. Single-threaded: it lives on the asset's thread
/// and is shared, as a [`TraceHandle`], by the session, the strategy and the
/// strategy's scanner.
pub struct TraceSink {
    asset: String,
    file: String,
    out: RefCell<Option<BufWriter<File>>>,
    from: i64,
    to: i64,
    detail: Detail,
    max_bytes: u64,
    base_tf: u16,
    bytes: Cell<u64>,
    records: Cell<u64>,
    seq: Cell<i64>,
    cur_b: Cell<i64>,
    cur_t: Cell<i64>,
    cur_tf: Cell<u16>,
    phase: Cell<Phase>,
    epoch: Cell<u32>,
    last_snap_day: Cell<i64>,
    failed: Cell<bool>,
    truncated_at: Cell<Option<i64>>,
    tf_names: RefCell<HashMap<u16, String>>,
}

impl std::fmt::Debug for TraceSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceSink")
            .field("asset", &self.asset)
            .field("file", &self.file)
            .field("records", &self.records.get())
            .field("epoch", &self.epoch.get())
            .finish()
    }
}

/// The shared form of a sink.
pub type TraceHandle = Rc<TraceSink>;

fn finite(x: f64) -> Value {
    // serde_json maps non-finite floats to null; spelled out for clarity.
    if x.is_finite() {
        json!(x)
    } else {
        Value::Null
    }
}

/// One `cmp` row: `{"l": .., "lv": .., "op": .., "r": .., "rv": .., "ok": ..}`.
pub fn cmp_row(l: &str, lv: f64, op: &str, r: &str, rv: f64, ok: bool) -> Value {
    json!({"l": l, "lv": finite(lv), "op": op, "r": r, "rv": finite(rv), "ok": ok})
}

impl TraceSink {
    /// Open an asset's sink. Never fails: a directory or file that cannot be
    /// created prints one warning and yields a sink that writes nothing.
    pub fn open(cfg: &TraceConfig, asset: &str) -> TraceHandle {
        let file = TraceConfig::file_name(asset);
        let sink = TraceSink {
            asset: asset.to_string(),
            file: file.clone(),
            out: RefCell::new(None),
            from: cfg.from,
            to: cfg.to,
            detail: cfg.detail,
            max_bytes: cfg.max_bytes,
            base_tf: base_tf_id(),
            bytes: Cell::new(0),
            records: Cell::new(0),
            seq: Cell::new(0),
            cur_b: Cell::new(i64::MIN),
            cur_t: Cell::new(i64::MIN),
            cur_tf: Cell::new(base_tf_id()),
            phase: Cell::new(Phase::Bar),
            epoch: Cell::new(0),
            last_snap_day: Cell::new(i64::MIN),
            failed: Cell::new(false),
            truncated_at: Cell::new(None),
            tf_names: RefCell::new(HashMap::new()),
        };
        let opened = std::fs::create_dir_all(&cfg.dir)
            .and_then(|_| File::create(cfg.dir.join(&file)));
        match opened {
            Ok(f) => *sink.out.borrow_mut() = Some(BufWriter::with_capacity(1 << 16, f)),
            Err(e) => sink.fail(&e),
        }
        Rc::new(sink)
    }

    fn fail(&self, e: &dyn std::fmt::Display) {
        if !self.failed.get() {
            eprintln!("warning: trace {}: {e}", self.asset);
            self.failed.set(true);
        }
        self.out.borrow_mut().take();
    }

    /// Whether a record written now would land in the file: the current base
    /// bar is inside the window, the sink has not failed and the byte budget
    /// is not spent.
    #[inline]
    pub fn on(&self) -> bool {
        let b = self.cur_b.get();
        b >= self.from && b < self.to && !self.failed.get() && self.truncated_at.get().is_none()
    }

    /// `on()` and the run asked for `full` detail.
    #[inline]
    pub fn full(&self) -> bool {
        self.detail == Detail::Full && self.on()
    }

    pub fn detail(&self) -> Detail {
        self.detail
    }

    /// What a script's `tracing()` returns: true while tracing is configured
    /// and can still write, and the current bar is not outside the window.
    /// Before the first bar (in `init`) it is true, so a script may read it
    /// once there and cache it.
    pub fn script_tracing(&self) -> bool {
        if self.failed.get() || self.truncated_at.get().is_some() {
            return false;
        }
        let b = self.cur_b.get();
        b == i64::MIN || (b >= self.from && b < self.to)
    }

    /// Make `candle` the current one. `t` and `tf` follow every candle; the
    /// replay clock `b` only moves on base-timeframe candles, so an HTF
    /// composite (stamped at its period start) carries the base bar it
    /// arrived with. Resets the phase to `bar`.
    pub fn set_bar(&self, candle: &Candle) {
        let t = candle.timestamp.and_utc().timestamp();
        self.cur_t.set(t);
        self.cur_tf.set(candle.timeframe);
        if candle.timeframe == self.base_tf {
            self.cur_b.set(t);
        }
        self.phase.set(Phase::Bar);
    }

    pub fn set_phase(&self, ph: Phase) {
        self.phase.set(ph);
    }

    pub fn phase(&self) -> Phase {
        self.phase.get()
    }

    /// The current base-bar time.
    pub fn b(&self) -> i64 {
        self.cur_b.get()
    }

    /// The current candle's timeframe id.
    pub fn tf(&self) -> u16 {
        self.cur_tf.get()
    }

    pub fn epoch(&self) -> u32 {
        self.epoch.get()
    }

    /// Advance the scanner epoch (a rebuild renumbers level ids).
    pub fn bump_epoch(&self) {
        self.epoch.set(self.epoch.get() + 1);
    }

    /// The name of a timeframe id, cached per sink (the global intern table
    /// takes a process-wide lock).
    pub fn tf_name(&self, id: u16) -> String {
        if let Some(s) = self.tf_names.borrow().get(&id) {
            return s.clone();
        }
        let s = tf_name(id).to_string();
        self.tf_names.borrow_mut().insert(id, s.clone());
        s
    }

    /// Run id of a level: `L{epoch}.{lid}`.
    pub fn level_run(&self, lid: i64) -> String {
        format!("L{}.{}", self.epoch.get(), lid)
    }

    /// Run id of a sweep crossing: `S{epoch}.{lid}.{tf}.{start_ts}`.
    pub fn sweep_run(&self, lid: i64, tf: &str, start_ts: i64) -> String {
        format!("S{}.{}.{}.{}", self.epoch.get(), lid, tf, start_ts)
    }

    /// True on the first in-window base bar and on the first base bar of
    /// each later UTC day. Call on base bars only, while `on()`.
    pub fn snapshot_due(&self) -> bool {
        let day = self.cur_b.get().div_euclid(86_400);
        day != self.last_snap_day.get()
    }

    /// Note that the snapshot for the current UTC day has been written.
    pub fn mark_snapshot(&self) {
        self.last_snap_day.set(self.cur_b.get().div_euclid(86_400));
    }

    /// Write one record. A no-op when `!on()`. `v` and `cmp` are written as
    /// given; NaN and infinities inside them must already be `null` (every
    /// `serde_json::Value` built from an `f64` is).
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        k: &str,
        n: &str,
        run: &str,
        p: Option<&str>,
        o: Option<&str>,
        v: Value,
        cmp: Option<Value>,
    ) {
        if !self.on() {
            return;
        }
        let q = self.seq.get();
        let mut line = String::with_capacity(256);
        line.push_str("{\"k\":");
        push_str_json(&mut line, k);
        line.push_str(",\"n\":");
        push_str_json(&mut line, n);
        line.push_str(&format!(
            ",\"b\":{},\"t\":{},\"tf\":",
            self.cur_b.get(),
            self.cur_t.get()
        ));
        let tf = self.tf_name(self.cur_tf.get());
        push_str_json(&mut line, &tf);
        line.push_str(",\"ph\":\"");
        line.push_str(self.phase.get().as_str());
        line.push_str(&format!("\",\"q\":{q},\"run\":"));
        push_str_json(&mut line, run);
        if let Some(p) = p {
            line.push_str(",\"p\":");
            push_str_json(&mut line, p);
        }
        if let Some(o) = o {
            line.push_str(",\"o\":");
            push_str_json(&mut line, o);
        }
        line.push_str(",\"v\":");
        line.push_str(&v.to_string());
        if let Some(c) = cmp {
            line.push_str(",\"cmp\":");
            line.push_str(&c.to_string());
        }
        line.push_str("}\n");

        let len = line.len() as u64;
        if self.bytes.get() + len > self.max_bytes {
            eprintln!(
                "warning: trace {}: byte budget of {} reached, trace stops at base bar {}",
                self.asset,
                self.max_bytes,
                self.cur_b.get()
            );
            self.truncated_at.set(Some(self.cur_b.get()));
            return;
        }
        let res = match self.out.borrow_mut().as_mut() {
            Some(w) => w.write_all(line.as_bytes()),
            None => return,
        };
        match res {
            Ok(()) => {
                self.seq.set(q + 1);
                self.bytes.set(self.bytes.get() + len);
                self.records.set(self.records.get() + 1);
            }
            Err(e) => self.fail(&e),
        }
    }

    /// Flush the file and report what was written. Safe to call more than
    /// once; later calls report the same totals.
    pub fn finish(&self) -> AssetStats {
        let taken = self.out.borrow_mut().take();
        if let Some(mut w) = taken {
            if let Err(e) = w.flush() {
                self.fail(&e);
            }
        }
        AssetStats {
            file: self.file.clone(),
            records: self.records.get(),
            bytes: self.bytes.get(),
            truncated_at: self.truncated_at.get(),
            failed: self.failed.get(),
        }
    }
}

impl Drop for TraceSink {
    fn drop(&mut self) {
        if let Some(mut w) = self.out.borrow_mut().take() {
            let _ = w.flush();
        }
    }
}

fn push_str_json(out: &mut String, s: &str) {
    // serde_json's string escaping, without building a Value.
    out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string()));
}

/// The run-level facts the manifest records beside the per-asset stats.
#[derive(Clone, Debug, Default)]
pub struct ManifestRun {
    pub strategy_file: Option<PathBuf>,
    pub fill_file: Option<PathBuf>,
    pub run_from: Option<String>,
    pub run_to: Option<String>,
    pub warmup_days: u32,
}

/// Field order of each row of `snap.levels` `v.levels`.
pub const SNAP_LEVEL_FIELDS: &[&str] = &[
    "lid", "price", "side", "source", "ltf", "lts", "touch", "sig", "decay", "last_touch",
    "developing",
];
/// Field order of each row of `snap.dev` `v.dev`.
pub const SNAP_DEV_FIELDS: &[&str] = &["lid", "tf", "start_ts", "extreme", "extreme_ts", "count"];

fn abs_path(p: &Path) -> String {
    std::fs::canonicalize(p)
        .unwrap_or_else(|_| p.to_path_buf())
        .to_string_lossy()
        .to_string()
}

fn iso(ts: i64) -> Value {
    if ts == i64::MIN || ts == i64::MAX {
        return Value::Null;
    }
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|d| json!(d.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string()))
        .unwrap_or(Value::Null)
}

/// The hex SHA-256 of a byte string.
pub fn sha256_hex(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Write `manifest.json` into the trace directory. `sidecar` is the JSON
/// report path and the exact body written there, when one was written; the
/// viewer matches `sidecar_sha256` against the report it displays so a stale
/// trace never decorates a newer run. Returns the manifest path.
pub fn write_manifest(
    cfg: &TraceConfig,
    run: &ManifestRun,
    stats: &[(String, AssetStats)],
    sidecar: Option<(&str, &str)>,
) -> std::io::Result<PathBuf> {
    let mut assets = serde_json::Map::new();
    for (asset, s) in stats {
        assets.insert(
            asset.clone(),
            json!({
                "file": s.file,
                "records": s.records,
                "bytes": s.bytes,
                "truncated_at": s.truncated_at,
                "failed": s.failed,
            }),
        );
    }
    let manifest = json!({
        "schema": 1,
        "engine_rev": concat!("backtest-engine ", env!("CARGO_PKG_VERSION")),
        "strategy_file": run.strategy_file.as_deref().map(abs_path),
        "fill_file": run.fill_file.as_deref().map(abs_path),
        "run_from": run.run_from,
        "run_to": run.run_to,
        "warmup_days": run.warmup_days,
        "trace_from": iso(cfg.from),
        "trace_to": if cfg.to == i64::MAX { Value::Null } else { iso(cfg.to - 1) },
        "detail": cfg.detail.as_str(),
        "sidecar": sidecar.map(|(p, _)| p),
        "sidecar_sha256": sidecar.map(|(_, body)| sha256_hex(body.as_bytes())),
        "assets": Value::Object(assets),
        "snap_level_fields": SNAP_LEVEL_FIELDS,
        "snap_dev_fields": SNAP_DEV_FIELDS,
    });
    std::fs::create_dir_all(&cfg.dir)?;
    let path = cfg.dir.join("manifest.json");
    let body = serde_json::to_string_pretty(&manifest).map_err(std::io::Error::other)?;
    std::fs::write(&path, body + "\n")?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{asset_id, tf_id};
    use tempfile::TempDir;

    fn candle(min: i64, tf: &str) -> Candle {
        Candle {
            asset: asset_id("TRACE_UNIT"),
            timeframe: tf_id(tf),
            timestamp: chrono::DateTime::from_timestamp(min * 60, 0).unwrap().naive_utc(),
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 0.0,
            complete: true,
        }
    }

    fn cfg(dir: &Path, from: i64, to: i64, max_bytes: u64) -> TraceConfig {
        TraceConfig {
            dir: dir.to_path_buf(),
            from,
            to,
            detail: Detail::Basic,
            max_bytes,
        }
    }

    #[test]
    fn records_carry_the_envelope_and_respect_the_window() {
        let d = TempDir::new().unwrap();
        let s = TraceSink::open(&cfg(d.path(), 120, 240, 1 << 20), "x:A");
        assert!(s.script_tracing(), "before the first bar tracing() reads true");
        s.set_bar(&candle(1, "1m"));
        assert!(!s.on());
        assert!(!s.script_tracing(), "warmup bars read false");
        s.record("early", "", "", None, None, json!({}), None);
        s.set_bar(&candle(2, "1m"));
        assert!(s.on());
        s.record(
            "a.b",
            "n1",
            "L0.1",
            Some("P"),
            Some("yes"),
            json!({"x": f64::NAN, "y": 1.5}),
            Some(json!([cmp_row("l", 1.0, ">", "r", f64::INFINITY, true)])),
        );
        s.set_bar(&candle(4, "1m"));
        s.record("late", "", "", None, None, json!({}), None);
        let st = s.finish();
        assert_eq!(st.records, 1);
        let text = std::fs::read_to_string(d.path().join("x_A.trace.jsonl")).unwrap();
        let line: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(line["k"], "a.b");
        assert_eq!(line["b"], 120);
        assert_eq!(line["q"], 0);
        assert_eq!(line["tf"], "1m");
        assert_eq!(line["ph"], "bar");
        assert_eq!(line["p"], "P");
        assert_eq!(line["o"], "yes");
        assert!(line["v"]["x"].is_null());
        assert!(line["cmp"][0]["rv"].is_null());
        assert_eq!(line["cmp"][0]["ok"], true);
    }

    #[test]
    fn the_byte_budget_truncates_instead_of_filling_the_disk() {
        let d = TempDir::new().unwrap();
        let s = TraceSink::open(&cfg(d.path(), i64::MIN, i64::MAX, 300), "B");
        s.set_bar(&candle(1, "1m"));
        for _ in 0..10 {
            s.record("k", "", "", None, None, json!({"pad": "x".repeat(50)}), None);
        }
        let st = s.finish();
        assert!(st.records >= 1 && st.records < 10, "{st:?}");
        assert_eq!(st.truncated_at, Some(60));
        assert!(st.bytes <= 300);
    }

    #[test]
    fn an_unwritable_directory_fails_quietly() {
        let d = TempDir::new().unwrap();
        let blocker = d.path().join("file");
        std::fs::write(&blocker, "not a dir").unwrap();
        let s = TraceSink::open(&cfg(&blocker.join("sub"), i64::MIN, i64::MAX, 1 << 20), "C");
        s.set_bar(&candle(1, "1m"));
        assert!(!s.on());
        s.record("k", "", "", None, None, json!({}), None);
        let st = s.finish();
        assert!(st.failed);
        assert_eq!(st.records, 0);
    }

    #[test]
    fn sha256_matches_a_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
