use crate::models::{asset_id, Candle, Tick};
use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::NaiveDateTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, LogicalType, TimeUnit as ParquetTimeUnit};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Load candles from a parquet file, optionally filtering by date range.
///
/// Tail-only optimization: when `start` is set, the reader inspects the
/// per-row-group timestamp statistics and skips any row group whose `[min, max]`
/// range falls entirely before `start` (or entirely after `end`). The collector
/// emits one row group per minute, so a year-old file may contain hundreds of
/// thousands of row groups — but a short warmup window only needs to decode
/// the last handful of them. If statistics are missing for a row group, the reader
/// falls back to decoding it (correct, but slower).
pub fn load_parquet(
    path: &Path,
    asset: &str,
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
) -> Vec<Candle> {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("Failed to open {}: {}", path.display(), e));

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("Failed to read parquet {}: {}", path.display(), e));

    if start.is_some() || end.is_some() {
        let keep = select_row_groups(&builder, start, end);
        builder = builder.with_row_groups(keep);
    }

    let reader = builder.build().expect("Failed to build parquet reader");

    let mut candles = Vec::new();

    for batch in reader {
        let batch = batch.expect("Failed to read record batch");
        let n = batch.num_rows();

        // Find columns by name (case-insensitive)
        let schema = batch.schema();
        let find_col = |names: &[&str]| -> Option<usize> {
            for name in names {
                for (i, field) in schema.fields().iter().enumerate() {
                    if field.name().eq_ignore_ascii_case(name) {
                        return Some(i);
                    }
                }
            }
            None
        };

        let ts_col = find_col(&["timestamp", "ts", "time", "datetime", "date"])
            .expect("No timestamp column");
        let open_col = find_col(&["open", "o"]).expect("No open column");
        let high_col = find_col(&["high", "h"]).expect("No high column");
        let low_col = find_col(&["low", "l"]).expect("No low column");
        let close_col = find_col(&["close", "c"]).expect("No close column");
        let vol_col = find_col(&["volume", "vol", "v"]);

        // Extract timestamp column
        let timestamps = extract_timestamps(batch.column(ts_col));
        let opens = extract_f64(batch.column(open_col));
        let highs = extract_f64(batch.column(high_col));
        let lows = extract_f64(batch.column(low_col));
        let closes = extract_f64(batch.column(close_col));
        let volumes = vol_col.map(|c| extract_f64(batch.column(c)));

        for i in 0..n {
            let ts = timestamps[i];

            if let Some(s) = start {
                if ts < s {
                    continue;
                }
            }
            if let Some(e) = end {
                if ts > e {
                    continue;
                }
            }

            candles.push(Candle {
                asset: asset_id(asset),
                timeframe: crate::models::base_tf_id(),
                open: opens[i],
                high: highs[i],
                low: lows[i],
                close: closes[i],
                volume: volumes.as_ref().map(|v| v[i]).unwrap_or(0.0),
                timestamp: ts,
                complete: true,
            });
        }
    }

    candles.sort_by_key(|c| c.timestamp);
    candles
}

// ─── Tick (raw trade print) loading ─────────────────────────────────────────

/// Load raw trade ticks from a per-asset tick parquet, sorted ascending by
/// timestamp. Preferred schema:
/// `timestamp`(ms, no tz), `price`(f64), `size`(f64), `side`(u8). Also tolerates
/// an alternative trades shape (`time` i64 ms, `px`, `sz`, `side`)
/// so either source can back the tick fill mode. Column lookup is
/// case-insensitive and name-aliased. An optional `[start, end]` bound trims the
/// stream. Millisecond timestamps are preserved (the whole point of tick mode).
pub fn load_ticks(
    path: &Path,
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
) -> Vec<Tick> {
    use arrow::array::UInt8Array;

    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("Failed to open tick file {}: {}", path.display(), e));
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("Failed to read tick parquet {}: {}", path.display(), e));
    if start.is_some() || end.is_some() {
        let keep = select_row_groups(&builder, start, end);
        builder = builder.with_row_groups(keep);
    }
    let reader = builder
        .build()
        .expect("Failed to build tick parquet reader");

    let mut ticks = Vec::new();
    for batch in reader {
        let batch = batch.expect("Failed to read tick record batch");
        let n = batch.num_rows();
        let schema = batch.schema();
        let find_col = |names: &[&str]| -> Option<usize> {
            for name in names {
                for (i, field) in schema.fields().iter().enumerate() {
                    if field.name().eq_ignore_ascii_case(name) {
                        return Some(i);
                    }
                }
            }
            None
        };
        let ts_col =
            find_col(&["timestamp", "time", "ts"]).expect("tick file: no timestamp column");
        let px_col = find_col(&["price", "px"]).expect("tick file: no price column");
        let sz_col = find_col(&["size", "sz", "qty"]);
        let side_col = find_col(&["side"]);

        let timestamps = extract_timestamps(batch.column(ts_col));
        let prices = extract_f64(batch.column(px_col));
        let sizes = sz_col.map(|c| extract_f64(batch.column(c)));
        // `side` may be written as u8 or i64 depending on the writer; accept both.
        let sides: Option<Vec<u8>> = side_col.map(|c| {
            let col = batch.column(c);
            if let Some(a) = col.as_any().downcast_ref::<UInt8Array>() {
                a.iter().map(|v| v.unwrap_or(0)).collect()
            } else {
                extract_f64(col).iter().map(|v| *v as u8).collect()
            }
        });

        for i in 0..n {
            let ts = timestamps[i];
            if let Some(s) = start {
                if ts < s {
                    continue;
                }
            }
            if let Some(e) = end {
                if ts > e {
                    continue;
                }
            }
            ticks.push(Tick {
                timestamp: ts,
                price: prices[i],
                size: sizes.as_ref().map(|v| v[i]).unwrap_or(0.0),
                side: sides.as_ref().map(|v| v[i]).unwrap_or(0),
            });
        }
    }
    ticks.sort_by_key(|t| t.timestamp);
    ticks
}

/// Subdirectory name searched inside each data dir for per-asset tick
/// parquets. A tick file for stem `S` is looked up as `{dir}/{TICKS_DIR}/{S}_ticks.parquet`.
pub const TICKS_DIR: &str = "ticks";

/// Discover the tick parquet for a source `stem` across `data_dirs`, checking
/// `{dir}/{TICKS_DIR}/{stem}_ticks.parquet` then `{dir}/{stem}_ticks.parquet`.
/// Returns the first existing path, or None.
///
/// Every candidate is derived from a caller-supplied `data_dirs` entry, so
/// nothing here depends on the process working directory.
pub fn tick_file_for_stem(stem: &str, data_dirs: &[&Path]) -> Option<PathBuf> {
    let file = format!("{}_ticks.parquet", stem);
    for dir in data_dirs {
        let nested = dir.join(TICKS_DIR).join(&file);
        if nested.exists() {
            return Some(nested);
        }
        let flat = dir.join(&file);
        if flat.exists() {
            return Some(flat);
        }
    }
    None
}

/// Load ticks for `asset` by resolving its backing source stems through the
/// `SourceMap` — so a `[[source]]` override pointing an asset at a differently
/// named file finds that file's ticks too — falling back to the asset's own
/// name as the stem. Returns the merged, time-sorted tick stream, or an empty
/// Vec when no tick file exists for any of the asset's stems. The
/// `[start, end]` bound is applied verbatim; widen it in the caller if warmup
/// needs to cross it.
pub fn load_asset_ticks(
    asset: &str,
    data_dirs: &[&Path],
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
    sources: &SourceMap,
) -> Vec<Tick> {
    let mut stems: Vec<String> = sources
        .sources_for(asset)
        .into_iter()
        .map(|s| s.stem)
        .collect();
    // Always also try the asset's own name as a stem (default/no-override case).
    if !stems.iter().any(|s| s == asset) {
        stems.push(asset.to_string());
    }
    let mut all: Vec<Tick> = Vec::new();
    let mut seen_path: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for stem in &stems {
        if let Some(path) = tick_file_for_stem(stem, data_dirs) {
            if !seen_path.insert(path.clone()) {
                continue;
            }
            all.extend(load_ticks(&path, start, end));
        }
    }
    all.sort_by_key(|t| t.timestamp);
    all
}

fn extract_timestamps(col: &arrow::array::ArrayRef) -> Vec<NaiveDateTime> {
    use arrow::array::*;

    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return arr
            .iter()
            .map(|v| {
                let us = v.unwrap_or(0);
                let secs = us / 1_000_000;
                let nsecs = ((us % 1_000_000) * 1000) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .naive_utc()
            })
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return arr
            .iter()
            .map(|v| {
                let ms = v.unwrap_or(0);
                let secs = ms / 1000;
                let nsecs = ((ms % 1000) * 1_000_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .naive_utc()
            })
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return arr
            .iter()
            .map(|v| {
                let ns = v.unwrap_or(0);
                let secs = ns / 1_000_000_000;
                let nsecs = (ns % 1_000_000_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .naive_utc()
            })
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        return arr
            .iter()
            .map(|v| {
                let s = v.unwrap_or(0);
                chrono::DateTime::from_timestamp(s, 0).unwrap().naive_utc()
            })
            .collect();
    }
    // Int64 — assume milliseconds epoch
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return arr
            .iter()
            .map(|v| {
                let ms = v.unwrap_or(0);
                let secs = ms / 1000;
                let nsecs = ((ms % 1000) * 1_000_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .naive_utc()
            })
            .collect();
    }
    // Utf8 string timestamps
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr
            .iter()
            .map(|v| {
                let s = v.unwrap_or("");
                NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .unwrap_or_else(|_| NaiveDateTime::default())
            })
            .collect();
    }

    panic!("Unsupported timestamp column type: {:?}", col.data_type());
}

fn extract_f64(col: &arrow::array::ArrayRef) -> Vec<f64> {
    use arrow::array::*;

    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return arr.iter().map(|v| v.unwrap_or(0.0)).collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
        return arr.iter().map(|v| v.unwrap_or(0.0) as f64).collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return arr.iter().map(|v| v.unwrap_or(0) as f64).collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
        return arr.iter().map(|v| v.unwrap_or(0) as f64).collect();
    }
    panic!("Unsupported numeric column type: {:?}", col.data_type());
}

/// A backing data source for an asset: a parquet file stem (the part before
/// `_{interval}.parquet`) plus a linear price transform applied to OHLC at
/// load time, so a donor series quoted on a different scale reads in the
/// target instrument's price terms: `price = src_price * scale + offset`.
#[derive(Clone, Debug)]
pub struct DataSource {
    /// File stem before the `_{interval}` suffix — the SOURCE symbol, which
    /// need not equal the asset name.
    pub stem: String,
    /// Multiplicative price scale (1.0 = identity).
    pub scale: f64,
    /// Additive price offset in target price units, applied after `scale`.
    pub offset: f64,
}

impl DataSource {
    /// Identity source whose stem equals the asset name (the default: an asset
    /// is backed by its own `{asset}_{interval}.parquet`).
    fn identity(stem: &str) -> Self {
        DataSource {
            stem: stem.to_string(),
            scale: 1.0,
            offset: 0.0,
        }
    }
}

/// Maps an asset name to the ordered list of backing sources it should load
/// from. An asset absent from the map uses the default: a single identity
/// source backed by its own `{asset}_{interval}.parquet`. An override replaces
/// that list wholesale, letting one asset be assembled from several files —
/// see `default_sources`.
#[derive(Clone, Debug, Default)]
pub struct SourceMap {
    overrides: std::collections::HashMap<String, Vec<DataSource>>,
}

impl SourceMap {
    pub fn new() -> Self {
        SourceMap {
            overrides: std::collections::HashMap::new(),
        }
    }

    /// Register an asset's backing sources, replacing any prior entry.
    pub fn insert(&mut self, asset: &str, sources: Vec<DataSource>) {
        self.overrides.insert(asset.to_string(), sources);
    }

    pub fn is_empty(&self) -> bool {
        self.overrides.is_empty()
    }

    /// The ordered sources for `asset`, falling back to the built-in default
    /// (a single identity self-source) when the asset has no override. Order matters: earlier sources win on timestamp dedup.
    pub fn sources_for(&self, asset: &str) -> Vec<DataSource> {
        if let Some(s) = self.overrides.get(asset) {
            return s.clone();
        }
        default_sources(asset)
    }
}

/// The built-in (un-overridden) source list for an asset: one identity source
/// backed by the asset's own `{asset}_{interval}` file.
///
/// Splicing a second file in front of it — a longer-history donor series, say,
/// or a predecessor instrument — is what [`SourceMap`] overrides are for.
fn default_sources(asset: &str) -> Vec<DataSource> {
    vec![DataSource::identity(asset)]
}

/// Glob the on-disk parquet files for a single source `stem` across `data_dirs`:
/// `{stem}_{interval}.parquet` plus dated shards `{stem}_{interval}_*.parquet`. Returns
/// paths in discovery order, deduplicated.
fn files_for_stem(stem: &str, iv: &str, data_dirs: &[&Path]) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for dir in data_dirs {
        if !dir.exists() {
            continue;
        }
        let main_file = dir.join(format!("{}_{}.parquet", stem, iv));
        if main_file.exists() && seen.insert(main_file.clone()) {
            paths.push(main_file);
        }
        // Date-ranged archives: {stem}_{interval}_*.parquet
        if let Ok(entries) = std::fs::read_dir(dir) {
            let prefix = format!("{}_{}_", stem, iv);
            for entry in entries.flatten() {
                let path = entry.path();
                let fstem = path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                if fstem.starts_with(&prefix)
                    && path.extension().map(|e| e == "parquet").unwrap_or(false)
                    && seen.insert(path.clone())
                {
                    paths.push(path);
                }
            }
        }
    }
    paths
}

/// Discover all parquet files for `asset` across `data_dirs`, honoring any
/// configured multi-file splice. This is the single source of truth for "what candles does
/// asset X have on disk" — called by both the live warmup loader and the
/// replay/backtest path so they always build engine state from the same input.
///
/// Backwards-compatible wrapper: uses the built-in default source map (no
/// config-driven overrides). Callers with a `SourceMap` should use
/// `load_asset_candles_with_sources` instead.
pub fn discover_files_for_asset(asset: &str, data_dirs: &[&Path]) -> Vec<PathBuf> {
    let iv = crate::models::base_interval();
    let mut out: Vec<PathBuf> = Vec::new();
    let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for src in default_sources(asset) {
        for p in files_for_stem(&src.stem, &iv, data_dirs) {
            if seen.insert(p.clone()) {
                out.push(p);
            }
        }
    }
    out
}

/// Load all candles for `asset` from `data_dirs`, applying the built-in
/// (un-overridden) source map and an optional `[start, end]` time bound.
/// Result is sorted by timestamp and deduplicated.
pub fn load_asset_candles(
    asset: &str,
    data_dirs: &[&Path],
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
) -> Vec<Candle> {
    load_asset_candles_with_sources(asset, data_dirs, start, end, &SourceMap::new())
}

/// Load all candles for `asset` from `data_dirs`, resolving its backing
/// sources through `sources` (config-driven override, or the built-in default
/// when the asset is absent). Each source's `scale`/`offset` is applied to OHLC
/// at load time so the engine sees one coherent price series. Sources are
/// loaded in order; the first to provide a given timestamp wins (so a primary
/// real feed takes precedence over a spliced donor). Result is sorted by
/// timestamp and deduplicated.
pub fn load_asset_candles_with_sources(
    asset: &str,
    data_dirs: &[&Path],
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
    sources: &SourceMap,
) -> Vec<Candle> {
    let iv = crate::models::base_interval();
    let mut all: Vec<Candle> = Vec::new();
    let mut seen_ts: std::collections::HashSet<NaiveDateTime> = std::collections::HashSet::new();
    let mut seen_path: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    for src in sources.sources_for(asset) {
        let identity = src.scale == 1.0 && src.offset == 0.0;
        for path in files_for_stem(&src.stem, &iv, data_dirs) {
            if !seen_path.insert(path.clone()) {
                continue;
            }
            for mut c in load_parquet(&path, asset, start, end) {
                if !identity {
                    c.open = c.open * src.scale + src.offset;
                    c.high = c.high * src.scale + src.offset;
                    c.low = c.low * src.scale + src.offset;
                    c.close = c.close * src.scale + src.offset;
                }
                if seen_ts.insert(c.timestamp) {
                    all.push(c);
                }
            }
        }
    }
    all.sort_by_key(|c| c.timestamp);
    all
}

/// Remove contract-roll discontinuities from a continuous futures series.
///
/// A continuous 1m file stitched from successive contracts carries a price
/// jump wherever the lead contract switched. Left in, that jump reads as a
/// market move: it distorts every indicator that spans it and can fire or
/// suppress signals on a gap that no position ever experienced. This applies
/// a back-adjustment: each roll gap is added to every bar before it, so the
/// series is continuous and anchored at the most recent contract's true
/// prices.
///
/// A roll is detected as a jump between two contiguous bars (at most
/// `max_gap_secs` apart) that straddle a UTC-day boundary and exceed
/// `4 × p99.9` of ordinary bar-to-bar jumps that do NOT straddle midnight.
/// The midnight condition matches how such files are usually built (the lead
/// contract switches at a day boundary); the multiple is wide enough that a
/// genuine move at that hour — thin evening session — essentially never
/// qualifies, while roll gaps sit an order of magnitude above it.
///
/// `candles` must be sorted by timestamp. Returns the number of splices
/// removed. Series shorter than a few hundred bars are left untouched: there
/// is no stable jump distribution to threshold against.
pub fn roll_adjust(candles: &mut [Candle], max_gap_secs: i64) -> usize {
    if candles.len() < 500 {
        return 0;
    }
    let day = |c: &Candle| c.timestamp.and_utc().timestamp().div_euclid(86_400);
    let secs = |c: &Candle| c.timestamp.and_utc().timestamp();

    let mut diffs: Vec<f64> = Vec::with_capacity(candles.len());
    for w in candles.windows(2) {
        let contiguous = secs(&w[1]) - secs(&w[0]) <= max_gap_secs;
        let midnight = day(&w[1]) != day(&w[0]);
        if contiguous && !midnight {
            diffs.push((w[1].open - w[0].close).abs());
        }
    }
    if diffs.len() < 100 {
        return 0;
    }
    diffs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p999 = diffs[((diffs.len() as f64) * 0.999) as usize];
    let threshold = 4.0 * p999;
    if !threshold.is_finite() || threshold <= 0.0 {
        return 0;
    }

    // (index of the first bar AFTER the splice, signed gap)
    let mut splices: Vec<(usize, f64)> = Vec::new();
    for i in 1..candles.len() {
        let contiguous = secs(&candles[i]) - secs(&candles[i - 1]) <= max_gap_secs;
        let midnight = day(&candles[i]) != day(&candles[i - 1]);
        let d = candles[i].open - candles[i - 1].close;
        if contiguous && midnight && d.abs() > threshold {
            splices.push((i, d));
        }
    }
    // Walk backwards, accumulating the offset applied to everything earlier.
    let mut offset = 0.0;
    let mut si = splices.len();
    for i in (0..candles.len()).rev() {
        while si > 0 && splices[si - 1].0 == i + 1 {
            si -= 1;
            offset += splices[si].1;
        }
        if offset != 0.0 {
            candles[i].open += offset;
            candles[i].high += offset;
            candles[i].low += offset;
            candles[i].close += offset;
        }
    }
    splices.len()
}

/// Discover parquet files and return (path, asset_name) pairs.
/// Deduplicates by asset, keeping the file with the most rows.
pub fn discover_parquet_files(data_dirs: &[&Path]) -> Vec<(std::path::PathBuf, String)> {
    use std::collections::HashMap;

    let mut asset_files: HashMap<String, (std::path::PathBuf, usize)> = HashMap::new();

    for dir in data_dirs {
        if !dir.exists() {
            continue;
        }
        let entries = std::fs::read_dir(dir).unwrap();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                let stem = path.file_stem().unwrap().to_string_lossy();
                // Must contain the base interval suffix (e.g. "_1m", or "_1h"
                // for a 1h-only backtest).
                let iv_suffix = format!("_{}", crate::models::base_interval());
                if !stem.contains(&iv_suffix) {
                    continue;
                }
                let asset = stem.split('_').next().unwrap_or("").to_string();
                if asset.is_empty() {
                    continue;
                }

                // Count rows
                let n_rows = count_parquet_rows(&path);
                let existing = asset_files.get(&asset);
                if existing.is_none() || existing.unwrap().1 < n_rows {
                    asset_files.insert(asset, (path, n_rows));
                }
            }
        }
    }

    let mut result: Vec<(std::path::PathBuf, String)> = asset_files
        .into_iter()
        .map(|(asset, (path, _))| (path, asset))
        .collect();
    result.sort_by(|a, b| a.1.cmp(&b.1));
    result
}

fn count_parquet_rows(path: &Path) -> usize {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    let reader = match parquet::file::reader::SerializedFileReader::new(file) {
        Ok(r) => r,
        Err(_) => return 0,
    };
    reader.metadata().file_metadata().num_rows() as usize
}

// ─── tail-only read filter ─────────────────────────────────────────────────────

/// Decide which row groups overlap the requested time range. Row groups
/// whose [min,max] timestamp range is strictly outside [start,end] are
/// dropped; row groups with no statistics are kept (correct fallback).
fn select_row_groups<R: parquet::file::reader::ChunkReader + 'static>(
    builder: &ParquetRecordBatchReaderBuilder<R>,
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
) -> Vec<usize> {
    let meta = builder.metadata();
    let schema = builder.parquet_schema();

    // Find the timestamp column index by name (matches load_parquet's logic).
    let mut ts_col: Option<usize> = None;
    let mut ts_unit_secs: Option<i64> = None; // ticks per second for the column's unit
    for (i, col) in schema.columns().iter().enumerate() {
        let name = col.name();
        let lower = name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "timestamp" | "ts" | "time" | "datetime" | "date"
        ) {
            ts_col = Some(i);
            ts_unit_secs = match col.logical_type() {
                Some(LogicalType::Timestamp { unit, .. }) => Some(match unit {
                    ParquetTimeUnit::MILLIS(_) => 1_000,
                    ParquetTimeUnit::MICROS(_) => 1_000_000,
                    ParquetTimeUnit::NANOS(_) => 1_000_000_000,
                }),
                _ => Some(1_000_000), // assume microseconds (matches existing files)
            };
            break;
        }
    }
    let ts_col = match ts_col {
        Some(i) => i,
        None => return (0..meta.num_row_groups()).collect(),
    };
    let unit = ts_unit_secs.unwrap_or(1_000_000);

    let to_native = |dt: NaiveDateTime| -> i64 {
        let secs = dt.and_utc().timestamp();
        let nanos = dt.and_utc().timestamp_subsec_nanos() as i64;
        match unit {
            1_000 => secs * 1_000 + nanos / 1_000_000,
            1_000_000 => secs * 1_000_000 + nanos / 1_000,
            1_000_000_000 => secs * 1_000_000_000 + nanos,
            _ => secs * 1_000_000 + nanos / 1_000,
        }
    };
    let start_native = start.map(to_native);
    let end_native = end.map(to_native);

    let mut keep = Vec::with_capacity(meta.num_row_groups());
    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        let col_meta = rg.column(ts_col);
        let stats = match col_meta.statistics() {
            Some(s) => s,
            None => {
                keep.push(rg_idx);
                continue;
            }
        };
        // Timestamps are physical Int64 in parquet.
        let (rg_min, rg_max) = match stats {
            Statistics::Int64(s) => (s.min_opt().copied(), s.max_opt().copied()),
            _ => {
                keep.push(rg_idx);
                continue;
            }
        };
        let mut overlaps = true;
        if let (Some(req_start), Some(rg_max)) = (start_native, rg_max) {
            if rg_max < req_start {
                overlaps = false;
            }
        }
        if let (Some(req_end), Some(rg_min)) = (end_native, rg_min) {
            if rg_min > req_end {
                overlaps = false;
            }
        }
        if overlaps {
            keep.push(rg_idx);
        }
    }
    keep
}

// ─── append helper ─────────────────────────────────────────────────────────────

/// The writer schema the collector emits. Matches the layout of files
/// produced by older writers (timestamp[us], no tz tag, Float64
/// OHLCV, columns nullable) so existing parquets can be appended to
/// without a rewrite and downstream tools see byte-identical schemas.
pub fn collector_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
    ]))
}

/// One row to append, in the collector's native units.
#[derive(Clone, Copy, Debug)]
pub struct CandleRow {
    pub timestamp_us: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Append a batch of rows to `{path}` as a fresh row group.
///
/// Strategy: open the existing file, copy every existing row group
/// **byte-for-byte** (no decode/encode) into a new `{path}.next`,
/// encode `rows` as a fresh row group, fsync, atomic rename.
///
/// If `path` does not exist we create it from scratch with the same
/// schema. Rows must be strictly increasing in `timestamp_us` and
/// strictly greater than the file's existing max timestamp; the caller
/// is responsible for deduplicating against its own written-through mark before
/// calling this.
pub fn append_row_group(path: &Path, rows: &[CandleRow]) -> std::io::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    // Verify the input batch is monotonically increasing.
    for w in rows.windows(2) {
        if w[1].timestamp_us <= w[0].timestamp_us {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "append_row_group: input rows not strictly increasing ({} <= {})",
                    w[1].timestamp_us, w[0].timestamp_us
                ),
            ));
        }
    }

    let schema = collector_schema();
    let next_path: PathBuf = {
        let mut p = path.as_os_str().to_owned();
        p.push(".next");
        PathBuf::from(p)
    };

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let next_file = File::create(&next_path)?;
    let mut writer = ArrowWriter::try_new(next_file, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // 1) Stream the existing file's row groups through, decoding and
    //    re-encoding (the parquet 54 ArrowWriter does not expose a raw
    //    row-group-copy API; the spec's "structural concat" reduces in
    //    practice to a decode→encode of historical row groups). The
    //    cost is one full read+write of the asset's history per minute,
    //    which is fine at the file sizes this writes and forces the
    //    fallback shard step before it gets prohibitive.
    let mut existing_max_ts: Option<i64> = None;
    if path.exists() {
        let existing = File::open(path)?;
        let rb_builder = ParquetRecordBatchReaderBuilder::try_new(existing)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        // Sanity check: existing schema must match our writer schema.
        let existing_schema = rb_builder.schema().clone();
        if !schema_compatible(&existing_schema, &schema) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "append_row_group: existing schema at {} does not match collector schema",
                    path.display()
                ),
            ));
        }
        let reader = rb_builder
            .build()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        for batch in reader {
            let batch = batch.map_err(|e| std::io::Error::other(e.to_string()))?;
            // Track the max timestamp seen so we can reject backward writes.
            if let Some(ts_col) = batch
                .column_by_name("timestamp")
                .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
            {
                for i in 0..ts_col.len() {
                    let v = ts_col.value(i);
                    existing_max_ts = Some(existing_max_ts.map_or(v, |m| m.max(v)));
                }
            }
            writer
                .write(&batch)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        // Force the historical block to flush into its own row group so the
        // append below becomes a separate, small row group (which the tail
        // reader can skip cheaply when warmup_start is past it).
        writer
            .flush()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
    }

    if let Some(max_ts) = existing_max_ts {
        if rows[0].timestamp_us <= max_ts {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "append_row_group: first input row ts={} not greater than existing max ts={}",
                    rows[0].timestamp_us, max_ts
                ),
            ));
        }
    }

    // 2) Encode the new rows as a fresh row group.
    let batch = candle_rows_to_batch(&schema, rows)?;
    writer
        .write(&batch)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    writer
        .close()
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // 3) fsync the tmpfile, then atomic rename. (Rename alone is atomic
    //    but not a flush; we need both so a power-cut between rename and
    //    a later page-cache flush can't leave a torn file.)
    {
        let f = File::open(&next_path)?;
        f.sync_all()?;
    }
    std::fs::rename(&next_path, path)?;
    Ok(())
}

fn schema_compatible(a: &Schema, b: &Schema) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    for (fa, fb) in a.fields().iter().zip(b.fields().iter()) {
        if fa.name() != fb.name() || fa.data_type() != fb.data_type() {
            return false;
        }
    }
    true
}

fn candle_rows_to_batch(schema: &Arc<Schema>, rows: &[CandleRow]) -> std::io::Result<RecordBatch> {
    let ts: Vec<i64> = rows.iter().map(|r| r.timestamp_us).collect();
    let ts_arr = TimestampMicrosecondArray::from(ts);
    let opens = Float64Array::from(rows.iter().map(|r| r.open).collect::<Vec<_>>());
    let highs = Float64Array::from(rows.iter().map(|r| r.high).collect::<Vec<_>>());
    let lows = Float64Array::from(rows.iter().map(|r| r.low).collect::<Vec<_>>());
    let closes = Float64Array::from(rows.iter().map(|r| r.close).collect::<Vec<_>>());
    let vols = Float64Array::from(rows.iter().map(|r| r.volume).collect::<Vec<_>>());
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts_arr),
            Arc::new(opens),
            Arc::new(highs),
            Arc::new(lows),
            Arc::new(closes),
            Arc::new(vols),
        ],
    )
    .map_err(|e| std::io::Error::other(e.to_string()))
}

/// Read the last timestamp written to a parquet file, in microseconds.
/// Returns None if the file does not exist or has no rows. Used by the
/// writer to verify its dedup mark on startup
/// and as a sanity check.
pub fn last_timestamp_us(path: &Path) -> std::io::Result<Option<i64>> {
    if !path.exists() {
        return Ok(None);
    }
    let file = File::open(path)?;
    let reader =
        SerializedFileReader::new(file).map_err(|e| std::io::Error::other(e.to_string()))?;
    let meta = reader.metadata();
    if meta.num_row_groups() == 0 {
        return Ok(None);
    }
    let mut max_ts: Option<i64> = None;
    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);
            if col.column_path().string() != "timestamp" {
                continue;
            }
            if let Some(Statistics::Int64(s)) = col.statistics() {
                if let Some(m) = s.max_opt() {
                    max_ts = Some(max_ts.map_or(*m, |cur| cur.max(*m)));
                }
            }
        }
    }
    // Statistics-based fast path covers the common case. If stats were
    // missing on every row group we fall back to scanning the last row
    // group's timestamp column.
    if max_ts.is_none() {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let last_rg = builder.metadata().num_row_groups() - 1;
        let reader = builder
            .with_row_groups(vec![last_rg])
            .build()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        for batch in reader {
            let batch = batch.map_err(|e| std::io::Error::other(e.to_string()))?;
            if let Some(arr) = batch
                .column_by_name("timestamp")
                .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
            {
                for i in 0..arr.len() {
                    let v = arr.value(i);
                    max_ts = Some(max_ts.map_or(v, |m| m.max(v)));
                }
            }
        }
    }
    Ok(max_ts)
}

#[cfg(test)]
mod roll_adjust_tests {
    use super::*;
    use crate::models::{asset_id, tf_id};
    use chrono::{Duration, NaiveDate};

    /// A flat 1m series over `days` UTC days with a small regular wobble,
    /// plus a jump of `gap` applied from `roll_at` onward.
    fn series(days: i64, roll_at: NaiveDateTime, gap: f64) -> Vec<Candle> {
        let t0 = NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        (0..days * 1440)
            .map(|i| {
                let ts = t0 + Duration::minutes(i);
                let wobble = if i % 2 == 0 { 0.1 } else { -0.1 };
                let base = 100.0 + wobble + if ts >= roll_at { gap } else { 0.0 };
                Candle {
                    asset: asset_id("T"),
                    timeframe: tf_id("1m"),
                    open: base,
                    high: base + 0.2,
                    low: base - 0.2,
                    close: base + 0.05,
                    volume: 1.0,
                    timestamp: ts,
                    complete: true,
                }
            })
            .collect()
    }

    #[test]
    fn a_midnight_jump_is_removed_and_earlier_bars_shifted() {
        let roll = NaiveDate::from_ymd_opt(2025, 1, 3)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let mut c = series(5, roll, 40.0);
        let before_last = c.last().unwrap().close;
        let n = roll_adjust(&mut c, 600);
        assert_eq!(n, 1);
        // Anchored at the latest prices: the tail is untouched.
        assert_eq!(c.last().unwrap().close, before_last);
        // Earlier bars were lifted by the gap, so the series is continuous.
        // The measured gap is open-after minus close-before: 140.1 - 99.95.
        assert!(
            (c[0].open - (100.1 + 40.15)).abs() < 1e-9,
            "got {}",
            c[0].open
        );
        let max_jump = c
            .windows(2)
            .map(|w| (w[1].open - w[0].close).abs())
            .fold(0.0, f64::max);
        assert!(max_jump < 1.0, "residual jump {max_jump}");
    }

    #[test]
    fn a_midday_jump_is_a_market_move_and_stays() {
        let mid = NaiveDate::from_ymd_opt(2025, 1, 3)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let mut c = series(5, mid, 40.0);
        let first = c[0].open;
        assert_eq!(roll_adjust(&mut c, 600), 0);
        assert_eq!(c[0].open, first);
    }

    #[test]
    fn short_series_are_left_alone() {
        let roll = NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(1, 0, 0)
            .unwrap();
        let mut c = series(5, roll, 40.0);
        c.truncate(100);
        assert_eq!(roll_adjust(&mut c, 600), 0);
    }
}

#[cfg(test)]
mod append_tests {
    use super::*;
    use tempfile::TempDir;

    fn row(ts_us: i64, base: f64) -> CandleRow {
        CandleRow {
            timestamp_us: ts_us,
            open: base,
            high: base + 1.0,
            low: base - 1.0,
            close: base + 0.5,
            volume: 10.0,
        }
    }

    #[test]
    fn append_to_empty_creates_file() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("BTC_1m.parquet");
        let rows = vec![row(1_000_000_000_000, 100.0), row(1_000_000_060_000, 101.0)];
        append_row_group(&p, &rows).unwrap();
        assert!(p.exists());
        let candles = load_parquet(&p, "BTC", None, None);
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].open, 100.0);
        assert_eq!(candles[1].open, 101.0);
    }

    #[test]
    fn append_then_append_preserves_history() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("BTC_1m.parquet");
        let r1 = vec![row(1_000_000_000_000, 100.0)];
        let r2 = vec![row(1_000_000_060_000, 101.0)];
        let r3 = vec![row(1_000_000_120_000, 102.0)];
        append_row_group(&p, &r1).unwrap();
        append_row_group(&p, &r2).unwrap();
        append_row_group(&p, &r3).unwrap();
        let candles = load_parquet(&p, "BTC", None, None);
        assert_eq!(candles.len(), 3);
        // Each append produces 2 row groups: a historical block (all prior
        // rows) plus the freshly written batch as its own small row group.
        // The small new-row-group is what makes tail filtering cheap.
        let f = std::fs::File::open(&p).unwrap();
        let r = SerializedFileReader::new(f).unwrap();
        assert_eq!(r.metadata().num_row_groups(), 2);
        // The last row group must contain only the latest append (1 row).
        let last = r.metadata().row_group(1);
        assert_eq!(last.num_rows(), 1);
    }

    #[test]
    fn append_rejects_backward_timestamp() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("BTC_1m.parquet");
        append_row_group(&p, &[row(1_000_000_060_000, 100.0)]).unwrap();
        // Earlier timestamp must be rejected.
        let err = append_row_group(&p, &[row(1_000_000_000_000, 99.0)]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        // Same timestamp must also be rejected.
        let err = append_row_group(&p, &[row(1_000_000_060_000, 99.0)]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn append_rejects_non_monotonic_input() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("BTC_1m.parquet");
        let bad = vec![row(2, 1.0), row(1, 2.0)];
        assert!(append_row_group(&p, &bad).is_err());
    }

    #[test]
    fn last_timestamp_returns_max() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("BTC_1m.parquet");
        assert!(last_timestamp_us(&p).unwrap().is_none());
        append_row_group(&p, &[row(1_000_000_000_000, 100.0)]).unwrap();
        append_row_group(&p, &[row(1_000_000_060_000, 101.0)]).unwrap();
        assert_eq!(last_timestamp_us(&p).unwrap(), Some(1_000_000_060_000));
    }

    #[test]
    fn tail_filter_skips_old_row_groups() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("BTC_1m.parquet");
        // 5 separate appends → 5 row groups, one minute apart starting at epoch.
        for i in 0..5i64 {
            let ts = 1_000_000_000_000_000 + i * 60_000_000;
            append_row_group(&p, &[row(ts, 100.0 + i as f64)]).unwrap();
        }
        // Filter to the last 2 minutes — should drop the first 3 row groups.
        let start_us = 1_000_000_000_000_000 + 3 * 60_000_000;
        let start_dt = chrono::DateTime::from_timestamp(
            start_us / 1_000_000,
            ((start_us % 1_000_000) * 1000) as u32,
        )
        .unwrap()
        .naive_utc();
        let candles = load_parquet(&p, "BTC", Some(start_dt), None);
        assert_eq!(candles.len(), 2);
    }
}
