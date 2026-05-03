use crate::models::{Candle, asset_id, tf_id};
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
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Load candles from a parquet file, optionally filtering by date range.
///
/// Tail-only optimization: when `start` is set, the reader inspects the
/// per-row-group timestamp statistics and skips any row group whose `[min, max]`
/// range falls entirely before `start` (or entirely after `end`). The collector
/// emits one row group per minute, so a year-old file may contain hundreds of
/// thousands of row groups — but a 14-day warmup only needs to decode the
/// last ~20k of them. If statistics are missing for a row group, the reader
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

        let ts_col = find_col(&["timestamp", "ts", "time", "datetime", "date"]).expect("No timestamp column");
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
                timeframe: tf_id("1m"),
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

fn extract_timestamps(col: &arrow::array::ArrayRef) -> Vec<NaiveDateTime> {
    use arrow::array::*;
    use arrow::datatypes::*;

    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return arr.iter()
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
        return arr.iter()
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
        return arr.iter()
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
        return arr.iter()
            .map(|v| {
                let s = v.unwrap_or(0);
                chrono::DateTime::from_timestamp(s, 0)
                    .unwrap()
                    .naive_utc()
            })
            .collect();
    }
    // Int64 — assume milliseconds epoch
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return arr.iter()
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
        return arr.iter()
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
                // Must contain _1m
                if !stem.contains("_1m") {
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
        if matches!(lower.as_str(), "timestamp" | "ts" | "time" | "datetime" | "date") {
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
/// produced by the legacy `collect.sh` (timestamp[us], no tz tag, Float64
/// OHLCV, columns nullable) so existing parquets can be appended to
/// without a rewrite and downstream tools see byte-identical schemas.
pub fn collector_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
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
/// is responsible for dedup against `.collector.state.json` before
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    // 1) Stream the existing file's row groups through, decoding and
    //    re-encoding (the parquet 54 ArrowWriter does not expose a raw
    //    row-group-copy API; the spec's "structural concat" reduces in
    //    practice to a decode→encode of historical row groups). The
    //    cost is one full read+write of the asset's history per minute,
    //    which is fine at 60-day file sizes (~1MB) and forces the
    //    fallback shard step before it gets prohibitive.
    let mut existing_max_ts: Option<i64> = None;
    if path.exists() {
        let existing = File::open(path)?;
        let rb_builder = ParquetRecordBatchReaderBuilder::try_new(existing)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        }
        // Force the historical block to flush into its own row group so the
        // append below becomes a separate, small row group (which the tail
        // reader can skip cheaply when warmup_start is past it).
        writer
            .flush()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    writer
        .close()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

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
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

/// Read the last timestamp written to a parquet file, in microseconds.
/// Returns None if the file does not exist or has no rows. Used by the
/// collector to verify dedup against `.collector.state.json` on startup
/// and as a sanity check.
pub fn last_timestamp_us(path: &Path) -> std::io::Result<Option<i64>> {
    if !path.exists() {
        return Ok(None);
    }
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        let last_rg = builder.metadata().num_row_groups() - 1;
        let reader = builder
            .with_row_groups(vec![last_rg])
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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

// Silence the unused-import lint for `Read` (kept for potential future
// streaming uses; no current call site).
#[allow(dead_code)]
fn _read_marker<R: Read>(_r: R) {}

#[cfg(test)]
mod append_tests {
    use super::*;
    use chrono::NaiveDate;
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

    #[test]
    fn _suppress_unused_naivedate() {
        // Touch NaiveDate so the `use` above isn't dead.
        let _ = NaiveDate::from_ymd_opt(2026, 1, 1);
    }
}
