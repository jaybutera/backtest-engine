use crate::models::{Candle, asset_id, tf_id};
use chrono::NaiveDateTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::Path;

/// Load candles from a parquet file, optionally filtering by date range.
pub fn load_parquet(
    path: &Path,
    asset: &str,
    start: Option<NaiveDateTime>,
    end: Option<NaiveDateTime>,
) -> Vec<Candle> {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("Failed to open {}: {}", path.display(), e));

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("Failed to read parquet {}: {}", path.display(), e));

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
    use parquet::file::reader::FileReader;
    reader.metadata().file_metadata().num_rows() as usize
}
