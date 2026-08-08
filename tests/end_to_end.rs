//! End-to-end tests over the whole engine: open, append, flush, restart.
//!
//! The unit tests each prove one module in isolation. These prove the seams
//! between them, which is where an integration bug actually lives: a record
//! written through `Engine::append` has to survive encoding, the WAL, a fsync, a
//! restart, decoding, sorting, dedup and the Parquet writer before a reader sees
//! it, and any one of those handoffs can lose or reshape it.

use std::path::Path;

use orc::config::Config;
use orc::engine::Engine;
use orc::flush::planner::series_dir;
use orc::record::{Row, Value};

/// Epoch microseconds for 2026-08-08T13:00:00Z, inside the default accept
/// window (which starts at 2000-01-01 and ends 24h after now).
const T13: i64 = 1_786_194_000_000_000;
const HOUR_US: i64 = 3_600_000_000;

fn config_json(dir: &Path) -> String {
    format!(
        r#"{{
          "data_dir": {},
          "flush": {{ "on_startup": false }},
          "wal": {{ "fsync_interval_ms": 1 }},
          "series": [
            {{ "name": "trades",
               "keys": [ {{"name": "symbol", "type": "string"}},
                         {{"name": "size", "type": "i64", "nullable": true}} ] }},
            {{ "name": "quotes", "keys": [] }}
          ]
        }}"#,
        serde_json::to_string(&dir.to_string_lossy()).unwrap()
    )
}

fn open(dir: &Path) -> Engine {
    let config: Config = serde_json::from_str(&config_json(dir)).expect("config parses");
    Engine::open(config).expect("engine opens")
}

/// Count rows across every Parquet file under a series directory.
fn parquet_rows(dir: &Path, series: &str) -> usize {
    let mut total = 0;
    for path in parquet_files(dir, series) {
        total += orc::flush::read::read_metadata(&path).unwrap().num_rows as usize;
    }
    total
}

fn parquet_files(dir: &Path, series: &str) -> Vec<std::path::PathBuf> {
    let root = series_dir(dir, series);
    let mut out = Vec::new();
    let Ok(hours) = std::fs::read_dir(&root) else {
        return out;
    };
    for hour in hours.flatten() {
        if !hour.path().is_dir() {
            continue;
        }
        for f in std::fs::read_dir(hour.path()).unwrap().flatten() {
            if f.path().extension().is_some_and(|e| e == "parquet") {
                out.push(f.path());
            }
        }
    }
    out.sort();
    out
}

#[test]
fn append_flush_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let trades = engine.series("trades").unwrap();

    for i in 0..100 {
        engine
            .append(
                &trades,
                &Row {
                    ts: T13 + i,
                    id: &format!("id-{i}"),
                    keys: &[Value::Str("AAPL"), Value::I64(i)],
                    extra: &[("feed", "itch")],
                    data: r#"{"px":1.0}"#,
                },
            )
            .unwrap();
    }

    let outcome = engine.flush().unwrap();
    assert_eq!(outcome.rows_written, 100);
    assert_eq!(outcome.rows_deduplicated, 0);
    assert_eq!(outcome.frames_rejected, 0);
    assert_eq!(parquet_rows(dir.path(), "trades"), 100);

    // A series that never received a record must not produce a directory full
    // of empty files.
    assert_eq!(parquet_rows(dir.path(), "quotes"), 0);
}

#[test]
fn records_survive_a_restart_without_an_explicit_flush() {
    // The case the engine exists for: it restarts constantly, and a restart
    // between append and flush must not lose anything.
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        let trades = engine.series("trades").unwrap();
        for i in 0..50 {
            engine
                .append(
                    &trades,
                    &Row {
                        ts: T13 + i,
                        id: "",
                        keys: &[Value::Str("MSFT"), Value::Null],
                        extra: &[],
                        data: "",
                    },
                )
                .unwrap();
        }
        // No flush: drop closes the WAL, leaving durable segments behind.
    }

    let engine = open(dir.path());
    let outcome = engine.flush().unwrap();
    assert_eq!(
        outcome.rows_written, 50,
        "everything appended before the restart must reach Parquet"
    );
    assert_eq!(parquet_rows(dir.path(), "trades"), 50);
}

#[test]
fn duplicates_collapse_and_hours_split() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let trades = engine.series("trades").unwrap();

    let push = |ts: i64, id: &str| {
        engine
            .append(
                &trades,
                &Row {
                    ts,
                    id,
                    keys: &[Value::Str("AAPL"), Value::I64(1)],
                    extra: &[],
                    data: "",
                },
            )
            .unwrap();
    };

    push(T13, "dup");
    push(T13, "dup"); // same (ts, id): collapses
    push(T13, "other"); // same ts, different id: survives
    push(T13 + HOUR_US, "next-hour"); // different hour partition

    let outcome = engine.flush().unwrap();
    assert_eq!(outcome.rows_deduplicated, 1, "one duplicate dropped");
    assert_eq!(outcome.rows_written, 3);

    // No file may span two hour partitions.
    let files = parquet_files(dir.path(), "trades");
    assert_eq!(files.len(), 2, "two hours, two files: {files:?}");
    assert_eq!(parquet_rows(dir.path(), "trades"), 3);
}

#[test]
fn bad_timestamps_are_rejected_with_their_units_named() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let trades = engine.series("trades").unwrap();

    let attempt = |ts: i64| {
        engine.append(
            &trades,
            &Row {
                ts,
                id: "",
                keys: &[Value::Str("AAPL"), Value::Null],
                extra: &[],
                data: "",
            },
        )
    };

    // The same instant in the three wrong units. Each must be refused, which is
    // what makes the accept window double as a unit check.
    assert!(attempt(T13 / 1_000_000).is_err(), "seconds");
    assert!(attempt(T13 / 1_000).is_err(), "milliseconds");
    assert!(attempt(T13.saturating_mul(1_000)).is_err(), "nanoseconds");
    assert!(
        attempt(T13).is_ok(),
        "microseconds are the one accepted unit"
    );

    assert_eq!(engine.stats().rejected_ts, 3);
}

#[test]
fn a_second_engine_cannot_open_the_same_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _first = open(dir.path());
    let config: Config = serde_json::from_str(&config_json(dir.path())).unwrap();
    assert!(
        Engine::open(config).is_err(),
        "two live engines on one data dir would interleave segment ids"
    );
}

#[test]
fn flush_is_a_noop_when_there_is_nothing_to_do() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());

    let outcome = engine.flush().unwrap();
    assert_eq!(outcome.rows_written, 0);
    assert!(
        outcome.segments_consumed.is_empty(),
        "an empty flush must not commit or write files"
    );
    assert!(parquet_files(dir.path(), "trades").is_empty());
}
