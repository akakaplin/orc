//! End-to-end tests over the whole engine: open, append, flush, restart.

use std::path::Path;

use orc::config::Config;
use orc::engine::Engine;
use orc::flush::planner::series_dir;
use orc::record::{Row, Value};

/// 2026-08-08T13:00:00Z in epoch microseconds, inside the default accept window.
const T13: u64 = 1_786_194_000_000_000;
const HOUR_US: u64 = 3_600_000_000;

fn config_json(dir: &Path) -> String {
    // `interval_ms: 0` disables the flush timer; tests flush explicitly.
    format!(
        r#"{{
          "data_dir": {},
          "flush": {{ "on_startup": false, "interval_ms": 0 }},
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

fn push(engine: &Engine, series: &orc::config::SeriesHandle, ts: u64, id: &str) {
    engine
        .append(
            series,
            &Row {
                ts,
                id,
                keys: &[Value::Str("AAPL"), Value::I64(1)],
                extra: &[],
                data: "",
            },
        )
        .unwrap();
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
                    keys: &[Value::Str("AAPL"), Value::I64(i as i64)],
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

    // An empty series writes no files.
    assert_eq!(parquet_rows(dir.path(), "quotes"), 0);
}

#[test]
fn records_survive_a_restart_without_an_explicit_flush() {
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
        // No flush: drop leaves durable WAL segments.
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

    let push = |ts: u64, id: &str| {
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

    let files = parquet_files(dir.path(), "trades");
    assert_eq!(files.len(), 2, "two hours, two files: {files:?}");
    assert_eq!(parquet_rows(dir.path(), "trades"), 3);
}

#[test]
fn bad_timestamps_are_rejected_with_their_units_named() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let trades = engine.series("trades").unwrap();

    let attempt = |ts: u64| {
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

    // The accept window doubles as a unit check.
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

/// The epoch stamped into frames must be persisted at open, or a restart with a
/// changed key list could reuse it and silently decode old frames into wrong columns.
#[test]
fn a_schema_epoch_is_durable_before_the_first_flush() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("manifest.json");

    {
        let engine = open(dir.path());
        let trades = engine.series("trades").unwrap();
        push(&engine, &trades, T13, "before");
        assert!(
            manifest.exists(),
            "the epoch these frames are stamped with must be on disk already"
        );
    }

    // Same arity and types, renamed keys.
    let renamed = format!(
        r#"{{
          "data_dir": {},
          "flush": {{ "on_startup": false, "interval_ms": 0 }},
          "series": [
            {{ "name": "trades",
               "keys": [ {{"name": "ticker", "type": "string"}},
                         {{"name": "qty", "type": "i64", "nullable": true}} ] }}
          ]
        }}"#,
        serde_json::to_string(&dir.path().to_string_lossy()).unwrap()
    );
    let config: Config = serde_json::from_str(&renamed).unwrap();
    let engine = Engine::open(config).unwrap();
    assert_eq!(
        engine.series("trades").unwrap().epoch(),
        1,
        "a renamed key list is a new epoch, not a reuse of the old number"
    );

    let outcome = engine.flush().unwrap();
    assert_eq!(outcome.rows_written, 1);
    assert_eq!(outcome.frames_rejected, 0, "nothing was misdecoded");
}

/// Parquet in `series/` past the watermark (only older binaries, which published
/// before committing, leave these) is deleted at open; its rows are still in the WAL.
#[test]
fn an_uncommitted_flushs_output_is_swept_instead_of_being_duplicated() {
    let dir = tempfile::tempdir().unwrap();
    let hour_dir = series_dir(dir.path(), "trades").join("hour=2026-08-08T13");

    {
        let engine = open(dir.path());
        let trades = engine.series("trades").unwrap();
        push(&engine, &trades, T13, "a");
        push(&engine, &trades, T13 + 1, "b");
        engine.flush().unwrap();
    }
    let committed = parquet_files(dir.path(), "trades");
    assert_eq!(committed.len(), 1, "one committed file to start from");

    // A real file named for a range past the watermark.
    let orphan = hour_dir.join("0000000001-0000009999.e0.parquet");
    std::fs::copy(&committed[0], &orphan).unwrap();
    assert_eq!(parquet_rows(dir.path(), "trades"), 4, "duplicated for now");

    let engine = open(dir.path());
    assert!(!orphan.exists(), "the orphan must be gone");
    assert_eq!(
        parquet_rows(dir.path(), "trades"),
        2,
        "and the committed file must not be"
    );

    let stranger = hour_dir.join("notes.txt");
    std::fs::write(&stranger, b"mine").unwrap();
    drop(engine);
    let _engine = open(dir.path());
    assert!(stranger.exists(), "unrecognised names are left alone");
}

/// Uncommitted staged files in `tmp/` (nested or legacy flat) are deleted at open.
#[test]
fn staged_files_from_an_interrupted_flush_are_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    drop(open(dir.path())); // a manifest, so the watermark is a real 0

    let tmp = dir.path().join("tmp");
    // Legacy flat layout.
    std::fs::create_dir_all(&tmp).unwrap();
    let legacy = tmp.join("trades.hour=2026-08-08T13.0000000001-0000000002.e0.parquet");
    std::fs::write(&legacy, vec![0u8; 4096]).unwrap();
    // Current `tmp/<series>/<hour>/` layout.
    let hour = tmp.join("trades").join("hour=2026-08-08T13");
    std::fs::create_dir_all(&hour).unwrap();
    let staged = hour.join("0000000001-0000000002.e0.parquet");
    std::fs::write(&staged, vec![0u8; 4096]).unwrap();

    let _engine = open(dir.path());
    assert!(
        !legacy.exists(),
        "legacy staging files must not survive open"
    );
    assert!(
        !staged.exists(),
        "uncommitted staged files must not survive open"
    );
    assert!(!hour.exists(), "nor the directories they emptied");
    assert!(
        parquet_files(dir.path(), "trades").is_empty(),
        "and nothing uncommitted is published"
    );
}

/// Committed files left in `tmp/` by a failed publish are the only copy of their
/// rows; open promotes them into `series/`.
#[test]
fn committed_staged_files_are_published_at_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        let trades = engine.series("trades").unwrap();
        push(&engine, &trades, T13, "a");
        push(&engine, &trades, T13 + 1, "b");
        engine.flush().unwrap();
    }
    let published = parquet_files(dir.path(), "trades");
    assert_eq!(published.len(), 1);

    // Simulate a crash between commit and publish.
    let rel = published[0]
        .strip_prefix(dir.path().join("series"))
        .unwrap();
    let staged = dir.path().join("tmp").join(rel);
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    std::fs::rename(&published[0], &staged).unwrap();
    assert_eq!(parquet_rows(dir.path(), "trades"), 0);

    let _engine = open(dir.path());
    assert!(!staged.exists(), "promoted out of tmp/");
    assert_eq!(parquet_files(dir.path(), "trades"), published);
    assert_eq!(parquet_rows(dir.path(), "trades"), 2);
}

/// `series/` holds only committed files, so an external mover can take them
/// without losing or repeating a row.
#[test]
fn moving_published_files_away_loses_and_repeats_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let mut moved = 0;
    let mut sweep_to_bucket = |dir: &Path| {
        for f in parquet_files(dir, "trades") {
            let to = bucket.path().join(format!("{moved}.parquet"));
            std::fs::copy(&f, &to).unwrap();
            std::fs::remove_file(&f).unwrap();
            moved += 1;
        }
    };

    {
        let e = open(dir.path());
        let trades = e.series("trades").unwrap();
        push(&e, &trades, T13, "a");
        push(&e, &trades, T13 + HOUR_US, "b");
        e.flush().unwrap();
        sweep_to_bucket(dir.path());

        push(&e, &trades, T13 + 1, "c");
        push(&e, &trades, T13 + HOUR_US + 1, "d");
        e.flush().unwrap();
    }
    let e = open(dir.path());
    let trades = e.series("trades").unwrap();
    push(&e, &trades, T13 + 2, "e");
    e.flush().unwrap();
    sweep_to_bucket(dir.path());

    let mut seen: Vec<(u64, String)> = Vec::new();
    for f in std::fs::read_dir(bucket.path()).unwrap().flatten() {
        seen.extend(orc::flush::read::read_ts_id(&f.path()).unwrap());
    }
    seen.sort();
    let ids: Vec<&str> = seen.iter().map(|(_, id)| id.as_str()).collect();
    assert_eq!(ids, ["a", "c", "e", "b", "d"]);
    assert!(parquet_files(dir.path(), "trades").is_empty());
}

/// A lost manifest also refuses to start over committed staged files.
#[test]
fn a_lost_manifest_protects_committed_staged_files_too() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        let trades = engine.series("trades").unwrap();
        push(&engine, &trades, T13, "a");
        engine.flush().unwrap();
    }
    let published = parquet_files(dir.path(), "trades").remove(0);
    let staged = dir
        .path()
        .join("tmp")
        .join(published.strip_prefix(dir.path().join("series")).unwrap());
    std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
    std::fs::rename(&published, &staged).unwrap();
    std::fs::remove_file(dir.path().join("manifest.json")).unwrap();

    let config: Config = serde_json::from_str(&config_json(dir.path())).unwrap();
    let err = Engine::open(config).unwrap_err().to_string();
    assert!(err.contains("Refusing to start"), "{err}");
    assert!(staged.exists(), "and the staged file was not touched");
}

/// `flush.interval_ms` triggers flushes on its own.
#[test]
fn the_interval_timer_flushes_without_being_asked() {
    let dir = tempfile::tempdir().unwrap();
    let config_json = format!(
        r#"{{
          "data_dir": {},
          "flush": {{ "on_startup": false, "interval_ms": 50 }},
          "wal": {{ "fsync_interval_ms": 1 }},
          "series": [ {{ "name": "trades", "keys": [] }} ]
        }}"#,
        serde_json::to_string(&dir.path().to_string_lossy()).unwrap()
    );
    let config: Config = serde_json::from_str(&config_json).unwrap();
    let engine = Engine::open(config).unwrap();
    let trades = engine.series("trades").unwrap();
    for i in 0..10 {
        engine
            .append(
                &trades,
                &Row {
                    ts: T13 + i,
                    id: "",
                    keys: &[],
                    extra: &[],
                    data: "",
                },
            )
            .unwrap();
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while parquet_rows(dir.path(), "trades") < 10 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(
        parquet_rows(dir.path(), "trades"),
        10,
        "the timer must flush with no explicit call"
    );

    engine.close().unwrap();
}

/// `Drop` → `close` joins the timer thread, so it never outlives the engine.
#[test]
fn dropping_an_engine_stops_its_flush_timer() {
    let dir = tempfile::tempdir().unwrap();
    let config_json = format!(
        r#"{{
          "data_dir": {},
          "flush": {{ "on_startup": false, "interval_ms": 20 }},
          "series": [ {{ "name": "trades", "keys": [] }} ]
        }}"#,
        serde_json::to_string(&dir.path().to_string_lossy()).unwrap()
    );
    let config: Config = serde_json::from_str(&config_json).unwrap();
    let engine = Engine::open(config).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(60));
    drop(engine);

    // Reopening proves the lock was released.
    let config: Config = serde_json::from_str(&config_json).unwrap();
    Engine::open(config).expect("the lock must have been released");
}

/// Without `manifest.json` the watermark reads as 0 and the sweep would delete
/// every file, so open refuses instead.
#[test]
fn a_lost_manifest_stops_the_engine_instead_of_wiping_the_dataset() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        let trades = engine.series("trades").unwrap();
        for i in 0..5 {
            push(&engine, &trades, T13 + i, "");
        }
        engine.flush().unwrap();
    }
    let committed = parquet_files(dir.path(), "trades");
    assert_eq!(committed.len(), 1);
    assert_eq!(parquet_rows(dir.path(), "trades"), 5);

    std::fs::remove_file(dir.path().join("manifest.json")).unwrap();

    let config: Config = serde_json::from_str(&config_json(dir.path())).unwrap();
    let err = Engine::open(config).unwrap_err().to_string();
    assert!(err.contains("manifest.json"), "{err}");
    assert!(err.contains("series"), "{err}");
    assert!(
        committed[0].exists() && parquet_rows(dir.path(), "trades") == 5,
        "a refusal must not be a deletion"
    );

    // Restoring the manifest is enough.
    std::fs::write(
        dir.path().join("manifest.json"),
        serde_json::to_vec(&serde_json::json!({"last_flushed_segment": 1})).unwrap(),
    )
    .unwrap();
    let engine = open(dir.path());
    assert_eq!(
        parquet_rows(dir.path(), "trades"),
        5,
        "and the data is still there"
    );
    drop(engine);
}

#[test]
fn a_fresh_directory_still_opens_without_a_manifest() {
    let dir = tempfile::tempdir().unwrap();
    assert!(!dir.path().join("manifest.json").exists());
    let engine = open(dir.path());
    let trades = engine.series("trades").unwrap();
    push(&engine, &trades, T13, "a");
    assert_eq!(engine.flush().unwrap().rows_written, 1);
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

/// A publish that fails after commit leaves the committed file in `tmp/`; the
/// next flush (no restart) promotes it instead of rewriting its rows.
#[test]
fn a_flush_that_failed_part_way_does_not_leave_duplicates_behind() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(dir.path());
    let trades = e.series("trades").unwrap();
    push(&e, &trades, T13, "a");
    push(&e, &trades, T13 + HOUR_US, "b");

    // Block the second hour's publish; the first is published, both are committed.
    let hour = orc::time::hour_partition(T13 + HOUR_US);
    let blocked = series_dir(dir.path(), "trades").join(&hour);
    std::fs::create_dir_all(blocked.parent().unwrap()).unwrap();
    std::fs::write(&blocked, b"not a directory").unwrap();

    assert!(e.flush().is_err(), "the second partition must fail");
    assert_eq!(
        parquet_files(dir.path(), "trades").len(),
        1,
        "the first partition is published"
    );
    let staged = dir.path().join("tmp").join("trades").join(&hour);
    assert_eq!(
        std::fs::read_dir(&staged).unwrap().count(),
        1,
        "the second is committed but still staged"
    );

    std::fs::remove_file(&blocked).unwrap();
    push(&e, &trades, T13 + 1, "c");
    e.flush().unwrap();
    assert!(!staged.exists(), "promoted and its directory cleared");

    let mut seen: Vec<(u64, String)> = Vec::new();
    for f in parquet_files(dir.path(), "trades") {
        seen.extend(orc::flush::read::read_ts_id(&f).unwrap());
    }
    seen.sort();
    let mut distinct = seen.clone();
    distinct.dedup();
    assert_eq!(seen, distinct, "a row is visible twice: {seen:?}");
    assert_eq!(parquet_rows(dir.path(), "trades"), 3);
}
