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
    // `interval_ms: 0` disables the flush timer, so every test below controls
    // exactly when a flush happens. The timer has its own test.
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

fn push(engine: &Engine, series: &orc::config::SeriesHandle, ts: i64, id: &str) {
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

/// A schema epoch is minted in memory at open and stamped into every frame from
/// that moment. If it is not written down, the next start re-mints the same
/// number for whatever the config says then — and yesterday's frames get decoded
/// against today's key list. When the arity happens to match, that is silent:
/// the values land under the wrong column names with no error and no counter.
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

    // Same key names and types as `config_json`, but renamed: with the epoch
    // lost, this would be minted as epoch 0 again and the frames above would
    // decode against it -- same arity, same types, wrong columns.
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

    // And the old frames still flush under the schema they were written with.
    let outcome = engine.flush().unwrap();
    assert_eq!(outcome.rows_written, 1);
    assert_eq!(outcome.frames_rejected, 0, "nothing was misdecoded");
}

/// Files are renamed into `series/` before the manifest commits, and the name
/// encodes the consumed segment range. A flush that fails after the rename
/// leaves a real file whose rows the next flush — now covering a wider range,
/// hence a different name — writes again beside it. Both match the reader's
/// glob, so every row in the overlap is returned twice.
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

    // Exactly what an interrupted flush leaves: a valid Parquet file, correctly
    // named for a segment range wider than anything committed, holding rows a
    // later flush will write again. Copied from the real one so it is a genuine
    // readable file rather than a stub.
    let orphan = hour_dir.join("0000000001-0000009999.e0.parquet");
    std::fs::copy(&committed[0], &orphan).unwrap();
    assert_eq!(parquet_rows(dir.path(), "trades"), 4, "duplicated for now");

    // Opening sweeps it: its range reaches past `last_flushed_segment`, which
    // only a flush that never committed can produce.
    let engine = open(dir.path());
    assert!(!orphan.exists(), "the orphan must be gone");
    assert_eq!(
        parquet_rows(dir.path(), "trades"),
        2,
        "and the committed file must not be"
    );

    // A file the engine did not write is not its business, swept or otherwise.
    let stranger = hour_dir.join("notes.txt");
    std::fs::write(&stranger, b"mine").unwrap();
    drop(engine);
    let _engine = open(dir.path());
    assert!(stranger.exists(), "unrecognised names are left alone");
}

/// A staged file is uncommitted by definition, and its name embeds the segment
/// range — so the next flush writes a differently-named file and never reclaims
/// it. Left alone, every interrupted flush leaks its whole output forever.
#[test]
fn staged_files_from_an_interrupted_flush_are_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    let staged = tmp.join("trades.hour=2026-08-08T13.0000000001-0000000002.e0.parquet");
    std::fs::write(&staged, vec![0u8; 4096]).unwrap();

    let _engine = open(dir.path());
    assert!(
        !staged.exists(),
        "stale staging files must not survive open"
    );
}

/// The flush the whole design is described in terms of. Nothing scheduled one:
/// `flush.interval_ms` had no consumer, so Parquet only ever appeared at startup
/// or on an explicit request, and a long-running server grew WAL forever.
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

    // Generous: the point is that it happens at all, not when. Polling rather
    // than one long sleep keeps a passing run fast.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while parquet_rows(dir.path(), "trades") < 10 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(
        parquet_rows(dir.path(), "trades"),
        10,
        "the timer must flush with no explicit call"
    );

    // And it must not keep the process alive or wedge the close.
    engine.close().unwrap();
}

/// `close` joins the timer, and `Drop` calls `close`. If the join were reachable
/// from the timer thread itself it would deadlock; if it were skipped, a dropped
/// engine would leave a thread flushing a directory it no longer owns.
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

    // The directory lock is released by the same Drop, so a second engine can
    // only open if the first shut down completely.
    let config: Config = serde_json::from_str(&config_json).unwrap();
    Engine::open(config).expect("the lock must have been released");
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
