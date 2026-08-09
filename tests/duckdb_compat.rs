//! Reader-compatibility tests against a real DuckDB binary.
//!
//! The Parquet round-trip in `flush::parquet` proves the files are valid — but
//! it reads them back with the same library that wrote them, so it cannot prove
//! the choices that only matter to an *outside* reader: Hive partition
//! discovery, `extra` arriving as a MAP rather than an opaque struct,
//! `union_by_name` reconciling two epochs with different columns, and the
//! generated `view.sql` being syntactically valid SQL.
//!
//! These skip when `duckdb` is not on PATH. A `cargo test` that fails because
//! an external binary is missing would be hostile. Set `ORC_REQUIRE_DUCKDB=1`
//! to turn a missing binary into a failure, so the coverage cannot silently
//! disappear on a machine that is supposed to have it.

use std::path::Path;
use std::process::Command;

use orc::config::Config;
use orc::engine::Engine;
use orc::record::{Row, Value};

const T13: i64 = 1_786_194_000_000_000;
const HOUR_US: i64 = 3_600_000_000;

/// Whether to run, and whether a missing binary is fatal.
///
/// Skipping keeps `cargo test` usable on a machine without DuckDB. But a test
/// that skips is a test that can quietly stop running forever, so setting
/// `ORC_REQUIRE_DUCKDB=1` makes a missing binary a failure instead. The
/// coverage cannot evaporate without someone being told.
fn duckdb_available() -> bool {
    let found = Command::new("duckdb")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success());
    assert!(
        found || std::env::var_os("ORC_REQUIRE_DUCKDB").is_none(),
        "ORC_REQUIRE_DUCKDB is set but the duckdb binary is not on PATH"
    );
    found
}

/// Run SQL with the data directory as the working directory.
///
/// The working directory is not incidental: `view.sql` uses relative globs, and
/// DuckDB resolves those against the process cwd rather than the SQL file's
/// location. Running from anywhere else is exactly the mistake the generated
/// file's header comment warns about.
fn query(data_dir: &Path, sql: &str) -> String {
    let out = Command::new("duckdb")
        .current_dir(data_dir)
        .args(["-csv", "-noheader", "-c", sql])
        .output()
        .expect("duckdb runs");
    assert!(
        out.status.success(),
        "duckdb failed on:\n{sql}\n--- stderr ---\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn config_json(dir: &Path, with_venue: bool) -> String {
    let venue = if with_venue {
        r#", {"name": "venue", "type": "string", "nullable": true}"#
    } else {
        ""
    };
    format!(
        r#"{{
          "data_dir": {},
          "flush": {{ "on_startup": false }},
          "wal": {{ "fsync_interval_ms": 1 }},
          "series": [
            {{ "name": "trades",
               "keys": [ {{"name": "symbol", "type": "string"}}{venue} ] }}
          ]
        }}"#,
        serde_json::to_string(&dir.to_string_lossy()).unwrap()
    )
}

fn open(dir: &Path, with_venue: bool) -> Engine {
    let config: Config = serde_json::from_str(&config_json(dir, with_venue)).unwrap();
    Engine::open(config).expect("engine opens")
}

/// Write two epochs' worth of data: epoch 0 without `venue` (so it lands in
/// `extra`), then epoch 1 with `venue` declared as a real column. That is the
/// promotion case the generated view exists to paper over.
fn build_two_epoch_dataset(dir: &Path) {
    {
        let engine = open(dir, false);
        let trades = engine.series("trades").unwrap();
        for i in 0..3 {
            engine
                .append(
                    &trades,
                    &Row {
                        ts: T13 + i,
                        id: &format!("e0-{i}"),
                        keys: &[Value::Str("AAPL")],
                        // `venue` is undeclared at this epoch, so it goes here.
                        extra: &[("venue", "XNAS"), ("feed", "itch")],
                        data: r#"{"px":1.5}"#,
                    },
                )
                .unwrap();
        }
        engine.flush().unwrap();
    }
    {
        // Reopening with an extra key bumps this series' epoch.
        let engine = open(dir, true);
        let trades = engine.series("trades").unwrap();
        for i in 0..2 {
            engine
                .append(
                    &trades,
                    &Row {
                        ts: T13 + HOUR_US + i,
                        id: &format!("e1-{i}"),
                        keys: &[Value::Str("MSFT"), Value::Str("XNYS")],
                        extra: &[("feed", "itch")],
                        data: r#"{"px":2.5}"#,
                    },
                )
                .unwrap();
        }
        engine.flush().unwrap();
    }
}

#[test]
fn duckdb_reads_the_dataset_with_hive_partitioning() {
    if !duckdb_available() {
        eprintln!("skipping: duckdb not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    build_two_epoch_dataset(dir.path());

    // The headline query from the plan's verification section. `union_by_name`
    // is required because the two epochs have different columns; without it
    // DuckDB errors on the schema mismatch rather than reconciling.
    // Compared as epoch microseconds, not as rendered text. `ts` is a TIMESTAMP
    // WITH TIME ZONE, which DuckDB prints in the *session's* timezone -- so a
    // string comparison here would pass in CI (UTC) and fail on any developer
    // machine set to anything else.
    let out = query(
        dir.path(),
        "select count(*), epoch_us(min(ts)), epoch_us(max(ts)) from read_parquet(
             'series/trades/**/*.parquet', hive_partitioning = 1, union_by_name = 1)",
    );
    let expected = format!("5,{},{}", T13, T13 + HOUR_US + 1);
    assert_eq!(
        out, expected,
        "row count and the exact ts range must survive the round trip"
    );

    // Hive partitioning must expose `hour` as a real column derived from the
    // directory name -- that is the whole reason for the `hour=` layout.
    let hours = query(
        dir.path(),
        "select distinct hour from read_parquet(
             'series/trades/**/*.parquet', hive_partitioning = 1, union_by_name = 1)
         order by hour",
    );
    assert_eq!(hours, "2026-08-08T13\n2026-08-08T14", "got: {hours}");
}

#[test]
fn extra_reads_back_as_a_map() {
    if !duckdb_available() {
        eprintln!("skipping: duckdb not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    build_two_epoch_dataset(dir.path());

    // Subscripting only works if the column really is a Parquet MAP. This is
    // what the non-default `key_value`/`key`/`value` field naming buys.
    let out = query(
        dir.path(),
        "select count(*) from read_parquet(
             'series/trades/**/*.parquet', hive_partitioning = 1, union_by_name = 1)
         where extra['feed'] = 'itch'",
    );
    assert_eq!(out, "5", "extra must subscript as a MAP: {out}");
}

#[test]
fn the_generated_view_runs_and_coalesces_a_promoted_key() {
    if !duckdb_available() {
        eprintln!("skipping: duckdb not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    build_two_epoch_dataset(dir.path());

    let view_path = dir.path().join("series/trades/view.sql");
    let view_sql = std::fs::read_to_string(&view_path)
        .unwrap_or_else(|e| panic!("view.sql at {view_path:?}: {e}"));

    // Run the generated file verbatim, then query through it. If the generator
    // emits anything DuckDB will not parse, this is where it surfaces.
    let out = query(
        dir.path(),
        &format!("{view_sql}\nselect count(*), count(venue) from trades;"),
    );
    assert_eq!(
        out, "5,5",
        "every row must have a venue: 2 from the declared column, \
         3 coalesced out of extra -- got {out}"
    );

    // And the values must be the right ones, not merely non-null.
    let venues = query(
        dir.path(),
        &format!("{view_sql}\nselect venue, count(*) from trades group by venue order by venue;"),
    );
    assert_eq!(venues, "XNAS,3\nXNYS,2", "got: {venues}");
}

/// Promoting a non-string key must leave `view.sql` runnable, and the column
/// with the type its config declares.
///
/// DuckDB refuses to coalesce a BIGINT column against the VARCHAR one that
/// `extra` yields — "an explicit cast is required" — so before the cast was
/// added this file raised a Binder Error and the view was unusable from the
/// moment a typed key was promoted. Every other view test uses `string` keys,
/// where both sides already agree and nothing is wrong.
#[test]
fn a_promoted_typed_key_still_compares_as_its_declared_type() {
    if !duckdb_available() {
        eprintln!("skipping: duckdb not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();

    let config = |with_size: bool| {
        let size = if with_size {
            r#", {"name": "size", "type": "i64", "nullable": true}"#
        } else {
            ""
        };
        format!(
            r#"{{
              "data_dir": {},
              "flush": {{ "on_startup": false }},
              "wal": {{ "fsync_interval_ms": 1 }},
              "series": [
                {{ "name": "trades",
                   "keys": [ {{"name": "symbol", "type": "string"}}{size} ] }}
              ]
            }}"#,
            serde_json::to_string(&dir.path().to_string_lossy()).unwrap()
        )
    };

    // Epoch 0: `size` undeclared, so 9 and 100 land in `extra` as text.
    {
        let cfg: Config = serde_json::from_str(&config(false)).unwrap();
        let engine = Engine::open(cfg).unwrap();
        let trades = engine.series("trades").unwrap();
        // "n/a" is the uncontrolled-producer case: `extra` is untyped text, so
        // nothing stopped it being written. A plain cast would turn it into a
        // Conversion Error that fails every query through the view.
        for (i, size) in ["9", "100", "n/a"].iter().enumerate() {
            engine
                .append(
                    &trades,
                    &Row {
                        ts: T13 + i as i64,
                        id: &format!("e0-{i}"),
                        keys: &[Value::Str("AAPL")],
                        extra: &[("size", size)],
                        data: "",
                    },
                )
                .unwrap();
        }
        engine.flush().unwrap();
    }
    // Epoch 1: `size` is a real BIGINT column.
    {
        let cfg: Config = serde_json::from_str(&config(true)).unwrap();
        let engine = Engine::open(cfg).unwrap();
        let trades = engine.series("trades").unwrap();
        engine
            .append(
                &trades,
                &Row {
                    ts: T13 + HOUR_US,
                    id: "e1-0",
                    keys: &[Value::Str("MSFT"), Value::I64(50)],
                    extra: &[],
                    data: "",
                },
            )
            .unwrap();
        engine.flush().unwrap();
    }

    let view_sql = std::fs::read_to_string(dir.path().join("series/trades/view.sql")).unwrap();

    // The column's type, straight from DuckDB rather than inferred.
    let ty = query(
        dir.path(),
        &format!("{view_sql}\nselect column_type from (describe select size from trades) limit 1;"),
    );
    assert_eq!(ty, "BIGINT", "a promoted i64 key must not become text");

    // Numeric comparison, not lexical -- and the unparseable value reads as
    // NULL instead of taking the query down with it.
    let big = query(
        dir.path(),
        &format!("{view_sql}\nselect size from trades where size > 9 order by size;"),
    );
    assert_eq!(big, "50\n100", "numeric comparison, not lexical: got {big}");

    let counts = query(
        dir.path(),
        &format!("{view_sql}\nselect count(*), count(size) from trades;"),
    );
    assert_eq!(counts, "4,3", "the junk value nulls its own row only");
}

#[test]
fn rows_are_sorted_by_ts_within_every_file() {
    if !duckdb_available() {
        eprintln!("skipping: duckdb not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    build_two_epoch_dataset(dir.path());

    // Per-file sorting is a promise the plan makes to readers; check it from
    // the reader's side rather than trusting the writer.
    let out = query(
        dir.path(),
        "select count(*) from (
           select ts, lag(ts) over (partition by filename order by ts) prev
           from read_parquet('series/trades/**/*.parquet',
                             hive_partitioning = 1, union_by_name = 1, filename = true)
         ) where prev is not null and ts < prev",
    );
    assert_eq!(out, "0", "ts must be non-decreasing within each file");
}
