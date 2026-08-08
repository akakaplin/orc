# orc

An embeddable time-series storage engine in Rust. Records go into a write-ahead log at
sub-microsecond latency and are periodically flushed to sorted Parquet that DuckDB,
polars and Spark read directly.

`orc` is **write-only**: it owns the write path and leaves reading to those tools. That
is the trade it makes — no query engine to build or maintain, and your data is in an
open format from the moment it lands.

> **Status: v0.1.** The embeddable engine works end to end — append, durable WAL,
> crash recovery, hourly flush to sorted Parquet. The ZeroMQ server and client are
> not built yet; see [Roadmap](#roadmap).

## Design in one screen

```
 append()                    committer thread              flush (hourly)
    |                              |                            |
    v                              v                            v
 [ encode frame ] --> [ WAL segment ] --fsync every 10ms--> [ sort + dedup ]
   ~200ns, no I/O       append-only                              |
                        64 MiB, rolls                            v
                                                    series/<name>/hour=.../*.parquet
```

- **Ingest never blocks on disk.** `append()` encodes into a thread-local buffer and
  copies it under a short mutex. A background committer thread owns `fsync`, so a
  process crash loses nothing already appended and a power cut loses at most one
  ~10 ms window.
- **Every frame is self-describing.** It carries its own series name and its own schema
  epoch, so a WAL segment is interpretable with nothing but itself — no registry, no
  manifest. A lost `manifest.json` costs schema history, never the ability to tell what
  a record is.
- **The write path is append-only.** No Parquet file is ever read back, merged or
  replaced. Files within an hour may overlap in time; each is internally sorted.

## Record shape

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ts` | `i64` epoch **microseconds**, UTC | yes | The sort key. |
| `id` | UTF-8 text | no | `(ts, id)` is the dedup key. May be empty. |
| `series` | UTF-8 name | yes | Must exist in `config.json`. |
| declared keys | typed, positional | per config | Real Parquet columns. |
| `extra` | `map<utf8,utf8>` | no | Undeclared keys land here, no schema change. |
| `data` | opaque UTF-8 | no | Stored verbatim, never parsed. |

### Where does a field go?

Three mechanisms can hold a field, which is one more than is obvious:

| Put it in | When | Cost of getting it wrong |
| --- | --- | --- |
| a **declared key** | you filter, group or join on it | anything else means full scans |
| **`extra`** | it varies per record, or you don't control the producer | string-typed, no pushdown |
| **`data`** | payload you read *after* selecting rows | invisible to the query planner |

The failure mode is putting a filter column inside `data`: Parquet cannot prune on it,
so every query reads and JSON-parses every row.

### Timestamps must be microseconds

Not nanoseconds, not milliseconds. Every `append` checks
`ts_min <= ts <= now + ts_max_skew`, which doubles as a **unit check** — each common
mistake lands far outside the window and is rejected immediately instead of quietly
writing records into the year 58,000:

| You send | Interpreted as | Verdict |
| --- | --- | --- |
| seconds | 1970-01-01 | rejected |
| milliseconds | 1970-01-21 | rejected |
| **microseconds** | today | **accepted** |
| nanoseconds | year ~58,600 | rejected |

## Layout on disk

```
data/
  config.json                       # series definitions + tunables
  manifest.json                     # schema history, last flushed segment
  wal/0000000042.wal                # active, append-only
  series/trades/
    hour=2026-08-08T13/0000000041-0000000042.e7.parquet
    view.sql                        # generated; unions epochs, coalesces promotions
  rejects/                          # decoded but invalid records, capped
```

Reading it:

```sh
duckdb -c "select count(*), min(ts), max(ts)
           from read_parquet('data/series/trades/**/*.parquet', hive_partitioning=1)"

# or use the generated view, which handles schema changes over time:
cd data && duckdb -c ".read series/trades/view.sql"
```

**Nothing is ever deleted.** There is no retention or compaction: an hourly flush leaves
~8,760 files per series per year. Files are immutable and the manifest holds no file
inventory, so external pruning is safe at any time, including mid-flush:

```sh
find data/series -name 'hour=*' -type d -mtime +90 -exec rm -rf {} +
```

## Configuration

```json
{
  "server": { "ingest_endpoint": "tcp://127.0.0.1:5555" },
  "wal":    { "fsync_interval_ms": 10, "segment_max_bytes": 67108864 },
  "flush":  { "interval_ms": 3600000, "compression": "zstd" },
  "series": [
    { "name": "trades",
      "keys": [ {"name": "symbol", "type": "string"},
                {"name": "venue",  "type": "string", "nullable": true} ] }
  ]
}
```

Every field has a default, so a minimal config is just `series`. Durations are integer
milliseconds.

**Endpoints default to loopback deliberately.** There is no authentication on the ingest
socket — anything that can reach it can write records and consume disk. Before binding a
real interface, put it on a private network or enable libzmq's built-in CURVE
encryption and authentication.

## Two things to know before using it

**Recent data is invisible until it flushes.** A record is durable the instant it is
appended, but no reader sees it for up to `interval_ms`. If you need fresher data,
lower the interval and accept more files.

**Remote ingest is fire-and-forget.** PUSH/PULL has no acknowledgement, so a successful
send is not proof of durability. Backpressure still works — the client blocks at the
high-water mark by default rather than dropping silently.

## Dependencies

Deliberately minimal: Parquet, ZeroMQ and JSON are the sanctioned surface, plus four
small approved conveniences. Everything else is hand-rolled.

| Crate | Where | Why |
| --- | --- | --- |
| `parquet`, `arrow-array`, `arrow-schema` | core | Output format. Sub-crates, not the `arrow` umbrella. |
| `zmq` | server, client | Transport. Builds libzmq from source; needs cmake and a C++ compiler. |
| `serde`, `serde_json` | core | Config and manifest only — never the ingest path. |
| `crc32fast`, `thiserror`, `tracing`, `clap` | various | Approved conveniences. |

Hand-rolled instead of pulled in: the record codec (no `rmp-serde`), the civil-date
helper (no `chrono` as a direct dep), the lock file (no `fs4`), the benchmark harness
(no `criterion`). `orc-core` compiles with no ZeroMQ and no CLI dependency at all —
`scripts/check-deps.sh` asserts it.

## Roadmap

- [x] **M0** — workspace, pinned dependencies
- [x] **M1** — config, manifest, per-series schema history
- [x] **M2** — frame codec + WAL writer, committer thread
- [x] **M3** — recovery: truncate-and-amend, heartbeated lock
- [x] **M4** — flush: sort, dedup, Parquet, hour partitions
- [ ] **M5** — external merge for flushes larger than memory
- [ ] **M6** — server: PULL ingest, control socket
- [ ] **M7** — client: PUSH, batching, schema handshake

## Development

```sh
cargo build
cargo test                                  # 119 tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
./scripts/check-deps.sh                     # orc-core dependency budget
```

Reader-compatibility tests shell out to a real `duckdb` binary and skip when it is
absent. Set `ORC_REQUIRE_DUCKDB=1` to make a missing binary a failure instead.

Toolchain: Rust 1.91, edition 2024. The first build compiles libzmq from source, so it
takes noticeably longer than later ones.
