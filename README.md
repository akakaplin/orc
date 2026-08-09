# orc

An embeddable time-series storage engine in Rust. Records go into a write-ahead log at sub-microsecond latency and are periodically flushed to sorted Parquet that DuckDB, polars and Spark read directly.

`orc` is **write-only**: it owns the write path and leaves reading to those tools. That is the trade it makes — no query engine to build or maintain, and your data is in an open format from the moment it lands.

> **Status: 0.1.0, unreleased.** Works end to end — append, durable WAL, crash recovery, hourly flush to sorted Parquet, and a ZeroMQ server and client. Nothing is published to crates.io yet.

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

- **Ingest never blocks on disk.** `append()` encodes into a thread-local buffer and copies it under a short mutex. A background committer thread owns `fsync`, so a process crash loses nothing already appended and a power cut loses at most one ~10 ms window.
- **Every frame is self-describing.** It carries its own series name and its own schema epoch, so a WAL segment is interpretable with nothing but itself — no registry, no manifest. A lost `manifest.json` costs schema history, never the ability to tell what a record is.
- **The write path is append-only.** No Parquet file is ever read back, merged or replaced. Files within an hour may overlap in time; each is internally sorted.

## Three ways to use it

```toml
orc = "0.1"                                   # embedded engine
orc = { version = "0.1", features = ["net"] } # + client and server
orc = { version = "0.1", features = ["cli"] } # + the orc-server and orc-cli binaries
```

The default build is pure Rust and needs no C toolchain. ZeroMQ lives behind `net`, and the binaries' argument parsing and log formatting behind `cli`, so embedding the client costs neither.

**Embedded** — the engine in your process:

```rust
let engine = Engine::open(Config::load("./data/config.json")?)?;
let trades = engine.series("trades")?;
engine.append(&trades, &Row { ts, id, keys, extra, data })?;
```

**Server** — the same engine behind a socket:

```rust
let engine = Arc::new(Engine::open(config.clone())?);
let server = Server::bind(engine, &config)?;
server.run()?;                               // or: orc-server --data ./data
```

**Client** — send to a remote server:

```rust
let mut client = Client::builder().ingest("tcp://host:5555").connect()?;
let trades = client.series("trades")?;       // schema handshake, once
client.send(&trades, &row)?;
```

Nothing stops one process being both: embed the engine, and bind a server to it so other processes can write to the same data directory through you.

## Record shape

| Field         | Type                              | Required   | Notes                                        |
| ------------- | --------------------------------- | ---------- | -------------------------------------------- |
| `ts`          | `u64` epoch **microseconds**, UTC | yes        | The sort key. Unsigned — nothing before 1970. |
| `id`          | UTF-8 text                        | no         | `(ts, id)` is the dedup key. May be empty.   |
| `series`      | UTF-8 name                        | yes        | Must exist in `config.json`.                 |
| declared keys | typed, positional                 | per config | Real Parquet columns.                        |
| `extra`       | `map<utf8,utf8>`                  | no         | Undeclared keys land here, no schema change. |
| `data`        | opaque UTF-8                      | no         | Stored verbatim, never parsed.               |

### Where does a field go?

Three mechanisms can hold a field, which is one more than is obvious:

| Put it in          | When                                                    | Cost of getting it wrong       |
| ------------------ | ------------------------------------------------------- | ------------------------------ |
| a **declared key** | you filter, group or join on it                         | anything else means full scans |
| **`extra`**        | it varies per record, or you don't control the producer | string-typed, no pushdown      |
| **`data`**         | payload you read *after* selecting rows                 | invisible to the query planner |

The failure mode is putting a filter column inside `data`: Parquet cannot prune on it, so every query reads and JSON-parses every row.

### Timestamps must be microseconds

Not nanoseconds, not milliseconds. Every `append` checks `ts_min <= ts <= now + ts_max_skew`, which doubles as a **unit check** — each common mistake lands far outside the window and is rejected immediately instead of quietly writing records into the year 58,000:

| You send         | Interpreted as | Verdict      |
| ---------------- | -------------- | ------------ |
| seconds          | 1970-01-01     | rejected     |
| milliseconds     | 1970-01-21     | rejected     |
| **microseconds** | today          | **accepted** |
| nanoseconds      | year ~58,600   | rejected     |

### Schema changes

Adding, removing or reordering a series' declared keys is allowed and takes effect without downtime: each change bumps that series' schema epoch, every frame carries the epoch it was written under, and each Parquet file holds exactly one epoch. **Retyping an existing key is rejected at startup** — it would make old and new files unreadable in one query. Rename the key or start a new series instead.

Removing a declared key does not lose data: values keep arriving in `extra`. Promoting an `extra` key to a declared column is what the generated `view.sql` coalesces across, so queries see one column spanning both eras.

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

**Nothing is ever deleted.** There is no retention or compaction: an hourly flush leaves ~8,760 files per series per year. Files are immutable and the manifest holds no file inventory, so external pruning is safe at any time, including mid-flush:

```sh
find data/series -name 'hour=*' -type d -mtime +90 -exec rm -rf {} +
```

## Configuration

```json
{
  "server": { "ingest_endpoint": "tcp://127.0.0.1:5555", "max_batch_bytes": 8388608 },
  "wal":    { "fsync_interval_ms": 10, "segment_max_bytes": 67108864 },
  "flush":  { "interval_ms": 3600000, "compression": "lz4_raw" },
  "series": [
    { "name": "trades",
      "keys": [ {"name": "symbol", "type": "string"},
                {"name": "venue",  "type": "string", "nullable": true} ] }
  ]
}
```

Every field has a default, so a minimal config is just `series`. Durations are integer milliseconds. `compression` accepts `lz4_raw` (default) or `uncompressed`; both are validated at load, so an unwritable codec fails immediately rather than at the first flush an hour later.

A few constraints are worth knowing before you hit them, all checked at load:

- **`flush.interval_ms: 0` disables the timer**, leaving flushing entirely to explicit `Engine::flush()` calls and the control socket. Any other value spawns a background thread that flushes on that period. The WAL is not capped, so turning the timer off means owning the schedule yourself.
- **A declared key may not be named `ts`, `id`, `extra` or `data`.** Every Parquet file already has those columns, and Parquet does not object to a duplicate field name — the resulting file reads back wrong rather than failing.
- **`limits.ts_min` must be 1970 or later.** Timestamps are unsigned, so there is nothing before the epoch to represent.
- **`server.max_batch_bytes` caps one ingest message**, and must be at least `limits.max_record_bytes`. libzmq discards an oversized message below the application, where neither side can log it, so the client learns this value in the handshake and splits its batches to fit — and refuses, loudly, a record too large to ever be delivered.

**Endpoints default to loopback deliberately.** There is no authentication on the ingest socket — anything that can reach it can write records and consume disk. Before binding a real interface, put it on a private network or enable libzmq's built-in CURVE encryption and authentication.

`orc-server --data DIR` overrides the config file's `data_dir`; without the flag the file decides, so `--config /etc/orc/config.json` alone writes where that file says. `orc-cli --control` likewise defaults to the ingest port + 1 rather than to localhost, so pointing `--ingest` at a remote host takes the handshake with it.

## When something is wrong

The engine never deletes data it cannot read. Two directories are where that shows up:

- **`wal/<id>.wal.corrupt`** — a segment whose *header* would not parse, moved aside at startup or at flush. Its records are not in the dataset and will not be replayed, but every byte is intact. A header that is merely missing (a file too short to hold one) is deleted instead, because such a file can hold no locatable frames at all.
- **`rejects/<id>.rej`** — individual frames that decoded but did not match their schema, plus the unreadable remainder of any segment whose scan stopped early. Capped by `limits.reject_max_bytes`.

A **format-version mismatch refuses to start** rather than quarantining: the WAL is fine, the binary is wrong, and starting would strand records the right build reads perfectly.

Startup also sweeps two kinds of debris from a flush that wrote files and then died before committing — staged files in `tmp/`, and Parquet in `series/` whose segment range runs past the manifest's watermark. Both are rows the next flush writes again, so leaving them would double-count.

A **missing `manifest.json` refuses to start**, naming the directory. It is the only record of how far the flush got; without it every Parquet file in `series/` is indistinguishable from debris left by a flush that never committed, and there is nothing to recover the watermark from. Restore it from a backup, or move `series/` aside to start fresh.

`stats` carries the counters worth alerting on. `flush_failures` rising while `wal_total_bytes` grows is a stalled flush — the WAL is uncapped by design, so this costs disk rather than availability, which is exactly why it needs watching. Alert on `wal_total_bytes`, not `wal_bytes`: the latter is the active segment alone, and a stalled flush keeps rolling past it.

## Known limits

- **Recent data is invisible until it flushes.** A record is durable the instant it is appended, but no reader sees it for up to `interval_ms`. If you need fresher data, lower the interval and accept more files.
- **Remote ingest is fire-and-forget.** PUSH/PULL has no acknowledgement, so a successful send is not proof of durability. Backpressure still works — the client blocks at the high-water mark by default rather than dropping silently.
- **Deduplication covers one flush.** `(ts, id)` duplicates collapse within a flush window, not across them. Two id-less records sharing a timestamp are treated as the same event, so a producer emitting genuinely distinct events at identical microseconds must set `id`.
- **A flush must fit in memory.** Every row of the segments being flushed is held until the last file is written. At high ingest rates an hour does not fit; lower `interval_ms` until external merge lands.
- **No retention and no compaction.** Files accumulate forever unless you prune them yourself — see [Layout on disk](#layout-on-disk).
- **No read path.** By design: point DuckDB, polars or Spark at the data directory.

## Dependencies

Parquet, ZeroMQ and JSON are the sanctioned surface. Everything else is hand-rolled: the record codec, the error types, the civil-date helper, the lock file, the benchmark harness.

| Crate                        | Where   | Why                                                                                             |
| ---------------------------- | ------- | ----------------------------------------------------------------------------------------------- |
| `parquet`                    | default | Output format. Written against the low-level API, so no Arrow.                                  |
| `serde`, `serde_json`        | default | Config, manifest and control protocol — never the ingest path.                                  |
| `crc32fast`, `tracing`       | default | Frame checksums and logging.                                                                    |
| `zmq`                        | `net`   | Transport. Builds libzmq and libsodium from source, so this one needs cmake and a C++ compiler. |
| `clap`, `tracing-subscriber` | `cli`   | The two binaries only.                                                                          |

Parquet output is LZ4_RAW-compressed. zstd would be ~35% smaller but costs eight crates and a C compiler, which would put a toolchain requirement on every embedder.

`scripts/check-deps.sh` enforces this: the default build must contain no transport, no CLI and nothing that compiles C, and `net` must add the transport without dragging the CLI along.

## Roadmap

Working: durable WAL with group-commit fsync, crash recovery with truncate-and-amend, per-series schema evolution, hourly flush to sorted deduplicated Parquet with hour partitioning, ZeroMQ server and client, generated DuckDB views.

Not yet:

- **External merge** — per-segment sorted runs and a bounded-fan-in k-way merge, so flush memory stays flat regardless of window size.
- **`reload-config` over the control socket** — the request exists and returns an explicit "not implemented" rather than pretending; restart to pick up config changes.
- **Retention** — safe to do externally today (see above), but nothing built in.

## Development

```sh
cargo build                                    # engine only, pure Rust, no C toolchain
cargo test
cargo build --features cli                     # + ZeroMQ and binaries (builds libzmq)
cargo test --features cli
cargo clippy --all-targets --features cli -- -D warnings
cargo fmt --all --check
./scripts/check-deps.sh                        # dependency budget
```

Reader-compatibility tests shell out to a real `duckdb` binary and skip when it is absent. Set `ORC_REQUIRE_DUCKDB=1` to make a missing binary a failure instead.

Toolchain: Rust 1.91, edition 2024. The first `--features net` build compiles libzmq from source, so it takes noticeably longer than later ones.
