# orc

An embeddable time-series storage engine in Rust. Records go into a write-ahead log at
sub-microsecond latency and are periodically flushed to sorted Parquet that DuckDB,
polars and Spark read directly.

`orc` is **write-only**: it owns the write path and leaves reading to those tools. That
is the trade it makes — no query engine to build or maintain, and your data is in an
open format from the moment it lands.

> **Status: v0.1.** Works end to end — append, durable WAL, crash recovery, hourly
> flush to sorted Parquet, and a ZeroMQ server and client. See [Roadmap](#roadmap).

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

## Three ways to use it

One crate, one dependency. ZeroMQ lives behind the optional `net` feature, so the
embedded case never compiles a C++ toolchain.

```toml
orc = "0.1"                                   # embedded engine only — 46 crates
orc = { version = "0.1", features = ["net"] } # + client and server — 77
orc = { version = "0.1", features = ["cli"] } # + the two binaries — 95
```

`cli` is separate from `net` on purpose: argument parsing and log formatting are
28 crates that a program embedding the client has no use for.

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

Nothing stops one process being both: embed the engine, and bind a server to it so
other processes can write to the same data directory through you.

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
  "flush":  { "interval_ms": 3600000, "compression": "lz4_raw" },
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

Deliberately minimal: Parquet, ZeroMQ and JSON are the sanctioned surface, plus two
small approved conveniences. Everything else is hand-rolled.

| Crate | Where | Why |
| --- | --- | --- |
| `parquet` | default | Output format. **Without the `arrow` feature** — see below. Codec is `lz4` (pure Rust, 1 crate), not `zstd` (8 crates and a C compiler). |
| `serde`, `serde_json` | default | Config, manifest and control protocol — never the ingest path. |
| `crc32fast`, `tracing` | default | Approved conveniences. |
| `zmq` | **`net`** | Transport. Always builds libzmq and libsodium from C source — `zmq-sys` has no system-library path — so this is the one that needs cmake and a C++ compiler. |
| `clap`, `tracing-subscriber` | **`cli`** | The two binaries only, and `tracing-subscriber` without `env-filter`: per-target filtering costs five crates and one binary has no targets to filter between. Set `RUST_LOG` to a bare level. |

Hand-rolled instead of pulled in: the record codec (no `rmp-serde`), the error types
(no `thiserror`), the civil-date helper (no `chrono` as a direct dep), the lock file
(no `fs4`), the benchmark harness (no `criterion`).

**LZ4_RAW, not zstd.** Measured on 500k rows of realistic output — a monotonic `ts`,
a 50-symbol column, incrementing ids, a near-constant `extra` map and a JSON `data`
blob:

| Codec | Adds | Toolchain | Size | Write | Read |
| --- | --- | --- | --- | --- | --- |
| **LZ4_RAW** | **+1** `lz4_flex` | **pure Rust** | 17.1 MB | 0.16s | **12 ms** |
| zstd | +8 incl. `cc` | **C compiler** | 11.0 MB | 0.17s | 15 ms |
| snappy | +1 | pure Rust | 18.1 MB | 0.17s | 15 ms |
| brotli | +4 | pure Rust | 11.3 MB | 0.26s | 27 ms |
| gzip | +4 | pure Rust | 11.3 MB | 0.91s | 21 ms |

zstd compresses 55% better, so the trade is real: **files are larger in exchange for a
pure-Rust build with no C compiler anywhere in the default tree.** LZ4_RAW reads
fastest of the six, which matters for a write-only engine whose whole output exists to
be queried. It is codec 7 (Parquet 2.9) — never the deprecated codec 5 `LZ4`, whose
non-standard Hadoop framing is why it was deprecated. Verified readable by DuckDB
1.5.5 and pyarrow 21.

Files written by an earlier version with zstd stay readable: orc never reads Parquet
back, so codec choice affects new files only.

**No Arrow.** `parquet`'s `arrow` feature is all-or-nothing: it costs 12 crates — the
six `arrow-*`, plus `arrow-ipc`'s `flatbuffers` and `bitflags`, plus `base64`,
`num-complex`, `rustc_version` and `semver` — and 5s of clean build, of which
`arrow-ipc` is pure toll since this engine never touches IPC. All `ArrowWriter`
actually does for a schema of four scalar types and one map is compute definition and
repetition levels, so [`src/flush/parquet.rs`](src/flush/parquet.rs) computes them
directly. Before the switch, both writers produced the same three fixtures and 2244
lines of dumped schema, row-group boundaries, statistics, metadata, data and per-row
map cardinality compared **identical**; `tests/parquet_reference.rs` pins that
contract. Files are also ~700 bytes smaller each, since `ArrowWriter` embedded an
`ARROW:schema` blob in every one.

`scripts/check-deps.sh` asserts the budget in both directions — that the default build
has no transport and no CLI, that `net` adds the transport *without* the CLI, and that
`cli` supplies what the binaries need. It also checks the `--edges all` graph, because
Cargo cannot feature-gate a dev-dependency: a `zmq` entry under `[dev-dependencies]`
would make plain `cargo test` build libzmq from source, and a `no-dev` check cannot
see it. The network tests reach ZeroMQ through the `orc::zmq` re-export instead.

## Roadmap

- [x] **M0** — workspace, pinned dependencies
- [x] **M1** — config, manifest, per-series schema history
- [x] **M2** — frame codec + WAL writer, committer thread
- [x] **M3** — recovery: truncate-and-amend, heartbeated lock
- [x] **M4** — flush: sort, dedup, Parquet, hour partitions
- [ ] **M5** — external merge for flushes larger than memory
- [x] **M6** — server: PULL ingest, control socket
- [x] **M7** — client: PUSH, batching, schema handshake

## Development

```sh
cargo build                                    # engine only, no C++ toolchain
cargo test                                     # 136 tests, still no libzmq
cargo build --features cli                     # + ZeroMQ and binaries (builds libzmq)
cargo test --features cli                      # 141 tests
cargo clippy --all-targets --features cli -- -D warnings
cargo fmt --all --check
./scripts/check-deps.sh                        # dependency budget
```

Reader-compatibility tests shell out to a real `duckdb` binary and skip when it is
absent. Set `ORC_REQUIRE_DUCKDB=1` to make a missing binary a failure instead.

Toolchain: Rust 1.91, edition 2024. The first build compiles libzmq from source, so it
takes noticeably longer than later ones.
