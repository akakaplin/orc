//! `orc-core` — an embeddable time-series storage engine.
//!
//! Records are appended to a write-ahead log at sub-microsecond latency and
//! periodically flushed to sorted Parquet that DuckDB, polars and Spark can read
//! directly. The engine is write-only: it owns the write path and leaves reading
//! to those tools.
//!
//! ```ignore
//! let engine = Engine::open(Config::load("./data/config.json")?)?;
//! let trades = engine.series("trades")?;
//! engine.append(trades, &Row {
//!     ts:    1_786_147_200_000_000, // epoch microseconds, UTC
//!     id:    "itch-8841203",        // (ts, id) is the dedup key; may be ""
//!     keys:  &[Value::Str("AAPL"), Value::Str("XNAS")],
//!     extra: &[("feed", "itch")],
//!     data:  r#"{"px":193.4}"#,
//! })?;
//! ```
//!
//! Two invariants carry most of the design:
//!
//! - **Every frame is self-describing.** It names its own series and its own
//!   schema epoch, so a WAL segment can be interpreted with nothing but itself —
//!   no registry, no manifest.
//! - **The write path is append-only.** No Parquet file is ever read back,
//!   merged or replaced, which removes a whole category of crash-recovery cases.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod engine;
pub mod error;
pub mod record;

// Filled in by M1/M2. Declared here so parallel work on each file never has to
// touch this one.
pub mod codec;
pub mod config;
pub mod flush;
pub mod manifest;
pub mod time;
pub mod wal;

pub use engine::{Engine, RawIngest, Stats};
pub use error::{Error, Result};
pub use record::{KeyType, RecordRef, Row, Value};
