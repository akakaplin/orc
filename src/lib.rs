//! `orc` — an embeddable time-series storage engine.
//!
//! Records go to a write-ahead log and are periodically flushed to sorted
//! Parquet for DuckDB/polars/Spark to read. The engine is write-only.
//!
//! Invariants: every WAL frame names its own series and schema epoch, and no
//! Parquet file is ever read back, merged or replaced.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod engine;
pub mod error;
pub mod record;

pub mod codec;
pub mod config;
pub mod flush;
pub mod manifest;
pub mod protocol;
pub mod time;
pub mod wal;

// Optional so the embedded engine needs no C++ toolchain.
#[cfg(feature = "net")]
pub mod client;
#[cfg(feature = "net")]
pub mod server;

/// Re-exported ZeroMQ binding, so callers and tests use the same libzmq build
/// (a `zmq` dev-dependency would make plain `cargo test` build libzmq).
#[cfg(feature = "net")]
pub use zmq;

pub use engine::{Engine, RawIngest, Stats};
pub use error::{Error, Result};
pub use record::{KeyType, RecordRef, Row, Value};
