//! `orc` — an embeddable time-series storage engine.
//!
//! Records are appended to a write-ahead log at sub-microsecond latency and
//! periodically flushed to sorted Parquet that DuckDB, polars and Spark read
//! directly. The engine is write-only: it owns the write path and leaves reading
//! to those tools.
//!
//! Two invariants carry most of the design:
//!
//! - **Every frame is self-describing.** It names its own series and schema
//!   epoch, so a WAL segment is interpretable with nothing but itself.
//! - **The write path is append-only.** No Parquet file is ever read back,
//!   merged or replaced, which removes a whole category of recovery cases.

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

// Optional so the embeddable engine never pulls in a C++ toolchain. One feature
// for both sides, so a process can be a server, a client, or both.
#[cfg(feature = "net")]
pub mod client;
#[cfg(feature = "net")]
pub mod server;

/// The ZeroMQ binding this crate was built against.
///
/// Re-exported so callers — and the integration tests — can reach raw sockets.
/// Cargo cannot feature-gate dev-dependencies, so a `zmq` entry there would make
/// plain `cargo test` build libzmq from source. It also pins the version: a
/// consumer mixing its own `zmq` with ours would link two libzmq builds.
#[cfg(feature = "net")]
pub use zmq;

pub use engine::{Engine, RawIngest, Stats};
pub use error::{Error, Result};
pub use record::{KeyType, RecordRef, Row, Value};
