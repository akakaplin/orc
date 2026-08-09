//! `orc` — an embeddable time-series storage engine.
//!
//! Records are appended to a write-ahead log at sub-microsecond latency and
//! periodically flushed to sorted Parquet that DuckDB, polars and Spark can read
//! directly. The engine is write-only: it owns the write path and leaves reading
//! to those tools.
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
pub mod protocol;
pub mod time;
pub mod wal;

// The ZeroMQ transport is optional so the embeddable engine -- the common case
// -- never pulls in a C++ toolchain. Both sides live behind the one feature, so
// a process can be a server, a client, or both.
#[cfg(feature = "net")]
pub mod client;
#[cfg(feature = "net")]
pub mod server;

/// The ZeroMQ binding this crate was built against.
///
/// Re-exported so callers can reach raw sockets -- and so the integration tests
/// can, without a dev-dependency. Cargo cannot feature-gate dev-dependencies, so
/// a `zmq` entry under `[dev-dependencies]` would make plain `cargo test` build
/// libzmq from source on a machine that asked for the embeddable engine.
///
/// It also pins the version: a consumer mixing its own `zmq` with ours would
/// link two libzmq builds. Going through this re-export makes that impossible.
#[cfg(feature = "net")]
pub use zmq;

pub use engine::{Engine, RawIngest, Stats};
pub use error::{Error, Result};
pub use record::{KeyType, RecordRef, Row, Value};
