//! `orc-client` — ZeroMQ PUSH client for an `orc-server`.
//!
//! Encodes records with the same codec the WAL uses, so the server appends the
//! bytes it receives without transcoding.
//!
//! ```ignore
//! let mut client = Client::builder()
//!     .ingest("tcp://host:5555")   // PUSH -> server PULL
//!     .control("tcp://host:5556")  // REQ  -> one schema handshake at connect
//!     .batch(1024)
//!     .batch_linger_ms(50)
//!     .on_full(OnFull::Block)
//!     .connect()?;
//! ```
//!
//! Ingest is **fire-and-forget**: there is no acknowledgement, so a send that
//! succeeds is not proof of durability. Backpressure still exists —
//! `OnFull::Block` is ZeroMQ's default behaviour at the send high-water mark, so
//! a saturated server slows producers instead of dropping silently. `OnFull::Drop`
//! opts out and counts what it discards.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
