//! The control-socket protocol, shared by the server and the client.
//!
//! Ingest is a binary frame stream (see [`crate::codec`]) because it runs per
//! record. Control is JSON over request/reply because it runs per *connection* —
//! a handshake, an occasional flush, a stats scrape. Nothing here is hot, so
//! legibility wins: an operator can drive the whole surface with `nc`.
//!
//! Compiled outside the `net` feature that gates the sockets: these are plain
//! serde types, so speaking the protocol without our client needs no ZeroMQ.

use serde::{Deserialize, Serialize};

use crate::config::KeyDef;

/// A request on the control socket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum ControlRequest {
    /// Liveness only.
    Ping,
    /// Counter snapshot.
    Stats,
    /// Flush now, synchronously. Returns once the manifest has committed.
    Flush,
    /// Every series' current epoch and key list — the connect handshake. A
    /// client encodes keys positionally, so it cannot build a frame until it
    /// knows the order and the epoch to stamp. One round trip at startup.
    Schema,
    /// Re-read `config.json` and adopt it.
    ReloadConfig,
    /// Drain and exit. The engine is crash-safe by construction, so this is a
    /// convenience — but it is the difference between stopping at a known-clean
    /// point and stopping wherever the process happened to be.
    Shutdown,
}

/// A reply on the control socket.
///
/// `Error` is a variant rather than a transport-level failure: REQ/REP has no
/// other channel, and a socket error would leave the client unable to tell a
/// rejected request from a dead server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum ControlResponse {
    Pong {
        version: String,
    },
    Stats(Box<StatsPayload>),
    Flushed(FlushPayload),
    Schema {
        series: Vec<SeriesSchema>,
        /// The server's `server.max_batch_bytes`, so a client can bound its
        /// batches by the limit that is actually enforced.
        ///
        /// libzmq drops an oversized message *below* the application: neither
        /// side can log it, so a client guessing this wrong loses records
        /// silently. `Option` so a client can still talk to a server that
        /// predates the field.
        #[serde(default)]
        max_batch_bytes: Option<u64>,
    },
    Reloaded {
        changed: Vec<String>,
    },
    ShuttingDown,
    Error {
        message: String,
    },
}

/// One series as the client needs to see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesSchema {
    pub name: String,
    /// The epoch a client must stamp into frames it encodes against `keys`.
    pub epoch: u32,
    /// Positional: index in this list is the order values appear in a frame.
    pub keys: Vec<KeyDef>,
}

/// Counters, mirroring `Engine::stats`.
///
/// `#[serde(default)]` so a counter added later reads as 0 from an older
/// server's reply rather than failing the parse.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StatsPayload {
    pub appended: u64,
    pub rejected_ts: u64,
    pub rejected_size: u64,
    pub rejected_series: u64,
    pub rejected_frames: u64,
    pub flush_failures: u64,
    pub rows_flushed: u64,
    pub rows_deduplicated: u64,
    /// Bytes in the *active* segment only: a sawtooth that resets on every roll.
    pub wal_bytes: u64,
    /// Every byte of WAL on disk. This is the one to alert on — the log is
    /// uncapped, so a stalled flush shows up here as steady growth long before
    /// the volume fills.
    pub wal_total_bytes: u64,
    pub segment: u64,
    /// Batches received on the ingest socket. Compared against a client's own
    /// send count, this is the only way to observe fire-and-forget loss.
    pub batches_received: u64,
}

/// The result of a flush.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushPayload {
    pub segments_consumed: Vec<u64>,
    pub rows_written: usize,
    pub rows_deduplicated: usize,
    pub frames_rejected: usize,
}

impl ControlRequest {
    /// Encode for the wire. Infallible in practice, but surfaced rather than
    /// unwrapped so a server never panics on a reply it built itself.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

impl ControlResponse {
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Convenience for turning any engine error into a reply.
    pub fn error(e: impl std::fmt::Display) -> Self {
        ControlResponse::Error {
            message: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::KeyType;

    #[test]
    fn requests_round_trip_and_are_readable() {
        for req in [
            ControlRequest::Ping,
            ControlRequest::Stats,
            ControlRequest::Flush,
            ControlRequest::Schema,
            ControlRequest::ReloadConfig,
            ControlRequest::Shutdown,
        ] {
            let bytes = req.to_bytes().unwrap();
            assert_eq!(ControlRequest::from_bytes(&bytes).unwrap(), req);
        }
        // Kebab-case on the wire, so `reload-config` is what an operator types.
        assert_eq!(
            String::from_utf8(ControlRequest::ReloadConfig.to_bytes().unwrap()).unwrap(),
            r#"{"op":"reload-config"}"#
        );
    }

    #[test]
    fn schema_reply_round_trips() {
        let reply = ControlResponse::Schema {
            series: vec![SeriesSchema {
                name: "trades".into(),
                epoch: 7,
                keys: vec![KeyDef {
                    name: "symbol".into(),
                    ty: KeyType::Str,
                    nullable: false,
                }],
            }],
            max_batch_bytes: Some(8_388_608),
        };
        let bytes = reply.to_bytes().unwrap();
        assert_eq!(ControlResponse::from_bytes(&bytes).unwrap(), reply);
    }

    /// Both fields were added after the first release, so upgrading the client
    /// before the server must not brick the handshake.
    #[test]
    fn a_reply_without_the_newer_fields_still_parses() {
        let schema = ControlResponse::from_bytes(br#"{"status":"schema","series":[]}"#).unwrap();
        assert_eq!(
            schema,
            ControlResponse::Schema {
                series: vec![],
                max_batch_bytes: None
            }
        );

        let stats = ControlResponse::from_bytes(br#"{"status":"stats","appended":5}"#).unwrap();
        match stats {
            ControlResponse::Stats(s) => {
                assert_eq!(s.appended, 5);
                assert_eq!(s.wal_total_bytes, 0, "absent reads as zero, not an error");
            }
            other => panic!("expected stats, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_op_is_an_error_not_a_panic() {
        assert!(ControlRequest::from_bytes(br#"{"op":"drop-everything"}"#).is_err());
        assert!(ControlRequest::from_bytes(b"not json at all").is_err());
    }
}
