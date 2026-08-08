//! The control-socket protocol, shared by the server and the client.
//!
//! Ingest is a binary frame stream (see [`crate::codec`]) because it runs per
//! record. Control is JSON over request/reply because it runs per *connection* —
//! a handshake, an occasional flush, a stats scrape. Nothing here is on a hot
//! path, so legibility wins: an operator can drive the whole surface with
//! `nc`, and a stuck server can be diagnosed without a decoder.
//!
//! Lives in `orc-core` rather than in either networked crate so the two cannot
//! drift apart. It brings no ZeroMQ dependency with it — these are plain serde
//! types, and the sockets that carry them are somebody else's problem.

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
    /// Every series' current epoch and key list.
    ///
    /// This is the connect handshake: a client encodes keys positionally, so it
    /// cannot build a single frame until it knows the order and the epoch to
    /// stamp. One round trip at startup, none afterwards.
    Schema,
    /// Re-read `config.json` and adopt it.
    ReloadConfig,
    /// Drain and exit.
    ///
    /// The engine is crash-safe by construction, so this is a convenience
    /// rather than a safety mechanism — but it is the difference between
    /// stopping at a known-clean point and stopping wherever the process
    /// happened to be.
    Shutdown,
}

/// A reply on the control socket.
///
/// `Error` is a variant rather than a transport-level failure because REQ/REP
/// has no other channel: a socket error would leave the client unable to tell a
/// rejected request from a dead server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum ControlResponse {
    Pong { version: String },
    Stats(Box<StatsPayload>),
    Flushed(FlushPayload),
    Schema { series: Vec<SeriesSchema> },
    Reloaded { changed: Vec<String> },
    ShuttingDown,
    Error { message: String },
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsPayload {
    pub appended: u64,
    pub rejected_ts: u64,
    pub rejected_size: u64,
    pub rejected_series: u64,
    pub rejected_frames: u64,
    pub flush_failures: u64,
    pub rows_flushed: u64,
    pub rows_deduplicated: u64,
    pub wal_bytes: u64,
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
    /// Encode for the wire. Infallible in practice — these types always
    /// serialise — but the error is surfaced rather than unwrapped so a server
    /// never panics on a malformed reply it built itself.
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
        };
        let bytes = reply.to_bytes().unwrap();
        assert_eq!(ControlResponse::from_bytes(&bytes).unwrap(), reply);
    }

    #[test]
    fn an_unknown_op_is_an_error_not_a_panic() {
        assert!(ControlRequest::from_bytes(br#"{"op":"drop-everything"}"#).is_err());
        assert!(ControlRequest::from_bytes(b"not json at all").is_err());
    }
}
