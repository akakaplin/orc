//! Control-socket protocol: JSON request/reply, shared by server and client.
//! Ingest is binary (see [`crate::codec`]); control is rare, so it stays readable.
//! Plain serde types, compiled without the `net` feature.

use serde::{Deserialize, Serialize};

use crate::config::KeyDef;
use crate::engine::Stats;

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
    /// Every series' epoch and key order — the connect handshake, needed to encode frames.
    Schema,
    /// Re-read `config.json` and adopt it.
    ReloadConfig,
    /// Drain and exit at a clean point (the engine is crash-safe regardless).
    Shutdown,
}

/// A reply on the control socket. `Error` is a reply, so a rejected request is
/// distinguishable from a dead server.
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
        /// The enforced `server.max_batch_bytes`; libzmq drops oversized
        /// messages silently. `None` from older servers.
        #[serde(default)]
        max_batch_bytes: Option<u64>,
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

/// [`Stats`] flattened, plus the socket-side batch counter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct StatsPayload {
    #[serde(flatten)]
    pub engine: Stats,
    /// Batches received on the ingest socket; compare with client sends to see loss.
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
    /// Encode for the wire.
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

    /// A newer client must still parse an older server's replies.
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
                assert_eq!(s.engine.appended, 5);
                assert_eq!(
                    s.engine.wal_total_bytes, 0,
                    "absent reads as zero, not an error"
                );
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
