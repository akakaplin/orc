//! Error types for the engine's public surface.

use crate::record::KeyType;

/// Format version written into every WAL segment header and ZeroMQ batch header.
///
/// Bump this if the frame layout or any wire tag changes.
pub const FORMAT_VERSION: u16 = 1;

/// Magic bytes identifying an orc WAL segment or batch: `"orc\0"` little-endian.
pub const MAGIC: u32 = 0x0063_726f;

/// The engine's error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown series {0:?}; it must be declared in config.json")]
    UnknownSeries(String),

    #[error("series {series:?} expects {expected} key(s), got {got}")]
    KeyArity {
        series: String,
        expected: usize,
        got: usize,
    },

    #[error("series {series:?} key {key:?} expects {expected:?}, got {got:?}")]
    TypeMismatch {
        series: String,
        key: String,
        expected: KeyType,
        got: Option<KeyType>,
    },

    #[error(
        "ts {ts} outside accept window [{min}, {max}] \
         (timestamps are epoch MICROSECONDS, UTC -- a value this far off usually means wrong units)"
    )]
    TsOutOfRange { ts: i64, min: i64, max: i64 },

    #[error("record is {size} bytes, over the {limit}-byte limit")]
    RecordTooLarge { size: usize, limit: usize },

    #[error(
        "frame declares schema epoch {epoch} for series {series:?}, which is not in the manifest history"
    )]
    UnknownEpoch { series: String, epoch: u32 },

    #[error("config: {0}")]
    Config(String),

    #[error(
        "data directory {0} is locked by a live process; \
         pass --force-unlock only if you are certain it is stuck"
    )]
    Locked(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
