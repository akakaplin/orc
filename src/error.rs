//! Engine error type. `Display`/`Error` are hand-written to avoid a proc-macro
//! dependency.

use crate::record::KeyType;

/// Format version in every WAL segment and batch header; bump on any layout change.
pub const FORMAT_VERSION: u16 = 1;

/// Magic bytes identifying an orc WAL segment or batch: `"orc\0"` little-endian.
pub const MAGIC: u32 = 0x0063_726f;

/// The engine's error type.
#[derive(Debug)]
pub enum Error {
    UnknownSeries(String),

    KeyArity {
        series: String,
        expected: usize,
        got: usize,
    },

    TypeMismatch {
        series: String,
        key: String,
        expected: KeyType,
        got: Option<KeyType>,
    },

    TsOutOfRange {
        ts: u64,
        min: u64,
        max: u64,
    },

    RecordTooLarge {
        size: usize,
        limit: usize,
    },

    /// Over `server.max_batch_bytes`: libzmq would silently drop it, so the
    /// sender refuses it instead.
    BatchTooLarge {
        size: usize,
        limit: usize,
    },

    Config(String),

    Locked(String),

    Io(std::io::Error),

    Json(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnknownSeries(name) => {
                write!(
                    f,
                    "unknown series {name:?}; it must be declared in config.json"
                )
            }
            Error::KeyArity {
                series,
                expected,
                got,
            } => write!(f, "series {series:?} expects {expected} key(s), got {got}"),
            Error::TypeMismatch {
                series,
                key,
                expected,
                got,
            } => write!(
                f,
                "series {series:?} key {key:?} expects {expected:?}, got {got:?}"
            ),
            Error::TsOutOfRange { ts, min, max } => write!(
                f,
                "ts {ts} outside accept window [{min}, {max}] \
                 (timestamps are epoch MICROSECONDS, UTC -- a value this far off usually means wrong units)"
            ),
            Error::RecordTooLarge { size, limit } => {
                write!(f, "record is {size} bytes, over the {limit}-byte limit")
            }
            Error::BatchTooLarge { size, limit } => write!(
                f,
                "batch is {size} bytes, over the server's {limit}-byte max_batch_bytes; \
                 sending it would have it silently discarded by zeromq"
            ),
            Error::Config(msg) => write!(f, "config: {msg}"),
            Error::Locked(dir) => write!(
                f,
                "data directory {dir} is locked by a live process; \
                 pass --force-unlock only if you are certain it is stuck"
            ),
            Error::Io(e) => e.fmt(f),
            Error::Json(e) => write!(f, "json: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_renders_and_reports_its_source() {
        let cases: Vec<(Error, &str, bool)> = vec![
            (Error::UnknownSeries("t".into()), "unknown series", false),
            (
                Error::KeyArity {
                    series: "t".into(),
                    expected: 2,
                    got: 1,
                },
                "expects 2 key(s), got 1",
                false,
            ),
            (
                Error::TypeMismatch {
                    series: "t".into(),
                    key: "sym".into(),
                    expected: KeyType::Str,
                    got: None,
                },
                "expects Str, got None",
                false,
            ),
            (
                Error::TsOutOfRange {
                    ts: 1,
                    min: 2,
                    max: 3,
                },
                "MICROSECONDS",
                false,
            ),
            (
                Error::RecordTooLarge {
                    size: 20,
                    limit: 16,
                },
                "record is 20 bytes, over the 16-byte limit",
                false,
            ),
            (
                Error::BatchTooLarge {
                    size: 100,
                    limit: 64,
                },
                "silently discarded",
                false,
            ),
            (Error::Config("bad".into()), "config: bad", false),
            (Error::Locked("./data".into()), "--force-unlock", false),
            (
                Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "nope")),
                "nope",
                true,
            ),
            (
                Error::Json(serde_json::from_str::<u8>("x").unwrap_err()),
                "json: ",
                true,
            ),
        ];

        for (err, needle, has_source) in cases {
            let rendered = err.to_string();
            assert!(
                rendered.contains(needle),
                "{rendered:?} should contain {needle:?}"
            );
            assert_eq!(
                std::error::Error::source(&err).is_some(),
                has_source,
                "source() for {err:?}"
            );
        }
    }

    #[test]
    fn the_question_mark_conversions_still_exist() {
        fn io() -> Result<()> {
            Err(std::io::Error::other("x"))?;
            Ok(())
        }
        fn json() -> Result<()> {
            serde_json::from_str::<u8>("x")?;
            Ok(())
        }
        assert!(matches!(io(), Err(Error::Io(_))));
        assert!(matches!(json(), Err(Error::Json(_))));
    }
}
