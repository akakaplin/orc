//! The record model shared by the ingest API, the codec and the flush path.
//!
//! This module is the contract every other module codes against. Nothing here
//! allocates: a [`Row`] borrows its strings from the caller, and a decoded
//! [`RecordRef`] borrows from the buffer it was decoded out of.

/// A typed value for a declared key column.
///
/// Deliberately small and `Copy`: values are passed by value on the ingest path
/// and never own their storage.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(&'a str),
}

/// The declared type of a key column, as written in `config.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Bool,
    I64,
    F64,
    Str,
}

/// Wire type tags. These bytes are **on disk** — changing one is a format break
/// and requires bumping `FORMAT_VERSION`.
pub mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const I64: u8 = 2;
    pub const F64: u8 = 3;
    pub const STR: u8 = 4;
}

impl Value<'_> {
    /// The wire tag for this value.
    pub fn tag(&self) -> u8 {
        match self {
            Value::Null => tag::NULL,
            Value::Bool(_) => tag::BOOL,
            Value::I64(_) => tag::I64,
            Value::F64(_) => tag::F64,
            Value::Str(_) => tag::STR,
        }
    }

    /// The declared type this value satisfies, or `None` for [`Value::Null`],
    /// which satisfies any nullable column.
    pub fn key_type(&self) -> Option<KeyType> {
        match self {
            Value::Null => None,
            Value::Bool(_) => Some(KeyType::Bool),
            Value::I64(_) => Some(KeyType::I64),
            Value::F64(_) => Some(KeyType::F64),
            Value::Str(_) => Some(KeyType::Str),
        }
    }
}

/// A record as supplied by a caller of `append`.
///
/// `keys` is **positional**, in the order declared by the series' config. That
/// is what keeps a name lookup off the hot path; `append_named` exists for code
/// that would rather be readable than fast.
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    /// Epoch **microseconds**, UTC. Validated against the accept window.
    pub ts: i64,
    /// Dedup key together with `ts`. May be empty.
    pub id: &'a str,
    /// Values for the declared key columns, in declaration order.
    pub keys: &'a [Value<'a>],
    /// Undeclared key/value pairs. Sorted by name at encode time.
    pub extra: &'a [(&'a str, &'a str)],
    /// Opaque payload, stored verbatim and never parsed by default.
    pub data: &'a str,
}

/// The fixed part of a decoded frame.
///
/// `keys` and `extra` are written into caller-supplied scratch vectors rather
/// than returned here, so decoding a stream of frames allocates only until those
/// vectors reach their high-water mark.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecordRef<'a> {
    pub ts: i64,
    /// The series' schema epoch this frame was encoded under.
    pub epoch: u32,
    pub series: &'a str,
    pub id: &'a str,
    pub data: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_stable() {
        // These are on-disk constants. If this test needs updating, the format
        // version needs bumping too.
        assert_eq!(Value::Null.tag(), 0);
        assert_eq!(Value::Bool(true).tag(), 1);
        assert_eq!(Value::I64(0).tag(), 2);
        assert_eq!(Value::F64(0.0).tag(), 3);
        assert_eq!(Value::Str("").tag(), 4);
    }

    #[test]
    fn null_satisfies_no_specific_type() {
        assert_eq!(Value::Null.key_type(), None);
        assert_eq!(Value::I64(1).key_type(), Some(KeyType::I64));
    }
}
