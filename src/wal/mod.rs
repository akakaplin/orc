//! The write-ahead log: append path, durability, and crash recovery.
//!
//! ```text
//! data/wal/0000000041.wal    sealed, awaiting flush
//! data/wal/0000000042.wal    active, append-only
//! ```
//!
//! Each file is a [segment header](crate::codec::encode_segment_header) followed
//! by frames in arrival order. Only the highest-numbered segment is ever written
//! to; a segment becomes immutable the moment the writer rolls past it.
//!
//! The directory is deliberately **flat and id-ordered**. Recovery then scans
//! exactly one directory and finds the tail with a single `max`, and the flush
//! consumes segments in the order they were written with no index to keep in
//! step. Names are zero-padded to [`SEGMENT_ID_DIGITS`] so lexical order is
//! numeric order for anything that lists the directory — `ls`, a shell glob, or
//! a person reading a bug report.
//!
//! **Segment ids start at 1.** Zero is reserved to mean "nothing has been
//! flushed yet", which is what [`Manifest::last_flushed_segment`] defaults to on
//! a fresh directory. If a segment could be numbered 0, the first restart after
//! writing it would delete it as already-durable-in-Parquet — losing every
//! record in it. Ids then increase monotonically forever; they are never reused,
//! even after a segment is deleted.
//!
//! [`Manifest::last_flushed_segment`]: crate::manifest::Manifest::last_flushed_segment

pub mod reader;
pub mod recovery;
pub mod writer;

use std::path::{Path, PathBuf};

use crate::error::Result;

/// The WAL directory, relative to `data_dir`.
pub const WAL_DIR: &str = "wal";

/// Extension of a segment file.
pub const SEGMENT_EXT: &str = "wal";

/// Segment ids are zero-padded to this width, so lexical order is numeric order
/// up to 10^10 segments — 600 exabytes at the default segment size.
pub const SEGMENT_ID_DIGITS: usize = 10;

/// The first id a fresh data directory uses. See the module docs for why it is
/// not 0.
pub const FIRST_SEGMENT: u64 = 1;

/// `<data_dir>/wal`.
pub fn wal_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(WAL_DIR)
}

/// The file name for a segment id: ten digits, zero-padded, plus `.wal`.
pub fn segment_name(id: u64) -> String {
    format!("{id:0width$}.{SEGMENT_EXT}", width = SEGMENT_ID_DIGITS)
}

/// `<data_dir>/wal/<id>.wal`.
pub fn segment_path(data_dir: impl AsRef<Path>, id: u64) -> PathBuf {
    wal_dir(data_dir).join(segment_name(id))
}

/// The id a segment file name encodes, or `None` if it is not one of ours.
///
/// The width rule is not pedantry: `0000000042.wal` and `00000000042.wal` would
/// otherwise both parse as 42, and two files claiming one id means the flush
/// reads a segment twice or the writer overwrites live data. So a name is ours
/// only if it is padded to exactly [`SEGMENT_ID_DIGITS`], or longer with no
/// leading zero — which is the single spelling [`segment_name`] can produce.
pub fn parse_segment_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(&format!(".{SEGMENT_EXT}"))?;
    if !digits.bytes().all(|b| b.is_ascii_digit()) || digits.is_empty() {
        return None;
    }
    if digits.len() != SEGMENT_ID_DIGITS && digits.starts_with('0') {
        return None;
    }
    if digits.len() < SEGMENT_ID_DIGITS {
        return None;
    }
    digits.parse().ok()
}

/// Every segment id present in `wal/`, ascending.
///
/// A missing directory is not an error — it is what an unopened data directory
/// looks like. Files that are not segments are ignored rather than rejected: the
/// engine has no claim on anything it did not write, and a stray `.DS_Store` is
/// not a reason to refuse to start.
pub fn list_segments(data_dir: impl AsRef<Path>) -> Result<Vec<u64>> {
    let dir = wal_dir(data_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry?;
        if let Some(id) = entry.file_name().to_str().and_then(parse_segment_name) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Total bytes of WAL currently on disk.
///
/// This is the number that makes a stalled flush observable: the WAL is
/// deliberately not size-capped, so a flush that stops making progress shows up
/// here as steady growth long before the volume fills.
pub fn total_bytes(data_dir: impl AsRef<Path>) -> Result<u64> {
    let data_dir = data_dir.as_ref();
    let mut total = 0;
    for id in list_segments(data_dir)? {
        match std::fs::metadata(segment_path(data_dir, id)) {
            Ok(m) => total += m.len(),
            // Raced with a flush deleting it; it is no longer WAL either way.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
}

/// `fsync` a directory, making the *names* it contains durable.
///
/// Syncing a file makes its contents durable; only syncing the directory makes
/// the fact that the file exists durable. Skipping this is the classic way to
/// lose a whole freshly-created segment to a power cut even though every byte in
/// it was fsynced — the file's directory entry never reached the platter, so on
/// reboot the segment is simply not there.
pub(crate) fn fsync_dir(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(test)]
    test_fsync_log::record(path);
    Ok(())
}

/// Per-directory `fsync` counter, so the durability rule above can be asserted
/// rather than reviewed. Keyed by path because tests run concurrently and a
/// single global counter would be a race, not a measurement.
#[cfg(test)]
mod test_fsync_log {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    fn counts() -> &'static Mutex<HashMap<PathBuf, u64>> {
        static COUNTS: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();
        COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn record(path: &Path) {
        *counts()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(path.to_path_buf())
            .or_default() += 1;
    }

    pub fn count(path: &Path) -> u64 {
        counts()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(path)
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
pub(crate) use test_fsync_log::count as dir_fsyncs;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_ten_digits_and_round_trip() {
        assert_eq!(segment_name(0), "0000000000.wal");
        assert_eq!(segment_name(1), "0000000001.wal");
        assert_eq!(segment_name(42), "0000000042.wal");
        assert_eq!(segment_name(9_999_999_999), "9999999999.wal");
        // Past ten digits the name grows rather than wrapping; still unique.
        assert_eq!(segment_name(10_000_000_000), "10000000000.wal");

        for id in [0, 1, 42, 9_999_999_999, 10_000_000_000, u64::MAX] {
            assert_eq!(parse_segment_name(&segment_name(id)), Some(id));
        }
    }

    #[test]
    fn foreign_names_are_not_segments() {
        for name in [
            "0000000042.tmp",     // not ours
            "42.wal",             // unpadded: would collide with the padded form
            "00000000042.wal",    // over-padded: same collision, other direction
            "000000004a.wal",     // not a number
            "0000000042.wal.tmp", // staging leftovers
            ".DS_Store",
            "wal",
            "",
        ] {
            assert_eq!(parse_segment_name(name), None, "{name:?} should not parse");
        }
    }

    #[test]
    fn listing_is_ascending_and_ignores_strays() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(wal_dir(dir.path())).unwrap();
        for id in [7u64, 1, 100] {
            std::fs::write(segment_path(dir.path(), id), b"x").unwrap();
        }
        std::fs::write(wal_dir(dir.path()).join("notes.txt"), b"hello").unwrap();

        assert_eq!(list_segments(dir.path()).unwrap(), vec![1, 7, 100]);
        assert_eq!(total_bytes(dir.path()).unwrap(), 3);
    }

    #[test]
    fn listing_a_fresh_directory_is_empty_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(list_segments(dir.path()).unwrap().is_empty());
        assert_eq!(total_bytes(dir.path()).unwrap(), 0);
    }
}
