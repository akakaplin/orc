//! The write-ahead log: append path, durability, and crash recovery.
//!
//! ```text
//! data/wal/0000000041.wal    sealed, awaiting flush
//! data/wal/0000000042.wal    active, append-only
//! ```
//!
//! Each file is a [segment header](crate::codec::encode_segment_header) plus
//! frames in arrival order. Only the highest-numbered segment is written; the
//! rest are immutable. The directory is flat and id-ordered, names zero-padded to
//! [`SEGMENT_ID_DIGITS`] so lexical order is numeric order.
//!
//! **Ids start at 1** and are never reused: 0 is the "nothing flushed yet"
//! default of [`Manifest::last_flushed_segment`], so segment 0 would be deleted
//! as already-durable on restart.
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

/// Zero-pad width of segment ids in file names.
pub const SEGMENT_ID_DIGITS: usize = 10;

/// The first id a fresh data directory uses (not 0; see module docs).
pub const FIRST_SEGMENT: u64 = 1;

/// `<data_dir>/wal`.
pub fn wal_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(WAL_DIR)
}

/// `<id, zero-padded>.wal`.
pub fn segment_name(id: u64) -> String {
    format!("{id:0width$}.{SEGMENT_EXT}", width = SEGMENT_ID_DIGITS)
}

/// `<data_dir>/wal/<id>.wal`.
pub fn segment_path(data_dir: impl AsRef<Path>, id: u64) -> PathBuf {
    wal_dir(data_dir).join(segment_name(id))
}

/// The id a segment file name encodes, or `None` if it is not one of ours.
/// Accepts only the exact spelling [`segment_name`] produces, so two files can
/// never claim the same id.
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

/// Extension for a quarantined segment; not `.wal`, so [`list_segments`]
/// ignores it.
pub const CORRUPT_EXT: &str = "wal.corrupt";

/// `<data_dir>/wal/<id>.wal.corrupt`.
pub fn corrupt_path(data_dir: impl AsRef<Path>, id: u64) -> PathBuf {
    segment_path(data_dir, id).with_extension(CORRUPT_EXT)
}

/// Rename a segment with an unreadable header out of the log, keeping its bytes
/// for manual recovery. Callers decide what a failure means.
pub fn quarantine_segment(data_dir: impl AsRef<Path>, id: u64) -> std::io::Result<PathBuf> {
    let data_dir = data_dir.as_ref();
    let to = corrupt_path(data_dir, id);
    std::fs::rename(segment_path(data_dir, id), &to)?;
    fsync_dir(&wal_dir(data_dir))?;
    Ok(to)
}

/// Every segment id in `wal/`, ascending. A missing directory is empty;
/// non-segment files are ignored.
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

/// Total bytes of WAL on disk; steady growth means a stalled flush.
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

/// `fsync` a directory so the file *names* in it survive a power cut.
pub(crate) fn fsync_dir(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(test)]
    test_fsync_log::record(path);
    Ok(())
}

/// Per-directory `fsync` counter for tests (keyed by path: tests run concurrently).
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
