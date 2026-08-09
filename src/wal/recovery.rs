//! Startup: take the directory lock, drop flushed segments, amend the tail.
//!
//! Recovery's whole job is to make the log consistent again *cheaply*, because
//! this engine is built for a process that restarts constantly. Two decisions
//! follow from that and neither is obvious.
//!
//! # The lock is heartbeated, not exclusive-create
//!
//! A plain `create_new` lockfile is the wrong primitive here. Every `kill -9`
//! would leave a stale file demanding manual intervention before the process
//! could start again — a crash turned into an outage. So [`DirLock`] holds the
//! pid and the committer thread refreshes its mtime about once a second; a lock
//! older than [`LOCK_STALE_MS`] is treated as abandoned and taken over, with a
//! warning naming the dead pid. The common case self-heals while two live
//! engines still cannot share one directory. (`flock` would give this for free
//! from the OS, but only via a *direct* `libc` dependency. `libc` is in the tree
//! transitively — `parquet` reaches it through `ahash` and `getrandom` — so the
//! cost would be the direct dependency and the `unsafe` call, not a new crate.
//! Worth revisiting if the heartbeat ever proves fragile.) `--force-unlock`
//! remains for the pathological case of a process that is genuinely stuck but
//! alive.
//!
//! # Only the tail segment is scanned
//!
//! [`recover`] validates the frames of the highest-numbered segment and no
//! other. Sealed segments are deliberately skipped: a segment is fully written
//! and fsynced *before* the writer rolls past it, so it cannot be torn, and the
//! flush validates every frame it reads anyway — a bad one is counted and routed
//! to `rejects/` there.
//!
//! Scanning them here would make startup time proportional to *unflushed* WAL:
//! tens of gigabytes of CRC checking before the first write is accepted, on an
//! engine whose whole premise is frequent restarts, and worse the further behind
//! the flush has fallen — exactly when you least want a slow start. Skipping
//! them bounds startup at one segment (64 MiB by default) no matter what.
//!
//! What is left after recovery is a log whose every segment either was sealed
//! intact or has been truncated at its last good frame, so the flush can read it
//! start to finish.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::codec::{self, SEGMENT_HEADER_BYTES};
use crate::error::{Error, Result};
use crate::wal::{fsync_dir, list_segments, segment_path, wal_dir};

/// The lockfile, in `data_dir`. Holds the pid of the engine that owns the
/// directory.
pub const LOCK_FILE: &str = "LOCK";

/// A lock whose mtime is older than this is abandoned, and is taken over with a
/// warning. Ten seconds is three orders of magnitude above the heartbeat, so a
/// live engine cannot look dead through scheduling noise or a slow disk.
pub const LOCK_STALE_MS: u64 = 10_000;

/// How often the WAL committer refreshes the lock. Well under
/// [`LOCK_STALE_MS`], because the cost of being late is another process
/// deciding you are dead.
pub const LOCK_HEARTBEAT_MS: u64 = 1_000;

/// Extension given to a segment whose header would not parse: `<id>.wal.corrupt`.
///
/// Deliberately not a `.wal` name, so [`list_segments`] stops seeing it and the
/// writer is free to move past the id — while the bytes stay where an operator
/// can find them.
pub const CORRUPT_EXT: &str = "wal.corrupt";

/// Is this `Corrupt` reason "the wrong binary" rather than "damaged bytes"?
fn version_mismatch(reason: &str) -> bool {
    reason == codec::UNSUPPORTED_VERSION
}

/// Ownership of a data directory, for as long as this value lives.
///
/// Advisory: it stops a second `orc` from opening the same directory, not a text
/// editor from writing into it. Dropping it releases the lock.
#[derive(Debug)]
pub struct DirLock {
    path: PathBuf,
    pid: u32,
}

impl DirLock {
    /// Take the lock, or fail if a live engine holds it.
    ///
    /// `force_unlock` takes it regardless — the documented escape for a process
    /// that is stuck but alive, and the only way past a genuinely concurrent
    /// engine.
    ///
    /// Two engines racing to take over the *same* stale lock is possible: there
    /// is no atomic compare-and-swap on a file's contents without a `libc`
    /// dependency. Writing the pid and reading it back closes the interesting
    /// window — the loser sees a pid that is not its own and refuses to start —
    /// but this is a narrowing, not a proof. The guarantee that matters is the
    /// one above it: a lock being heartbeated by a live process is never taken.
    pub fn acquire(data_dir: impl AsRef<Path>, force_unlock: bool) -> Result<DirLock> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(LOCK_FILE);
        let pid = std::process::id();

        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut f) => {
                write_pid(&mut f, pid)?;
                return Ok(DirLock { path, pid });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }

        let holder = read_pid(&path);
        let age = lock_age(&path);
        let stale = age.is_some_and(|a| a >= Duration::from_millis(LOCK_STALE_MS));

        if !stale && !force_unlock {
            tracing::error!(
                path = %path.display(),
                holder = ?holder,
                age_ms = ?age.map(|a| a.as_millis()),
                "the data directory is locked by a live engine"
            );
            return Err(Error::Locked(data_dir.display().to_string()));
        }

        tracing::warn!(
            path = %path.display(),
            dead_pid = ?holder,
            age_ms = ?age.map(|a| a.as_millis()),
            forced = force_unlock,
            "taking over an abandoned directory lock"
        );

        let mut f = OpenOptions::new().write(true).truncate(true).open(&path)?;
        write_pid(&mut f, pid)?;
        drop(f);

        if read_pid(&path) != Some(pid) {
            return Err(Error::Locked(data_dir.display().to_string()));
        }
        Ok(DirLock { path, pid })
    }

    /// Where the lockfile lives — this is what [`crate::wal::writer::WalOptions`]
    /// takes as its heartbeat path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The pid recorded in the lock: this process.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Prove the owner is still alive. The WAL committer calls this about once a
    /// second; see [`touch_lock`].
    pub fn heartbeat(&self) -> std::io::Result<()> {
        touch_lock(&self.path)
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        // Only remove a lock that is still ours. If another engine decided we
        // were dead and took over, deleting the file would hand the directory to
        // a third process while that one is running.
        if read_pid(&self.path) != Some(self.pid) {
            tracing::warn!(path = %self.path.display(), "the directory lock was taken over; leaving it alone");
            return;
        }
        if let Err(e) = std::fs::remove_file(&self.path) {
            tracing::warn!(error = %e, path = %self.path.display(), "could not release the directory lock");
        }
    }
}

/// Move the lock's mtime forward, which is the whole heartbeat.
///
/// Rewriting the pid is how the mtime moves: `std` has no `utimensat`, and
/// adding `libc` for one syscall is not worth it. Two properties are
/// deliberate — the write is not fsynced, because only the *visibility* of the
/// mtime to another process matters and a durable heartbeat would put a real
/// disk write on a one-second timer forever; and the pid is rewritten rather
/// than appended, so the file stays one short line.
pub fn touch_lock(path: &Path) -> std::io::Result<()> {
    let mut f = OpenOptions::new().write(true).truncate(true).open(path)?;
    write_pid(&mut f, std::process::id())
}

fn write_pid(f: &mut File, pid: u32) -> std::io::Result<()> {
    // Trailing newline so `cat data/LOCK` reads cleanly; the parser trims it.
    f.write_all(format!("{pid}\n").as_bytes())
}

/// The pid a lockfile names, if it can be read. Advisory only: a lock caught
/// mid-heartbeat is momentarily empty.
pub fn read_pid(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// How long since the lock was last heartbeated.
///
/// `None` when the mtime is unreadable or in the future — a clock that went
/// backwards must not make a live engine look abandoned, so an unknown age
/// counts as fresh.
fn lock_age(path: &Path) -> Option<Duration> {
    let mtime = std::fs::metadata(path).ok()?.modified().ok()?;
    SystemTime::now().duration_since(mtime).ok()
}

/// What recovery found and did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Recovery {
    /// Segments deleted because the flush had already turned them into Parquet.
    pub deleted: Vec<u64>,
    /// Segments left for the flush to consume, ascending. Includes the amended
    /// tail, which is sealed by the fact that the writer opens a new segment.
    pub pending: Vec<u64>,
    /// The segment that was scanned, if there was one.
    pub tail: Option<u64>,
    /// Intact frames found in the tail.
    pub tail_frames: u64,
    /// Bytes chopped off the tail: at most one torn record, unless the storage
    /// itself damaged the file.
    pub discarded_bytes: u64,
    /// Segments moved aside as `<id>.wal.corrupt` because their header did not
    /// parse. Their frames are **not** in the dataset and never will be without
    /// manual work — but every byte is still on disk, which is the difference
    /// between a recoverable incident and silent data loss.
    pub quarantined: Vec<u64>,
    /// The id the writer must open. Always greater than every id ever seen in
    /// this directory, so a segment is never reused or overwritten.
    pub next_segment: u64,
}

/// Bring `wal/` into a state the flush can read start to finish.
///
/// Takes `last_flushed_segment` rather than the whole manifest, because that
/// single number is all recovery uses: the engine reads the manifest (which also
/// discards a stale `manifest.json.tmp`) and passes it in.
///
/// This does not re-validate records. A record that was accepted once stays
/// accepted even if the config has since been tightened, because rejecting it
/// would mean discarding durable data on a config edit — which is also why the
/// scan bounds frame lengths by the segment's own size rather than by
/// `limits.max_record_bytes`. That cap exists to stop a corrupt length prefix
/// becoming a huge allocation; the scan has the file in memory already and
/// allocates nothing from the length, so applying it here would buy nothing and
/// cost every record above a freshly lowered limit.
pub fn recover(data_dir: impl AsRef<Path>, last_flushed_segment: u64) -> Result<Recovery> {
    let data_dir = data_dir.as_ref();
    let dir = wal_dir(data_dir);
    std::fs::create_dir_all(&dir)?;
    fsync_dir(data_dir)?;

    let mut ids = list_segments(data_dir)?;
    let highest_seen = ids.last().copied().unwrap_or(0).max(last_flushed_segment);
    let mut out = Recovery {
        next_segment: highest_seen + 1,
        ..Recovery::default()
    };

    // Anything at or below `last_flushed_segment` is already durable in Parquet.
    // These are the orphans a crash between the manifest commit and the segment
    // deletion leaves behind.
    let flushed = ids.partition_point(|&id| id <= last_flushed_segment);
    for id in ids.drain(..flushed) {
        std::fs::remove_file(segment_path(data_dir, id))?;
        out.deleted.push(id);
    }
    if !out.deleted.is_empty() {
        fsync_dir(&dir)?;
        tracing::info!(
            segments = ?out.deleted,
            "deleted wal segments already durable in parquet"
        );
    }

    // Only the tail. See the module docs: this is what bounds startup cost.
    if let Some(&tail) = ids.last() {
        let path = segment_path(data_dir, tail);
        match scan_tail(&path)? {
            TailHeader::Scanned(scan) => {
                out.tail = Some(tail);
                out.tail_frames = scan.frames;
                out.discarded_bytes = scan.discarded;
            }
            TailHeader::Absent => {
                // The header never made it to disk, so the file can hold no
                // frames at all -- there is nothing to amend, only to remove.
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                std::fs::remove_file(&path)?;
                fsync_dir(&dir)?;
                ids.pop();
                out.deleted.push(tail);
                out.discarded_bytes += size;
                tracing::warn!(
                    segment = tail,
                    bytes = size,
                    "discarded a wal segment too short to hold a header"
                );
            }
            // A header that is present but wrong says nothing about the frames
            // behind it, which may be entirely intact -- so this must never
            // unlink the file. Quarantining takes it out of the log (its records
            // are not replayed, and the name is free for the writer to reuse)
            // while leaving every byte on disk to be recovered by hand.
            TailHeader::Corrupt(reason) if !version_mismatch(reason) => {
                let quarantined = path.with_extension(CORRUPT_EXT);
                std::fs::rename(&path, &quarantined)?;
                fsync_dir(&dir)?;
                ids.pop();
                out.quarantined.push(tail);
                tracing::error!(
                    segment = tail,
                    reason,
                    path = %quarantined.display(),
                    "wal segment header is corrupt; quarantined rather than deleted -- \
                     its frames may still be readable and are NOT in the dataset"
                );
            }
            // A format-version mismatch is not damage, it is the wrong binary.
            // Starting anyway would quarantine a whole WAL that the right binary
            // would read perfectly, so refuse instead.
            TailHeader::Corrupt(reason) => {
                return Err(Error::Config(format!(
                    "wal segment {tail} was written in a format this build cannot read \
                     ({reason}). Refusing to start: it holds durable records, and \
                     starting would strand them. Run the version of orc that wrote \
                     them and flush, or move {} aside deliberately.",
                    dir.display()
                )));
            }
        }
    }

    out.pending = ids;
    tracing::info!(
        pending = out.pending.len(),
        deleted = out.deleted.len(),
        quarantined = ?out.quarantined,
        tail = ?out.tail,
        tail_frames = out.tail_frames,
        discarded_bytes = out.discarded_bytes,
        next_segment = out.next_segment,
        "wal recovery complete"
    );
    Ok(out)
}

struct TailScan {
    frames: u64,
    discarded: u64,
}

/// What the tail segment's header turned out to be.
///
/// The three cases call for three different answers, and collapsing them is how
/// a version bump or a single flipped bit used to unlink a segment full of
/// perfectly good records. `parse_segment_header` reports them distinctly for
/// exactly this caller — it is the one whose response has to differ.
enum TailHeader {
    /// A usable header; the frames after it were scanned and amended.
    Scanned(TailScan),
    /// Shorter than a header. The file cannot hold a single locatable frame, so
    /// there is genuinely nothing in it to keep.
    Absent,
    /// A header is present but wrong. The bytes after it may be entirely
    /// recoverable by hand, so they are quarantined rather than deleted.
    Corrupt(&'static str),
}

/// Validate the tail's frames and truncate it at the last good offset.
///
/// The scan checks exactly what can be checked without a schema: that each frame
/// is delimited by the buffer, and the CRC. Whether the *contents* decode is the
/// flush's question, asked once an hour instead of on every startup.
///
/// The frame-length cap is the buffer's own length, not `limits.max_record_bytes`
/// — see [`recover`] on why a config edit must never invalidate a record that was
/// accepted when it was written. Nothing here reserves memory from the length, so
/// the cap that exists to bound allocation has no work to do.
fn scan_tail(path: &Path) -> Result<TailHeader> {
    let bytes = std::fs::read(path)?;
    let size = bytes.len() as u64;

    match codec::parse_segment_header(&bytes) {
        Ok(id) => {
            let named = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(crate::wal::parse_segment_name);
            if named != Some(id) {
                // Not fatal, but `arrival` order is (segment_id, offset), so a
                // renamed segment silently reorders records across a replay.
                tracing::warn!(path = %path.display(), header_id = id, "segment header disagrees with its file name");
            }
        }
        Err(e) if e.is_truncated() => return Ok(TailHeader::Absent),
        Err(codec::DecodeError::Corrupt { reason }) => return Ok(TailHeader::Corrupt(reason)),
        Err(_) => unreachable!("parse_segment_header returns only Truncated or Corrupt"),
    }

    let max_record_bytes = bytes.len();
    let mut at = SEGMENT_HEADER_BYTES;
    let mut frames = 0;
    let stopped_by = loop {
        if at >= bytes.len() {
            break None;
        }
        match codec::validate_prefix(&bytes[at..], max_record_bytes) {
            Ok(prefix) => {
                at += prefix.frame_len;
                frames += 1;
            }
            Err(e) => break Some(e),
        }
    };

    let discarded = size - at as u64;
    if discarded > 0 {
        let err = stopped_by.expect("a short read is the only way to stop early");
        // A crash mid-write costs one partial record; anything larger means the
        // storage damaged bytes that were already acknowledged, which is worth
        // saying loudly.
        tracing::warn!(
            path = %path.display(),
            frames,
            good_bytes = at,
            discarded_bytes = discarded,
            reason = %err,
            "amending a torn wal segment: truncating at the last good frame"
        );
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(at as u64)?;
        f.sync_all()?;
    }

    Ok(TailHeader::Scanned(TailScan { frames, discarded }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{encode, encode_segment_header};
    use crate::record::{Row, Value};
    use crate::wal::{FIRST_SEGMENT, segment_name};

    const MAX: usize = 16 * 1024;

    fn frame(id: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let keys = [Value::Str("AAPL")];
        encode(
            &mut out,
            "trades",
            0,
            &Row {
                ts: 1_786_147_200_000_000,
                id,
                keys: &keys,
                extra: &[],
                data: "{}",
            },
        )
        .unwrap();
        out
    }

    /// Write a segment holding `n` frames, and return its bytes.
    fn write_segment(data_dir: &Path, id: u64, n: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_segment_header(&mut buf, id);
        for i in 0..n {
            buf.extend_from_slice(&frame(&format!("s{id}-{i}")));
        }
        std::fs::create_dir_all(wal_dir(data_dir)).unwrap();
        std::fs::write(segment_path(data_dir, id), &buf).unwrap();
        buf
    }

    fn size(data_dir: &Path, id: u64) -> u64 {
        std::fs::metadata(segment_path(data_dir, id)).unwrap().len()
    }

    #[test]
    fn a_fresh_directory_starts_at_the_first_segment() {
        let dir = tempfile::tempdir().unwrap();
        let r = recover(dir.path(), 0).unwrap();

        assert_eq!(r.next_segment, FIRST_SEGMENT);
        assert!(r.pending.is_empty());
        assert!(r.deleted.is_empty());
        assert_eq!(r.tail, None);
        assert!(wal_dir(dir.path()).is_dir());
    }

    #[test]
    fn an_intact_tail_is_left_exactly_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = write_segment(dir.path(), 3, 5);

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail, Some(3));
        assert_eq!(r.tail_frames, 5);
        assert_eq!(r.discarded_bytes, 0);
        assert_eq!(r.pending, vec![3]);
        assert_eq!(r.next_segment, 4);
        assert_eq!(size(dir.path(), 3), bytes.len() as u64);
    }

    #[test]
    fn a_tail_torn_mid_frame_is_truncated_to_the_last_good_offset() {
        // A crash inside write(2): the file ends in the middle of a record.
        for cut in [1usize, 5, 17] {
            let dir = tempfile::tempdir().unwrap();
            let whole = write_segment(dir.path(), 1, 4);
            let last = frame("s1-3").len();
            let good = (whole.len() - last) as u64;

            let torn = &whole[..whole.len() - cut];
            std::fs::write(segment_path(dir.path(), 1), torn).unwrap();

            let r = recover(dir.path(), 0).unwrap();
            assert_eq!(r.tail_frames, 3, "cut {cut}");
            assert_eq!(r.discarded_bytes, last as u64 - cut as u64, "cut {cut}");
            // Physically truncated, not merely reported: the writer appends
            // straight after this offset, so a torn frame left in place would be
            // spliced into the next record.
            assert_eq!(size(dir.path(), 1), good, "cut {cut}");

            // ...and recovery is a fixed point.
            let again = recover(dir.path(), 0).unwrap();
            assert_eq!(again.discarded_bytes, 0);
            assert_eq!(again.tail_frames, 3);
            assert_eq!(size(dir.path(), 1), good);
        }
    }

    #[test]
    fn a_flipped_crc_byte_truncates_at_that_frame() {
        let dir = tempfile::tempdir().unwrap();
        let mut whole = write_segment(dir.path(), 1, 4);
        let last = frame("s1-3").len();
        let good = (whole.len() - last) as u64;

        // Damage a byte inside the final frame's payload; the CRC is what
        // catches it, and everything before it must survive untouched.
        let victim = whole.len() - 3;
        whole[victim] ^= 0x40;
        std::fs::write(segment_path(dir.path(), 1), &whole).unwrap();

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail_frames, 3);
        assert_eq!(r.discarded_bytes, last as u64);
        assert_eq!(size(dir.path(), 1), good);
    }

    #[test]
    fn a_zero_padded_tail_is_discarded() {
        // What a sparse write, or a filesystem that zero-fills a lost block,
        // leaves behind. A run of zeros is a frame claiming length 0, which is
        // below the smallest frame the format can produce.
        let dir = tempfile::tempdir().unwrap();
        let whole = write_segment(dir.path(), 1, 3);
        let good = whole.len() as u64;

        let mut padded = whole.clone();
        padded.resize(padded.len() + 8192, 0);
        std::fs::write(segment_path(dir.path(), 1), &padded).unwrap();

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail_frames, 3);
        assert_eq!(r.discarded_bytes, 8192);
        assert_eq!(size(dir.path(), 1), good);
    }

    #[test]
    fn a_length_prefix_claiming_gigabytes_is_corruption_not_a_short_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut whole = write_segment(dir.path(), 1, 2);
        let good = whole.len() as u64;
        whole.extend_from_slice(&u32::MAX.to_le_bytes());
        whole.extend_from_slice(&0u32.to_le_bytes());
        std::fs::write(segment_path(dir.path(), 1), &whole).unwrap();

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail_frames, 2);
        assert_eq!(size(dir.path(), 1), good);
    }

    #[test]
    fn segments_at_or_below_last_flushed_are_deleted() {
        let dir = tempfile::tempdir().unwrap();
        for id in 1..=5 {
            write_segment(dir.path(), id, 2);
        }

        let r = recover(dir.path(), 3).unwrap();
        assert_eq!(r.deleted, vec![1, 2, 3]);
        assert_eq!(r.pending, vec![4, 5]);
        assert_eq!(r.next_segment, 6);
        for id in 1..=3 {
            assert!(!segment_path(dir.path(), id).exists(), "segment {id}");
        }
        for id in 4..=5 {
            assert!(segment_path(dir.path(), id).exists(), "segment {id}");
        }
    }

    #[test]
    fn ids_never_go_backwards_even_with_no_segments_left() {
        // Everything was flushed, so the directory is empty -- but reusing an id
        // would let a stale Parquet filename describe different records.
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 9, 1);
        let r = recover(dir.path(), 9).unwrap();
        assert_eq!(r.deleted, vec![9]);
        assert!(r.pending.is_empty());
        assert_eq!(r.next_segment, 10);

        // ...and the manifest alone is enough to know that, with no files left.
        let r = recover(dir.path(), 12).unwrap();
        assert_eq!(r.next_segment, 13);
    }

    #[test]
    fn only_the_tail_segment_is_scanned() {
        // The load-bearing performance decision, asserted so it cannot regress:
        // a sealed segment is not read at all, even when it is damaged. The
        // flush validates it later and rejects the bad frame there.
        let dir = tempfile::tempdir().unwrap();
        let mut sealed = write_segment(dir.path(), 1, 4);
        let sealed_len = sealed.len() as u64;
        let corrupt_at = sealed.len() - 3;
        sealed[corrupt_at] ^= 0xff;
        std::fs::write(segment_path(dir.path(), 1), &sealed).unwrap();
        write_segment(dir.path(), 2, 2);

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail, Some(2), "only the highest id is scanned");
        assert_eq!(r.tail_frames, 2);
        assert_eq!(
            size(dir.path(), 1),
            sealed_len,
            "a sealed segment must not be touched, damaged or not"
        );
        assert_eq!(r.pending, vec![1, 2]);
    }

    #[test]
    fn a_segment_with_no_header_is_dropped_but_still_burns_its_id() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 1, 1);
        std::fs::create_dir_all(wal_dir(dir.path())).unwrap();
        std::fs::write(segment_path(dir.path(), 2), b"\x00\x00").unwrap();

        let r = recover(dir.path(), 0).unwrap();
        assert!(!segment_path(dir.path(), 2).exists());
        assert_eq!(r.pending, vec![1]);
        assert_eq!(r.tail, None);
        // The id is spent regardless: a new segment 2 would be a second file
        // claiming an arrival range that already existed.
        assert_eq!(r.next_segment, 3);
    }

    /// A header that is *wrong* is a different thing from a header that is
    /// *missing*, and only the second means the file is empty of records.
    /// Deleting on the first cost a whole segment of durable frames for one bad
    /// byte, so it now moves aside instead.
    #[test]
    fn a_segment_with_a_corrupt_header_is_quarantined_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 1, 1);
        let good = write_segment(dir.path(), 2, 5);

        // One flipped bit in the magic. Every frame behind it is intact.
        let mut damaged = good.clone();
        damaged[0] ^= 0x01;
        std::fs::write(segment_path(dir.path(), 2), &damaged).unwrap();

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.quarantined, vec![2]);
        assert_eq!(r.pending, vec![1], "it is not replayed");
        assert!(!segment_path(dir.path(), 2).exists());

        // The bytes survive, in full and unmodified.
        let aside = segment_path(dir.path(), 2).with_extension(CORRUPT_EXT);
        assert_eq!(std::fs::read(&aside).unwrap(), damaged, "nothing was lost");
        assert_eq!(r.next_segment, 3, "and the id is still spent");

        // Idempotent: a second start must not trip over the quarantined file.
        let again = recover(dir.path(), 0).unwrap();
        assert!(again.quarantined.is_empty());
        assert_eq!(again.pending, vec![1]);
        assert!(aside.exists());
    }

    /// A version mismatch is the wrong binary, not damage. Quarantining would
    /// strand a WAL that the right binary reads perfectly, so refuse to start.
    #[test]
    fn an_unreadable_format_version_refuses_to_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut buf = write_segment(dir.path(), 1, 3);
        // Bump the version field in place, leaving the magic correct.
        let bumped = crate::error::FORMAT_VERSION.wrapping_add(1);
        buf[4..6].copy_from_slice(&bumped.to_le_bytes());
        std::fs::write(segment_path(dir.path(), 1), &buf).unwrap();

        let err = recover(dir.path(), 0).unwrap_err().to_string();
        assert!(err.contains("format"), "{err}");
        assert!(
            segment_path(dir.path(), 1).exists(),
            "refusing must not touch the file"
        );
    }

    /// The scan bounds a frame by the file it is reading, not by the current
    /// `limits.max_record_bytes` -- lowering that must never invalidate records
    /// that were durable before the edit.
    #[test]
    fn lowering_the_record_limit_does_not_truncate_a_durable_segment() {
        let dir = tempfile::tempdir().unwrap();
        // Frames here are a few hundred bytes; the old code passed the config's
        // cap straight in, so any cap below a frame's length chopped the segment
        // at its first record.
        write_segment(dir.path(), 1, 8);
        let before = size(dir.path(), 1);

        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail_frames, 8, "every frame is still found");
        assert_eq!(r.discarded_bytes, 0);
        assert_eq!(size(dir.path(), 1), before, "and nothing was truncated");
    }

    #[test]
    fn stray_files_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 1, 1);
        std::fs::write(wal_dir(dir.path()).join("README"), b"not a segment").unwrap();
        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.pending, vec![1]);
        assert!(wal_dir(dir.path()).join("README").exists());
    }

    #[test]
    fn the_scan_matches_what_the_reader_will_later_decode() {
        // Recovery checks the frame envelope; the flush decodes the contents.
        // They must agree on where the frames are, or the flush would start
        // reading at an offset recovery considered good.
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 1, 6);
        let r = recover(dir.path(), 0).unwrap();

        let bytes = crate::wal::reader::read_segment(segment_path(dir.path(), 1)).unwrap();
        let mut reader =
            crate::wal::reader::SegmentReader::new(&bytes, MAX, |_, _| Some(1)).unwrap();
        let mut decoded = 0;
        while let Some(item) = reader.next_frame() {
            item.unwrap();
            decoded += 1;
        }
        assert_eq!(decoded, r.tail_frames);
    }

    // ---- the lock --------------------------------------------------------

    /// Backdate the lock's mtime, standing in for a process that died `ago`.
    fn age_lock(path: &Path, ago: Duration) {
        let f = OpenOptions::new().write(true).open(path).unwrap();
        let when = SystemTime::now() - ago;
        f.set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
    }

    #[test]
    fn a_fresh_lock_is_refused_and_a_stale_one_is_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        let held = DirLock::acquire(dir.path(), false).unwrap();
        assert_eq!(read_pid(held.path()), Some(std::process::id()));

        // A live engine holds it: refuse, and say so with a typed error rather
        // than by clobbering someone else's log.
        let err = DirLock::acquire(dir.path(), false).unwrap_err();
        assert!(matches!(err, Error::Locked(_)), "{err:?}");
        assert!(err.to_string().contains("force-unlock"));

        // The same lock, a `kill -9` ago: taking it over is what keeps a crash
        // from becoming an outage.
        age_lock(held.path(), Duration::from_millis(LOCK_STALE_MS + 5_000));
        let taken = DirLock::acquire(dir.path(), false).unwrap();
        assert_eq!(read_pid(taken.path()), Some(std::process::id()));
    }

    #[test]
    fn a_heartbeat_keeps_a_lock_from_going_stale() {
        let dir = tempfile::tempdir().unwrap();
        let held = DirLock::acquire(dir.path(), false).unwrap();

        age_lock(held.path(), Duration::from_millis(LOCK_STALE_MS + 5_000));
        assert!(lock_age(held.path()).unwrap() > Duration::from_millis(LOCK_STALE_MS));

        held.heartbeat().unwrap();
        assert!(lock_age(held.path()).unwrap() < Duration::from_millis(LOCK_STALE_MS));
        assert!(DirLock::acquire(dir.path(), false).is_err());
        assert_eq!(read_pid(held.path()), Some(std::process::id()));
    }

    #[test]
    fn force_unlock_overrides_a_live_lock() {
        let dir = tempfile::tempdir().unwrap();
        let _held = DirLock::acquire(dir.path(), false).unwrap();
        assert!(DirLock::acquire(dir.path(), false).is_err());
        assert!(DirLock::acquire(dir.path(), true).is_ok());
    }

    #[test]
    fn releasing_removes_the_lock_and_a_taken_over_one_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = {
            let held = DirLock::acquire(dir.path(), false).unwrap();
            held.path().to_path_buf()
        };
        assert!(!path.exists(), "dropping the lock must release it");

        // A lock we lost to a takeover belongs to whoever holds it now, so our
        // Drop must not delete it out from under them.
        let held = DirLock::acquire(dir.path(), false).unwrap();
        std::fs::write(&path, b"999999\n").unwrap();
        drop(held);
        assert!(path.exists());
        assert_eq!(read_pid(&path), Some(999_999));
    }

    #[test]
    fn the_lock_is_created_with_the_data_directory() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("does/not/exist/yet");
        let held = DirLock::acquire(&nested, false).unwrap();
        assert_eq!(held.path(), nested.join(LOCK_FILE));
        assert!(held.path().exists());
    }

    #[test]
    fn segment_names_recovery_reports_are_the_ones_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 41, 1);
        let r = recover(dir.path(), 0).unwrap();
        assert_eq!(r.pending, vec![41]);
        assert!(
            wal_dir(dir.path())
                .join(segment_name(41))
                .to_str()
                .unwrap()
                .ends_with("0000000041.wal")
        );
    }
}
