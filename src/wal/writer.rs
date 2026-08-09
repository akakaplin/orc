//! The append path and the committer thread — where the latency budget lives.
//!
//! Two threads share one segment file and the split between them is the whole
//! design:
//!
//! - **The caller's thread** encodes the frame (before this module ever sees
//!   it), takes a `Mutex<BufWriter<File>>`, `memcpy`s the bytes in, and leaves.
//!   No channel, no allocation, and in the steady state no syscall at all — the
//!   `BufWriter` absorbs the write and the uncontended lock costs ~20 ns, so p99
//!   is sub-microsecond.
//! - **The committer thread** owns durability. Every `fsync_interval_ms`, or as
//!   soon as `fsync_bytes` of dirty data have accumulated, it flushes the buffer
//!   and fsyncs; past `segment_max_bytes` it rolls to a new segment. It also
//!   heartbeats the directory lock, because it is the one thread that is alive
//!   for exactly as long as the engine is writing.
//!
//! A process crash therefore loses nothing already appended — the bytes are in
//! the page cache and the kernel writes them back. A power cut loses at most one
//! group-commit window.
//!
//! # The group-commit fsync happens outside the append lock
//!
//! The committer flushes the `BufWriter` under the lock (a `memcpy` and at most
//! one `write(2)`), then releases it and fsyncs through a *second descriptor* on
//! the same file. Holding the mutex across the fsync would put multi-millisecond
//! disk stalls on every appender's critical path, which is the exact latency the
//! background thread exists to avoid. Ordering is still exact: the `write` has
//! already returned before the lock is dropped, so the fsync covers at least the
//! bytes we counted — and covering *more* (appends that landed meanwhile) is
//! free correctness, never a hazard.
//!
//! **A segment roll is the exception, and is not free.** [`Shared::roll`] fsyncs
//! the outgoing segment, then creates the incoming one — a second fsync plus a
//! directory fsync — with the append lock held throughout, so appenders block for
//! all three. That is not an oversight to be tidied away later: recovery scans
//! only the tail segment, and it may do so *because* a sealed segment is fully
//! durable before its successor exists. Moving either fsync out from under the
//! lock would let an appender write into the new segment while the old one is
//! still unsynced, which is precisely the ordering the guarantee rests on.
//!
//! What that costs is one stall per `segment_max_bytes` of ingest — at the 64 MiB
//! default, rare enough to sit well outside p99. It is only visible with a small
//! `segment_max_bytes`, which is a test configuration rather than a real one.
//!
//! **A roll is also the only way out of a torn write.** If `write_all` fails
//! part-way the segment ends mid-frame, and anything appended after it would
//! bury that fragment where the flush's scan stops at it and abandons every
//! record behind. So appends are refused until the segment is sealed — but only
//! until: the committer retries the roll every tick, so a disk that fills and
//! drains recovers without a restart.
//!
//! # The WAL is deliberately not size-capped
//!
//! Ingest never refuses a record because of accumulated WAL. If the flush
//! stalls, the log grows and the filesystem is the only limit. A byte ceiling
//! that started rejecting writes would trade a recoverable operational problem —
//! visible in [`WalStats`] and in [`crate::wal::total_bytes`] long before the
//! volume fills — for a hard ingest outage, and every record is on disk either
//! way.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::codec::{SEGMENT_HEADER_BYTES, encode_segment_header};
use crate::config::WalConfig;
use crate::error::{Error, Result};
use crate::wal::recovery::{LOCK_HEARTBEAT_MS, touch_lock};
use crate::wal::{FIRST_SEGMENT, fsync_dir, segment_name, wal_dir};

/// Capacity of the `BufWriter` in front of the segment file.
///
/// Big enough that an appender's `write_all` is a `memcpy` essentially always —
/// it holds sixteen maximum-size records — and small enough that the `write(2)`
/// it eventually costs some unlucky caller is one page-aligned chunk, not a
/// stall. Durability does not depend on it: the committer flushes on a timer.
const WRITE_BUFFER_BYTES: usize = 256 * 1024;

/// How to open a WAL writer.
///
/// A struct rather than four positional arguments because `first_segment` and
/// `heartbeat` both come from recovery, and a caller that swapped two `u64`s or
/// passed the wrong path would produce a subtly wrong engine rather than a
/// compile error.
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// Root of the on-disk layout; segments land in `<data_dir>/wal/`.
    pub data_dir: PathBuf,
    pub wal: WalConfig,
    /// The id of the segment to create. Must be greater than every id already
    /// in `wal/` — [`crate::wal::recovery::Recovery::next_segment`] computes it.
    pub first_segment: u64,
    /// The `LOCK` file to keep fresh, if this writer owns the directory lock.
    /// The committer touches it about once a second; a lock older than
    /// [`crate::wal::recovery::LOCK_STALE_MS`] is treated as abandoned.
    pub heartbeat: Option<PathBuf>,
}

impl WalOptions {
    /// Options for a fresh data directory: segment [`FIRST_SEGMENT`], no lock.
    pub fn new(data_dir: impl Into<PathBuf>, wal: WalConfig) -> Self {
        Self {
            data_dir: data_dir.into(),
            wal,
            first_segment: FIRST_SEGMENT,
            heartbeat: None,
        }
    }
}

/// What the engine reports about the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalStats {
    /// The segment currently being appended to.
    pub segment: u64,
    /// Bytes in the active segment, header included.
    pub segment_bytes: u64,
    /// Appended but not yet fsynced: what a power cut would cost right now.
    pub dirty_bytes: u64,
    /// Frame bytes this writer has accepted since it was opened.
    pub appended_bytes: u64,
    /// Successful fsyncs, and failures. A rising failure count with a rising
    /// `dirty_bytes` is a dying disk, and is visible here before it is visible
    /// as data loss.
    pub fsyncs: u64,
    pub fsync_failures: u64,
    /// Epoch microseconds of the last successful fsync; 0 if there has not been
    /// one. Idle engines do not fsync, so a stale value is not by itself a
    /// fault — compare it against `dirty_bytes`.
    pub last_fsync_us: i64,
    /// True once [`WalWriter::close`] has run; every further append fails.
    pub closed: bool,
}

/// The active segment, plus the thread that makes it durable.
///
/// Cloneable across threads by sharing: every method takes `&self`, so the
/// engine can hand out `Arc<WalWriter>` and appenders contend only on the one
/// mutex the design is built around.
#[derive(Debug)]
pub struct WalWriter {
    shared: Arc<Shared>,
    committer: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct Shared {
    dir: PathBuf,
    cfg: WalConfig,
    heartbeat: Option<PathBuf>,
    state: Mutex<State>,
    /// Woken by an appender that has crossed `fsync_bytes` or filled a segment,
    /// and by `close`. Without it those thresholds would only be noticed at the
    /// next interval tick, which at a high ingest rate is most of a segment
    /// late.
    wake: Condvar,
    fsyncs: AtomicU64,
    fsync_failures: AtomicU64,
    last_fsync_us: AtomicI64,
    /// When the last commit failure was logged. A disk that stays broken fails
    /// on every tick, and one line per attempt buries the first -- which is the
    /// one that says what actually happened.
    last_failure_log_us: AtomicI64,
}

#[derive(Debug)]
struct State {
    /// `None` once closed. Appending to a writer in that state is an error,
    /// never silent.
    open: Option<Active>,
    stop: bool,
    appended: u64,
    /// Set when a partial write left a torn frame and the roll that would seal
    /// it failed. Appends are refused while it is set — the damage has to stay
    /// confined to a tail nobody writes to again — and the committer clears it
    /// on the first roll that succeeds. Closing the writer instead, which is
    /// what used to happen, needed a restart to undo.
    needs_roll: bool,
}

#[derive(Debug)]
struct Active {
    id: u64,
    file: BufWriter<File>,
    /// A second descriptor on the same file, so the committer can fsync after
    /// releasing the append lock. `Arc` because it outlives the guard.
    sync: Arc<File>,
    /// Bytes written to the file, header included.
    bytes: u64,
    /// Bytes known durable.
    synced: u64,
}

impl Active {
    fn dirty(&self) -> u64 {
        self.bytes - self.synced
    }
}

/// Take a lock, ignoring poisoning.
///
/// A panic while holding the append lock cannot corrupt what this module
/// protects — the critical sections are a `memcpy` and two counter updates, with
/// no invariant spanning them — and refusing to append for the rest of the
/// process's life because an unrelated thread panicked would turn a bug into
/// data loss.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn closed() -> Error {
    Error::Io(std::io::Error::other("the wal writer is closed"))
}

fn torn() -> Error {
    Error::Io(std::io::Error::other(
        "the active wal segment holds a torn frame and could not be sealed; \
         appends are refused until the committer rolls to a new segment",
    ))
}

fn now_us() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_micros() as i64,
        Err(e) => -(e.duration().as_micros() as i64),
    }
}

impl WalWriter {
    /// Create `<data_dir>/wal/`, open segment `first_segment`, and start the
    /// committer.
    ///
    /// The segment exists and is durable before this returns, so the first
    /// append never pays for file creation.
    pub fn open(opts: WalOptions) -> Result<WalWriter> {
        let dir = wal_dir(&opts.data_dir);
        std::fs::create_dir_all(&dir)?;
        // `wal/` itself is a name in `data_dir`, and needs the same treatment as
        // the segments inside it.
        fsync_dir(&opts.data_dir)?;

        let shared = Arc::new(Shared {
            dir,
            // Clamped in one place, so both the appender and the committer read
            // the same numbers. A zero here is a degenerate config rather than a
            // wish: `fsync_bytes: 0` would leave the committer's wait predicate
            // permanently false and spin a core, and a `segment_max_bytes` below
            // one header would roll a fresh empty segment every tick, forever.
            cfg: WalConfig {
                fsync_interval_ms: opts.wal.fsync_interval_ms.max(1),
                fsync_bytes: opts.wal.fsync_bytes.max(1),
                segment_max_bytes: opts
                    .wal
                    .segment_max_bytes
                    .max(SEGMENT_HEADER_BYTES as u64 + 1),
            },
            heartbeat: opts.heartbeat,
            state: Mutex::new(State {
                open: None,
                stop: false,
                appended: 0,
                needs_roll: false,
            }),
            wake: Condvar::new(),
            fsyncs: AtomicU64::new(0),
            fsync_failures: AtomicU64::new(0),
            last_fsync_us: AtomicI64::new(0),
            last_failure_log_us: AtomicI64::new(0),
        });

        let active = shared.create_segment(opts.first_segment)?;
        lock(&shared.state).open = Some(active);

        let committer_shared = Arc::clone(&shared);
        let handle = std::thread::Builder::new()
            .name("orc-wal-committer".into())
            .spawn(move || committer(committer_shared))?;

        Ok(WalWriter {
            shared,
            committer: Mutex::new(Some(handle)),
        })
    }

    /// Append one already-encoded frame.
    ///
    /// Encoding happens before the lock is taken — this method never inspects
    /// the bytes. That is what lets the server hand over frames a client
    /// encoded, with no transcoding, and what keeps the critical section to a
    /// `memcpy`.
    pub fn append(&self, frame: &[u8]) -> Result<()> {
        self.append_bytes(frame)
    }

    /// Append several already-encoded frames laid out back to back, under one
    /// lock acquisition.
    ///
    /// This is the shape a ZeroMQ ingest message already has, so the server's
    /// whole batch is copied from the socket buffer into the WAL exactly once.
    pub fn append_batch(&self, frames: &[u8]) -> Result<()> {
        self.append_bytes(frames)
    }

    fn append_bytes(&self, bytes: &[u8]) -> Result<()> {
        let wake = {
            let mut guard = lock(&self.shared.state);
            // Reborrowed once, so `state.open` and `state.appended` are two
            // disjoint fields rather than two borrows of the guard.
            let state = &mut *guard;
            if state.needs_roll {
                // Appending past the torn frame would bury it mid-file, where
                // the flush's scan stops and abandons every record after it.
                return Err(torn());
            }
            let Some(active) = state.open.as_mut() else {
                return Err(closed());
            };

            match active.file.write_all(bytes) {
                Ok(()) => {
                    let n = bytes.len() as u64;
                    active.bytes += n;
                    let (dirty, size) = (active.dirty(), active.bytes);
                    state.appended += n;
                    dirty >= self.shared.cfg.fsync_bytes
                        || size >= self.shared.cfg.segment_max_bytes
                }
                Err(e) => {
                    // `write_all` can fail having written part of the frame,
                    // which would leave a torn record in the *middle* of a
                    // segment -- where the flush stops at it and abandons
                    // everything after. Rolling confines the damage to the tail
                    // of a segment nobody will write to again, which is the
                    // exact shape the format is built to survive.
                    tracing::error!(error = %e, "wal append failed; sealing the segment to confine the torn frame");
                    // Set before the attempt, cleared only on success.
                    state.needs_roll = true;
                    match self.shared.roll(state) {
                        Ok(_) => state.needs_roll = false,
                        // Deliberately not `state.open = None`: the usual cause
                        // is the disk that just failed the write, and it may
                        // clear. The committer retries every tick, where closing
                        // the writer made a transient ENOSPC cost a restart.
                        Err(roll) => tracing::error!(
                            error = %roll,
                            "could not seal the torn segment; appends are refused until the committer can"
                        ),
                    }
                    return Err(e.into());
                }
            }
        };

        // Outside the lock: waking a waiter is a futex syscall, and the
        // committer would only have to wait for us to release it anyway.
        if wake {
            self.shared.wake.notify_one();
        }
        Ok(())
    }

    /// Close the active segment and open the next one.
    ///
    /// This is what the flush calls before it starts: everything appended so far
    /// is in a sealed, immutable segment, and ingest keeps running into the new
    /// one throughout. Returns the id sealed, or `None` if the active segment
    /// held no records — sealing an empty segment would mint an empty file per
    /// flush interval on an idle series, forever.
    pub fn seal(&self) -> Result<Option<u64>> {
        let mut guard = lock(&self.shared.state);
        let state = &mut *guard;
        let Some(active) = state.open.as_ref() else {
            return Err(closed());
        };
        if active.bytes == SEGMENT_HEADER_BYTES as u64 {
            return Ok(None);
        }
        // `roll_now`, so a seal that lands while a roll is owed resolves it
        // rather than leaving ingest refused behind a flag whose reason is gone.
        let id = roll_now(&self.shared, state)?;
        tracing::debug!(segment = id, "sealed wal segment");
        Ok(Some(id))
    }

    /// Stop the committer, fsync what is outstanding, and release the segment.
    ///
    /// Idempotent, and called by [`Drop`] — a forgotten `close()` must not be a
    /// lost-data bug. After it returns, every append fails rather than silently
    /// writing to a file nobody will ever fsync.
    pub fn close(&self) -> Result<()> {
        lock(&self.shared.state).stop = true;
        self.shared.wake.notify_all();

        // Join with no lock held: the committer takes the same mutex every tick.
        if let Some(handle) = lock(&self.committer).take()
            && handle.join().is_err()
        {
            tracing::error!("the wal committer thread panicked");
        }

        let mut state = lock(&self.shared.state);
        match state.open.take() {
            Some(mut active) => {
                let r = self.shared.sync_in_lock(&mut active);
                let (id, bytes) = (active.id, active.bytes);
                drop(active);
                tracing::debug!(segment = id, bytes, "closed the wal");
                r.map_err(Error::from)
            }
            None => Ok(()),
        }
    }

    /// Everything the engine reports about the log.
    pub fn stats(&self) -> WalStats {
        let state = lock(&self.shared.state);
        let (segment, segment_bytes, dirty_bytes) = match state.open.as_ref() {
            Some(a) => (a.id, a.bytes, a.dirty()),
            None => (0, 0, 0),
        };
        WalStats {
            segment,
            segment_bytes,
            dirty_bytes,
            appended_bytes: state.appended,
            closed: state.open.is_none(),
            fsyncs: self.shared.fsyncs.load(Ordering::Relaxed),
            fsync_failures: self.shared.fsync_failures.load(Ordering::Relaxed),
            last_fsync_us: self.shared.last_fsync_us.load(Ordering::Relaxed),
        }
    }

    /// The segment being appended to right now.
    pub fn segment(&self) -> u64 {
        lock(&self.shared.state).open.as_ref().map_or(0, |a| a.id)
    }

    /// Put the writer into the state a partial `write(2)` leaves behind.
    ///
    /// A real torn write needs the disk to fail mid-frame, which no safe-Rust
    /// test can arrange. The detection is one `match` arm; what is worth pinning
    /// is the recovery.
    #[cfg(test)]
    pub(crate) fn force_needs_roll(&self) {
        lock(&self.shared.state).needs_roll = true;
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            tracing::error!(error = %e, "closing the wal failed; up to one commit window may be lost");
        }
    }
}

impl Shared {
    /// Create and durably register a new segment file.
    ///
    /// The order is the durability rule: write the header, fsync the *file* so
    /// the bytes are real, then fsync the *directory* so the file's existence
    /// is. Without that last step a power cut can lose an entire freshly-rolled
    /// segment whose every record was fsynced — the records were durable, the
    /// name that reaches them was not.
    ///
    /// `create_new` rather than `create`: an id that already exists means
    /// recovery and the writer disagree about the tail, and overwriting would
    /// destroy unflushed records. Failing loudly is the only safe answer.
    fn create_segment(&self, id: u64) -> Result<Active> {
        let path = self.dir.join(segment_name(id));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;

        let mut header = Vec::with_capacity(SEGMENT_HEADER_BYTES);
        encode_segment_header(&mut header, id);
        file.write_all(&header)?;
        file.sync_all()?;
        fsync_dir(&self.dir)?;

        tracing::info!(segment = id, path = %path.display(), "opened a wal segment");
        let sync = Arc::new(file.try_clone()?);
        Ok(Active {
            id,
            file: BufWriter::with_capacity(WRITE_BUFFER_BYTES, file),
            sync,
            bytes: SEGMENT_HEADER_BYTES as u64,
            synced: SEGMENT_HEADER_BYTES as u64,
        })
    }

    /// Make everything written to `active` durable, holding the append lock.
    ///
    /// Used for the rare, non-latency-critical transitions — sealing, rolling,
    /// closing — where simplicity beats concurrency. `sync_all` rather than
    /// `sync_data` because these are the points at which the file's *metadata*
    /// (its length, above all) must be as durable as its contents.
    fn sync_in_lock(&self, active: &mut Active) -> std::io::Result<()> {
        active.file.flush()?;
        active.sync.sync_all()?;
        active.synced = active.bytes;
        self.note_fsync();
        Ok(())
    }

    /// Seal the active segment and open its successor. Returns the sealed id.
    ///
    /// The order matters twice over. The old segment is fully synced **before**
    /// the new one exists, because recovery deliberately never scans a sealed
    /// segment — it trusts exactly this. And the new segment is created before
    /// the old handle is dropped, so a failure to create one leaves the writer
    /// with a working segment instead of a dead engine.
    fn roll(&self, state: &mut State) -> Result<u64> {
        let active = state.open.as_mut().ok_or_else(closed)?;
        self.sync_in_lock(active)?;
        let id = active.id;

        let next = self.create_segment(id + 1)?;
        drop(state.open.replace(next));
        Ok(id)
    }

    fn note_fsync(&self) {
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.last_fsync_us.store(now_us(), Ordering::Relaxed);
    }

    fn note_failure(&self, e: &std::io::Error, what: &str) {
        let failures = self.fsync_failures.fetch_add(1, Ordering::Relaxed) + 1;
        // Always counted, at most once a second logged. `fsync_failures` is the
        // signal an operator alerts on; the log is for reading afterwards, and a
        // million identical lines make it useless for that.
        let now = now_us();
        let last = self.last_failure_log_us.load(Ordering::Relaxed);
        if failures == 1 || now.saturating_sub(last) >= 1_000_000 {
            self.last_failure_log_us.store(now, Ordering::Relaxed);
            tracing::error!(
                error = %e,
                failures,
                "wal commit failed while {what}; retrying on the next tick"
            );
        }
    }
}

/// Roll, and clear the "a roll is owed" flag if it worked. Pairing them keeps a
/// caller from clearing it on a roll that failed.
fn roll_now(shared: &Shared, state: &mut State) -> Result<u64> {
    let id = shared.roll(state)?;
    state.needs_roll = false;
    Ok(id)
}

/// The committer thread: group-commit, segment rolling, and the lock heartbeat.
fn committer(shared: Arc<Shared>) {
    let interval = Duration::from_millis(shared.cfg.fsync_interval_ms.max(1));
    let beat_every = Duration::from_millis(LOCK_HEARTBEAT_MS);
    // The lock heartbeat is on the same thread as the commit, so the wait has to
    // be short enough for both. A `fsync_interval_ms` of minutes must still
    // leave the lock looking alive.
    let tick = match shared.heartbeat {
        Some(_) => interval.min(beat_every),
        None => interval,
    };

    let mut last_sync = Instant::now();
    let mut last_beat = Instant::now();
    // Set when an fsync fails: the next tick must retry even if no new bytes
    // arrived, or a transient error would leave data unsynced indefinitely.
    let mut retry = false;

    loop {
        let to_sync = {
            let state = lock(&shared.state);
            let (mut state, _) = shared
                .wake
                .wait_timeout_while(state, tick, |s| {
                    // Sleep until the interval expires, unless there is already
                    // a reason not to: enough dirty bytes, a full segment, or a
                    // shutdown.
                    !s.stop
                        && s.open.as_ref().is_some_and(|a| {
                            a.dirty() < shared.cfg.fsync_bytes
                                && a.bytes < shared.cfg.segment_max_bytes
                        })
                })
                .unwrap_or_else(|e| e.into_inner());

            if state.stop {
                break;
            }
            let Some(active) = state.open.as_mut() else {
                break;
            };

            let due = retry
                || (active.dirty() > 0
                    && (last_sync.elapsed() >= interval
                        || active.dirty() >= shared.cfg.fsync_bytes));

            match due {
                false => None,
                true => match active.file.flush() {
                    Ok(()) => Some((active.id, Arc::clone(&active.sync), active.bytes)),
                    Err(e) => {
                        retry = true;
                        shared.note_failure(&e, "flushing the write buffer");
                        None
                    }
                },
            }
        };

        // The fsync itself, with the append lock released.
        if let Some((id, file, upto)) = to_sync {
            match file.sync_data() {
                Ok(()) => {
                    retry = false;
                    last_sync = Instant::now();
                    shared.note_fsync();
                    let mut state = lock(&shared.state);
                    // Only credit the segment we actually synced: a `seal` may
                    // have swapped in a new one while the lock was free.
                    if let Some(active) = state.open.as_mut()
                        && active.id == id
                    {
                        active.synced = active.synced.max(upto);
                    }
                }
                Err(e) => {
                    retry = true;
                    shared.note_failure(&e, "fsyncing the segment");
                }
            }
        }

        // A failed commit leaves `synced` where it was, so `dirty()` stays above
        // `fsync_bytes` and the wait predicate above is already false -- meaning
        // `wait_timeout_while` returns without parking and the retry runs flat
        // out on a broken disk. The roll path below has the same hazard and the
        // same fix; this is the one it was missing.
        if retry {
            std::thread::sleep(tick);
        }

        {
            let mut state = lock(&shared.state);
            let full = state
                .open
                .as_ref()
                .is_some_and(|a| a.bytes >= shared.cfg.segment_max_bytes);
            // `needs_roll`: an append tore a frame and could not seal the
            // segment itself, so ingest is refused until this succeeds.
            if (full || state.needs_roll)
                && let Err(e) = roll_now(&shared, &mut state)
            {
                tracing::error!(error = %e, "rolling to a new wal segment failed; still writing to the old one");
                drop(state);
                // The segment is over its limit (or torn), so the wait predicate
                // will not park us: without this the retry would spin a core for
                // as long as the disk stays full. For the `full` case ingest
                // keeps working throughout -- an oversized segment costs startup
                // scan time, not correctness.
                std::thread::sleep(tick);
            }
        }

        if let Some(path) = &shared.heartbeat
            && last_beat.elapsed() >= beat_every
        {
            last_beat = Instant::now();
            if let Err(e) = touch_lock(path) {
                // Not fatal on its own, but it is how a live engine's lock goes
                // stale and gets taken over by a second one.
                tracing::warn!(error = %e, path = %path.display(), "could not refresh the directory lock");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::codec::encode;
    use crate::record::{Row, Value};
    use crate::wal::reader::{SegmentReader, read_segment};
    use crate::wal::{dir_fsyncs, list_segments, segment_path};

    const MAX: usize = 16 * 1024;

    fn cfg(fsync_interval_ms: u64, fsync_bytes: u64, segment_max_bytes: u64) -> WalConfig {
        WalConfig {
            fsync_interval_ms,
            fsync_bytes,
            segment_max_bytes,
        }
    }

    fn opts(dir: &Path, wal: WalConfig) -> WalOptions {
        WalOptions::new(dir, wal)
    }

    /// One encoded frame carrying `id`, with a single string key.
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

    /// Every record id in `wal/`, in segment then arrival order.
    fn read_back(data_dir: &Path) -> Vec<String> {
        let mut ids = Vec::new();
        for segment in list_segments(data_dir).unwrap() {
            let bytes = read_segment(segment_path(data_dir, segment)).unwrap();
            let mut r = SegmentReader::new(&bytes, MAX, |_, _| Some(1)).unwrap();
            assert_eq!(
                r.segment_id(),
                segment,
                "header disagrees with the filename"
            );
            while let Some(item) = r.next_frame() {
                ids.push(item.expect("every frame must decode").record.id.to_string());
            }
        }
        ids
    }

    /// Poll until `f` holds, or fail. Used where the committer thread, not the
    /// caller, is what makes something happen.
    fn eventually(what: &str, mut f: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if f() {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("timed out waiting for {what}");
    }

    #[test]
    fn appends_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let expected: Vec<String> = (0..500).map(|i| format!("rec-{i}")).collect();

        {
            let w = WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).unwrap();
            for id in &expected {
                w.append(&frame(id)).unwrap();
            }
            w.close().unwrap();
        }

        assert_eq!(read_back(dir.path()), expected);
        assert_eq!(list_segments(dir.path()).unwrap(), vec![FIRST_SEGMENT]);
    }

    #[test]
    fn a_dropped_writer_still_flushes() {
        // A forgotten close() must not be a lost-data bug.
        let dir = tempfile::tempdir().unwrap();
        {
            let w = WalWriter::open(opts(dir.path(), cfg(60_000, 1 << 30, 1 << 30))).unwrap();
            w.append(&frame("only")).unwrap();
            // No close(), no fsync interval short enough to have fired: Drop is
            // the only thing that can have written these bytes out.
        }
        assert_eq!(read_back(dir.path()), ["only"]);
    }

    #[test]
    fn creating_a_segment_fsyncs_the_directory_before_the_first_append() {
        let dir = tempfile::tempdir().unwrap();
        let wal = wal_dir(dir.path());
        let before = dir_fsyncs(&wal);

        let w = WalWriter::open(opts(dir.path(), cfg(60_000, 1 << 30, 1 << 30))).unwrap();
        assert_eq!(
            dir_fsyncs(&wal),
            before + 1,
            "the segment's directory entry must be durable before anything is appended"
        );

        w.append(&frame("a")).unwrap();
        assert_eq!(
            dir_fsyncs(&wal),
            before + 1,
            "appending must not touch the directory"
        );

        w.seal().unwrap().unwrap();
        assert_eq!(
            dir_fsyncs(&wal),
            before + 2,
            "every new segment pays the same directory fsync"
        );
        w.close().unwrap();
    }

    #[test]
    fn sealing_rolls_to_the_next_id_and_skips_empty_segments() {
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(60_000, 1 << 30, 1 << 30))).unwrap();

        assert_eq!(w.seal().unwrap(), None, "nothing to seal yet");
        assert_eq!(w.segment(), FIRST_SEGMENT);

        w.append(&frame("a")).unwrap();
        assert_eq!(w.seal().unwrap(), Some(FIRST_SEGMENT));
        assert_eq!(w.segment(), FIRST_SEGMENT + 1);
        assert_eq!(w.seal().unwrap(), None, "the new segment is empty");

        w.append(&frame("b")).unwrap();
        w.close().unwrap();

        assert_eq!(
            list_segments(dir.path()).unwrap(),
            vec![FIRST_SEGMENT, FIRST_SEGMENT + 1]
        );
        assert_eq!(read_back(dir.path()), ["a", "b"]);
    }

    #[test]
    fn rolling_produces_sequential_ten_digit_segments() {
        let dir = tempfile::tempdir().unwrap();
        // One frame is ~50 bytes, so a 200-byte segment fills every few records.
        let w = WalWriter::open(opts(dir.path(), cfg(1, 1 << 30, 200))).unwrap();

        // A roll seals the active segment, it does not split it, so the records
        // have to arrive across several commit windows for several segments to
        // exist -- which is also what actually happens under a live ingest.
        let mut expected = Vec::new();
        for round in 0..3 {
            let before = w.segment();
            for i in 0..20 {
                let id = format!("rec-{round}-{i}");
                w.append(&frame(&id)).unwrap();
                expected.push(id);
            }
            eventually("the committer to roll past a full segment", || {
                w.segment() > before
            });
        }
        w.close().unwrap();

        let ids = list_segments(dir.path()).unwrap();
        assert_eq!(ids.len(), 4, "three rolls, plus the segment left active");
        // Sequential from the first id with no gaps: the flush consumes them in
        // this order, and a gap would look like a lost segment.
        assert_eq!(ids, vec![1, 2, 3, 4]);

        for id in &ids {
            let path = segment_path(dir.path(), *id);
            let name = path.file_name().unwrap().to_str().unwrap();
            assert_eq!(name, segment_name(*id));
            assert_eq!(name.len(), "0000000001.wal".len());
            assert!(path.exists());
        }
        // Rolling is not allowed to lose or reorder a single record.
        assert_eq!(read_back(dir.path()), expected);
    }

    #[test]
    fn the_committer_fsyncs_on_its_interval_without_being_asked() {
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(2, 1 << 30, 1 << 30))).unwrap();
        w.append(&frame("a")).unwrap();

        eventually("a timer-driven fsync", || w.stats().dirty_bytes == 0);
        let s = w.stats();
        assert!(s.fsyncs > 0);
        assert!(s.last_fsync_us > 0);
        assert_eq!(s.segment, FIRST_SEGMENT);
        assert_eq!(s.appended_bytes, frame("a").len() as u64);
        assert_eq!(s.fsync_failures, 0);
        w.close().unwrap();
    }

    #[test]
    fn a_full_buffer_wakes_the_committer_early() {
        let dir = tempfile::tempdir().unwrap();
        // An interval long enough that only the byte threshold can explain a
        // commit inside the test's lifetime.
        let w = WalWriter::open(opts(dir.path(), cfg(60_000, 256, 1 << 30))).unwrap();
        for i in 0..64 {
            w.append(&frame(&format!("rec-{i}"))).unwrap();
        }
        eventually("a byte-threshold fsync", || w.stats().fsyncs > 0);
        w.close().unwrap();
    }

    #[test]
    fn closing_is_idempotent_and_final() {
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).unwrap();
        w.append(&frame("a")).unwrap();

        w.close().unwrap();
        w.close().unwrap();

        assert!(w.stats().closed);
        assert!(w.append(&frame("b")).is_err(), "appends must not be silent");
        assert!(w.seal().is_err());
        assert_eq!(read_back(dir.path()), ["a"]);
    }

    #[test]
    fn an_id_that_already_exists_is_refused_rather_than_overwritten() {
        // Only reachable if recovery and the writer disagree about the tail, and
        // the file it would clobber holds unflushed records.
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).unwrap();
        w.append(&frame("a")).unwrap();
        w.close().unwrap();

        assert!(WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).is_err());
        assert_eq!(read_back(dir.path()), ["a"]);
    }

    #[test]
    fn a_crash_and_restart_resumes_past_the_amended_tail() {
        // The whole of M2b and M3 end to end: write, die mid-record, recover,
        // and keep writing. The restart must cost exactly the torn record.
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).unwrap();
        for i in 0..10 {
            w.append(&frame(&format!("before-{i}"))).unwrap();
        }
        w.close().unwrap();

        let path = segment_path(dir.path(), FIRST_SEGMENT);
        let len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 7)
            .unwrap();

        let r = crate::wal::recovery::recover(dir.path(), 0).unwrap();
        assert_eq!(r.tail_frames, 9);
        assert_eq!(
            r.discarded_bytes,
            frame("before-9").len() as u64 - 7,
            "exactly the torn record, and nothing before it"
        );
        assert_eq!(r.next_segment, FIRST_SEGMENT + 1);

        // The recovered tail is never reopened for append: the writer always
        // starts a fresh file, so every segment on disk is either sealed intact
        // or was amended once and then left alone.
        let w = WalWriter::open(WalOptions {
            first_segment: r.next_segment,
            ..opts(dir.path(), cfg(5, 4096, 1 << 20))
        })
        .unwrap();
        for i in 0..5 {
            w.append(&frame(&format!("after-{i}"))).unwrap();
        }
        w.close().unwrap();

        let expected: Vec<String> = (0..9)
            .map(|i| format!("before-{i}"))
            .chain((0..5).map(|i| format!("after-{i}")))
            .collect();
        assert_eq!(read_back(dir.path()), expected);
    }

    #[test]
    fn every_record_from_every_thread_is_present_exactly_once() {
        // The whole design rests on one shared mutex: N threads, one file, and
        // no record may be lost, duplicated, or interleaved into another's.
        const THREADS: usize = 8;
        const PER_THREAD: usize = 1_000;

        let dir = tempfile::tempdir().unwrap();
        // Small segments, so the committer is rolling underneath the appenders
        // for the whole run.
        let w = Arc::new(WalWriter::open(opts(dir.path(), cfg(1, 4096, 16 * 1024))).unwrap());

        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let w = Arc::clone(&w);
                scope.spawn(move || {
                    for i in 0..PER_THREAD {
                        w.append(&frame(&format!("{t}-{i}"))).unwrap();
                    }
                });
            }
        });
        w.close().unwrap();

        let mut ids = read_back(dir.path());
        assert_eq!(ids.len(), THREADS * PER_THREAD);
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            THREADS * PER_THREAD,
            "a record was written twice or lost"
        );
        assert!(
            list_segments(dir.path()).unwrap().len() > 1,
            "the test is only meaningful if the committer rolled"
        );
    }

    /// The refusal used to be permanent: the writer closed itself, so a full
    /// disk that drained still needed a restart. What matters is the recovery.
    #[test]
    fn a_torn_segment_refuses_appends_and_the_committer_recovers_it() {
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(2, 1 << 30, 1 << 30))).unwrap();
        w.append(&frame("before")).unwrap();
        let torn_segment = w.segment();

        w.force_needs_roll();
        assert!(
            w.append(&frame("during")).is_err(),
            "nothing may go in behind the torn frame"
        );

        eventually("the committer to seal the torn segment", || {
            w.segment() > torn_segment
        });
        w.append(&frame("after")).unwrap();
        w.close().unwrap();

        // The torn segment is sealed intact and the new one holds what came
        // next -- no record was lost and none was written past the tear.
        assert_eq!(read_back(dir.path()), ["before", "after"]);
        assert_eq!(list_segments(dir.path()).unwrap().len(), 2);
    }

    /// `seal` is the other way out: a flush that lands while a roll is owed
    /// resolves it, rather than leaving ingest refused behind a stale flag.
    #[test]
    fn sealing_also_clears_a_pending_roll() {
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(60_000, 1 << 30, 1 << 30))).unwrap();
        w.append(&frame("a")).unwrap();

        w.force_needs_roll();
        assert!(w.append(&frame("blocked")).is_err());

        assert_eq!(w.seal().unwrap(), Some(FIRST_SEGMENT));
        w.append(&frame("b")).unwrap();
        w.close().unwrap();
        assert_eq!(read_back(dir.path()), ["a", "b"]);
    }

    #[test]
    fn a_batch_lands_as_the_frames_it_contains() {
        // The server's path: many frames, one lock acquisition, no transcoding.
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).unwrap();

        let mut batch = Vec::new();
        for i in 0..10 {
            batch.extend_from_slice(&frame(&format!("b-{i}")));
        }
        w.append_batch(&batch).unwrap();
        w.close().unwrap();

        let expected: Vec<String> = (0..10).map(|i| format!("b-{i}")).collect();
        assert_eq!(read_back(dir.path()), expected);
    }
}
