//! The append path and the committer thread.
//!
//! - **Appenders** `memcpy` an encoded frame into a 256 KiB `BufWriter` under a
//!   mutex (~125 ns median). Tail latency: the appender that fills the buffer
//!   pays the `write(2)` with the lock held, and segment rolls fsync with it held.
//! - **The committer** flushes and fsyncs every `fsync_interval_ms` or once
//!   `fsync_bytes` accumulate, rolls past `segment_max_bytes`, and heartbeats the
//!   directory lock.
//!
//! A process crash or a power cut can lose up to one group-commit window:
//! buffered bytes live in process memory. `append` returning means "in this
//! process", not "on disk".
//!
//! The group-commit fsync runs outside the lock, on a second descriptor, so disk
//! stalls stay off the append path. A roll is the exception: the outgoing segment
//! is fsynced under the lock before its successor exists, because recovery scans
//! only the tail. A torn write (partial `write_all`) refuses appends until the
//! segment is sealed; the committer retries the roll every tick.
//!
//! The WAL is deliberately uncapped: a stalled flush shows as growth in
//! [`WalStats`] and [`crate::wal::total_bytes`], not as an ingest outage.

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

/// Capacity of the `BufWriter` in front of the segment file: sixteen max-size
/// records, so an append is almost always just a `memcpy`.
const WRITE_BUFFER_BYTES: usize = 256 * 1024;

/// How to open a WAL writer.
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// Root of the on-disk layout; segments land in `<data_dir>/wal/`.
    pub data_dir: PathBuf,
    pub wal: WalConfig,
    /// Segment to create; must exceed every id in `wal/`
    /// (see [`crate::wal::recovery::Recovery::next_segment`]).
    pub first_segment: u64,
    /// The `LOCK` file the committer keeps fresh, if this writer owns it.
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
    /// Successful and failed fsyncs.
    pub fsyncs: u64,
    pub fsync_failures: u64,
    /// Epoch µs of the last successful fsync, 0 if none. Idle engines don't
    /// fsync, so judge staleness against `dirty_bytes`.
    pub last_fsync_us: i64,
    /// True once [`WalWriter::close`] has run; every further append fails.
    pub closed: bool,
}

/// The active segment plus the committer thread. All methods take `&self`.
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
    /// Wakes the committer early: `fsync_bytes` crossed, segment full, or close.
    wake: Condvar,
    fsyncs: AtomicU64,
    fsync_failures: AtomicU64,
    last_fsync_us: AtomicI64,
    /// When the last commit failure was logged (rate limit).
    last_failure_log_us: AtomicI64,
}

#[derive(Debug)]
struct State {
    /// `None` once closed; appends then fail.
    open: Option<Active>,
    stop: bool,
    appended: u64,
    /// A torn frame awaits sealing: appends are refused until a roll succeeds.
    needs_roll: bool,
}

#[derive(Debug)]
struct Active {
    id: u64,
    file: BufWriter<File>,
    /// Second descriptor, so the committer can fsync without the append lock.
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

/// Take a lock, ignoring poisoning: the critical sections can't leave it
/// inconsistent, and refusing all later appends would turn a bug into data loss.
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
    /// Create `wal/`, durably create segment `first_segment`, start the committer.
    pub fn open(opts: WalOptions) -> Result<WalWriter> {
        let dir = wal_dir(&opts.data_dir);
        std::fs::create_dir_all(&dir)?;
        fsync_dir(&opts.data_dir)?;

        let shared = Arc::new(Shared {
            dir,
            // `Config::validate` already rejects these, but this type is public
            // and a zero would spin the committer.
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

    /// Append one already-encoded frame; the bytes are not inspected.
    pub fn append(&self, frame: &[u8]) -> Result<()> {
        self.append_bytes(frame)
    }

    /// Append back-to-back encoded frames under one lock acquisition.
    pub fn append_batch(&self, frames: &[u8]) -> Result<()> {
        self.append_bytes(frames)
    }

    fn append_bytes(&self, bytes: &[u8]) -> Result<()> {
        let wake = {
            let mut guard = lock(&self.shared.state);
            let state = &mut *guard;
            if state.needs_roll {
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
                    // A partial write may have left a torn frame; roll so it
                    // stays at a segment's tail, never mid-segment.
                    tracing::error!(error = %e, "wal append failed; sealing the segment to confine the torn frame");
                    state.needs_roll = true;
                    match self.shared.roll(state) {
                        Ok(_) => state.needs_roll = false,
                        // Not closing: the disk may recover; the committer retries.
                        Err(roll) => tracing::error!(
                            error = %roll,
                            "could not seal the torn segment; appends are refused until the committer can"
                        ),
                    }
                    return Err(e.into());
                }
            }
        };

        // Notify outside the lock.
        if wake {
            self.shared.wake.notify_one();
        }
        Ok(())
    }

    /// Seal the active segment and open the next; called by the flush. Returns
    /// the sealed id, or `None` if the segment is empty (nothing is rolled).
    pub fn seal(&self) -> Result<Option<u64>> {
        let mut guard = lock(&self.shared.state);
        let state = &mut *guard;
        let Some(active) = state.open.as_ref() else {
            return Err(closed());
        };
        if active.bytes == SEGMENT_HEADER_BYTES as u64 {
            return Ok(None);
        }
        // `roll_now` also clears a pending `needs_roll`.
        let id = roll_now(&self.shared, state)?;
        tracing::debug!(segment = id, "sealed wal segment");
        Ok(Some(id))
    }

    /// Stop the committer and fsync what is outstanding. Idempotent; also run by
    /// [`Drop`]. Later appends fail.
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

    /// Simulate the state a failed partial `write(2)` leaves behind.
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
    /// Create a segment: write the header, fsync the file, then fsync the
    /// directory so the name survives a power cut. `create_new` so an existing
    /// id (unflushed records) is never overwritten.
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

    /// Flush and `sync_all` under the append lock; for seal, roll and close only.
    fn sync_in_lock(&self, active: &mut Active) -> std::io::Result<()> {
        active.file.flush()?;
        active.sync.sync_all()?;
        active.synced = active.bytes;
        self.note_fsync();
        Ok(())
    }

    /// Seal the active segment and open its successor; returns the sealed id.
    /// The old one is synced before the new one exists (recovery relies on it),
    /// and is kept open until the new one is created, so a failure is harmless.
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
        // Always counted; logged at most once a second.
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

/// Roll, clearing `needs_roll` only on success.
fn roll_now(shared: &Shared, state: &mut State) -> Result<u64> {
    let id = shared.roll(state)?;
    state.needs_roll = false;
    Ok(id)
}

/// The committer thread: group-commit, segment rolling, and the lock heartbeat.
fn committer(shared: Arc<Shared>) {
    let interval = Duration::from_millis(shared.cfg.fsync_interval_ms.max(1));
    let beat_every = Duration::from_millis(LOCK_HEARTBEAT_MS);
    // Wake often enough for both commit and lock heartbeat.
    let tick = match shared.heartbeat {
        Some(_) => interval.min(beat_every),
        None => interval,
    };

    let mut last_sync = Instant::now();
    let mut last_beat = Instant::now();
    // After a failure, retry next tick even with no new bytes.
    let mut retry = false;

    loop {
        let to_sync = {
            let state = lock(&shared.state);
            let (mut state, _) = shared
                .wake
                .wait_timeout_while(state, tick, |s| {
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
                    // A `seal` may have swapped segments while unlocked.
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

        // The wait predicate may not park us after a failure; don't spin.
        if retry {
            std::thread::sleep(tick);
        }

        {
            let mut state = lock(&shared.state);
            let full = state
                .open
                .as_ref()
                .is_some_and(|a| a.bytes >= shared.cfg.segment_max_bytes);
            if (full || state.needs_roll)
                && let Err(e) = roll_now(&shared, &mut state)
            {
                tracing::error!(error = %e, "rolling to a new wal segment failed; still writing to the old one");
                drop(state);
                // Same: a full or torn segment won't park us. Don't spin.
                std::thread::sleep(tick);
            }
        }

        if let Some(path) = &shared.heartbeat
            && last_beat.elapsed() >= beat_every
        {
            last_beat = Instant::now();
            if let Err(e) = touch_lock(path) {
                // Left unfixed, a second engine may take over the stale lock.
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

    /// Poll until `f` holds, or fail.
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
        let dir = tempfile::tempdir().unwrap();
        {
            let w = WalWriter::open(opts(dir.path(), cfg(60_000, 1 << 30, 1 << 30))).unwrap();
            w.append(&frame("only")).unwrap();
            // No close() and no interval fsync: only Drop can write this out.
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
        let w = WalWriter::open(opts(dir.path(), cfg(1, 1 << 30, 200))).unwrap();

        // A roll seals, it doesn't split, so spread records over several windows.
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
        assert_eq!(ids, vec![1, 2, 3, 4]);

        for id in &ids {
            let path = segment_path(dir.path(), *id);
            let name = path.file_name().unwrap().to_str().unwrap();
            assert_eq!(name, segment_name(*id));
            assert_eq!(name.len(), "0000000001.wal".len());
            assert!(path.exists());
        }
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
        // Long interval: only the byte threshold can trigger a commit.
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
        let dir = tempfile::tempdir().unwrap();
        let w = WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).unwrap();
        w.append(&frame("a")).unwrap();
        w.close().unwrap();

        assert!(WalWriter::open(opts(dir.path(), cfg(5, 4096, 1 << 20))).is_err());
        assert_eq!(read_back(dir.path()), ["a"]);
    }

    #[test]
    fn a_crash_and_restart_resumes_past_the_amended_tail() {
        // Write, tear the last record, recover, keep writing: lose only that record.
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

        // The recovered tail is never reopened; the writer starts a fresh file.
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
        const THREADS: usize = 8;
        const PER_THREAD: usize = 1_000;

        let dir = tempfile::tempdir().unwrap();
        // Small segments, so the committer rolls under the appenders.
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

        assert_eq!(read_back(dir.path()), ["before", "after"]);
        assert_eq!(list_segments(dir.path()).unwrap().len(), 2);
    }

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
