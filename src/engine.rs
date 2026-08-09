//! The engine: the one type an embedding application holds.
//!
//! Wires the pieces together and owns the two things a caller must not have to
//! think about — validation on the way in, and durability behind the scenes.
//!
//! ```ignore
//! let engine = Engine::open(Config::load("./data/config.json")?)?;
//! let trades = engine.series("trades")?;
//! engine.append(&trades, &Row { ts, id, keys, extra, data })?;
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::codec::{self, Encoder};
use crate::config::{Config, SeriesHandle};
use crate::error::{Error, Result};
use crate::flush::planner::{self, FlushOutcome};
use crate::manifest::Manifest;
use crate::record::Row;
use crate::time::format_rfc3339_utc;
use crate::wal::recovery::{self, DirLock};
use crate::wal::writer::{WalOptions, WalWriter};

/// Wall clock in epoch microseconds.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        // A clock before 1970 is not something to panic over. It reads as 0,
        // which is below any `ts_min`, and `ts_upper_bound` responds by
        // suspending the window's upper bound until the clock is set.
        .unwrap_or(0)
}

// Per-thread encode scratch, so `append` allocates nothing on the steady path
// once the buffers reach their high-water mark.
thread_local! {
    static SCRATCH: std::cell::RefCell<(Vec<u8>, Encoder)> =
        std::cell::RefCell::new((Vec::new(), Encoder::default()));
}

/// Counters an operator needs to see a problem coming.
#[derive(Debug, Default)]
struct Counters {
    appended: AtomicU64,
    rejected_ts: AtomicU64,
    rejected_size: AtomicU64,
    rejected_series: AtomicU64,
    rejected_frames: AtomicU64,
    flush_failures: AtomicU64,
    rows_flushed: AtomicU64,
    rows_deduplicated: AtomicU64,
}

/// A snapshot of engine counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub appended: u64,
    pub rejected_ts: u64,
    pub rejected_size: u64,
    pub rejected_series: u64,
    pub rejected_frames: u64,
    /// Consecutive failed flushes. Rising while `wal_bytes` grows is the shape
    /// of a stalled flush, which is the failure worth alerting on.
    pub flush_failures: u64,
    pub rows_flushed: u64,
    pub rows_deduplicated: u64,
    pub wal_bytes: u64,
    pub segment: u64,
}

/// How many frames `append_raw` took and dropped.
///
/// Returned rather than errored because remote ingest is fire-and-forget: there
/// is no client to hand an error back to, so the counts are the only signal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawIngest {
    pub accepted: usize,
    pub rejected: usize,
    /// Bytes dropped because a frame's length could not be trusted, so no frame
    /// after it could be located.
    ///
    /// Separate from `rejected` because they measure different things: this is
    /// the tail of a batch, and how many records were in it is precisely what is
    /// unknowable. Non-zero means a client is sending malformed frames.
    pub discarded_bytes: usize,
}

/// The embeddable engine.
///
/// Everything the engine *is* lives in [`Inner`], behind an `Arc` the flush timer
/// shares. What stays out here is the timer's `JoinHandle`, and that placement is
/// load-bearing: the thread owns a strong reference, so if the handle lived
/// alongside it, the last `Arc` could be dropped *by the timer itself* at the end
/// of a flush — and `Drop` would then try to join the thread it is running on.
#[derive(Debug)]
pub struct Engine {
    inner: Arc<Inner>,
    /// `None` when `flush.interval_ms` is 0, or once `close` has joined it.
    flusher: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// The engine's actual state, shared with the flush timer.
#[derive(Debug)]
struct Inner {
    data_dir: PathBuf,
    config: Config,
    manifest: Mutex<Manifest>,
    handles: HashMap<String, Arc<SeriesHandle>>,
    wal: WalWriter,
    /// Held for the lifetime of the engine; `Drop` releases the directory.
    _lock: DirLock,
    /// Flushes are single-flight: the interval timer, an explicit `flush()` and
    /// the control socket all funnel through here, so an overrunning flush can
    /// never overlap the next one.
    flush_lock: Mutex<()>,
    counters: Counters,
    ts_min: u64,
    ts_max_skew_us: u64,
    /// Set by `close` to stop the flush timer. Paired with `wake` so shutdown
    /// does not wait out an hour-long interval.
    stop: Mutex<bool>,
    wake: Condvar,
}

impl Engine {
    /// Open (or create) the data directory described by `config`.
    ///
    /// Order matters and is fixed: take the directory lock, read the manifest,
    /// recover the WAL against it, then open the writer past the highest segment
    /// recovery saw. Opening the writer earlier would let it create a segment
    /// that recovery is still reasoning about.
    pub fn open(config: Config) -> Result<Engine> {
        Self::open_with(config, false)
    }

    /// As [`Engine::open`], but able to steal a lock held by a live process.
    pub fn open_with(config: Config, force_unlock: bool) -> Result<Engine> {
        config.validate()?;
        let data_dir = PathBuf::from(&config.data_dir);

        let lock = DirLock::acquire(&data_dir, force_unlock)?;

        let mut manifest = Manifest::load(&data_dir)?;
        // Commit *before* the writer opens a segment, not at the next flush.
        // `reconcile` mints epochs, and every frame appended below is stamped
        // with one; an epoch that lives only in memory is re-minted on the next
        // start for whatever the config says then, so yesterday's frames get
        // decoded against today's key list -- silently, when the arity happens
        // to match. Nothing else writes the manifest until a flush commits, and
        // there may never be one.
        if manifest.reconcile(&config)? {
            manifest.commit(&data_dir)?;
        }

        // Before the WAL is touched: a flush that wrote files and then failed
        // before committing left staged files in `tmp/` and real Parquet in
        // `series/`, and the next flush -- covering a wider segment range, so a
        // different filename -- writes those same rows again beside them.
        planner::sweep_uncommitted(&data_dir, manifest.last_flushed_segment)?;

        let rec = recovery::recover(&data_dir, manifest.last_flushed_segment)?;
        if !rec.quarantined.is_empty() {
            tracing::error!(
                segments = ?rec.quarantined,
                "wal segments were quarantined; their records are not in the dataset"
            );
        }
        if !rec.deleted.is_empty() {
            tracing::info!(
                count = rec.deleted.len(),
                "removed already-flushed segments"
            );
        }

        let wal = WalWriter::open(WalOptions {
            data_dir: data_dir.clone(),
            wal: config.wal.clone(),
            first_segment: rec.next_segment,
            heartbeat: Some(lock.path().to_path_buf()),
        })?;

        // Resolve every declared series once. The handle carries the schema and
        // the pre-encoded name bytes, so `append` never touches a hash map.
        let mut handles = HashMap::with_capacity(config.series.len());
        for s in &config.series {
            let epoch = manifest.current_epoch(&s.name).unwrap_or(0);
            handles.insert(s.name.clone(), Arc::new(SeriesHandle::new(s, epoch)?));
        }

        let ts_min = config.limits.ts_min_us()?;
        let interval_ms = config.flush.interval_ms;
        let inner = Arc::new(Inner {
            data_dir,
            ts_min,
            ts_max_skew_us: config.limits.ts_max_skew_ms.saturating_mul(1_000),
            manifest: Mutex::new(manifest),
            handles,
            wal,
            _lock: lock,
            flush_lock: Mutex::new(()),
            counters: Counters::default(),
            config,
            stop: Mutex::new(false),
            wake: Condvar::new(),
        });

        // A restart must not sit on segments the previous run never flushed.
        // "if overdue", not "always": an engine that restarts every few minutes
        // would otherwise emit a tiny Parquet file per restart.
        if inner.config.flush.on_startup && (!rec.pending.is_empty() && inner.flush_overdue()) {
            match inner.flush() {
                Ok(o) if o.rows_written > 0 => {
                    tracing::info!(rows = o.rows_written, "startup flush")
                }
                Ok(_) => {}
                Err(e) => tracing::error!(error = %e, "startup flush failed; ingest continues"),
            }
        }

        // The periodic flush the whole design is stated in terms of. Without it
        // Parquet only ever appears at startup or on an explicit request, so a
        // long-running server accumulates WAL forever and no reader sees a row.
        let flusher = match interval_ms {
            0 => None,
            ms => {
                let shared = Arc::clone(&inner);
                let interval = Duration::from_millis(ms);
                Some(
                    std::thread::Builder::new()
                        .name("orc-flush".into())
                        .spawn(move || flusher(shared, interval))?,
                )
            }
        };

        Ok(Engine {
            inner,
            flusher: Mutex::new(flusher),
        })
    }

    /// Stop the flush timer and wait for it, then close the WAL.
    ///
    /// The order is the point: joining first lets a flush already in progress
    /// finish against a live WAL, where closing first would fail it on a writer
    /// that no longer exists. Idempotent — `Drop` calls this, and so may the
    /// caller.
    pub fn close(&self) -> Result<()> {
        let handle = {
            let mut stop = self.inner.stop.lock().unwrap_or_else(|e| e.into_inner());
            *stop = true;
            self.inner.wake.notify_all();
            drop(stop);
            self.flusher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
        };
        if let Some(handle) = handle {
            // A panicking flush thread has already logged; losing the join is
            // not a reason to fail a close that still has a WAL to flush.
            if handle.join().is_err() {
                tracing::error!("the flush timer thread panicked");
            }
        }
        self.inner.close()
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            tracing::error!(error = %e, "closing the engine during drop");
        }
    }
}

/// The periodic flush.
///
/// Waits on a condvar rather than sleeping, so `close` interrupts an interval of
/// any length immediately instead of shutdown taking up to an hour.
fn flusher(inner: Arc<Inner>, interval: Duration) {
    loop {
        let stop = inner.stop.lock().unwrap_or_else(|e| e.into_inner());
        let (stop, timeout) = inner
            .wake
            .wait_timeout_while(stop, interval, |stop| !*stop)
            .unwrap_or_else(|e| e.into_inner());
        if *stop {
            return;
        }
        drop(stop);
        // The predicate is re-checked internally, so reaching here without a
        // timeout would be a spurious wake with nothing to do.
        if !timeout.timed_out() {
            continue;
        }

        // Failures are logged, never fatal: the WAL is uncapped precisely so a
        // stalled flush costs disk rather than availability, and `flush_failures`
        // rising alongside `wal_bytes` is the documented signal for it.
        match inner.flush() {
            Ok(o) if o.rows_written > 0 => {
                tracing::info!(
                    rows = o.rows_written,
                    files = o.files_written.len(),
                    "periodic flush"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error = %e, "periodic flush failed; ingest continues"),
        }
    }
}

impl Inner {
    fn flush_overdue(&self) -> bool {
        // No conversion: both sides are u64 microseconds. This used to cast a
        // saturated u64 to i64, which wrapped to -1 -- and "elapsed >= -1" is
        // always true, so an absurd interval meant "always overdue", the exact
        // inverse of what it says.
        let interval = self.config.flush.interval_ms.saturating_mul(1_000);
        match self.manifest.lock().ok().and_then(|m| m.last_flush_at) {
            // Never flushed: any pending segment is already overdue.
            None => true,
            Some(last) => now_us().saturating_sub(last) >= interval,
        }
    }

    /// Resolve a series name to a handle. Do this once, outside the hot loop.
    pub fn series(&self, name: &str) -> Result<Arc<SeriesHandle>> {
        self.handles
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownSeries(name.to_string()))
    }

    /// The accept window's upper bound right now.
    ///
    /// Suspended when the host clock reads before `ts_min`, which is the state a
    /// device that boots with an unset clock is in. Rejecting everything until
    /// NTP lands would be the worse failure.
    fn ts_upper_bound(&self) -> Option<u64> {
        let now = now_us();
        if now < self.ts_min {
            None
        } else {
            Some(now.saturating_add(self.ts_max_skew_us))
        }
    }

    fn check_ts(&self, ts: u64) -> Result<()> {
        let max = self.ts_upper_bound();
        if ts < self.ts_min || max.is_some_and(|m| ts > m) {
            self.counters.rejected_ts.fetch_add(1, Ordering::Relaxed);
            return Err(Error::TsOutOfRange {
                ts,
                min: self.ts_min,
                max: max.unwrap_or(u64::MAX),
            });
        }
        Ok(())
    }

    /// Validate `row` against `handle`'s declared schema.
    fn check_row(&self, handle: &SeriesHandle, row: &Row<'_>) -> Result<()> {
        self.check_ts(row.ts)?;
        if row.keys.len() != handle.arity() {
            return Err(Error::KeyArity {
                series: handle.name().to_string(),
                expected: handle.arity(),
                got: row.keys.len(),
            });
        }
        for (def, value) in std::iter::zip(handle.keys(), row.keys) {
            match value.key_type() {
                // Null satisfies a nullable column and nothing else.
                None if def.nullable => {}
                None => {
                    return Err(Error::TypeMismatch {
                        series: handle.name().to_string(),
                        key: def.name.clone(),
                        expected: def.ty,
                        got: None,
                    });
                }
                Some(got) if got == def.ty => {}
                got => {
                    return Err(Error::TypeMismatch {
                        series: handle.name().to_string(),
                        key: def.name.clone(),
                        expected: def.ty,
                        got,
                    });
                }
            }
        }
        Ok(())
    }

    /// Append one record.
    ///
    /// Runs entirely on the caller's thread: validate, encode into thread-local
    /// scratch, then copy into the WAL under a short lock. No disk I/O happens
    /// here — the committer thread owns that.
    pub fn append(&self, handle: &SeriesHandle, row: &Row<'_>) -> Result<()> {
        self.check_row(handle, row)?;
        let limit = self.config.limits.max_record_bytes;
        SCRATCH.with(|cell| {
            let (buf, enc) = &mut *cell.borrow_mut();
            buf.clear();
            enc.encode(buf, handle.name(), handle.epoch(), row)?;
            if buf.len() > limit {
                self.counters.rejected_size.fetch_add(1, Ordering::Relaxed);
                return Err(Error::RecordTooLarge {
                    size: buf.len(),
                    limit,
                });
            }
            self.wal.append(buf)
        })?;
        self.counters.appended.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Append many records under a single WAL lock acquisition.
    pub fn append_batch(&self, handle: &SeriesHandle, rows: &[Row<'_>]) -> Result<()> {
        for row in rows {
            self.check_row(handle, row)?;
        }
        let limit = self.config.limits.max_record_bytes;
        SCRATCH.with(|cell| {
            let (buf, enc) = &mut *cell.borrow_mut();
            buf.clear();
            for row in rows {
                let start = buf.len();
                enc.encode(buf, handle.name(), handle.epoch(), row)?;
                if buf.len() - start > limit {
                    self.counters.rejected_size.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::RecordTooLarge {
                        size: buf.len() - start,
                        limit,
                    });
                }
            }
            self.wal.append_batch(buf)
        })?;
        self.counters
            .appended
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Append frames a client already encoded, without re-encoding them.
    ///
    /// This is what makes sharing the codec with the wire pay off: the server
    /// hands over the bytes it received and they are copied exactly once, from
    /// the ZeroMQ buffer into the WAL. Only the fixed prefix is validated —
    /// CRC, length, `ts`, and that the series and epoch are known. Walking the
    /// variable-length tail is deferred to the flush, where it is paid once an
    /// hour instead of once per record.
    ///
    /// Invalid frames are counted and skipped, never fatal: with fire-and-forget
    /// there is no client to return an error to, and one bad frame must not cost
    /// the whole batch.
    pub fn append_raw(&self, mut bytes: &[u8]) -> Result<RawIngest> {
        let limit = self.config.limits.max_record_bytes;
        let mut out = RawIngest::default();
        // Grown as frames are accepted, not reserved from the message length.
        // The length is attacker-supplied: reserving it up front paid for a
        // whole batch before a single frame had been validated, so a hostile
        // message cost its full size even when its first frame was garbage.
        let mut good: Vec<u8> = Vec::new();

        while !bytes.is_empty() {
            let prefix = match codec::validate_prefix(bytes, limit) {
                Ok(p) => p,
                Err(e) => {
                    // A corrupt frame has an untrustworthy length, so there is no
                    // safe way to find the next one: stop here rather than
                    // resynchronising onto whatever happens to look like a frame.
                    //
                    // Everything after it is discarded with it, and the byte
                    // count is the only honest measure of how much -- the frames
                    // cannot be counted precisely for the same reason they
                    // cannot be found. Reporting a flat 1 made a corrupt frame
                    // near the head of a 1024-record batch look like one lost
                    // record.
                    out.rejected += 1;
                    out.discarded_bytes += bytes.len();
                    tracing::warn!(
                        error = %e,
                        discarded_bytes = bytes.len(),
                        "stopped reading a batch at an undecodable frame; the rest of it is dropped"
                    );
                    break;
                }
            };
            let (frame, rest) = bytes.split_at(prefix.frame_len);
            bytes = rest;

            if self.check_ts(prefix.ts).is_err() {
                out.rejected += 1;
                continue;
            }
            let known = self
                .handles
                .get(prefix.series)
                .is_some_and(|h| h.epoch() >= prefix.epoch);
            if !known {
                self.counters
                    .rejected_series
                    .fetch_add(1, Ordering::Relaxed);
                out.rejected += 1;
                continue;
            }
            good.extend_from_slice(frame);
            out.accepted += 1;
        }

        if !good.is_empty() {
            self.wal.append_batch(&good)?;
            self.counters
                .appended
                .fetch_add(out.accepted as u64, Ordering::Relaxed);
        }
        if out.rejected > 0 {
            self.counters
                .rejected_frames
                .fetch_add(out.rejected as u64, Ordering::Relaxed);
        }
        Ok(out)
    }

    /// Seal the active segment and turn every sealed one into Parquet.
    ///
    /// Single-flight; a concurrent caller waits rather than starting a second
    /// flush over the same segments.
    pub fn flush(&self) -> Result<FlushOutcome> {
        let _guard = self.flush_lock.lock().unwrap_or_else(|e| e.into_inner());

        self.wal.seal()?;
        let pending = crate::wal::list_segments(&self.data_dir)?;
        let mut manifest = self.manifest.lock().unwrap_or_else(|e| e.into_inner());

        // Never consume the segment the writer is appending to, and never
        // reconsume one the manifest already calls durable. The second half is
        // the rule startup applies too: without it, a `delete_segments` that
        // fails after the commit leaves a segment that the next flush re-reads
        // and rewrites under a wider name, so both files match the reader's glob
        // and every row in the overlap comes back twice.
        let active = self.wal.segment();
        let floor = manifest.last_flushed_segment;
        let consumable: Vec<u64> = pending
            .into_iter()
            .filter(|id| *id < active && *id > floor)
            .collect();

        match planner::run(&self.data_dir, &self.config, &mut manifest, &consumable) {
            Ok(outcome) => {
                self.counters.flush_failures.store(0, Ordering::Relaxed);
                self.counters
                    .rows_flushed
                    .fetch_add(outcome.rows_written as u64, Ordering::Relaxed);
                self.counters
                    .rows_deduplicated
                    .fetch_add(outcome.rows_deduplicated as u64, Ordering::Relaxed);
                Ok(outcome)
            }
            Err(e) => {
                // The WAL is not capped, so a failing flush costs disk, not
                // availability: ingest keeps working and the counter makes the
                // stall visible.
                self.counters.flush_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %e, "flush failed; ingest continues and the wal grows");
                Err(e)
            }
        }
    }

    /// Counter snapshot.
    pub fn stats(&self) -> Stats {
        let w = self.wal.stats();
        let c = &self.counters;
        Stats {
            appended: c.appended.load(Ordering::Relaxed),
            rejected_ts: c.rejected_ts.load(Ordering::Relaxed),
            rejected_size: c.rejected_size.load(Ordering::Relaxed),
            rejected_series: c.rejected_series.load(Ordering::Relaxed),
            rejected_frames: c.rejected_frames.load(Ordering::Relaxed),
            flush_failures: c.flush_failures.load(Ordering::Relaxed),
            rows_flushed: c.rows_flushed.load(Ordering::Relaxed),
            rows_deduplicated: c.rows_deduplicated.load(Ordering::Relaxed),
            wal_bytes: w.segment_bytes,
            segment: w.segment,
        }
    }

    /// Every resolved series handle, in arbitrary order.
    ///
    /// The control socket's schema handshake is built from these: a client
    /// encodes keys positionally, so it needs each series' epoch and key order
    /// before it can build a single frame.
    pub fn series_handles(&self) -> impl Iterator<Item = &Arc<SeriesHandle>> {
        self.handles.values()
    }

    /// The data directory this engine owns.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Human-readable accept window, for diagnostics.
    pub fn accept_window(&self) -> (String, Option<String>) {
        (
            format_rfc3339_utc(self.ts_min),
            self.ts_upper_bound().map(format_rfc3339_utc),
        )
    }

    /// Stop the committer and fsync. The directory lock goes with `Inner`.
    fn close(&self) -> Result<()> {
        self.wal.close()
    }
}

/// Everything below forwards to [`Inner`]. The split exists only so the flush
/// timer can hold the state without holding the thread handle that joins it.
impl Engine {
    /// Resolve a series name to a handle. Do this once, outside the hot loop.
    pub fn series(&self, name: &str) -> Result<Arc<SeriesHandle>> {
        self.inner.series(name)
    }

    /// Append one record.
    pub fn append(&self, handle: &SeriesHandle, row: &Row<'_>) -> Result<()> {
        self.inner.append(handle, row)
    }

    /// Append many records under a single WAL lock acquisition.
    pub fn append_batch(&self, handle: &SeriesHandle, rows: &[Row<'_>]) -> Result<()> {
        self.inner.append_batch(handle, rows)
    }

    /// Append frames a client already encoded, without re-encoding them.
    pub fn append_raw(&self, bytes: &[u8]) -> Result<RawIngest> {
        self.inner.append_raw(bytes)
    }

    /// Seal the active segment and turn every sealed one into Parquet.
    ///
    /// Single-flight with the interval timer and the control socket: a
    /// concurrent caller waits rather than starting a second flush over the same
    /// segments.
    pub fn flush(&self) -> Result<FlushOutcome> {
        self.inner.flush()
    }

    /// Counter snapshot.
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

    /// Every resolved series handle, in arbitrary order.
    pub fn series_handles(&self) -> impl Iterator<Item = &Arc<SeriesHandle>> {
        self.inner.series_handles()
    }

    /// The data directory this engine owns.
    pub fn data_dir(&self) -> &Path {
        self.inner.data_dir()
    }

    /// Human-readable accept window, for diagnostics.
    pub fn accept_window(&self) -> (String, Option<String>) {
        self.inner.accept_window()
    }
}
