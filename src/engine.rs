//! The engine: validates appends, writes the WAL, and runs the periodic flush.

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
        // Pre-1970 clock reads as 0; `ts_upper_bound` then suspends the bound.
        .unwrap_or(0)
}

// Per-thread encode scratch: no allocation on the steady path.
thread_local! {
    static SCRATCH: std::cell::RefCell<(Vec<u8>, Encoder)> =
        std::cell::RefCell::new((Vec::new(), Encoder::default()));
}

/// Cheap lower bound on encoded size (`id`, `data`, string keys), so a huge
/// record is refused before it grows [`SCRATCH`]. The exact check is post-encode.
fn oversize(row: &Row<'_>, limit: usize) -> Option<usize> {
    let mut n = row.id.len().saturating_add(row.data.len());
    for v in row.keys {
        if let crate::record::Value::Str(s) = v {
            n = n.saturating_add(s.len());
        }
    }
    (n > limit).then_some(n)
}

/// Release a rejected record's capacity, keeping up to `limit` for reuse.
fn shrink_scratch(buf: &mut Vec<u8>, limit: usize) {
    buf.clear();
    buf.shrink_to(limit);
}

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

/// A snapshot of engine counters, flattened into the `stats` reply by
/// [`StatsPayload`](crate::protocol::StatsPayload). Missing fields read as 0.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Stats {
    pub appended: u64,
    pub rejected_ts: u64,
    pub rejected_size: u64,
    pub rejected_series: u64,
    pub rejected_frames: u64,
    /// Consecutive failed flushes; with rising `wal_total_bytes`, a stalled flush.
    pub flush_failures: u64,
    pub rows_flushed: u64,
    pub rows_deduplicated: u64,
    /// Bytes in the active segment only (resets on roll).
    pub wal_bytes: u64,
    /// All WAL bytes on disk: the backlog gauge.
    pub wal_total_bytes: u64,
    pub segment: u64,
}

/// How many frames `append_raw` took and dropped (counts, not errors: remote
/// ingest is fire-and-forget).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawIngest {
    pub accepted: usize,
    pub rejected: usize,
    /// Batch tail dropped after an undecodable frame (its frame count is unknowable).
    pub discarded_bytes: usize,
}

/// The embeddable engine. State lives in `Inner`, shared with the flush timer;
/// the timer's `JoinHandle` must stay out here, or the timer could drop the last
/// `Arc` and try to join itself.
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
    /// Makes flushes single-flight across timer, `flush()` and control socket.
    flush_lock: Mutex<()>,
    counters: Counters,
    ts_min: u64,
    ts_max_skew_us: u64,
    /// Set by `close` to stop the flush timer; `wake` interrupts its wait.
    stop: Mutex<bool>,
    wake: Condvar,
}

impl Engine {
    /// Open (or create) the data directory. Order is fixed: lock, load and
    /// reconcile the manifest, sweep staged files, recover the WAL, then open the
    /// writer past the highest recovered segment.
    pub fn open(config: Config) -> Result<Engine> {
        Self::open_with(config, false)
    }

    /// As [`Engine::open`], but able to steal a lock held by a live process.
    pub fn open_with(config: Config, force_unlock: bool) -> Result<Engine> {
        config.validate()?;
        let data_dir = PathBuf::from(&config.data_dir);

        let lock = DirLock::acquire(&data_dir, force_unlock)?;

        // A lost manifest would make the sweep treat all output as uncommitted.
        if !Manifest::exists(&data_dir) {
            planner::require_manifest_for_output(&data_dir)?;
        }

        let mut manifest = Manifest::load(&data_dir)?;
        // Commit new epochs before any frame is stamped with them; otherwise a
        // restart could re-mint the same epoch for different keys.
        if manifest.reconcile(&config)? {
            manifest.commit(&data_dir)?;
        }

        // Before recovery deletes flushed segments: promote committed staged
        // files from `tmp/` into `series/`, delete uncommitted ones.
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

        // Only if overdue, so frequent restarts do not each emit a tiny file.
        if inner.config.flush.on_startup && (!rec.pending.is_empty() && inner.flush_overdue()) {
            match inner.flush() {
                Ok(o) if o.rows_written > 0 => {
                    tracing::info!(rows = o.rows_written, "startup flush")
                }
                Ok(_) => {}
                Err(e) => tracing::error!(error = %e, "startup flush failed; ingest continues"),
            }
        }

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

    /// Stop and join the flush timer, then close the WAL (so an in-progress
    /// flush finishes against a live WAL). Idempotent; `Drop` calls it.
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
            // A failed join must not fail a close that still has a WAL to flush.
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

/// The periodic flush. Waits on a condvar so `close` can interrupt it.
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
        if !timeout.timed_out() {
            continue;
        }

        // Failures are logged, never fatal: the WAL is uncapped.
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
        let interval = self.config.flush.interval_ms.saturating_mul(1_000);
        match self.manifest.lock().ok().and_then(|m| m.last_flush_at) {
            None => true,
            Some(last) => now_us().saturating_sub(last) >= interval,
        }
    }

    /// The accept window's upper bound; `None` while the host clock reads
    /// before `ts_min` (unset clock), rather than rejecting everything.
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

    /// Seal the active segment and flush every sealed one. Single-flight.
    pub fn flush(&self) -> Result<FlushOutcome> {
        let _guard = self.flush_lock.lock().unwrap_or_else(|e| e.into_inner());

        self.wal.seal()?;
        let pending = crate::wal::list_segments(&self.data_dir)?;
        let mut manifest = self.manifest.lock().unwrap_or_else(|e| e.into_inner());

        // Skip the active segment and any already committed (else a segment
        // whose deletion failed would be flushed twice).
        let active = self.wal.segment();
        let floor = manifest.last_flushed_segment;

        // After a failed flush, settle its staged files in `tmp/` (startup may
        // be months away). Skipped otherwise: the sweep is not free.
        if self.counters.flush_failures.load(Ordering::Relaxed) > 0 {
            planner::sweep_uncommitted(&self.data_dir, floor)?;
        }

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
                self.counters.flush_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %e, "flush failed; ingest continues and the wal grows");
                Err(e)
            }
        }
    }

    /// Stop the committer and fsync. The directory lock goes with `Inner`.
    fn close(&self) -> Result<()> {
        self.wal.close()
    }
}

impl Engine {
    /// Resolve a series name to a handle. Do this once, outside the hot loop.
    pub fn series(&self, name: &str) -> Result<Arc<SeriesHandle>> {
        self.inner
            .handles
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownSeries(name.to_string()))
    }

    /// Append one record: validate, encode, copy into the WAL's in-memory
    /// buffer. No disk I/O; durable after the next committer fsync.
    pub fn append(&self, handle: &SeriesHandle, row: &Row<'_>) -> Result<()> {
        self.inner.check_row(handle, row)?;
        let limit = self.inner.config.limits.max_record_bytes;
        if let Some(size) = oversize(row, limit) {
            self.inner
                .counters
                .rejected_size
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::RecordTooLarge { size, limit });
        }
        SCRATCH.with(|cell| {
            let (buf, enc) = &mut *cell.borrow_mut();
            buf.clear();
            enc.encode(buf, handle.name(), handle.epoch(), row)?;
            if buf.len() > limit {
                self.inner
                    .counters
                    .rejected_size
                    .fetch_add(1, Ordering::Relaxed);
                let size = buf.len();
                shrink_scratch(buf, limit);
                return Err(Error::RecordTooLarge { size, limit });
            }
            self.inner.wal.append(buf)
        })?;
        self.inner.counters.appended.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Append many records under a single WAL lock acquisition.
    pub fn append_batch(&self, handle: &SeriesHandle, rows: &[Row<'_>]) -> Result<()> {
        let limit = self.inner.config.limits.max_record_bytes;
        for row in rows {
            self.inner.check_row(handle, row)?;
            if let Some(size) = oversize(row, limit) {
                self.inner
                    .counters
                    .rejected_size
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::RecordTooLarge { size, limit });
            }
        }
        SCRATCH.with(|cell| {
            let (buf, enc) = &mut *cell.borrow_mut();
            buf.clear();
            for row in rows {
                let start = buf.len();
                enc.encode(buf, handle.name(), handle.epoch(), row)?;
                if buf.len() - start > limit {
                    self.inner
                        .counters
                        .rejected_size
                        .fetch_add(1, Ordering::Relaxed);
                    let size = buf.len() - start;
                    shrink_scratch(buf, limit);
                    return Err(Error::RecordTooLarge { size, limit });
                }
            }
            self.inner.wal.append_batch(buf)
        })?;
        self.inner
            .counters
            .appended
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Append client-encoded frames as-is. Only the fixed prefix (CRC, length,
    /// `ts`, series/epoch) is checked here; the rest at flush. Invalid frames
    /// are counted and skipped.
    pub fn append_raw(&self, mut bytes: &[u8]) -> Result<RawIngest> {
        let limit = self.inner.config.limits.max_record_bytes;
        let mut out = RawIngest::default();
        // Not pre-reserved: the batch length is untrusted.
        let mut good: Vec<u8> = Vec::new();

        while !bytes.is_empty() {
            let prefix = match codec::validate_prefix(bytes, limit) {
                Ok(p) => p,
                Err(e) => {
                    // Untrusted length: the next frame cannot be located, so
                    // drop the rest of the batch.
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

            if self.inner.check_ts(prefix.ts).is_err() {
                out.rejected += 1;
                continue;
            }
            let known = self
                .inner
                .handles
                .get(prefix.series)
                .is_some_and(|h| h.epoch() >= prefix.epoch);
            if !known {
                self.inner
                    .counters
                    .rejected_series
                    .fetch_add(1, Ordering::Relaxed);
                out.rejected += 1;
                continue;
            }
            good.extend_from_slice(frame);
            out.accepted += 1;
        }

        if !good.is_empty() {
            self.inner.wal.append_batch(&good)?;
            self.inner
                .counters
                .appended
                .fetch_add(out.accepted as u64, Ordering::Relaxed);
        }
        if out.rejected > 0 {
            self.inner
                .counters
                .rejected_frames
                .fetch_add(out.rejected as u64, Ordering::Relaxed);
        }
        Ok(out)
    }

    /// Counter snapshot.
    pub fn stats(&self) -> Stats {
        let w = self.inner.wal.stats();
        let c = &self.inner.counters;
        // On failure fall back to the active segment, never 0.
        let wal_total_bytes = match crate::wal::total_bytes(&self.inner.data_dir) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "could not measure total wal bytes");
                w.segment_bytes
            }
        };
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
            wal_total_bytes,
            segment: w.segment,
        }
    }

    /// Every series handle, in arbitrary order (feeds the control handshake).
    pub fn series_handles(&self) -> impl Iterator<Item = &Arc<SeriesHandle>> {
        self.inner.handles.values()
    }

    /// The data directory this engine owns.
    pub fn data_dir(&self) -> &Path {
        &self.inner.data_dir
    }

    /// Human-readable accept window, for diagnostics.
    pub fn accept_window(&self) -> (String, Option<String>) {
        (
            format_rfc3339_utc(self.inner.ts_min),
            self.inner.ts_upper_bound().map(format_rfc3339_utc),
        )
    }

    /// Seal the active segment and flush every sealed one to Parquet.
    /// Single-flight: a concurrent caller waits.
    pub fn flush(&self) -> Result<FlushOutcome> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, SeriesConfig};

    /// 2026-08-08T13:00:00Z.
    const T13: u64 = 1_786_194_000_000_000;

    fn engine(dir: &Path) -> Engine {
        let config = Config {
            data_dir: dir.to_path_buf(),
            series: vec![SeriesConfig {
                name: "trades".into(),
                keys: vec![],
            }],
            ..Config::default()
        };
        Engine::open(config).unwrap()
    }

    fn row<'a>(ts: u64, data: &'a str) -> Row<'a> {
        Row {
            ts,
            id: "",
            keys: &[],
            extra: &[],
            data,
        }
    }

    #[test]
    fn an_oversized_record_does_not_pin_the_encode_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(dir.path());
        let trades = e.series("trades").unwrap();
        let limit = Config::default().limits.max_record_bytes;

        e.append(&trades, &row(T13, "small")).unwrap();
        let huge = "x".repeat(64 * 1024 * 1024);
        assert!(matches!(
            e.append(&trades, &row(T13 + 1, &huge)),
            Err(Error::RecordTooLarge { .. })
        ));

        SCRATCH.with(|cell| {
            assert!(
                cell.borrow().0.capacity() <= limit,
                "the scratch kept {} bytes for a record that was refused",
                cell.borrow().0.capacity()
            );
        });

        e.append(&trades, &row(T13 + 2, "after")).unwrap();
        assert_eq!(e.stats().appended, 2);
        assert_eq!(e.stats().rejected_size, 1);
    }

    #[test]
    fn stats_report_the_whole_wal_not_just_the_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config {
            data_dir: dir.path().to_path_buf(),
            wal: crate::config::WalConfig {
                segment_max_bytes: 2048,
                fsync_interval_ms: 1,
                ..Config::default().wal
            },
            series: vec![SeriesConfig {
                name: "trades".into(),
                keys: vec![],
            }],
            ..Config::default()
        };
        // A segment must hold one max-size record.
        config.limits.max_record_bytes = 512;

        let e = Engine::open(config).unwrap();
        let trades = e.series("trades").unwrap();

        let payload = "x".repeat(200);
        for i in 0..400u64 {
            e.append(&trades, &row(T13 + i, &payload)).unwrap();
            // The committer rolls segments.
            std::thread::sleep(Duration::from_millis(1));
        }

        // Measured before the snapshot: the log only grows, so no race.
        let real = crate::wal::total_bytes(dir.path()).unwrap();
        let s = e.stats();
        assert!(
            s.segment > crate::wal::FIRST_SEGMENT,
            "the test needs rolls"
        );
        assert!(
            s.wal_total_bytes >= real,
            "reported {} against {real} already on disk",
            s.wal_total_bytes
        );
        assert!(
            s.wal_bytes < s.wal_total_bytes,
            "the active segment alone ({}) cannot be the whole log ({})",
            s.wal_bytes,
            s.wal_total_bytes
        );
    }
}
