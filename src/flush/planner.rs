//! The flush job: sealed WAL segments in, sorted Parquet out.
//!
//! [`run`]'s step order is the crash-safety guarantee:
//!
//! 1. decode segments one at a time into a [`Batch`]; spill it to `runs/` as
//!    sorted runs whenever it exceeds `flush.memory_budget_bytes`
//! 2. per `(series, epoch)`, merge runs with the in-memory last batch and stream
//!    one Parquet file per hour into `tmp/<series>/<hour>/`; fsync
//! 3. commit the manifest (**the commit point**)
//! 4. rename staged files into `series/`, so anything in `series/` is committed
//! 5. delete consumed WAL segments, then regenerate `view.sql` (best-effort)
//!
//! A crash before 3 replays the flush; after it, [`sweep_uncommitted`] promotes
//! committed staged files and deletes the rest. Bad frames are counted, copied
//! to `rejects/`, and skipped, so a flush of only bad frames still commits.
//! Output is identical whether or not anything spilled.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::codec::decode_verified;
use crate::config::{Config, KeyDef};
use crate::error::{Error, Result};
use crate::flush::merge::{
    Batch, BoxedSource, GroupKey, MemRun, MergeStats, merge_bounded, runs_dir,
};
use crate::flush::parquet::{
    PartitionWriter, WrittenFile, output_file_name, parse_output_file_name,
};
use crate::flush::view::view_sql;
use crate::manifest::Manifest;
use crate::record::Value;
use crate::time::{US_PER_HOUR, hour_partition};
use crate::wal::reader::{FrameFault, SegmentReader, read_segment_into};
use crate::wal::{SEGMENT_ID_DIGITS, fsync_dir, segment_path, wal_dir};

/// Where flushed Parquet lands, relative to `data_dir`.
pub const SERIES_DIR: &str = "series";

/// Staging for [`SERIES_DIR`]; same filesystem so the rename is atomic.
pub const TMP_DIR: &str = "tmp";

/// Undecodable frames, copied verbatim.
pub const REJECTS_DIR: &str = "rejects";

pub const REJECT_EXT: &str = "rej";

pub const VIEW_FILE: &str = "view.sql";

pub fn series_dir(data_dir: impl AsRef<Path>, series: &str) -> PathBuf {
    data_dir.as_ref().join(SERIES_DIR).join(series)
}

pub fn tmp_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(TMP_DIR)
}

pub fn rejects_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(REJECTS_DIR)
}

/// What one flush did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Consumed and deleted, ascending; empty means nothing was committed.
    pub segments_consumed: Vec<u64>,
    pub files_written: Vec<WrittenFile>,
    pub rows_written: usize,
    pub rows_deduplicated: usize,
    pub frames_rejected: usize,
    /// Segment bytes past an undecodable length (record count unknowable);
    /// non-zero means storage damaged acked data. Copied to `rejects/`.
    pub bytes_skipped: u64,
}

/// Turn `segments` into Parquet, commit, and delete them. The caller must
/// ensure only one flush runs at a time.
pub fn run(
    data_dir: &Path,
    config: &Config,
    manifest: &mut Manifest,
    segments: &[u64],
) -> Result<FlushOutcome> {
    // Arrival is `(segment_id, offset)`: read order decides dedup survivors.
    let mut ids: Vec<u64> = segments.to_vec();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(FlushOutcome::default());
    }

    let mut rejects = RejectSink::new(data_dir, config.limits.reject_max_bytes);
    let mut spill = Spill::new(data_dir)?;
    let mut outcome = FlushOutcome::default();
    let mut consumed: Vec<u64> = Vec::with_capacity(ids.len());
    let mut rows_decoded = 0usize;
    let mut schemas: BTreeMap<(String, u32), Vec<KeyDef>> = BTreeMap::new();

    // Phase 1: decode into the batch, spilling over budget. One reused buffer.
    let budget = usize::try_from(config.flush.memory_budget_bytes).unwrap_or(usize::MAX);
    let mut batch = Batch::default();
    let mut buf = Vec::new();
    for (i, &id) in ids.iter().enumerate() {
        read_segment_into(segment_path(data_dir, id), &mut buf)?;
        let mut decoded = decode_segment(data_dir, id, &buf, manifest, &mut rejects, &mut batch);
        outcome.frames_rejected += decoded.frames_rejected;
        outcome.bytes_skipped += decoded.bytes_skipped;

        if decoded.unreadable {
            if !decoded.quarantined {
                // Still in `wal/`: the watermark must not pass it, or recovery
                // would delete it. Stop here.
                break;
            }
            // Moved aside, so the watermark may pass it.
            continue;
        }
        consumed.push(id);
        schemas.append(&mut decoded.schemas);
        spill.touched.append(&mut decoded.touched);
        rows_decoded += decoded.rows;

        // Never spill the last batch; it is merged from memory.
        if batch.bytes() >= budget && i + 1 < ids.len() {
            spill.spill(&mut batch)?;
        }
    }
    let ids = consumed;

    if ids.is_empty() {
        // Nothing readable to consume, so nothing may be committed.
        return Ok(FlushOutcome {
            frames_rejected: outcome.frames_rejected,
            bytes_skipped: outcome.bytes_skipped,
            ..FlushOutcome::default()
        });
    }
    if rows_decoded == 0 && outcome.frames_rejected == 0 && outcome.bytes_skipped == 0 {
        // Genuine no-op: no file, no manifest write; segments stay.
        return Ok(FlushOutcome::default());
    }

    let sink = Sink {
        data_dir,
        config,
        first_segment: *ids.first().expect("non-empty"),
        last_segment: *ids.last().expect("non-empty"),
    };
    std::fs::create_dir_all(tmp_dir(data_dir))?;

    // Phase 2: per `(series, epoch)`, merge and stage one file per hour.
    let mut staged = Vec::new();
    {
        let mut memory: BTreeMap<GroupKey, MemRun<'_>> = batch.into_sources().into_iter().collect();
        let mut groups: BTreeSet<GroupKey> = memory.keys().cloned().collect();
        groups.extend(spill.groups.keys().cloned());
        for key in groups {
            let runs = spill.groups.remove(&key).unwrap_or_default();
            let in_memory = memory.remove(&key).map(|m| Box::new(m) as BoxedSource<'_>);
            let keys = schemas.get(&key).map(Vec::as_slice).unwrap_or(&[]);
            let stats = sink.merge_partition(
                (&key.0, key.1),
                keys,
                runs,
                in_memory,
                &mut spill,
                &mut staged,
            )?;
            outcome.rows_written += stats.emitted;
            outcome.rows_deduplicated += stats.deduplicated;
        }
    }
    outcome.rows_deduplicated += batch.deduplicated();
    drop(batch);
    outcome.files_written = staged.iter().map(|(w, _)| w.clone()).collect();
    let staged: Vec<PathBuf> = staged.into_iter().map(|(_, p)| p).collect();
    let touched = std::mem::take(&mut spill.touched);
    let touched: BTreeSet<&str> = touched.iter().map(String::as_str).collect();
    // Make staged names durable: after commit they are the only copy.
    fsync_staging(data_dir, &staged)?;

    // Commit point: above is replayable; nothing below may run before it.
    manifest.last_flushed_segment = manifest.last_flushed_segment.max(sink.last_segment);
    manifest.last_flush_at = Some(now_us());
    manifest.commit(data_dir)?;
    outcome.segments_consumed = ids;

    // On failure the rest stay in `tmp/`; `sweep_uncommitted` promotes them.
    for path in &staged {
        promote(data_dir, path)?;
    }

    delete_segments(data_dir, &outcome.segments_consumed)?;
    regenerate_views(data_dir, manifest, &touched);
    drop(spill);

    tracing::info!(
        segments = ?outcome.segments_consumed,
        files = outcome.files_written.len(),
        rows = outcome.rows_written,
        deduplicated = outcome.rows_deduplicated,
        rejected = outcome.frames_rejected,
        bytes_skipped = outcome.bytes_skipped,
        "flush committed"
    );
    Ok(outcome)
}

/// Empty a vector and change its element lifetime, reusing the allocation
/// (in-place collect), so per-row decode scratch is not reallocated.
fn recycle<T, U>(mut v: Vec<T>) -> Vec<U> {
    v.clear();
    v.into_iter()
        .map(|_| unreachable!("emptied above"))
        .collect()
}

/// Arity and types match the schema. The codec does not check types, so this
/// keeps column builders from failing on data. [`Value::Null`] matches any type.
fn matches_schema(keys: &[KeyDef], values: &[Value<'_>]) -> bool {
    keys.len() == values.len()
        && std::iter::zip(keys, values).all(|(k, v)| v.key_type().is_none_or(|ty| ty == k.ty))
}

/// What decoding one segment produced, besides the batched records.
#[derive(Debug, Default)]
struct Decoded {
    /// Owned: the manifest is mutated at commit.
    schemas: BTreeMap<(String, u32), Vec<KeyDef>>,
    /// Series seen, even if all rows were rejected; their views are regenerated.
    touched: BTreeSet<String>,
    rows: usize,
    frames_rejected: usize,
    bytes_skipped: u64,
    /// Header unparseable: never consumed or deleted.
    unreadable: bool,
    /// Moved out of `wal/` by [`quarantine_segment`].
    quarantined: bool,
}

/// Decode one segment into `batch`. Bad frames go to `rejects/`; an unreadable
/// header is reported for the caller to handle.
fn decode_segment(
    data_dir: &Path,
    segment_id: u64,
    buf: &[u8],
    manifest: &Manifest,
    rejects: &mut RejectSink,
    batch: &mut Batch,
) -> Decoded {
    let mut out = Decoded::default();
    // Small linear caches of manifest lookups; the reader owns the arity one.
    let mut keys_cache: Vec<(String, u32, Vec<KeyDef>)> = Vec::new();
    let mut arities: Vec<(String, u32, Option<usize>)> = Vec::new();

    // Frame cap is the segment size, not `limits.max_record_bytes`, so lowering
    // that config cannot truncate sealed segments.
    let key_count = |series: &str, epoch: u32| -> Option<usize> {
        if let Some(&(_, _, arity)) = arities.iter().find(|(s, e, _)| s == series && *e == epoch) {
            return arity;
        }
        let arity = manifest.schema(series, epoch).map(<[KeyDef]>::len);
        arities.push((series.to_string(), epoch, arity));
        arity
    };
    let mut reader = match SegmentReader::new(buf, buf.len(), key_count) {
        Ok(r) => r,
        Err(err) => {
            // Frames may be intact: mark unreadable (never deleted) whether or
            // not the quarantine rename succeeds.
            tracing::error!(segment = segment_id, %err, "wal segment header is unreadable");
            out.unreadable = true;
            out.quarantined = quarantine_segment(data_dir, segment_id);
            out.bytes_skipped += buf.len() as u64;
            return out;
        }
    };
    if reader.segment_id() != segment_id {
        // Arrival uses the file id, so a renamed segment can reorder dedup.
        tracing::warn!(
            segment = segment_id,
            header_id = reader.segment_id(),
            "segment header disagrees with its file name"
        );
    }

    while let Some(item) = reader.next_frame() {
        let frame = match item {
            Ok(frame) => frame,
            Err(fault) => {
                out.frames_rejected += 1;
                match fault {
                    // Extent known (CRC agreed): keep the frame verbatim.
                    FrameFault::BadRecord { offset, len, .. }
                    | FrameFault::UnknownSchema { offset, len, .. } => {
                        tracing::warn!(segment = segment_id, %fault, "rejecting a frame");
                        rejects.append(segment_id, &buf[offset..offset + len]);
                    }
                    // Nothing after this is locatable: storage damaged acked
                    // bytes. Copy the tail to rejects and count its bytes.
                    FrameFault::Undecodable { offset, .. } => {
                        let tail = &buf[offset..];
                        out.bytes_skipped += tail.len() as u64;
                        tracing::error!(
                            segment = segment_id,
                            %fault,
                            bytes_skipped = tail.len(),
                            "stopping this segment's scan; the remainder goes to rejects/"
                        );
                        rejects.append(segment_id, tail);
                    }
                }
                continue;
            }
        };

        let record = frame.record;
        let bytes = &buf[frame.offset..frame.offset + frame.len];
        if !out.touched.contains(record.series) {
            out.touched.insert(record.series.to_string());
        }
        // Resolved already, or the frame would be `UnknownSchema`.
        let keys = cached_keys(&mut keys_cache, manifest, record.series, record.epoch);

        // Types are unchecked by the codec; reject rather than fail the flush.
        if !matches_schema(keys, reader.keys()) {
            out.frames_rejected += 1;
            tracing::warn!(
                series = record.series,
                epoch = record.epoch,
                segment = segment_id,
                offset = frame.offset,
                "rejecting a frame whose key values disagree with the schema its epoch names"
            );
            rejects.append(segment_id, bytes);
            continue;
        }

        batch.push(
            (record.series, record.epoch),
            record.ts,
            record.id,
            (segment_id, frame.offset as u64),
            bytes,
        );
        out.rows += 1;
    }
    out.schemas = keys_cache
        .into_iter()
        .map(|(series, epoch, keys)| ((series, epoch), keys))
        .collect();
    out
}

fn cached_keys<'c>(
    cache: &'c mut Vec<(String, u32, Vec<KeyDef>)>,
    manifest: &Manifest,
    series: &str,
    epoch: u32,
) -> &'c [KeyDef] {
    let i = match cache
        .iter()
        .position(|(s, e, _)| s == series && *e == epoch)
    {
        Some(i) => i,
        None => {
            let keys = manifest.schema(series, epoch).map(<[KeyDef]>::to_vec);
            cache.push((series.to_string(), epoch, keys.unwrap_or_default()));
            cache.len() - 1
        }
    };
    &cache[i].2
}

#[derive(Debug)]
struct Sink<'a> {
    data_dir: &'a Path,
    config: &'a Config,
    first_segment: u64,
    last_segment: u64,
}

impl Sink<'_> {
    /// Merge one `(series, epoch)` and stream it into one staged file per hour,
    /// appended to `staged`. Rows arrive sorted by `ts`, so hours are contiguous.
    fn merge_partition(
        &self,
        (series, epoch): (&str, u32),
        keys: &[KeyDef],
        runs: Vec<PathBuf>,
        in_memory: Option<BoxedSource<'_>>,
        spill: &mut Spill,
        staged: &mut Vec<(WrittenFile, PathBuf)>,
    ) -> Result<MergeStats> {
        // Named by segment range, so a re-run overwrites its own output.
        let name = output_file_name(self.first_segment, self.last_segment, epoch);
        let flush = &self.config.flush;

        let finish = |(hour, path, writer): (String, PathBuf, PartitionWriter),
                      staged: &mut Vec<(WrittenFile, PathBuf)>|
         -> Result<()> {
            let rows = writer.finish()?;
            let written = WrittenFile {
                series: series.to_string(),
                epoch,
                hour,
                rows,
            };
            staged.push((written, path));
            Ok(())
        };

        let mut open: Option<(u64, (String, PathBuf, PartitionWriter))> = None;
        let mut spare_values: Vec<Value<'static>> = Vec::with_capacity(keys.len());
        let mut spare_extra: Vec<(&'static str, &'static str)> = Vec::new();
        let stats = merge_bounded(
            runs,
            in_memory,
            flush.merge_fan_in,
            || spill.next_path(),
            |entry| {
                let hour_index = entry.ts / US_PER_HOUR;
                if open.as_ref().is_none_or(|(h, _)| *h != hour_index) {
                    if let Some((_, file)) = open.take() {
                        finish(file, staged)?;
                    }
                    // `tmp/` mirrors `series/`.
                    let hour = hour_partition(entry.ts);
                    let dir = tmp_dir(self.data_dir).join(series).join(&hour);
                    std::fs::create_dir_all(&dir)?;
                    let path = dir.join(&name);
                    let writer = PartitionWriter::create(
                        &path,
                        series,
                        epoch,
                        keys,
                        &flush.compression,
                        flush.row_group_rows,
                    )?;
                    open = Some((hour_index, (hour, path, writer)));
                }

                // Verified at segment read; no second CRC.
                let mut values = recycle(std::mem::take(&mut spare_values));
                let mut extra = recycle(std::mem::take(&mut spare_extra));
                let (record, _) = decode_verified(entry.frame, keys.len(), &mut values, &mut extra)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("re-decoding a frame of {series}: {e}"),
                        )
                    })?;
                let (_, (_, _, writer)) = open.as_mut().expect("opened above");
                writer.append(record.ts, record.id, &values, &extra, record.data)?;
                spare_values = recycle(values);
                spare_extra = recycle(extra);
                Ok(())
            },
        )?;
        if let Some((_, file)) = open {
            finish(file, staged)?;
        }
        Ok(stats)
    }
}

/// Spilled runs by `(series, epoch)`. Dropping it removes `runs/`; after a crash
/// [`sweep_uncommitted`] does.
#[derive(Debug)]
struct Spill {
    dir: PathBuf,
    next: u64,
    groups: BTreeMap<GroupKey, Vec<PathBuf>>,
    /// Series whose `view.sql` to regenerate.
    touched: BTreeSet<String>,
}

impl Spill {
    fn new(data_dir: &Path) -> Result<Self> {
        let dir = runs_dir(data_dir);
        clear_runs(&dir);
        Ok(Self {
            dir,
            next: 0,
            groups: BTreeMap::new(),
            touched: BTreeSet::new(),
        })
    }

    fn next_path(&mut self) -> PathBuf {
        self.next += 1;
        self.dir.join(format!("{:010}.run", self.next))
    }

    fn spill(&mut self, batch: &mut Batch) -> Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let runs = batch.spill(|| self.next_path())?;
        for (key, path) in runs {
            self.groups.entry(key).or_default().push(path);
        }
        Ok(())
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        clear_runs(&self.dir);
    }
}

/// Remove `runs/`. Best-effort: leftovers cost only disk.
fn clear_runs(dir: &Path) {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(error = %e, path = %dir.display(), "could not clear merge runs"),
    }
}

/// Fsync each directory between `tmp/` and the staged files, once.
fn fsync_staging(data_dir: &Path, staged: &[PathBuf]) -> Result<()> {
    let root = tmp_dir(data_dir);
    let mut dirs: BTreeSet<&Path> = BTreeSet::new();
    for path in staged {
        let mut dir = path.parent();
        while let Some(d) = dir.filter(|d| *d != root) {
            dirs.insert(d);
            dir = d.parent();
        }
    }
    for dir in dirs {
        fsync_dir(dir)?;
    }
    fsync_dir(&root)?;
    Ok(())
}

/// Rename a committed staged file from `tmp/` into `series/`. Only call after
/// the manifest covers it. Idempotent.
fn promote(data_dir: &Path, staged: &Path) -> Result<()> {
    let rel = staged
        .strip_prefix(tmp_dir(data_dir))
        .map_err(|_| Error::Config(format!("{} is not under tmp/", staged.display())))?;
    let dest = data_dir.join(SERIES_DIR).join(rel);
    let dir = dest
        .parent()
        .expect("tmp/<series>/<hour>/<file> has a parent");
    std::fs::create_dir_all(dir)?;
    std::fs::rename(staged, &dest)?;

    // Sync the whole chain: a new `hour=` dir could otherwise be lost.
    fsync_dir(dir)?;
    if let Some(series_root) = dir.parent() {
        fsync_dir(series_root)?;
    }
    fsync_dir(&data_dir.join(SERIES_DIR))?;
    Ok(())
}

/// Clean up after an interrupted flush. A file is committed iff its
/// `last_segment <= last_flushed_segment`.
///
/// - staged files: committed ones are promoted, others deleted (a delete
///   failure fails the sweep, or a later commit would make it look committed)
/// - flat files directly in `tmp/` (older layout): deleted
/// - uncommitted Parquet in `series/` (older binaries): deleted
///
/// `last_flushed_segment` must come from a manifest read off disk (see
/// [`require_manifest_for_output`]). Callers must hold the flush or directory lock.
pub fn sweep_uncommitted(data_dir: &Path, last_flushed_segment: u64) -> Result<()> {
    clear_runs(&runs_dir(data_dir));

    let mut promoted = 0usize;
    let mut discarded = 0usize;
    let mut emptied: BTreeSet<PathBuf> = BTreeSet::new();
    for (path, range) in staged_files(data_dir) {
        match range {
            Some((_, last, _)) if last <= last_flushed_segment => {
                promote(data_dir, &path)?;
                promoted += 1;
            }
            _ => {
                std::fs::remove_file(&path)?;
                discarded += 1;
            }
        }
        emptied.extend(path.parent().map(Path::to_path_buf));
    }
    if promoted > 0 {
        tracing::warn!(
            files = promoted,
            last_flushed_segment,
            "published committed parquet that an interrupted flush left in tmp/"
        );
    }
    if discarded > 0 {
        tracing::info!(
            files = discarded,
            "removed staged files from an uncommitted flush"
        );
    }
    // `remove_dir` only removes empty dirs: hour first, then series.
    for hour in &emptied {
        let _ = std::fs::remove_dir(hour);
        if let Some(series) = hour.parent() {
            let _ = std::fs::remove_dir(series);
        }
    }

    // Flat files in `tmp/`: older layout, uncommitted by definition.
    let (files, bytes) = std::fs::read_dir(tmp_dir(data_dir))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().is_file())
        .fold((0u64, 0u64), |(files, bytes), e| {
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(e.path()) {
                Ok(()) => (files + 1, bytes + size),
                Err(err) => {
                    tracing::warn!(error = %err, path = %e.path().display(), "could not remove a stale staged file");
                    (files, bytes)
                }
            }
        });
    if files > 0 {
        tracing::info!(
            files,
            bytes,
            "removed staged files from an uncommitted flush"
        );
    }

    let mut removed = Vec::new();
    let mut emptied: BTreeSet<PathBuf> = BTreeSet::new();
    for (path, (_, last, _)) in output_files(data_dir) {
        if last <= last_flushed_segment {
            continue;
        }
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(error = %e, path = %path.display(), "could not remove an orphan parquet file");
            continue;
        }
        emptied.extend(path.parent().map(Path::to_path_buf));
        removed.push(path);
    }
    if !removed.is_empty() {
        // Loud on purpose; nothing is lost, the rows are still in the WAL.
        tracing::warn!(
            files = removed.len(),
            last_flushed_segment,
            "removed parquet files from a flush that never committed; their rows are still in the wal"
        );
        for path in &removed {
            tracing::info!(path = %path.display(), "removed orphan parquet");
        }
    }
    // Only partitions left empty are removed.
    for hour in &emptied {
        let _ = std::fs::remove_dir(hour);
    }
    Ok(())
}

/// Refuse a data directory with flush output but no manifest: a watermark of 0
/// would make [`sweep_uncommitted`] delete everything. Not repaired, since the
/// watermark cannot be inferred. A fresh directory passes.
pub fn require_manifest_for_output(data_dir: &Path) -> Result<()> {
    // Staged files count too: they may be the only copy of committed rows.
    let mut found: Vec<PathBuf> = output_files(data_dir).into_iter().map(|(p, _)| p).collect();
    found.extend(
        staged_files(data_dir)
            .into_iter()
            .filter(|(_, range)| range.is_some())
            .map(|(p, _)| p),
    );
    let Some(first) = found.first() else {
        return Ok(());
    };
    Err(Error::Config(format!(
        "{} has no {} but {}/ or {}/ already holds {} parquet file(s) this engine wrote \
         (for example {}). Refusing to start: the manifest is the only record of how \
         far the flush got, and without it every one of those files looks like debris \
         from a flush that never committed -- so starting would delete them. \
         Restore {} from a backup, or move {}/ and {}/ aside if you mean to start fresh.",
        data_dir.display(),
        crate::manifest::MANIFEST_FILE,
        SERIES_DIR,
        TMP_DIR,
        found.len(),
        first.display(),
        crate::manifest::MANIFEST_FILE,
        SERIES_DIR,
        TMP_DIR,
    )))
}

/// Flush-output files under `series/` with their parsed range. Unrecognised
/// names are excluded; unlistable directories are skipped (external pruning).
fn output_files(data_dir: &Path) -> Vec<(PathBuf, (u64, u64, u32))> {
    let entries = |dir: PathBuf| std::fs::read_dir(dir).into_iter().flatten().flatten();
    let mut out: Vec<_> = entries(data_dir.join(SERIES_DIR))
        .flat_map(|series| entries(series.path()))
        .flat_map(|hour| entries(hour.path()))
        .filter_map(|f| Some((f.path(), parse_output_file_name(f.file_name().to_str()?)?)))
        .collect();
    out.sort();
    out
}

/// `(first_segment, last_segment, epoch)` from [`parse_output_file_name`].
type OutputRange = (u64, u64, u32);

/// Files under `tmp/<series>/<hour>/` with their range, `None` if unparseable
/// (`tmp/` is the engine's, so those get cleared too).
fn staged_files(data_dir: &Path) -> Vec<(PathBuf, Option<OutputRange>)> {
    let entries = |dir: PathBuf| std::fs::read_dir(dir).into_iter().flatten().flatten();
    let dirs = |e: &std::fs::DirEntry| e.file_type().is_ok_and(|t| t.is_dir());
    let mut out: Vec<_> = entries(tmp_dir(data_dir))
        .filter(dirs)
        .flat_map(|series| entries(series.path()))
        .filter(dirs)
        .flat_map(|hour| entries(hour.path()))
        .filter(|f| f.file_type().is_ok_and(|t| t.is_file()))
        .map(|f| {
            let range = f.file_name().to_str().and_then(parse_output_file_name);
            (f.path(), range)
        })
        .collect();
    out.sort();
    out
}

/// Move a segment out of `wal/` without deleting it. Returns whether the rename
/// happened; failure is logged only, as the segment stays unconsumed anyway.
fn quarantine_segment(data_dir: &Path, id: u64) -> bool {
    match crate::wal::quarantine_segment(data_dir, id) {
        Ok(to) => {
            tracing::error!(
                segment = id,
                path = %to.display(),
                "quarantined an unreadable wal segment; its records are NOT in the dataset"
            );
            true
        }
        Err(e) => {
            tracing::error!(error = %e, segment = id, "could not quarantine an unreadable wal segment");
            false
        }
    }
}

/// Delete committed segments; only after the commit. Missing files are fine.
fn delete_segments(data_dir: &Path, ids: &[u64]) -> Result<()> {
    for &id in ids {
        match std::fs::remove_file(segment_path(data_dir, id)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    fsync_dir(&wal_dir(data_dir))?;
    Ok(())
}

/// Rewrite `view.sql` for touched series. Failures are logged only.
fn regenerate_views(data_dir: &Path, manifest: &Manifest, series: &BTreeSet<&str>) {
    for name in series {
        let dir = series_dir(data_dir, name);
        let history = manifest.series.get(*name).map(Vec::as_slice).unwrap_or(&[]);
        let sql = view_sql(name, history);
        let path = dir.join(VIEW_FILE);
        if let Err(e) = std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, sql)) {
            tracing::warn!(error = %e, path = %path.display(), "could not regenerate view.sql");
        }
    }
}

/// Epoch microseconds, saturating; a pre-1970 clock gives 0 (always overdue).
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// `rejects/` sink: bad frames verbatim, best-effort, capped in total bytes
/// (past the cap frames are only counted).
#[derive(Debug)]
struct RejectSink {
    dir: PathBuf,
    cap: u64,
    used: Option<u64>,
    capped_logged: bool,
}

impl RejectSink {
    fn new(data_dir: &Path, cap: u64) -> Self {
        Self {
            dir: rejects_dir(data_dir),
            cap,
            // Measured lazily, on the first reject.
            used: None,
            capped_logged: false,
        }
    }

    fn append(&mut self, segment: u64, frame: &[u8]) {
        let used = *self.used.get_or_insert_with(|| dir_bytes(&self.dir));
        if used.saturating_add(frame.len() as u64) > self.cap {
            if !self.capped_logged {
                self.capped_logged = true;
                tracing::warn!(
                    dir = %self.dir.display(),
                    cap = self.cap,
                    "rejects directory is at its cap; further rejected frames are counted only"
                );
            }
            return;
        }
        if let Err(e) = self.write(segment, frame) {
            tracing::warn!(error = %e, dir = %self.dir.display(), "could not record a rejected frame");
            return;
        }
        self.used = Some(used + frame.len() as u64);
    }

    fn write(&self, segment: u64, frame: &[u8]) -> std::io::Result<()> {
        use std::io::Write;

        std::fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(format!(
            "{segment:0width$}.{REJECT_EXT}",
            width = SEGMENT_ID_DIGITS
        ));
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;
        f.write_all(frame)
    }
}

/// Total size of files directly in `dir`; 0 if missing.
fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use crate::codec::{encode, encode_segment_header, reseal};
    use crate::config::SeriesConfig;
    use crate::record::{KeyType, Row};
    use crate::wal::{CORRUPT_EXT, FIRST_SEGMENT};

    /// 2026-08-08T13:00:00Z, the start of an hour.
    const T13: u64 = 1_786_194_000_000_000;

    fn key(name: &str) -> KeyDef {
        KeyDef {
            name: name.into(),
            ty: KeyType::Str,
            nullable: true,
        }
    }

    fn config(series: &[(&str, Vec<KeyDef>)]) -> Config {
        Config {
            series: series
                .iter()
                .map(|(name, keys)| SeriesConfig {
                    name: (*name).into(),
                    keys: keys.clone(),
                })
                .collect(),
            ..Config::default()
        }
    }

    /// `trades` with one `symbol` key at epoch 0.
    fn trades() -> (Config, Manifest) {
        let cfg = config(&[("trades", vec![key("symbol")])]);
        let mut m = Manifest::default();
        m.reconcile(&cfg).unwrap();
        (cfg, m)
    }

    struct Rec {
        series: &'static str,
        epoch: u32,
        ts: u64,
        id: &'static str,
        keys: Vec<Value<'static>>,
    }

    fn rec(ts: u64, id: &'static str) -> Rec {
        Rec {
            series: "trades",
            epoch: 0,
            ts,
            id,
            keys: vec![Value::Str("AAPL")],
        }
    }

    fn write_segment(data_dir: &Path, id: u64, recs: &[Rec]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_segment_header(&mut buf, id);
        for r in recs {
            encode(
                &mut buf,
                r.series,
                r.epoch,
                &Row {
                    ts: r.ts,
                    id: r.id,
                    keys: &r.keys,
                    extra: &[("feed", "itch")],
                    data: "{}",
                },
            )
            .unwrap();
        }
        fs::create_dir_all(wal_dir(data_dir)).unwrap();
        fs::write(segment_path(data_dir, id), &buf).unwrap();
        buf
    }

    fn parquet_files(data_dir: &Path) -> Vec<PathBuf> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            let Ok(entries) = fs::read_dir(dir) else {
                return;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "parquet") {
                    out.push(p);
                }
            }
        }
        let mut out = Vec::new();
        walk(&data_dir.join(SERIES_DIR), &mut out);
        out.sort();
        out
    }

    fn read_rows(path: &Path) -> Vec<(u64, String)> {
        crate::flush::read::read_ts_id(path).unwrap()
    }

    #[test]
    fn an_empty_segment_list_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        let before = m.clone();

        let out = run(dir.path(), &cfg, &mut m, &[]).unwrap();
        assert_eq!(out, FlushOutcome::default());
        assert_eq!(m, before, "a no-op must not touch the manifest");
        assert!(!dir.path().join("manifest.json").exists());
    }

    #[test]
    fn a_segment_with_an_unreadable_header_is_quarantined_and_never_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), 1, &[rec(T13, "a")]);
        let mut bad = write_segment(dir.path(), 2, &[rec(T13 + 1, "b")]);
        bad[0] ^= 0xff; // ruin the magic; every frame behind it is intact
        fs::write(segment_path(dir.path(), 2), &bad).unwrap();

        let out = run(dir.path(), &cfg, &mut m, &[1, 2]).unwrap();

        assert_eq!(out.rows_written, 1, "the readable segment still flushes");
        assert_eq!(out.segments_consumed, vec![1], "2 was not consumed");
        assert!(out.bytes_skipped >= bad.len() as u64, "and is counted");

        let aside = segment_path(dir.path(), 2).with_extension(CORRUPT_EXT);
        assert!(!segment_path(dir.path(), 2).exists());
        assert_eq!(fs::read(&aside).unwrap(), bad, "every byte survives");
    }

    /// Quarantine is best-effort; staying unconsumed is not.
    #[test]
    fn a_segment_that_cannot_be_quarantined_is_still_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), 1, &[rec(T13, "a")]);
        let mut bad = write_segment(dir.path(), 2, &[rec(T13 + 1, "b")]);
        bad[0] ^= 0xff;
        fs::write(segment_path(dir.path(), 2), &bad).unwrap();

        // Block the rename with a non-empty directory at the target.
        let blocked = segment_path(dir.path(), 2).with_extension(CORRUPT_EXT);
        fs::create_dir_all(&blocked).unwrap();
        fs::write(blocked.join("occupied"), b"x").unwrap();

        let out = run(dir.path(), &cfg, &mut m, &[1, 2]).unwrap();

        assert_eq!(out.segments_consumed, vec![1], "2 was never consumed");
        assert_eq!(m.last_flushed_segment, 1, "and the watermark stops short");
        assert!(
            segment_path(dir.path(), 2).exists(),
            "the segment must still be on disk, in full"
        );
        assert_eq!(fs::read(segment_path(dir.path(), 2)).unwrap(), bad);
    }

    /// Recovery deletes ids <= watermark, so the watermark must not pass it.
    #[test]
    fn an_unquarantinable_segment_blocks_the_watermark_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        let mut bad = write_segment(dir.path(), 1, &[rec(T13, "a"), rec(T13 + 1, "b")]);
        write_segment(dir.path(), 2, &[rec(T13 + 2, "c")]);
        write_segment(dir.path(), 3, &[rec(T13 + 3, "d")]);
        bad[0] ^= 0xff; // ruin segment 1's magic; every frame behind it is intact
        fs::write(segment_path(dir.path(), 1), &bad).unwrap();

        // Block the rename.
        let blocked = segment_path(dir.path(), 1).with_extension(CORRUPT_EXT);
        fs::create_dir_all(&blocked).unwrap();
        fs::write(blocked.join("occupied"), b"x").unwrap();

        let out = run(dir.path(), &cfg, &mut m, &[1, 2, 3]).unwrap();
        assert!(
            out.segments_consumed.is_empty(),
            "nothing above it may be consumed: {:?}",
            out.segments_consumed
        );
        assert_eq!(m.last_flushed_segment, 0, "so the watermark cannot move");
        assert!(
            parquet_files(dir.path()).is_empty(),
            "and 2 and 3's rows must not be written under a range that excludes them"
        );

        // A restart must leave the segment alone.
        let rec = crate::wal::recovery::recover(dir.path(), m.last_flushed_segment).unwrap();
        assert!(rec.deleted.is_empty(), "recovery deleted {:?}", rec.deleted);
        assert_eq!(fs::read(segment_path(dir.path(), 1)).unwrap(), bad);
        for id in [2, 3] {
            assert!(
                segment_path(dir.path(), id).exists(),
                "segment {id} is still pending"
            );
        }
    }

    /// A moved-aside segment does not stall the watermark.
    #[test]
    fn a_quarantined_segment_does_not_block_the_ones_above_it() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        let mut bad = write_segment(dir.path(), 1, &[rec(T13, "a")]);
        write_segment(dir.path(), 2, &[rec(T13 + 2, "c")]);
        bad[0] ^= 0xff;
        fs::write(segment_path(dir.path(), 1), &bad).unwrap();

        let out = run(dir.path(), &cfg, &mut m, &[1, 2]).unwrap();
        assert_eq!(out.segments_consumed, vec![2]);
        assert_eq!(m.last_flushed_segment, 2);
        assert_eq!(out.rows_written, 1);
        assert!(
            segment_path(dir.path(), 1)
                .with_extension(CORRUPT_EXT)
                .exists()
        );
    }

    /// Lowering `limits.max_record_bytes` must not drop already-durable records.
    #[test]
    fn lowering_max_record_bytes_does_not_discard_durable_records() {
        let dir = tempfile::tempdir().unwrap();
        let (mut cfg, mut m) = trades();
        write_segment(
            dir.path(),
            1,
            &[rec(T13, "a"), rec(T13 + 1, "b"), rec(T13 + 2, "c")],
        );

        // Far below one frame.
        cfg.limits.max_record_bytes = 8;

        let out = run(dir.path(), &cfg, &mut m, &[1]).unwrap();
        assert_eq!(out.rows_written, 3, "every durable record still flushes");
        assert_eq!(out.frames_rejected, 0);
        assert_eq!(out.bytes_skipped, 0);
    }

    #[test]
    fn segments_that_decode_to_no_rows_write_no_file_and_commit_nothing() {
        // A sealed but empty segment.
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), FIRST_SEGMENT, &[]);

        let out = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(out, FlushOutcome::default());
        assert_eq!(m.last_flushed_segment, 0);
        assert!(m.last_flush_at.is_none());
        assert!(parquet_files(dir.path()).is_empty());
        assert!(
            segment_path(dir.path(), FIRST_SEGMENT).exists(),
            "nothing was committed, so nothing may be deleted"
        );
    }

    #[test]
    fn rows_are_sorted_deduplicated_and_split_by_hour() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        // Out of order, across three hours, with one duplicate.
        write_segment(
            dir.path(),
            FIRST_SEGMENT,
            &[
                rec(T13 + 2 * US_PER_HOUR, "c"),
                rec(T13 + 30, "b"),
                rec(T13 + US_PER_HOUR, "x"),
                rec(T13 + 10, "a"),
                rec(T13 + 30, "b"), // duplicate (ts, id)
            ],
        );

        let out = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(out.rows_written, 4);
        assert_eq!(out.rows_deduplicated, 1);
        assert_eq!(out.frames_rejected, 0);
        assert_eq!(out.segments_consumed, vec![FIRST_SEGMENT]);

        let files = parquet_files(dir.path());
        assert_eq!(files.len(), 3, "one file per hour: {files:?}");

        let mut total = 0;
        for path in &files {
            let hour = path
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let rows = read_rows(path);
            total += rows.len();
            assert!(!rows.is_empty());
            // No file spans two hour partitions...
            for (ts, _) in &rows {
                assert_eq!(&hour_partition(*ts), hour, "{path:?} row at {ts}");
            }
            // ...and rows within a file are non-decreasing in ts.
            assert!(rows.windows(2).all(|w| w[0].0 <= w[1].0), "{rows:?}");
        }
        assert_eq!(total, 4);
        assert_eq!(
            read_rows(&files[0]),
            [(T13 + 10, "a".into()), (T13 + 30, "b".into())]
        );
    }

    #[test]
    fn a_poison_frame_costs_one_record_and_the_flush_still_commits() {
        // Valid CRC and prefix, corrupt `keys`: only the flush can notice.
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();

        let mut buf = Vec::new();
        encode_segment_header(&mut buf, FIRST_SEGMENT);
        let mut offsets = Vec::new();
        for r in [
            rec(T13 + 1, "before"),
            rec(T13 + 2, "poison"),
            rec(T13 + 3, "after"),
        ] {
            offsets.push(buf.len());
            encode(
                &mut buf,
                r.series,
                r.epoch,
                &Row {
                    ts: r.ts,
                    id: r.id,
                    keys: &r.keys,
                    extra: &[],
                    data: "{}",
                },
            )
            .unwrap();
        }
        let poison_at = offsets[1];
        let poison_len = offsets[2] - poison_at;
        // The first key's tag byte: len+crc, ts, epoch, series, id.
        let tag_at = poison_at + 8 + 8 + 4 + (2 + "trades".len()) + (2 + "poison".len());
        buf[tag_at] = 0x7f;
        reseal(&mut buf[poison_at..offsets[2]]);
        fs::create_dir_all(wal_dir(dir.path())).unwrap();
        fs::write(segment_path(dir.path(), FIRST_SEGMENT), &buf).unwrap();

        let out = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(out.frames_rejected, 1);
        assert_eq!(out.rows_written, 2, "every other record must be written");
        assert_eq!(out.segments_consumed, vec![FIRST_SEGMENT]);
        assert_eq!(m.last_flushed_segment, FIRST_SEGMENT);

        let files = parquet_files(dir.path());
        assert_eq!(files.len(), 1);
        assert_eq!(
            read_rows(&files[0]),
            [(T13 + 1, "before".into()), (T13 + 3, "after".into())]
        );

        // The offending bytes are kept in rejects/.
        let rejected =
            fs::read(rejects_dir(dir.path()).join(format!("{FIRST_SEGMENT:010}.{REJECT_EXT}")))
                .unwrap();
        assert_eq!(rejected, buf[poison_at..poison_at + poison_len]);
    }

    #[test]
    fn a_segment_of_nothing_but_poison_still_commits() {
        // Must commit, or the segment is re-scanned forever.
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        let mut buf = Vec::new();
        encode_segment_header(&mut buf, FIRST_SEGMENT);
        // An epoch not in the history.
        encode(
            &mut buf,
            "trades",
            9,
            &Row {
                ts: T13,
                id: "stale",
                keys: &[],
                extra: &[],
                data: "{}",
            },
        )
        .unwrap();
        fs::create_dir_all(wal_dir(dir.path())).unwrap();
        fs::write(segment_path(dir.path(), FIRST_SEGMENT), &buf).unwrap();

        let out = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(out.frames_rejected, 1);
        assert_eq!(out.rows_written, 0);
        assert!(out.files_written.is_empty(), "no rows, so no empty file");
        assert_eq!(out.segments_consumed, vec![FIRST_SEGMENT]);
        assert!(!segment_path(dir.path(), FIRST_SEGMENT).exists());
    }

    #[test]
    fn a_value_that_disagrees_with_its_schema_is_rejected_not_fatal() {
        // An i64 in a string column passes ingest and the codec.
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(
            dir.path(),
            FIRST_SEGMENT,
            &[
                rec(T13, "good"),
                Rec {
                    series: "trades",
                    epoch: 0,
                    ts: T13 + 1,
                    id: "mistyped",
                    keys: vec![Value::I64(7)],
                },
            ],
        );

        let out = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(out.frames_rejected, 1);
        assert_eq!(out.rows_written, 1);
        assert_eq!(out.segments_consumed, vec![FIRST_SEGMENT]);
        assert_eq!(
            read_rows(&parquet_files(dir.path())[0]),
            [(T13, "good".into())]
        );
        assert!(
            rejects_dir(dir.path())
                .join(format!("{FIRST_SEGMENT:010}.{REJECT_EXT}"))
                .exists()
        );
    }

    #[test]
    fn duplicates_collapse_to_the_first_arrival_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        // The empty id dedups like any other.
        write_segment(dir.path(), 1, &[rec(T13, ""), rec(T13 + 1, "keep")]);
        write_segment(dir.path(), 2, &[rec(T13, ""), rec(T13 + 2, "keep")]);

        let out = run(dir.path(), &cfg, &mut m, &[1, 2]).unwrap();
        assert_eq!(out.rows_deduplicated, 1);
        assert_eq!(out.rows_written, 3);

        let files = parquet_files(dir.path());
        assert_eq!(files.len(), 1);
        assert_eq!(
            read_rows(&files[0]),
            [
                (T13, String::new()),
                (T13 + 1, "keep".into()),
                (T13 + 2, "keep".into())
            ]
        );
        // The filename spans the consumed range.
        assert_eq!(
            files[0].file_name().unwrap().to_str().unwrap(),
            "0000000001-0000000002.e0.parquet"
        );
    }

    /// Spilling and intermediate merge passes must not change a single byte.
    #[test]
    fn spilling_and_the_merge_fan_in_do_not_change_the_output() {
        let ids: Vec<u64> = (FIRST_SEGMENT..FIRST_SEGMENT + 9).collect();
        let flush_with = |budget: u64, fan_in: usize| {
            let dir = tempfile::tempdir().unwrap();
            let (mut cfg, mut m) = trades();
            cfg.flush.memory_budget_bytes = budget;
            cfg.flush.merge_fan_in = fan_in;
            cfg.flush.row_group_rows = 3;
            for &id in &ids {
                // Unordered, overlapping, spanning an hour boundary.
                let recs: Vec<Rec> = (0..12u64)
                    .map(|i| {
                        let ts = T13 + US_PER_HOUR - 6 + (i * 7 + id) % 13;
                        rec(ts, ["a", "b", "c"][(i % 3) as usize])
                    })
                    .collect();
                write_segment(dir.path(), id, &recs);
            }
            let outcome = run(dir.path(), &cfg, &mut m, &ids).unwrap();
            assert!(
                !runs_dir(dir.path()).exists(),
                "runs are cleared after the flush"
            );
            let files: Vec<(PathBuf, Vec<u8>)> = parquet_files(dir.path())
                .into_iter()
                .map(|p| {
                    let bytes = fs::read(&p).unwrap();
                    (p.strip_prefix(dir.path()).unwrap().to_path_buf(), bytes)
                })
                .collect();
            let rows: Vec<(u64, String)> = parquet_files(dir.path())
                .iter()
                .flat_map(|p| read_rows(p))
                .collect();
            (outcome, files, rows)
        };

        // In memory; spill per segment; plus intermediate merge passes.
        let (wide, wide_files, rows) = flush_with(1 << 30, 64);
        for (budget, fan_in) in [(1, 64), (1, 2), (1, 3)] {
            let (other, other_files, _) = flush_with(budget, fan_in);
            let case = format!("budget {budget}, fan-in {fan_in}");
            assert_eq!(other.rows_written, wide.rows_written, "{case}");
            assert_eq!(other.rows_deduplicated, wide.rows_deduplicated, "{case}");
            assert_eq!(other_files, wide_files, "{case}: byte-identical files");
        }
        assert_eq!(wide_files.len(), 2, "one file per hour");

        let mut expected = rows.clone();
        expected.sort();
        expected.dedup();
        assert_eq!(rows, expected, "sorted, with no duplicate (ts, id)");
        assert_eq!(wide.rows_written + wide.rows_deduplicated, ids.len() * 12);
    }

    /// A flush that fails part-way must not leave its runs behind.
    #[test]
    fn a_failed_flush_clears_its_runs() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), FIRST_SEGMENT, &[rec(T13, "a")]);
        // `tmp/` as a file: staging cannot create its directories.
        fs::write(tmp_dir(dir.path()), b"in the way").unwrap();
        assert!(run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).is_err());
        assert!(!runs_dir(dir.path()).exists());
    }

    #[test]
    fn a_re_run_after_a_crash_before_the_manifest_commit_is_byte_identical() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        let segment = write_segment(
            dir.path(),
            FIRST_SEGMENT,
            &[
                rec(T13 + 5, "b"),
                rec(T13 + 1, "a"),
                rec(T13 + US_PER_HOUR, "c"),
                rec(T13 + 1, "a"),
            ],
        );
        let before = m.clone();

        let first = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        let paths = parquet_files(dir.path());
        let bytes: Vec<Vec<u8>> = paths.iter().map(|p| fs::read(p).unwrap()).collect();

        // Crash before the manifest commit: the flush re-runs.
        let mut m = before;
        fs::write(segment_path(dir.path(), FIRST_SEGMENT), &segment).unwrap();

        let second = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(second.rows_written, first.rows_written);
        assert_eq!(second.rows_deduplicated, first.rows_deduplicated);

        let paths_again = parquet_files(dir.path());
        assert_eq!(paths_again, paths, "same segment range, same paths");
        for (path, was) in std::iter::zip(&paths_again, &bytes) {
            assert_eq!(
                &fs::read(path).unwrap(),
                was,
                "{path:?} must be byte-identical"
            );
        }
        // ...and no row was written twice.
        let total: usize = paths_again.iter().map(|p| read_rows(p).len()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn each_series_and_epoch_gets_its_own_file() {
        // Two epochs of one series plus another series in one segment.
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&[
            ("trades", vec![key("symbol"), key("venue")]),
            ("quotes", vec![key("px")]),
        ]);
        let mut m = Manifest::default();
        m.reconcile(&config(&[
            ("trades", vec![key("symbol")]),
            ("quotes", vec![key("px")]),
        ]))
        .unwrap();
        m.reconcile(&cfg).unwrap();
        assert_eq!(m.current_epoch("trades"), Some(1));

        write_segment(
            dir.path(),
            FIRST_SEGMENT,
            &[
                rec(T13, "old"),
                Rec {
                    series: "trades",
                    epoch: 1,
                    ts: T13 + 1,
                    id: "new",
                    keys: vec![Value::Str("AAPL"), Value::Str("XNAS")],
                },
                Rec {
                    series: "quotes",
                    epoch: 0,
                    ts: T13 + 2,
                    id: "q",
                    keys: vec![Value::Str("1.5")],
                },
            ],
        );

        let out = run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();
        assert_eq!(out.rows_written, 3);
        let names: Vec<String> = out
            .files_written
            .iter()
            .map(|f| format!("{}.e{}", f.series, f.epoch))
            .collect();
        assert_eq!(names, ["quotes.e0", "trades.e0", "trades.e1"]);

        // One schema per file.
        let files = parquet_files(dir.path());
        assert_eq!(files.len(), 3);
        assert!(files.iter().any(|p| p.to_str().unwrap().contains("trades")
            && p.to_str().unwrap().ends_with(".e1.parquet")));
    }

    #[test]
    fn committing_deletes_the_segments_and_records_the_high_water_mark() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), 4, &[rec(T13, "a")]);
        write_segment(dir.path(), 5, &[rec(T13 + 1, "b")]);
        write_segment(dir.path(), 6, &[rec(T13 + 2, "c")]);

        // Segment 6 is not offered.
        let out = run(dir.path(), &cfg, &mut m, &[4, 5]).unwrap();
        assert_eq!(out.segments_consumed, vec![4, 5]);
        assert_eq!(m.last_flushed_segment, 5);
        assert!(m.last_flush_at.is_some());
        assert!(!segment_path(dir.path(), 4).exists());
        assert!(!segment_path(dir.path(), 5).exists());
        assert!(segment_path(dir.path(), 6).exists());

        // The commit is durable, not just in memory.
        assert_eq!(Manifest::load(dir.path()).unwrap(), m);
    }

    #[test]
    fn view_sql_is_regenerated_for_every_touched_series() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(&[
            ("trades", vec![key("symbol"), key("venue")]),
            ("quotes", vec![key("px")]),
        ]);
        let mut m = Manifest::default();
        m.reconcile(&config(&[("trades", vec![key("symbol")])]))
            .unwrap();
        m.reconcile(&cfg).unwrap();

        write_segment(dir.path(), FIRST_SEGMENT, &[rec(T13, "a")]);
        run(dir.path(), &cfg, &mut m, &[FIRST_SEGMENT]).unwrap();

        let sql = fs::read_to_string(series_dir(dir.path(), "trades").join(VIEW_FILE)).unwrap();
        assert!(sql.contains("CREATE OR REPLACE VIEW \"trades\""), "{sql}");
        // `venue` was undeclared at epoch 0, so its old values are in `extra`.
        assert!(
            sql.contains("coalesce(\"venue\", try_cast(extra['venue'] AS VARCHAR))"),
            "{sql}"
        );
        // Untouched series get no view.
        assert!(!series_dir(dir.path(), "quotes").join(VIEW_FILE).exists());
    }

    #[test]
    fn flush_output_without_a_manifest_refuses_rather_than_being_swept() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), 1, &[rec(T13, "a")]);
        run(dir.path(), &cfg, &mut m, &[1]).unwrap();
        let committed = parquet_files(dir.path());
        assert_eq!(committed.len(), 1);

        // A fresh directory is the case that must still pass.
        let fresh = tempfile::tempdir().unwrap();
        assert!(require_manifest_for_output(fresh.path()).is_ok());

        let err = require_manifest_for_output(dir.path())
            .unwrap_err()
            .to_string();
        assert!(err.contains("manifest.json"), "{err}");
        assert!(err.contains("Refusing to start"), "{err}");
        assert!(committed[0].exists(), "and nothing was touched");

        // Files the engine did not write don't count.
        let stranger = tempfile::tempdir().unwrap();
        let hour = series_dir(stranger.path(), "trades").join("hour=2026-08-08T13");
        fs::create_dir_all(&hour).unwrap();
        fs::write(hour.join("notes.txt"), b"mine").unwrap();
        assert!(require_manifest_for_output(stranger.path()).is_ok());
    }

    /// Only partitions the sweep emptied are removed.
    #[test]
    fn the_sweep_removes_a_partition_it_emptied_but_not_one_it_did_not() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), 1, &[rec(T13, "a")]);
        run(dir.path(), &cfg, &mut m, &[1]).unwrap();

        let hour = parquet_files(dir.path())[0].parent().unwrap().to_path_buf();
        let orphan = hour.join(output_file_name(1, 9_999, 0));
        fs::copy(&parquet_files(dir.path())[0], &orphan).unwrap();

        // A second partition whose orphan sits next to a file of the operator's.
        let other = series_dir(dir.path(), "trades").join("hour=2026-08-08T14");
        fs::create_dir_all(&other).unwrap();
        fs::copy(
            &parquet_files(dir.path())[0],
            other.join(output_file_name(1, 9_999, 0)),
        )
        .unwrap();
        fs::write(other.join("notes.txt"), b"mine").unwrap();

        sweep_uncommitted(dir.path(), m.last_flushed_segment).unwrap();

        assert!(!orphan.exists());
        assert!(hour.is_dir(), "a partition with a committed file stays");
        assert!(other.is_dir(), "and so does one with anything else in it");
        assert!(other.join("notes.txt").exists());

        let solo = tempfile::tempdir().unwrap();
        let hour = series_dir(solo.path(), "trades").join("hour=2026-08-08T13");
        fs::create_dir_all(&hour).unwrap();
        fs::write(hour.join(output_file_name(1, 9_999, 0)), b"x").unwrap();
        sweep_uncommitted(solo.path(), 0).unwrap();
        assert!(!hour.exists(), "an emptied partition is removed");
    }
}
