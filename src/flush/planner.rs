//! The flush job: sealed WAL segments in, sorted Parquet out.
//!
//! [`run`] is the whole job, and **its step order is the crash-safety
//! guarantee**, not an implementation detail:
//!
//! 1. decode every listed segment, grouping rows by `(series, epoch)`
//! 2. stable-sort each group by `(ts, id, arrival)` and drop `(ts, id)`
//!    duplicates, keeping the first arrival
//! 3. write one Parquet file per `(series, epoch, hour)` into `tmp/`, fsync it,
//!    rename it into `series/<name>/hour=…/`, fsync that directory
//! 4. commit the manifest — **this rename is the commit point**
//! 5. only then delete the consumed WAL segments
//! 6. regenerate `view.sql`, which is derived and therefore not worth failing on
//!
//! A crash anywhere leaves either the old manifest — the segments are still on
//! disk, so the flush re-runs — or the new one, whose orphan segments startup
//! deletes on `id <= last_flushed_segment`. No window loses data or
//! double-writes: [`output_file_name`] is a function of the *consumed segment
//! range*, so a re-run writes the same paths with the same bytes.
//!
//! # A bad frame costs one record, never the flush
//!
//! Ingest validated only a frame's fixed prefix, so a malformed `keys` section
//! first surfaces here, an hour later. Aborting would mean no Parquet is ever
//! produced again and a WAL that grows until the volume fills, so every fault is
//! counted, copied to `rejects/<segment_id>.rej`, and skipped.
//!
//! Which is why a flush that decoded *only* bad frames still commits. The
//! documented no-op — no empty files, no manifest write — is the genuinely empty
//! case; refusing to commit an all-rejected segment would rescan it forever.
//!
//! # Memory: M4 holds one flush in memory
//!
//! Every row of every listed segment is held until the last file is written, and
//! each group takes a single in-memory sort. Bounded by what the caller hands
//! over, not by a budget: an hour at a high ingest rate does not fit. **M5
//! replaces this with per-segment sorted runs and a bounded-fan-in k-way
//! merge** — correct before scale-proof, deliberately.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{Config, KeyDef};
use crate::error::{Error, Result};
use crate::flush::parquet::{
    RowBuilder, WrittenFile, output_file_name, parse_output_file_name, write_file,
};
use crate::flush::view::view_sql;
use crate::manifest::Manifest;
use crate::record::Value;
use crate::time::hour_partition;
use crate::wal::reader::{FrameFault, SegmentReader, read_segment};
use crate::wal::recovery::CORRUPT_EXT;
use crate::wal::{SEGMENT_ID_DIGITS, fsync_dir, segment_path, wal_dir};

/// Where flushed Parquet lands, relative to `data_dir`.
pub const SERIES_DIR: &str = "series";

/// Staging for files that are about to be renamed into [`SERIES_DIR`]. It must
/// share a filesystem with it, or the rename stops being atomic — which is why
/// it is a sibling directory rather than the system temp dir.
pub const TMP_DIR: &str = "tmp";

/// Where frames that could not be decoded are copied verbatim.
pub const REJECTS_DIR: &str = "rejects";

/// Extension of a rejects file.
pub const REJECT_EXT: &str = "rej";

/// The generated DuckDB view, one per series directory.
pub const VIEW_FILE: &str = "view.sql";

/// Microseconds in an hour — the width of one `hour=` partition.
const US_PER_HOUR: u64 = 3_600_000_000;

/// `<data_dir>/series/<name>`.
pub fn series_dir(data_dir: impl AsRef<Path>, series: &str) -> PathBuf {
    data_dir.as_ref().join(SERIES_DIR).join(series)
}

/// `<data_dir>/tmp`.
pub fn tmp_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(TMP_DIR)
}

/// `<data_dir>/rejects`.
pub fn rejects_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(REJECTS_DIR)
}

/// What one flush did, for `stats` and for the caller's logs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Segments turned into Parquet and then deleted, ascending. Empty for a
    /// no-op, which is what tells the caller nothing was committed.
    pub segments_consumed: Vec<u64>,
    pub files_written: Vec<WrittenFile>,
    pub rows_written: usize,
    /// Rows dropped because an earlier arrival carried the same `(ts, id)`.
    pub rows_deduplicated: usize,
    /// Frames that could not be decoded, copied to `rejects/`.
    pub frames_rejected: usize,
    /// Bytes of sealed segment never scanned, because an undecodable length made
    /// everything after it unlocatable.
    ///
    /// Not `frames_rejected`, which counts records: how many records are in here
    /// is precisely what is unknowable. Non-zero means the storage damaged
    /// already-acknowledged data. The bytes go to `rejects/`.
    pub bytes_skipped: u64,
}

/// Turn `segments` into Parquet, commit, and delete them.
///
/// The caller owns single-flight: the hourly timer, a manual `flush()` and the
/// control socket all funnel through one lock, because two concurrent runs over
/// overlapping segment lists would write the same filenames from two different
/// row sets.
pub fn run(
    data_dir: &Path,
    config: &Config,
    manifest: &mut Manifest,
    segments: &[u64],
) -> Result<FlushOutcome> {
    // Sorted and deduplicated up front: `arrival` is `(segment_id, offset)`, so
    // read order *is* part of the dedup answer and reading one segment twice
    // would invent duplicate arrivals for the same bytes.
    let mut ids: Vec<u64> = segments.to_vec();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(FlushOutcome::default());
    }

    // Held for the whole job: every decoded row borrows its strings from these
    // buffers rather than owning them. See the module docs on M4 memory.
    let mut bufs = Vec::with_capacity(ids.len());
    for &id in &ids {
        bufs.push(read_segment(segment_path(data_dir, id))?);
    }

    let mut rejects = RejectSink::new(data_dir, config.limits.reject_max_bytes);
    let mut decoded = decode_segments(data_dir, &ids, &bufs, manifest, &mut rejects);

    // Dropping an unreadable segment from `ids` is what keeps the commit below
    // from advancing the watermark over it and `delete_segments` from unlinking
    // it. `unreadable`, not `quarantined`: the two differ when the rename fails,
    // and it is the failed *read* that makes a segment unsafe to call durable.
    ids.retain(|id| !decoded.unreadable.contains(id));
    if ids.is_empty() {
        // Every segment offered was unreadable. There is nothing to write, and
        // above all nothing to commit: advancing the watermark here would be the
        // engine asserting that unreadable segments are durable in Parquet.
        return Ok(FlushOutcome {
            frames_rejected: decoded.frames_rejected,
            bytes_skipped: decoded.bytes_skipped,
            ..FlushOutcome::default()
        });
    }

    let rows_decoded: usize = decoded.groups.values().map(Vec::len).sum();
    if rows_decoded == 0 && decoded.frames_rejected == 0 && decoded.bytes_skipped == 0 {
        // The genuine no-op: no empty Parquet file, no manifest write, and the
        // segments stay put. They cost one cheap re-scan on the next flush.
        return Ok(FlushOutcome::default());
    }

    let mut outcome = FlushOutcome {
        frames_rejected: decoded.frames_rejected,
        bytes_skipped: decoded.bytes_skipped,
        ..FlushOutcome::default()
    };

    let sink = Sink {
        data_dir,
        config,
        first_segment: *ids.first().expect("non-empty"),
        last_segment: *ids.last().expect("non-empty"),
    };
    std::fs::create_dir_all(tmp_dir(data_dir))?;

    let touched: BTreeSet<&str> = decoded.groups.keys().map(|&(series, _)| series).collect();

    for (&(series, epoch), rows) in &mut decoded.groups {
        let keys = decoded
            .schemas
            .get(&(series.to_string(), epoch))
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        // The last thing that can disagree with the schema: `decode` took arity
        // from the manifest, but the codec reads whatever type tag it finds, so
        // a hand-rolled client's i64 in a string column gets this far. Left
        // alone it would fail the whole flush inside a column builder.
        let mut rejected = 0;
        rows.retain(|row| {
            if matches_schema(keys, &row.keys) {
                return true;
            }
            rejected += 1;
            tracing::warn!(
                series,
                epoch,
                segment = row.arrival.0,
                offset = row.arrival.1,
                "rejecting a frame whose key values disagree with the schema its epoch names"
            );
            rejects.append(row.arrival.0, row.frame);
            false
        });
        outcome.frames_rejected += rejected;

        // Total, because `arrival` is unique: the same segments sort the same way
        // on every replay, which is what makes a re-run byte-identical.
        rows.sort_by(|a, b| (a.ts, a.id, a.arrival).cmp(&(b.ts, b.id, b.arrival)));

        let before = rows.len();
        // Duplicates are adjacent after that sort, and the survivor is the first
        // arrival. An empty id dedups like any other value -- `(100, "")` twice
        // is one row -- so a producer emitting distinct events at one microsecond
        // must set `id`.
        rows.dedup_by(|later, first| later.ts == first.ts && later.id == first.id);
        outcome.rows_deduplicated += before - rows.len();

        // Sorted by `ts`, so the rows of one hour are contiguous: no file spans
        // two partitions, and every file is non-decreasing in `ts`.
        let mut start = 0;
        while start < rows.len() {
            let bucket = rows[start].ts / US_PER_HOUR;
            let mut end = start + 1;
            while end < rows.len() && rows[end].ts / US_PER_HOUR == bucket {
                end += 1;
            }
            outcome.files_written.push(sink.write_partition(
                series,
                epoch,
                keys,
                &rows[start..end],
            )?);
            outcome.rows_written += end - start;
            start = end;
        }
    }

    // The commit point. Everything above is recoverable by replay; nothing
    // below may run before this rename lands.
    manifest.last_flushed_segment = manifest.last_flushed_segment.max(sink.last_segment);
    manifest.last_flush_at = Some(now_us());
    manifest.commit(data_dir)?;
    outcome.segments_consumed = ids;

    delete_segments(data_dir, &outcome.segments_consumed)?;
    regenerate_views(data_dir, manifest, &touched);

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

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// One row, still borrowing its strings from the segment it was decoded out of.
#[derive(Debug)]
struct DecodedRow<'a> {
    ts: u64,
    id: &'a str,
    keys: Vec<Value<'a>>,
    extra: Vec<(&'a str, &'a str)>,
    data: &'a str,
    /// `(segment_id, frame_offset)`: a total order over every record ever
    /// written, identical on every replay. Breaking `(ts, id)` ties with it is
    /// what makes "first arrival wins" mean the same thing on a re-run.
    arrival: (u64, usize),
    /// The frame's own bytes, so a row rejected after decoding goes to
    /// `rejects/` verbatim rather than as a log line.
    frame: &'a [u8],
}

/// Does this row's key list satisfy the schema it would be written under?
///
/// `decode` checked arity against the manifest but **not types**, because the
/// codec reads whatever tag byte it finds. Both are checked here so the column
/// builders cannot fail on data. [`Value::Null`] satisfies any column:
/// nullability is an ingest rule, and Parquet key columns are always nullable.
fn matches_schema(keys: &[KeyDef], values: &[Value<'_>]) -> bool {
    keys.len() == values.len()
        && std::iter::zip(keys, values).all(|(k, v)| v.key_type().is_none_or(|ty| ty == k.ty))
}

/// Everything the decode pass produces.
///
/// The group key borrows the series name out of the segment buffer, so grouping
/// millions of frames allocates nothing per row. `schemas` owns its key lists
/// instead: the manifest is needed mutably later, and a borrow would outlive the
/// commit.
#[derive(Debug, Default)]
struct Decoded<'a> {
    groups: BTreeMap<(&'a str, u32), Vec<DecodedRow<'a>>>,
    schemas: BTreeMap<(String, u32), Vec<KeyDef>>,
    frames_rejected: usize,
    bytes_skipped: u64,
    /// Segments whose header would not parse. Not consumed, and above all not
    /// deleted — whether or not the move aside succeeded.
    unreadable: Vec<u64>,
    /// The subset of `unreadable` that [`quarantine_segment`] managed to rename.
    /// Reported to the caller so a failed move is visible as the difference.
    quarantined: Vec<u64>,
}

fn decode_segments<'a>(
    data_dir: &Path,
    ids: &[u64],
    bufs: &'a [Vec<u8>],
    manifest: &Manifest,
    rejects: &mut RejectSink,
) -> Decoded<'a> {
    let mut out = Decoded::default();
    // The same `(series, epoch)` repeats for millions of consecutive frames, so
    // the manifest lookup is cached. Linear: a data directory has a handful of
    // live schemas, and a scan of three tuples beats hashing a string per frame.
    let mut cache: Vec<(String, u32, Option<usize>)> = Vec::new();

    for (&segment_id, buf) in std::iter::zip(ids, bufs) {
        let schemas = &mut out.schemas;
        let key_count = |series: &str, epoch: u32| -> Option<usize> {
            if let Some(&(_, _, arity)) = cache.iter().find(|(s, e, _)| s == series && *e == epoch)
            {
                return arity;
            }
            let keys = manifest.schema(series, epoch).map(<[KeyDef]>::to_vec);
            let arity = keys.as_ref().map(Vec::len);
            if let Some(keys) = keys {
                schemas.insert((series.to_string(), epoch), keys);
            }
            cache.push((series.to_string(), epoch, arity));
            arity
        };

        // The frame-length cap is the segment's own size, not
        // `limits.max_record_bytes`: that cap bounds allocation, and the whole
        // segment is already in memory. Applying it would make lowering the
        // config truncate every sealed segment at its first oversized record.
        let mut reader = match SegmentReader::new(buf, buf.len(), key_count) {
            Ok(r) => r,
            Err(err) => {
                // No readable header means no locatable frames -- but the bytes
                // behind it may be entirely intact, so it must not be deleted.
                // Recorded before the move is attempted and regardless of whether
                // it works: gating this on the rename left a segment that could
                // not be moved in `ids`, where the commit declared it durable and
                // the unlink took it.
                tracing::error!(segment = segment_id, %err, "wal segment header is unreadable");
                out.unreadable.push(segment_id);
                if quarantine_segment(data_dir, segment_id) {
                    out.quarantined.push(segment_id);
                }
                out.bytes_skipped += buf.len() as u64;
                continue;
            }
        };
        if reader.segment_id() != segment_id {
            // `arrival` is (segment_id, offset), so a hand-copied or renamed
            // segment silently reorders records relative to a previous replay.
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
                        // The CRC agreed, so the frame's extent is known: its
                        // bytes can be set aside verbatim for inspection.
                        FrameFault::BadRecord { offset, len, .. }
                        | FrameFault::UnknownSchema { offset, len, .. } => {
                            tracing::warn!(segment = segment_id, %fault, "rejecting a frame");
                            rejects.append(segment_id, &buf[offset..offset + len]);
                        }
                        // No delimitable length means no frame after this can be
                        // found. Recovery amends the *tail*, so reaching this in
                        // a sealed segment means the storage damaged acked bytes.
                        // The remainder is lost to the dataset either way, so
                        // copy it verbatim and count it -- reporting one rejected
                        // "frame" for a whole tail is what made this invisible.
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
            out.groups
                .entry((record.series, record.epoch))
                .or_default()
                .push(DecodedRow {
                    ts: record.ts,
                    id: record.id,
                    keys: reader.keys().to_vec(),
                    extra: reader.extra().to_vec(),
                    data: record.data,
                    arrival: (segment_id, frame.offset),
                    frame: &buf[frame.offset..frame.offset + frame.len],
                });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// The staging-and-rename half of the job, bundled so the per-file call stays
/// short.
#[derive(Debug)]
struct Sink<'a> {
    data_dir: &'a Path,
    config: &'a Config,
    first_segment: u64,
    last_segment: u64,
}

impl Sink<'_> {
    /// Build, stage, fsync and rename one `(series, epoch, hour)` file.
    ///
    /// `rows` must be sorted, deduplicated and within one hour — the caller's
    /// loop guarantees all three, and the Parquet writer declares `ts` a sorting
    /// column on the strength of it.
    fn write_partition(
        &self,
        series: &str,
        epoch: u32,
        keys: &[KeyDef],
        rows: &[DecodedRow<'_>],
    ) -> Result<WrittenFile> {
        let mut builder = RowBuilder::new(series, keys, rows.len())?;
        for row in rows {
            builder.append(row.ts, row.id, &row.keys, &row.extra, row.data)?;
        }
        let cols = builder.finish()?;

        let hour = hour_partition(rows[0].ts);
        // The segment range is what makes a re-run idempotent: the same
        // segments produce the same name, so an interrupted flush overwrites
        // its own output instead of appending a duplicate file.
        let name = output_file_name(self.first_segment, self.last_segment, epoch);

        // `tmp/` is flat, so the staging name has to carry what the destination
        // directory would otherwise disambiguate.
        let staged = tmp_dir(self.data_dir).join(format!("{series}.{hour}.{name}"));
        write_file(
            &staged,
            &cols,
            series,
            epoch,
            &self.config.flush.compression,
            self.config.flush.row_group_rows,
        )?;

        let series_root = series_dir(self.data_dir, series);
        let dir = series_root.join(&hour);
        std::fs::create_dir_all(&dir)?;
        std::fs::rename(&staged, dir.join(&name))?;

        // `write_file` synced the contents; only a directory fsync makes the
        // *name* durable, and the manifest is about to claim this file exists.
        // The whole chain, not just the leaf: an unsynced `series/<name>/` can
        // lose a new `hour=` directory whole, file and all.
        fsync_dir(&dir)?;
        fsync_dir(&series_root)?;
        fsync_dir(&self.data_dir.join(SERIES_DIR))?;

        Ok(WrittenFile {
            series: series.to_string(),
            epoch,
            hour,
            rows: rows.len(),
        })
    }
}

/// Remove everything an interrupted flush left behind.
///
/// Two kinds of debris, both from a flush that wrote files then failed before
/// [`Manifest::commit`]:
///
/// - **Staged files in `tmp/`.** A crash between `write_file` and the rename
///   leaves one there, and nothing overwrites it because the staging name embeds
///   the segment range. Staged files are uncommitted by definition, so `tmp/` is
///   emptied unconditionally.
/// - **Orphan Parquet in `series/`.** Files are renamed into place *before* the
///   commit, so a failure between the two leaves a real file whose rows the next
///   flush — wider range, different name — writes again beside it. Both match the
///   reader's glob, so every row in the overlap comes back twice.
///
/// The rule for the second is exact: a committed file's `last_segment` is at most
/// `last_flushed_segment`, so a range reaching past the watermark can only be
/// uncommitted. A name that does not parse is left alone — this is the only place
/// the engine deletes from `series/`.
///
/// **`last_flushed_segment` must come from a manifest that was actually read off
/// disk.** A missing `manifest.json` reads as 0, under which every committed
/// file's range runs past it and this deletes the whole dataset.
/// [`require_manifest_for_output`] is what stops that reaching here.
///
/// Runs at open, under the directory lock, so no flush can be in flight.
pub fn sweep_uncommitted(data_dir: &Path, last_flushed_segment: u64) -> Result<()> {
    let tmp = tmp_dir(data_dir);
    if let Ok(entries) = std::fs::read_dir(&tmp) {
        let mut files = 0u64;
        let mut bytes = 0u64;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(&path) {
                Ok(()) => files += 1,
                Err(e) => {
                    tracing::warn!(error = %e, path = %path.display(), "could not remove a stale staged file")
                }
            }
        }
        if files > 0 {
            tracing::info!(
                files,
                bytes,
                "removed staged files from an uncommitted flush"
            );
        }
    }

    let mut removed = Vec::new();
    let mut emptied: BTreeSet<PathBuf> = BTreeSet::new();
    for (path, (_, last, _)) in output_files(data_dir) {
        if last <= last_flushed_segment {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                if let Some(hour) = path.parent() {
                    emptied.insert(hour.to_path_buf());
                }
                removed.push(path);
            }
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(), "could not remove an orphan parquet file");
            }
        }
    }
    if !removed.is_empty() {
        // Loud on purpose. Nothing is lost -- the segments these came from were
        // never consumed, so their rows are still in the WAL and the next flush
        // writes them again -- but the engine deleting from `series/` at all is
        // rare enough to be worth seeing.
        tracing::warn!(
            files = removed.len(),
            last_flushed_segment,
            "removed parquet files from a flush that never committed; their rows are still in the wal"
        );
        for path in &removed {
            tracing::info!(path = %path.display(), "removed orphan parquet");
        }
    }
    // `remove_dir` fails harmlessly when anything is left, which is exactly the
    // check wanted: a partition emptied of orphans goes, one with a committed
    // file or a hand-placed one stays.
    for hour in &emptied {
        let _ = std::fs::remove_dir(hour);
    }
    Ok(())
}

/// Refuse to open a data directory that holds flush output but no manifest.
///
/// `manifest.json` is the only record of how far the flush got. Without it
/// `last_flushed_segment` reads as 0, under which every Parquet file in
/// `series/` looks like debris from a flush that never committed and
/// [`sweep_uncommitted`] deletes the entire dataset.
///
/// Deliberately not a repair: the filenames say which segment ranges were
/// written, not which of those the manifest had accepted, so there is nothing to
/// guess the watermark back from. It stops and leaves the decision to someone
/// who knows whether the manifest was lost or the Parquet was copied in.
///
/// A fresh directory has no output and passes, which is what lets an engine
/// start for the first time.
pub fn require_manifest_for_output(data_dir: &Path) -> Result<()> {
    let found = output_files(data_dir);
    let Some((first, _)) = found.first() else {
        return Ok(());
    };
    Err(Error::Config(format!(
        "{} has no {} but {}/ already holds {} parquet file(s) this engine wrote \
         (for example {}). Refusing to start: the manifest is the only record of how \
         far the flush got, and without it every one of those files looks like debris \
         from a flush that never committed -- so starting would delete them. \
         Restore {} from a backup, or move {}/ aside if you mean to start fresh.",
        data_dir.display(),
        crate::manifest::MANIFEST_FILE,
        SERIES_DIR,
        found.len(),
        first.display(),
        crate::manifest::MANIFEST_FILE,
        SERIES_DIR,
    )))
}

/// Every flush-output file under `series/`, with the `(first, last, epoch)` its
/// name encodes.
///
/// Anything the parse does not recognise is left out: `series/` holds `view.sql`
/// and whatever else an operator keeps there, and both callers either delete or
/// refuse to start over what this returns. A directory that cannot be listed is
/// skipped rather than reported — this walks a tree the README invites people to
/// prune, so racing a `find -delete` must not fail startup.
fn output_files(data_dir: &Path) -> Vec<(PathBuf, (u64, u64, u32))> {
    let mut out = Vec::new();
    let Ok(series) = std::fs::read_dir(data_dir.join(SERIES_DIR)) else {
        return out;
    };
    for s in series.flatten() {
        let Ok(hours) = std::fs::read_dir(s.path()) else {
            continue;
        };
        for hour in hours.flatten() {
            let Ok(files) = std::fs::read_dir(hour.path()) else {
                continue;
            };
            for f in files.flatten() {
                let name = f.file_name();
                if let Some(parsed) = name.to_str().and_then(parse_output_file_name) {
                    out.push((f.path(), parsed));
                }
            }
        }
    }
    out.sort();
    out
}

/// Move a segment out of `wal/` without destroying it.
///
/// Same move as recovery's, for the same reason: an unreadable header says
/// nothing about the frames behind it, so the file must leave the log without
/// ceasing to exist. [`list_segments`](crate::wal::list_segments) stops seeing
/// it and every byte stays put.
///
/// Returns whether the rename happened. A failure is logged and swallowed,
/// which really does cost nothing but a repeated error on the next flush: the
/// caller records the segment as unreadable either way, so it stays out of the
/// consumed set and the commit never claims it is durable.
fn quarantine_segment(data_dir: &Path, id: u64) -> bool {
    let from = segment_path(data_dir, id);
    let to = from.with_extension(CORRUPT_EXT);
    match std::fs::rename(&from, &to) {
        Ok(()) => {
            tracing::error!(
                segment = id,
                path = %to.display(),
                "quarantined an unreadable wal segment; its records are NOT in the dataset"
            );
            let _ = fsync_dir(&wal_dir(data_dir));
            true
        }
        Err(e) => {
            tracing::error!(error = %e, segment = id, "could not quarantine an unreadable wal segment");
            false
        }
    }
}

/// Drop the segments the manifest now says are durable in Parquet.
///
/// Runs strictly after the commit. A crash mid-way leaves orphans that startup
/// deletes on the same `id <= last_flushed_segment` rule, so a missing file here
/// is a completed job rather than a failure.
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

/// Rewrite `series/<name>/view.sql` for every series this flush touched.
///
/// Derived: pure text, reconstructible from the manifest plus the epoch tags in
/// the filenames. A failure is logged and swallowed — the flush it would fail has
/// already committed.
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

/// Now, in epoch microseconds. Saturating, not panicking: an absurd wall clock
/// is a reason for an odd `last_flush_at`, not for failing a flush that has
/// already written its data. A pre-1970 clock collapses to 0, which
/// `flush_overdue` reads as always overdue — the safe direction.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

// ---------------------------------------------------------------------------
// Rejects
// ---------------------------------------------------------------------------

/// The `rejects/` sink: bad frames, verbatim, capped in total.
///
/// Best-effort throughout. The sink makes bad data inspectable; failing a flush
/// because it is full would trade a diagnostic for the outage the reject path
/// exists to avoid. Past the cap, frames are counted but not written.
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
            // Measured lazily: a flush with no rejects must not pay for a
            // directory listing.
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
        // Same zero padding as segment names, so `ls` sorts rejects in the
        // order the segments were written.
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

/// Total size of the files directly inside `dir`; 0 if it does not exist.
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

    use crate::codec::{encode, encode_segment_header};
    use crate::config::SeriesConfig;
    use crate::record::{KeyType, Row};
    use crate::wal::FIRST_SEGMENT;

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

    /// A one-series setup: `trades` with a single `symbol` key at epoch 0.
    fn trades() -> (Config, Manifest) {
        let cfg = config(&[("trades", vec![key("symbol")])]);
        let mut m = Manifest::default();
        m.reconcile(&cfg).unwrap();
        (cfg, m)
    }

    /// One frame's worth of input, owned so tests can build tables of them.
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

    /// Write a WAL segment holding `recs`, and return its bytes.
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

    /// Every Parquet file under `series/`, sorted by path.
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

    /// `(ts, id)` of every row in a Parquet file, in file order.
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

    /// An unreadable header says nothing about the frames behind it. Skipping
    /// the segment while the commit advanced the watermark past it meant the
    /// deletion below removed a file that might have been entirely recoverable.
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

    /// Quarantining is best-effort; staying out of the consumed set is not.
    /// Gating that on the rename meant a segment that could not be moved was
    /// declared durable and unlinked.
    #[test]
    fn a_segment_that_cannot_be_quarantined_is_still_not_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        write_segment(dir.path(), 1, &[rec(T13, "a")]);
        let mut bad = write_segment(dir.path(), 2, &[rec(T13 + 1, "b")]);
        bad[0] ^= 0xff;
        fs::write(segment_path(dir.path(), 2), &bad).unwrap();

        // Block the rename: `wal.corrupt` is a directory, so rename(2) fails
        // whatever the platform spells the error.
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

    /// Frames larger than the *current* `limits.max_record_bytes` used to stop a
    /// sealed segment's scan dead, discarding everything after them and then
    /// deleting the file. Lowering a config value must not destroy records that
    /// were durable before the edit.
    #[test]
    fn lowering_max_record_bytes_does_not_discard_durable_records() {
        let dir = tempfile::tempdir().unwrap();
        let (mut cfg, mut m) = trades();
        write_segment(
            dir.path(),
            1,
            &[rec(T13, "a"), rec(T13 + 1, "b"), rec(T13 + 2, "c")],
        );

        // Far below one frame: the old code passed this straight to the decoder,
        // which rejected the first frame's length and stopped the scan there.
        cfg.limits.max_record_bytes = 8;

        let out = run(dir.path(), &cfg, &mut m, &[1]).unwrap();
        assert_eq!(out.rows_written, 3, "every durable record still flushes");
        assert_eq!(out.frames_rejected, 0);
        assert_eq!(out.bytes_skipped, 0);
    }

    #[test]
    fn segments_that_decode_to_no_rows_write_no_file_and_commit_nothing() {
        // A sealed but empty segment: legitimate, and the one case where writing
        // a Parquet file would mean an empty file per flush forever.
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
        // Deliberately out of order, across three hours, with one duplicate.
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
        // Valid CRC and prefix, corrupt `keys` section: exactly what an
        // ill-behaved client gets past `append_raw`, and only the flush can
        // notice. Aborting here would mean the WAL never drains and no Parquet
        // is ever produced again.
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

        // ...and the offending bytes are inspectable rather than gone.
        let rejected =
            fs::read(rejects_dir(dir.path()).join(format!("{FIRST_SEGMENT:010}.{REJECT_EXT}")))
                .unwrap();
        assert_eq!(rejected, buf[poison_at..poison_at + poison_len]);
    }

    #[test]
    fn a_segment_of_nothing_but_poison_still_commits() {
        // The liveness case the reject path exists for: if this did not commit,
        // the segment would be re-scanned forever and the WAL would never drain.
        let dir = tempfile::tempdir().unwrap();
        let (cfg, mut m) = trades();
        let mut buf = Vec::new();
        encode_segment_header(&mut buf, FIRST_SEGMENT);
        // An epoch that is not in the history: skippable, never guessed.
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
        // The codec reads whatever type tag it finds, so a hand-rolled client
        // can encode an i64 into a column the config declares a string. It gets
        // past ingest -- which only reads the fixed prefix -- and past the
        // decoder, and would otherwise fail the flush inside the column builder.
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
        // The empty id dedups like any other value -- the documented cost of
        // literal (ts, id) equality.
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
        // The filename spans the consumed range, which is what a re-run repeats.
        assert_eq!(
            files[0].file_name().unwrap().to_str().unwrap(),
            "0000000001-0000000002.e0.parquet"
        );
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

        // The crash: the files are on disk and fsynced, but the manifest rename
        // never happened -- so the segment is still there and the flush re-runs.
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
        // A segment spanning a reload-config: two epochs of one series, plus a
        // bystander. Each frame names its own epoch, so nothing outside it says
        // which schema to decode against.
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

        // Every Parquet file holds exactly one schema even though the segment
        // held two -- that is what grouping by (series, epoch) buys.
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

        // Segment 6 is not offered: only what the caller listed is consumed.
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
        // A series with no rows in this flush is not touched, and gets no view.
        assert!(!series_dir(dir.path(), "quotes").join(VIEW_FILE).exists());
    }

    /// Without a manifest there is no watermark, and `sweep_uncommitted` reads
    /// the resulting 0 as "every file here is uncommitted".
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

        // A directory holding only files the engine did not write is not its
        // business either way.
        let stranger = tempfile::tempdir().unwrap();
        let hour = series_dir(stranger.path(), "trades").join("hour=2026-08-08T13");
        fs::create_dir_all(&hour).unwrap();
        fs::write(hour.join("notes.txt"), b"mine").unwrap();
        assert!(require_manifest_for_output(stranger.path()).is_ok());
    }

    /// An `hour=` directory left behind by the sweep reads as a partition with
    /// no data. Only one that the sweep actually emptied is removed.
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

        // Now empty the first partition entirely: with nothing left, it goes.
        let solo = tempfile::tempdir().unwrap();
        let hour = series_dir(solo.path(), "trades").join("hour=2026-08-08T13");
        fs::create_dir_all(&hour).unwrap();
        fs::write(hour.join(output_file_name(1, 9_999, 0)), b"x").unwrap();
        sweep_uncommitted(solo.path(), 0).unwrap();
        assert!(!hour.exists(), "an emptied partition is removed");
    }

    /// Recompute a frame's CRC after forging its bytes, so a test can build a
    /// frame that is intact but structurally wrong.
    fn reseal(frame: &mut [u8]) {
        let mut h = crc32fast::Hasher::new();
        h.update(&frame[..4]);
        h.update(&frame[8..]);
        frame[4..8].copy_from_slice(&h.finalize().to_le_bytes());
    }
}
