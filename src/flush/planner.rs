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
//! disk, so the flush simply re-runs — or the new one, whose orphan segments
//! startup deletes because their ids are `<= last_flushed_segment`. No window
//! loses data, and no window double-writes: the output filename is
//! [`output_file_name`] over the *consumed segment range*, so a re-run of an
//! interrupted flush writes the same paths with the same bytes rather than a
//! second copy of the same rows.
//!
//! # A bad frame costs one record, never the flush
//!
//! Ingest validated only a frame's fixed prefix, so a malformed `keys` section
//! first surfaces here, an hour after it was accepted. Aborting would be the
//! worst possible response: a flush that can never finish means no Parquet is
//! ever produced again and the WAL grows until the volume fills. So every fault
//! the reader yields is counted, copied to `rejects/<segment_id>.rej` where it
//! can be inspected, and skipped.
//!
//! That is also why a flush that decoded *only* bad frames still commits. The
//! no-op case this job is documented to have — no empty Parquet files, no
//! manifest write — is the genuinely empty one; refusing to commit a segment
//! whose every frame was rejected would resurrect exactly the stall the
//! paragraph above exists to prevent.
//!
//! # Memory: M4 holds one flush in memory
//!
//! Every row of every listed segment is decoded and held until the last file is
//! written, and each group is sorted with a single in-memory sort. That is
//! bounded by the segments the caller hands over, not by a budget: an hour at a
//! high ingest rate does not fit. **M5 replaces this with per-segment sorted
//! runs and a bounded-fan-in k-way merge**; splitting it that way is deliberate,
//! so an hour-scale flush is correct before it is scale-proof.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{Config, KeyDef};
use crate::error::Result;
use crate::flush::parquet::{RowBuilder, WrittenFile, output_file_name, write_file};
use crate::flush::view::view_sql;
use crate::manifest::Manifest;
use crate::record::Value;
use crate::time::hour_partition;
use crate::wal::reader::{FrameFault, SegmentReader, read_segment};
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
const US_PER_HOUR: i64 = 3_600_000_000;

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
    // the order segments are read in *is* part of the dedup answer, and reading
    // one twice would invent duplicate arrivals for the same bytes.
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
    let mut decoded = decode_segments(
        &ids,
        &bufs,
        manifest,
        config.limits.max_record_bytes,
        &mut rejects,
    );

    let rows_decoded: usize = decoded.groups.values().map(Vec::len).sum();
    if rows_decoded == 0 && decoded.frames_rejected == 0 {
        // The genuine no-op: no empty Parquet file, no manifest write, and the
        // segments stay put. They cost one cheap re-scan on the next flush.
        return Ok(FlushOutcome::default());
    }

    let mut outcome = FlushOutcome {
        frames_rejected: decoded.frames_rejected,
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

        // The last thing that can disagree with the schema. `decode` took its
        // arity from the manifest and the codec reads whatever type tag it
        // finds, so a hand-rolled client can put an i64 where the config
        // declares a string and get all the way here. Left alone it would
        // surface inside the column builders and fail the whole flush over one
        // record -- the exact stall `rejects/` exists to prevent.
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

        // Stable, and total: `arrival` is unique, so this order is identical on
        // every replay of the same segments -- which is what makes a re-run
        // produce byte-identical files.
        rows.sort_by(|a, b| (a.ts, a.id, a.arrival).cmp(&(b.ts, b.id, b.arrival)));

        let before = rows.len();
        // Duplicates are adjacent after that sort, so one pass is enough, and
        // the survivor is the first arrival. An empty id dedups like any other
        // value: `(100, "")` twice is one row, which is why a producer emitting
        // genuinely distinct events at one microsecond must set `id`.
        rows.dedup_by(|later, first| later.ts == first.ts && later.id == first.id);
        outcome.rows_deduplicated += before - rows.len();

        // Sorted by `ts`, so the rows of one hour are contiguous: no file spans
        // two partitions, and every file is non-decreasing in `ts`.
        let mut start = 0;
        while start < rows.len() {
            let bucket = rows[start].ts.div_euclid(US_PER_HOUR);
            let mut end = start + 1;
            while end < rows.len() && rows[end].ts.div_euclid(US_PER_HOUR) == bucket {
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
    ts: i64,
    id: &'a str,
    keys: Vec<Value<'a>>,
    extra: Vec<(&'a str, &'a str)>,
    data: &'a str,
    /// `(segment_id, frame_offset)` — a total order over every record ever
    /// written, identical on every replay. It breaks `(ts, id)` ties, so "first
    /// arrival wins" means the same thing on a re-run as it did on the run the
    /// crash interrupted.
    arrival: (u64, usize),
    /// The frame's own bytes, so a row rejected after decoding can still be
    /// copied to `rejects/` verbatim rather than described in a log line.
    frame: &'a [u8],
}

/// Does this row's key list satisfy the schema it would be written under?
///
/// Arity was already checked by `decode`, which took it from the manifest;
/// **types were not**, because the codec reads whatever tag byte it finds. Both
/// are checked here so the column builders downstream cannot fail on data.
/// [`Value::Null`] satisfies any column — nullability is enforced at ingest, and
/// every key column is written nullable in Parquet regardless.
fn matches_schema(keys: &[KeyDef], values: &[Value<'_>]) -> bool {
    keys.len() == values.len()
        && std::iter::zip(keys, values).all(|(k, v)| v.key_type().is_none_or(|ty| ty == k.ty))
}

/// Everything the decode pass produces.
///
/// The group key borrows the series name out of the segment buffer, so grouping
/// millions of frames allocates nothing per row. `schemas` owns its key lists
/// instead, because the manifest is needed mutably later and a borrow held here
/// would outlive the commit.
#[derive(Debug, Default)]
struct Decoded<'a> {
    groups: BTreeMap<(&'a str, u32), Vec<DecodedRow<'a>>>,
    schemas: BTreeMap<(String, u32), Vec<KeyDef>>,
    frames_rejected: usize,
}

fn decode_segments<'a>(
    ids: &[u64],
    bufs: &'a [Vec<u8>],
    manifest: &Manifest,
    max_record_bytes: usize,
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

        let mut reader = match SegmentReader::new(buf, max_record_bytes, key_count) {
            Ok(r) => r,
            Err(err) => {
                // A sealed segment with no readable header holds no locatable
                // frames at all. Counting it and moving on is the only choice
                // that keeps the flush able to finish.
                out.frames_rejected += 1;
                tracing::error!(segment = segment_id, %err, "skipping a wal segment with an unreadable header");
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
                        // Unknown length means the bytes cannot be delimited, so
                        // there is nothing well-defined to copy and nothing
                        // after this point can be found. Recovery amends the
                        // *tail* segment, so reaching this in a sealed one means
                        // the storage damaged bytes that were already acked.
                        FrameFault::Undecodable { .. } => {
                            tracing::error!(segment = segment_id, %fault, "stopping this segment's scan");
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
    /// `rows` must be sorted, deduplicated, and entirely within one hour — the
    /// caller's loop guarantees all three, and the Parquet writer declares `ts`
    /// a sorting column on the strength of it.
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

        // `write_file` synced the file's *contents*; only syncing a directory
        // makes the names it holds durable, and the manifest is about to claim
        // this file exists. The chain matters as much as the leaf: an unsynced
        // `series/<name>/` can lose a freshly created `hour=` directory whole,
        // taking a file whose every byte was on the platter with it.
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

/// Drop the segments the manifest now says are durable in Parquet.
///
/// Runs strictly after the commit. A crash in the middle leaves orphans, which
/// startup deletes on the same `id <= last_flushed_segment` rule — so a missing
/// file here is a completed job, not a failure.
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
/// Derived output: pure text, rewritable at any moment, reconstructible from the
/// manifest plus the epoch tags in the filenames. A failure here is logged and
/// swallowed, because the flush it would otherwise fail has already committed.
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

/// Now, in epoch microseconds. Saturating rather than panicking: a wall clock
/// far enough out of range to overflow is a reason to record an odd
/// `last_flush_at`, not to fail a flush that has already written its data.
fn now_us() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_micros()).map_or(i64::MIN, |us| -us),
    }
}

// ---------------------------------------------------------------------------
// Rejects
// ---------------------------------------------------------------------------

/// The `rejects/` sink: bad frames, verbatim, capped in total.
///
/// Every write here is best-effort. The sink exists so bad data is inspectable
/// rather than invisible; failing the flush because the sink is full or
/// unwritable would trade a diagnostic for the outage the whole reject path
/// exists to avoid. Once the cap is reached, frames are still counted.
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
    const T13: i64 = 1_786_194_000_000_000;

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
        ts: i64,
        id: &'static str,
        keys: Vec<Value<'static>>,
    }

    fn rec(ts: i64, id: &'static str) -> Rec {
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
    fn read_rows(path: &Path) -> Vec<(i64, String)> {
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
        assert!(sql.contains("coalesce(\"venue\", extra['venue'])"), "{sql}");
        // A series with no rows in this flush is not touched, and gets no view.
        assert!(!series_dir(dir.path(), "quotes").join(VIEW_FILE).exists());
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
