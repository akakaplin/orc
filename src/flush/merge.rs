//! Sorting a flush's records within a memory budget.
//!
//! Records accumulate in a [`Batch`]. Past `flush.memory_budget_bytes` it is
//! sorted and spilled to `runs/` as one run per `(series, epoch)`. The last
//! batch stays in memory and joins the k-way merge ([`merge_bounded`]).
//!
//! Run entry (private scratch): `segment_id u64 LE | frame_offset u64 LE | frame`.
//! The arrival pair is the dedup tie-breaker across runs. Frames were verified
//! when their segment was read, so runs skip CRC/UTF-8 checks; runs are not
//! fsynced, since a crash before commit replays the flush from the WAL.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::codec::{FRAME_HEADER_BYTES, verified_sort_key};
use crate::error::Result;

/// Runs directory under `data_dir`; not inside `tmp/`, which mirrors series names.
pub const RUNS_DIR: &str = "runs";

/// I/O buffer per open run; the merge holds up to `merge_fan_in` of these.
const RUN_BUF_BYTES: usize = 256 * 1024;

/// Bytes ahead of each frame: the arrival pair.
const ARRIVAL_BYTES: usize = 16;

/// `<data_dir>/runs`.
pub fn runs_dir(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(RUNS_DIR)
}

fn corrupt(path: &Path, what: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("run file {}: {what}", path.display()),
    )
}

/// `(series, epoch)`: one output file per hour of each.
pub type GroupKey = (String, u32);

/// One [`Batch`] record: frame location in the arena plus sort key. Fixed-size.
#[derive(Debug, Clone, Copy)]
struct MemEntry {
    ts: u64,
    arrival: (u64, u64),
    frame: usize,
    frame_len: u32,
    /// Index into [`Batch::groups`].
    group: u32,
    /// Id offset and length within the frame.
    id_off: u16,
    id_len: u16,
}

/// Decoded records held in memory, up to the flush's budget.
#[derive(Debug, Default)]
pub struct Batch {
    arena: Vec<u8>,
    entries: Vec<MemEntry>,
    /// Interned group keys; few enough that a linear scan beats hashing.
    groups: Vec<GroupKey>,
    deduplicated: usize,
}

impl Batch {
    /// Copy one verified frame in. `id` must be a subslice of `frame`.
    pub fn push(
        &mut self,
        group: (&str, u32),
        ts: u64,
        id: &str,
        arrival: (u64, u64),
        frame: &[u8],
    ) {
        let id_off = (id.as_ptr() as usize).wrapping_sub(frame.as_ptr() as usize);
        debug_assert!(
            id_off + id.len() <= frame.len(),
            "id must lie inside its frame"
        );
        let entry = MemEntry {
            ts,
            arrival,
            frame: self.arena.len(),
            frame_len: frame.len() as u32,
            group: self.intern(group),
            // Fits u16: the id is a str16 in the frame's fixed prefix.
            id_off: id_off as u16,
            id_len: id.len() as u16,
        };
        self.arena.extend_from_slice(frame);
        self.entries.push(entry);
    }

    fn intern(&mut self, (series, epoch): (&str, u32)) -> u32 {
        if let Some(last) = self.entries.last() {
            let (s, e) = &self.groups[last.group as usize];
            if *e == epoch && s == series {
                return last.group;
            }
        }
        match self
            .groups
            .iter()
            .position(|(s, e)| *e == epoch && s == series)
        {
            Some(i) => i as u32,
            None => {
                self.groups.push((series.to_string(), epoch));
                (self.groups.len() - 1) as u32
            }
        }
    }

    /// Bytes held; measured against the budget.
    pub fn bytes(&self) -> usize {
        self.arena.len() + self.entries.len() * std::mem::size_of::<MemEntry>()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Records dropped as duplicates by every [`Batch::sort`] so far.
    pub fn deduplicated(&self) -> usize {
        self.deduplicated
    }

    fn id<'a>(arena: &'a [u8], e: &MemEntry) -> &'a [u8] {
        let start = e.frame + e.id_off as usize;
        &arena[start..start + e.id_len as usize]
    }

    /// Sort by `(group, ts, id, arrival)` and drop `(ts, id)` duplicates per
    /// group, keeping the first arrival. `arrival` is unique, so unstable is fine.
    pub fn sort(&mut self) {
        let Self {
            arena,
            entries,
            groups,
            deduplicated,
        } = self;
        // Order by group key, not interning index, for a deterministic order.
        let mut rank: Vec<u32> = (0..groups.len() as u32).collect();
        rank.sort_by(|&a, &b| groups[a as usize].cmp(&groups[b as usize]));
        let mut order = vec![0u32; groups.len()];
        for (pos, &g) in rank.iter().enumerate() {
            order[g as usize] = pos as u32;
        }
        entries.sort_unstable_by(|a, b| {
            (order[a.group as usize], a.ts, Self::id(arena, a), a.arrival).cmp(&(
                order[b.group as usize],
                b.ts,
                Self::id(arena, b),
                b.arrival,
            ))
        });
        let before = entries.len();
        entries.dedup_by(|later, first| {
            later.group == first.group
                && later.ts == first.ts
                && Self::id(arena, later) == Self::id(arena, first)
        });
        *deduplicated += before - entries.len();
    }

    /// Each group's contiguous entry range; valid after [`Batch::sort`].
    fn group_ranges(&self) -> Vec<(GroupKey, std::ops::Range<usize>)> {
        let mut out = Vec::new();
        let mut start = 0;
        while start < self.entries.len() {
            let g = self.entries[start].group;
            let len = self.entries[start..]
                .iter()
                .position(|e| e.group != g)
                .unwrap_or(self.entries.len() - start);
            out.push((self.groups[g as usize].clone(), start..start + len));
            start += len;
        }
        out
    }

    /// Sort, write one run per group, and clear the batch, keeping its
    /// allocations for reuse.
    pub fn spill(
        &mut self,
        mut next_path: impl FnMut() -> PathBuf,
    ) -> Result<Vec<(GroupKey, PathBuf)>> {
        self.sort();
        let mut out = Vec::new();
        for (key, range) in self.group_ranges() {
            let path = next_path();
            let mut w = RunWriter::create(&path)?;
            for e in &self.entries[range] {
                let frame = &self.arena[e.frame..e.frame + e.frame_len as usize];
                w.push(e.arrival, frame)?;
            }
            w.finish()?;
            out.push((key, path));
        }
        self.arena.clear();
        self.entries.clear();
        self.groups.clear();
        Ok(out)
    }

    /// Sort and return one in-memory source per group for the final merge.
    pub fn into_sources(&mut self) -> Vec<(GroupKey, MemRun<'_>)> {
        self.sort();
        let ranges = self.group_ranges();
        let (arena, entries) = (&self.arena, &self.entries);
        ranges
            .into_iter()
            .map(|(key, range)| {
                let run = MemRun {
                    arena,
                    entries: &entries[range],
                    pos: 0,
                };
                (key, run)
            })
            .collect()
    }
}

/// The record a source is positioned on.
#[derive(Debug, Clone, Copy)]
pub struct Head<'a> {
    pub ts: u64,
    /// Raw bytes (UTF-8 already verified; byte order equals `str` order).
    pub id: &'a [u8],
    pub arrival: (u64, u64),
    /// The WAL frame, verbatim.
    pub frame: &'a [u8],
}

impl Head<'_> {
    fn key(&self) -> (u64, &[u8], (u64, u64)) {
        (self.ts, self.id, self.arrival)
    }
}

/// A sorted stream of records, positioned on its first one until exhausted.
pub trait Source {
    fn head(&self) -> Head<'_>;
    /// Move to the next record; `false` once there is none.
    fn advance(&mut self) -> Result<bool>;
}

/// A group's sorted slice of the final in-memory batch.
#[derive(Debug)]
pub struct MemRun<'a> {
    arena: &'a [u8],
    entries: &'a [MemEntry],
    pos: usize,
}

impl Source for MemRun<'_> {
    fn head(&self) -> Head<'_> {
        let e = &self.entries[self.pos];
        Head {
            ts: e.ts,
            id: Batch::id(self.arena, e),
            arrival: e.arrival,
            frame: &self.arena[e.frame..e.frame + e.frame_len as usize],
        }
    }

    fn advance(&mut self) -> Result<bool> {
        self.pos += 1;
        Ok(self.pos < self.entries.len())
    }
}

/// Appends entries to one run file in the given order.
#[derive(Debug)]
pub struct RunWriter {
    out: BufWriter<File>,
    entries: usize,
}

impl RunWriter {
    pub fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            out: BufWriter::with_capacity(RUN_BUF_BYTES, File::create(path)?),
            entries: 0,
        })
    }

    pub fn push(&mut self, arrival: (u64, u64), frame: &[u8]) -> Result<()> {
        self.out.write_all(&arrival.0.to_le_bytes())?;
        self.out.write_all(&arrival.1.to_le_bytes())?;
        self.out.write_all(frame)?;
        self.entries += 1;
        Ok(())
    }

    /// Flush the buffer (no fsync) and return the entry count.
    pub fn finish(mut self) -> Result<usize> {
        self.out.flush()?;
        Ok(self.entries)
    }
}

/// Forward cursor over one run file, reusing one frame buffer.
#[derive(Debug)]
struct RunReader {
    path: PathBuf,
    input: BufReader<File>,
    ts: u64,
    arrival: (u64, u64),
    frame: Vec<u8>,
    id: std::ops::Range<usize>,
}

impl RunReader {
    /// Positioned on the first entry, or `None` if empty.
    fn open(path: &Path) -> Result<Option<Self>> {
        let mut r = Self {
            path: path.to_path_buf(),
            input: BufReader::with_capacity(RUN_BUF_BYTES, File::open(path)?),
            ts: 0,
            arrival: (0, 0),
            frame: Vec::new(),
            id: 0..0,
        };
        Ok(if r.read_next()? { Some(r) } else { None })
    }

    /// `false` at clean EOF; a truncated entry is an error.
    fn read_next(&mut self) -> Result<bool> {
        let mut head = [0u8; ARRIVAL_BYTES + FRAME_HEADER_BYTES];
        if !read_exact_or_eof(&mut self.input, &mut head).map_err(|e| corrupt(&self.path, e))? {
            return Ok(false);
        }
        let seg = u64::from_le_bytes(head[0..8].try_into().unwrap());
        let off = u64::from_le_bytes(head[8..16].try_into().unwrap());
        let len = u32::from_le_bytes(head[16..20].try_into().unwrap()) as usize;

        self.frame.clear();
        self.frame.extend_from_slice(&head[ARRIVAL_BYTES..]);
        self.frame.resize(FRAME_HEADER_BYTES + len, 0);
        self.input
            .read_exact(&mut self.frame[FRAME_HEADER_BYTES..])
            .map_err(|e| corrupt(&self.path, e))?;

        // Structure check only; no CRC or UTF-8 (verified at segment read).
        let (ts, id, _) = verified_sort_key(&self.frame).map_err(|e| corrupt(&self.path, e))?;
        let start = id.as_ptr() as usize - self.frame.as_ptr() as usize;
        self.id = start..start + id.len();
        self.ts = ts;
        self.arrival = (seg, off);
        Ok(true)
    }
}

impl Source for RunReader {
    fn head(&self) -> Head<'_> {
        Head {
            ts: self.ts,
            id: &self.frame[self.id.clone()],
            arrival: self.arrival,
            frame: &self.frame,
        }
    }

    fn advance(&mut self) -> Result<bool> {
        self.read_next()
    }
}

/// `read_exact`, but EOF before the first byte returns `false`.
fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<bool> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) if got == 0 => return Ok(false),
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "truncated entry",
                ));
            }
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MergeStats {
    pub emitted: usize,
    /// Dropped because an earlier arrival had the same `(ts, id)`.
    pub deduplicated: usize,
}

pub type BoxedSource<'s> = Box<dyn Source + 's>;

/// Stream sorted `sources` to `sink` in `(ts, id, arrival)` order, keeping
/// only the first arrival of each `(ts, id)`.
pub fn merge(
    mut sources: Vec<BoxedSource<'_>>,
    mut sink: impl FnMut(&Head<'_>) -> Result<()>,
) -> Result<MergeStats> {
    let less = |s: &[BoxedSource<'_>], a: usize, b: usize| {
        s[a].head().key().cmp(&s[b].head().key()) == Ordering::Less
    };
    // Hand-rolled min-heap of source indices: the keys live in the sources.
    let mut heap: Vec<usize> = (0..sources.len()).collect();
    for i in (0..heap.len() / 2).rev() {
        sift_down(&mut heap, i, |a, b| less(&sources, a, b));
    }

    let mut stats = MergeStats::default();
    let mut last: Option<(u64, Vec<u8>)> = None;
    while let Some(&top) = heap.first() {
        let head = sources[top].head();
        let duplicate = last
            .as_ref()
            .is_some_and(|(ts, id)| *ts == head.ts && id.as_slice() == head.id);
        if duplicate {
            stats.deduplicated += 1;
        } else {
            sink(&head)?;
            stats.emitted += 1;
            let (ts, id) = last.get_or_insert_with(Default::default);
            *ts = head.ts;
            id.clear();
            id.extend_from_slice(head.id);
        }

        if !sources[top].advance()? {
            let end = heap.len() - 1;
            heap.swap(0, end);
            heap.pop();
        }
        sift_down(&mut heap, 0, |a, b| less(&sources, a, b));
    }
    Ok(stats)
}

fn sift_down(heap: &mut [usize], mut i: usize, less: impl Fn(usize, usize) -> bool) {
    loop {
        let l = 2 * i + 1;
        let r = l + 1;
        let mut min = i;
        if l < heap.len() && less(heap[l], heap[min]) {
            min = l;
        }
        if r < heap.len() && less(heap[r], heap[min]) {
            min = r;
        }
        if min == i {
            return;
        }
        heap.swap(i, min);
        i = min;
    }
}

/// [`merge`] over disk `runs` plus an optional in-memory source, opening at
/// most `fan_in` sources. Excess runs are pre-merged smallest first, only as
/// many as needed; merged inputs are deleted.
pub fn merge_bounded<'s>(
    mut runs: Vec<PathBuf>,
    memory: Option<BoxedSource<'s>>,
    fan_in: usize,
    mut next_run: impl FnMut() -> PathBuf,
    sink: impl FnMut(&Head<'_>) -> Result<()>,
) -> Result<MergeStats> {
    let fan_in = fan_in.max(2);
    let in_memory = usize::from(memory.is_some());
    let mut deduplicated = 0;
    while runs.len() + in_memory > fan_in {
        let take = (runs.len() + in_memory - fan_in + 1).min(fan_in);
        runs.sort_by_cached_key(|p| std::fs::metadata(p).map_or(0, |m| m.len()));
        let batch: Vec<PathBuf> = runs.drain(..take).collect();
        let path = next_run();
        let mut out = RunWriter::create(&path)?;
        let stats = merge(open_runs(&batch)?, |h| out.push(h.arrival, h.frame))?;
        out.finish()?;
        deduplicated += stats.deduplicated;
        for p in &batch {
            let _ = std::fs::remove_file(p);
        }
        runs.push(path);
    }
    let mut sources = open_runs(&runs)?;
    sources.extend(memory);
    let mut stats = merge(sources, sink)?;
    stats.deduplicated += deduplicated;
    Ok(stats)
}

fn open_runs<'s>(runs: &[PathBuf]) -> Result<Vec<BoxedSource<'s>>> {
    let mut out: Vec<BoxedSource<'s>> = Vec::with_capacity(runs.len());
    for path in runs {
        if let Some(r) = RunReader::open(path)? {
            out.push(Box::new(r));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode;
    use crate::record::Row;

    fn frame(ts: u64, id: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let row = Row {
            ts,
            id,
            keys: &[],
            extra: &[],
            data: "",
        };
        encode(&mut out, "s", 0, &row).unwrap();
        out
    }

    fn write_run(path: &Path, entries: &[(u64, &str, (u64, u64))]) {
        let mut w = RunWriter::create(path).unwrap();
        for &(ts, id, arrival) in entries {
            w.push(arrival, &frame(ts, id)).unwrap();
        }
        w.finish().unwrap();
    }

    type Seen = Vec<(u64, String, (u64, u64))>;

    fn record(seen: &mut Seen) -> impl FnMut(&Head<'_>) -> Result<()> + '_ {
        |h| {
            seen.push((h.ts, String::from_utf8(h.id.to_vec()).unwrap(), h.arrival));
            Ok(())
        }
    }

    fn push(batch: &mut Batch, ts: u64, id: &str, arrival: (u64, u64)) {
        let f = frame(ts, id);
        let (_, id, _) = verified_sort_key(&f).unwrap();
        let id = std::str::from_utf8(id).unwrap();
        batch.push(("s", 0), ts, id, arrival, &f);
    }

    #[test]
    fn runs_merge_in_order_and_keep_the_first_arrival() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        write_run(&a, &[(1, "x", (2, 0)), (3, "y", (2, 9)), (5, "", (2, 20))]);
        write_run(&b, &[(1, "x", (1, 7)), (2, "z", (1, 8)), (5, "", (3, 0))]);

        let mut seen = Vec::new();
        let stats = merge(open_runs(&[a, b]).unwrap(), record(&mut seen)).unwrap();
        assert_eq!(
            seen,
            [
                (1, "x".to_string(), (1, 7)),
                (2, "z".to_string(), (1, 8)),
                (3, "y".to_string(), (2, 9)),
                (5, String::new(), (2, 20)),
            ]
        );
        assert_eq!(
            stats,
            MergeStats {
                emitted: 4,
                deduplicated: 2
            }
        );
    }

    #[test]
    fn a_batch_sorts_dedups_and_merges_with_disk_runs() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().join("r");
        write_run(&run, &[(1, "x", (1, 0)), (4, "w", (1, 5))]);

        let mut batch = Batch::default();
        push(&mut batch, 3, "y", (2, 9));
        push(&mut batch, 1, "x", (2, 0)); // a later arrival of (1, "x")
        push(&mut batch, 3, "y", (2, 1)); // the earlier arrival of (3, "y")
        let mut sources = batch.into_sources();
        assert_eq!(sources.len(), 1);
        let (_, mem) = sources.pop().unwrap();
        assert_eq!(mem.entries.len(), 2, "(3, y) collapsed inside the batch");

        let mut seen = Vec::new();
        let stats = merge_bounded(
            vec![run],
            Some(Box::new(mem)),
            64,
            || unreachable!("two sources fit any fan-in"),
            record(&mut seen),
        )
        .unwrap();
        assert_eq!(
            seen,
            [
                (1, "x".to_string(), (1, 0)),
                (3, "y".to_string(), (2, 1)),
                (4, "w".to_string(), (1, 5)),
            ]
        );
        assert_eq!(stats.deduplicated, 1, "(1, x) across the two sources");
    }

    #[test]
    fn a_spilled_batch_reads_back_as_the_same_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut batch = Batch::default();
        for (i, ts) in [5u64, 1, 3, 1].into_iter().enumerate() {
            push(&mut batch, ts, "k", (7, i as u64));
        }
        let mut n = 0;
        let runs = batch
            .spill(|| {
                n += 1;
                dir.path().join(format!("{n}"))
            })
            .unwrap();
        assert!(batch.is_empty(), "spilling empties the batch");
        assert_eq!(batch.deduplicated(), 1);

        let paths: Vec<PathBuf> = runs.into_iter().map(|(_, p)| p).collect();
        let mut seen = Vec::new();
        merge(open_runs(&paths).unwrap(), record(&mut seen)).unwrap();
        let ts: Vec<u64> = seen.iter().map(|s| s.0).collect();
        assert_eq!(ts, [1, 3, 5]);
        assert_eq!(seen[0].2, (7, 1), "the first arrival of ts 1 survives");
    }

    #[test]
    fn a_bounded_merge_matches_an_unbounded_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut runs = Vec::new();
        // Overlapping keys so duplicates span intermediate passes too.
        for r in 0..7u64 {
            let path = dir.path().join(format!("r{r}"));
            let entries: Vec<(u64, String, (u64, u64))> = (0..(10 + r * 5))
                .map(|i| (i * 3 % 17 + r % 3, format!("k{}", i % 4), (r, i)))
                .collect();
            let mut sorted = entries.clone();
            sorted.sort_by(|a, b| (a.0, &a.1, a.2).cmp(&(b.0, &b.1, b.2)));
            sorted.dedup_by(|l, f| l.0 == f.0 && l.1 == f.1);
            let borrowed: Vec<_> = sorted
                .iter()
                .map(|(t, i, a)| (*t, i.as_str(), *a))
                .collect();
            write_run(&path, &borrowed);
            runs.push(path);
        }
        let copy = |runs: &[PathBuf], tag: &str| -> Vec<PathBuf> {
            runs.iter()
                .map(|p| {
                    let to = p.with_extension(tag);
                    std::fs::copy(p, &to).unwrap();
                    to
                })
                .collect()
        };

        let mut whole = Vec::new();
        let all = merge(open_runs(&copy(&runs, "all")).unwrap(), record(&mut whole)).unwrap();

        for fan_in in [2, 3, 6] {
            let mut n = 0;
            let mut bounded = Vec::new();
            let tmp = dir.path().to_path_buf();
            let got = merge_bounded(
                copy(&runs, &format!("f{fan_in}")),
                None,
                fan_in,
                || {
                    n += 1;
                    tmp.join(format!("m{fan_in}-{n}"))
                },
                record(&mut bounded),
            )
            .unwrap();
            assert_eq!(bounded, whole, "fan-in {fan_in}");
            assert_eq!(got, all, "fan-in {fan_in}: same rows emitted and dropped");
            if fan_in == 6 {
                assert_eq!(n, 1, "7 runs into 6 needs one merge, of the 2 smallest");
            }
        }
    }

    #[test]
    fn a_truncated_run_is_an_error_not_a_short_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r");
        write_run(&path, &[(1, "a", (1, 0)), (2, "b", (1, 1))]);
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 3).unwrap();

        let err = open_runs(&[path])
            .and_then(|s| merge(s, |_| Ok(())))
            .unwrap_err()
            .to_string();
        assert!(err.contains("run file"), "{err}");
    }
}
