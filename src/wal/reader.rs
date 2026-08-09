//! Reading a segment back, one frame at a time — the flush's input side.
//!
//! The contract is what the flush must be able to do with a bad record: **count
//! it, route it aside, keep going.** Ingest validated only a frame's fixed
//! prefix, so a malformed `keys` section first surfaces here, an hour later, with
//! no way to reject it at the source — and a flush that can never finish means no
//! Parquet is ever produced again. So a fault is a *value* this reader yields,
//! never an error that ends iteration.
//!
//! Two faults, and the difference is whether the reader can find the next frame:
//!
//! - [`FrameFault::BadRecord`] and [`FrameFault::UnknownSchema`] — the frame's
//!   CRC checked out, so its `len` is trustworthy and the next frame's offset is
//!   known. The reader skips exactly this frame and continues.
//! - [`FrameFault::Undecodable`] — the CRC failed, or the length is impossible,
//!   or the buffer ends mid-frame. Nothing after this point can be located: a
//!   frame that fails CRC is by definition of unknown length, so "skip it" has
//!   no meaning. The reader stops, and everything before the fault is still
//!   good.
//!
//! # Key counts come from the caller
//!
//! [`crate::codec::decode`] needs the arity of the schema at the frame's epoch,
//! which lives in the manifest — see the codec's module docs for why. So a reader
//! takes a lookup closure and does the cheap prefix read first, to learn which
//! `(series, epoch)` to ask about.

use std::path::Path;

use crate::codec::{self, DecodeError, DecodeResult, SEGMENT_HEADER_BYTES};
use crate::error::Result;
use crate::record::{RecordRef, Value};

/// Read a whole segment file into memory.
///
/// Whole rather than streamed, deliberately: the flush sorts an index of offsets
/// into this buffer rather than decoded rows, so a segment costs its own size and
/// nothing per record.
///
/// That bounds flush memory only as well as segment size is bounded, which is
/// *approximately* `wal.segment_max_bytes`: a segment rolls once it is over the
/// limit, so one oversized `append_batch` makes one oversized segment.
/// `server.max_batch_bytes` upstream is what keeps the overshoot to one batch.
pub fn read_segment(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    Ok(std::fs::read(path)?)
}

/// One decoded frame and where it sits in the segment.
///
/// `offset` is half of the flush's `arrival` order — see
/// [`crate::flush::planner`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Frame<'a> {
    pub record: RecordRef<'a>,
    /// Byte offset of the frame within the segment file.
    pub offset: usize,
    /// Total bytes the frame occupies, `len` and `crc32` included.
    pub len: usize,
}

/// A frame that could not be turned into a record.
///
/// Every variant carries the offset: the flush copies the offending bytes to
/// `rejects/`, and "which record?" is always the next question.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFault<'a> {
    /// The frame is intact but its contents do not decode under the schema its
    /// epoch names — a key section that disagrees with the arity, an unknown
    /// type tag, a string that is not UTF-8. A well-behaved encoder cannot
    /// produce this; a hand-rolled client can. Skippable.
    BadRecord {
        offset: usize,
        len: usize,
        err: DecodeError,
    },

    /// The frame names an epoch that is not in the manifest's history for its
    /// series. Old epochs are never removed, so this means the manifest was
    /// truncated or hand-edited. Skippable.
    UnknownSchema {
        offset: usize,
        len: usize,
        series: &'a str,
        epoch: u32,
    },

    /// The bytes at this offset are not a locatable frame, so nothing after them
    /// can be found either. Iteration stops here; this is the fault recovery
    /// turns into a truncation.
    Undecodable { offset: usize, err: DecodeError },
}

impl std::fmt::Display for FrameFault<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameFault::BadRecord { offset, err, .. } => {
                write!(f, "frame at offset {offset} does not decode: {err}")
            }
            FrameFault::UnknownSchema {
                offset,
                series,
                epoch,
                ..
            } => write!(
                f,
                "frame at offset {offset} names series {series:?} at schema epoch {epoch}, \
                 which is absent from the manifest history"
            ),
            FrameFault::Undecodable { offset, err } => {
                write!(f, "frame at offset {offset} cannot be delimited: {err}")
            }
        }
    }
}

impl std::error::Error for FrameFault<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FrameFault::BadRecord { err, .. } | FrameFault::Undecodable { err, .. } => Some(err),
            FrameFault::UnknownSchema { .. } => None,
        }
    }
}

impl FrameFault<'_> {
    /// Where the offending frame starts.
    pub fn offset(&self) -> usize {
        match *self {
            FrameFault::BadRecord { offset, .. }
            | FrameFault::UnknownSchema { offset, .. }
            | FrameFault::Undecodable { offset, .. } => offset,
        }
    }

    /// True when the reader could not resynchronise and has stopped.
    pub fn is_terminal(&self) -> bool {
        matches!(self, FrameFault::Undecodable { .. })
    }
}

/// A frame, or the reason it is not one.
pub type FrameResult<'a> = std::result::Result<Frame<'a>, FrameFault<'a>>;

/// A forward cursor over the frames of one segment.
///
/// `L` maps `(series, epoch)` to that schema's key count; `None` produces
/// [`FrameFault::UnknownSchema`]. A closure rather than a `&Manifest` so the
/// flush can cache it — the same pair repeats for millions of consecutive
/// frames.
pub struct SegmentReader<'a, L> {
    id: u64,
    buf: &'a [u8],
    at: usize,
    max_record_bytes: usize,
    key_count: L,
    stopped: bool,
    // Reused across frames, so a scan of a 64 MiB segment allocates only until
    // these reach the high-water mark of one record.
    keys: Vec<Value<'a>>,
    extra: Vec<(&'a str, &'a str)>,
}

impl<L> std::fmt::Debug for SegmentReader<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("id", &self.id)
            .field("len", &self.buf.len())
            .field("at", &self.at)
            .field("stopped", &self.stopped)
            .finish_non_exhaustive()
    }
}

impl<'a, L> SegmentReader<'a, L>
where
    L: FnMut(&str, u32) -> Option<usize>,
{
    /// Parse the segment header and position at the first frame.
    ///
    /// A bad header is the one fatal failure: without it the file is not known to
    /// be an orc segment this build understands, and decoding anyway would be
    /// inventing records.
    pub fn new(buf: &'a [u8], max_record_bytes: usize, key_count: L) -> DecodeResult<Self> {
        let id = codec::parse_segment_header(buf)?;
        Ok(Self {
            id,
            buf,
            at: SEGMENT_HEADER_BYTES,
            max_record_bytes,
            key_count,
            stopped: false,
            keys: Vec::new(),
            extra: Vec::new(),
        })
    }

    /// The id the segment's own header declares. Worth comparing against the
    /// file name: they disagree only for a hand-copied or renamed segment, which
    /// silently reorders `arrival`.
    pub fn segment_id(&self) -> u64 {
        self.id
    }

    /// Offset of the next frame — also, after iteration ends, the offset of the
    /// first byte the reader could not use.
    ///
    /// The same offset startup recovery truncates at, though it computes it with
    /// its own scan: this one needs a schema for every frame, and recovery
    /// deliberately runs before it has one.
    pub fn offset(&self) -> usize {
        self.at
    }

    /// The declared key values of the frame most recently returned by
    /// [`Self::next_frame`], positional in the order its epoch declares.
    pub fn keys(&self) -> &[Value<'a>] {
        &self.keys
    }

    /// The `extra` pairs of the frame most recently returned, sorted by name.
    pub fn extra(&self) -> &[(&'a str, &'a str)] {
        &self.extra
    }

    /// The next frame, or the fault in its place. `None` at the end of the
    /// segment, or once a terminal fault has stopped the scan.
    pub fn next_frame(&mut self) -> Option<FrameResult<'a>> {
        if self.stopped || self.at >= self.buf.len() {
            return None;
        }

        // Copy the slice reference out of `self` first: the records handed back
        // borrow from the segment buffer, not from this reader, so they must not
        // be tied to the `&mut self` borrow.
        let buf = self.buf;
        let offset = self.at;
        let rest = &buf[offset..];

        // The cheap read first -- ts, epoch, series, id and a CRC check -- which
        // is what tells us both which schema to decode against and where the
        // frame ends.
        let prefix = match codec::validate_prefix(rest, self.max_record_bytes) {
            Ok(p) => p,
            Err(err) => {
                self.stopped = true;
                return Some(Err(FrameFault::Undecodable { offset, err }));
            }
        };

        let Some(key_count) = (self.key_count)(prefix.series, prefix.epoch) else {
            self.at += prefix.frame_len;
            return Some(Err(FrameFault::UnknownSchema {
                offset,
                len: prefix.frame_len,
                series: prefix.series,
                epoch: prefix.epoch,
            }));
        };

        match codec::decode(
            rest,
            key_count,
            self.max_record_bytes,
            &mut self.keys,
            &mut self.extra,
        ) {
            Ok((record, len)) => {
                self.at += len;
                Some(Ok(Frame {
                    record,
                    offset,
                    len,
                }))
            }
            // The CRC already agreed, so `frame_len` is trustworthy even though
            // the contents are not: skip precisely this record and carry on.
            Err(err) => {
                self.at += prefix.frame_len;
                Some(Err(FrameFault::BadRecord {
                    offset,
                    len: prefix.frame_len,
                    err,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{encode, encode_segment_header, reseal};
    use crate::record::Row;

    const MAX: usize = 16 * 1024;

    fn row<'a>(id: &'a str, keys: &'a [Value<'a>]) -> Row<'a> {
        Row {
            ts: 1_786_147_200_000_000,
            id,
            keys,
            extra: &[("feed", "itch")],
            data: "{}",
        }
    }

    /// A segment holding one frame per `(series, epoch, id, arity)` given.
    fn segment(id: u64, frames: &[(&str, u32, &str, usize)]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_segment_header(&mut buf, id);
        for &(series, epoch, rec_id, arity) in frames {
            let keys = vec![Value::I64(7); arity];
            encode(&mut buf, series, epoch, &row(rec_id, &keys)).unwrap();
        }
        buf
    }

    /// The schema lookup a well-formed corpus needs: one key everywhere.
    fn one_key(_series: &str, _epoch: u32) -> Option<usize> {
        Some(1)
    }

    #[test]
    fn reads_every_frame_with_its_offset() {
        let buf = segment(42, &[("trades", 0, "a", 1), ("quotes", 3, "b", 1)]);
        let mut r = SegmentReader::new(&buf, MAX, one_key).unwrap();
        assert_eq!(r.segment_id(), 42);

        let first = r.next_frame().unwrap().unwrap();
        assert_eq!(first.offset, SEGMENT_HEADER_BYTES);
        assert_eq!(first.record.series, "trades");
        assert_eq!(first.record.id, "a");
        assert_eq!(r.keys(), [Value::I64(7)]);
        assert_eq!(r.extra(), [("feed", "itch")]);

        let second = r.next_frame().unwrap().unwrap();
        assert_eq!(second.offset, first.offset + first.len);
        assert_eq!(second.record.series, "quotes");
        assert_eq!(second.record.epoch, 3);

        assert!(r.next_frame().is_none());
        assert_eq!(r.offset(), buf.len());
    }

    #[test]
    fn the_lookup_sees_each_frames_own_series_and_epoch() {
        // A segment spanning a reload-config: two epochs of one series, decoded
        // each under its own arity. Nothing outside the frame says which.
        let mut buf = Vec::new();
        encode_segment_header(&mut buf, 1);
        encode(&mut buf, "trades", 0, &row("old", &[Value::Str("AAPL")])).unwrap();
        encode(
            &mut buf,
            "trades",
            1,
            &row("new", &[Value::Str("AAPL"), Value::I64(100)]),
        )
        .unwrap();

        let mut seen = Vec::new();
        let mut r = SegmentReader::new(&buf, MAX, |series, epoch| {
            seen.push((series.to_string(), epoch));
            Some(if epoch == 0 { 1 } else { 2 })
        })
        .unwrap();

        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "old");
        assert_eq!(r.keys().len(), 1);
        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "new");
        assert_eq!(r.keys().len(), 2);
        assert!(r.next_frame().is_none());
        drop(r);

        assert_eq!(seen, [("trades".into(), 0), ("trades".into(), 1)]);
    }

    #[test]
    fn a_poison_frame_costs_one_record_and_no_more() {
        // Valid CRC, valid prefix, corrupt `keys` section: exactly what an
        // ill-behaved client can get past `append_raw`. The flush must write
        // every other record.
        let mut buf = segment(1, &[("s", 0, "before", 1), ("s", 0, "poison", 1)]);
        let poison_at = buf.len() - {
            let mut one = Vec::new();
            encode(&mut one, "s", 0, &row("poison", &[Value::I64(7)])).unwrap();
            one.len()
        };
        // The tag byte of the first key: len+crc, ts, epoch, series, id.
        let tag_at = poison_at + 8 + 8 + 4 + (2 + 1) + (2 + "poison".len());
        buf[tag_at] = 0x7f;
        reseal(&mut buf[poison_at..]);
        encode(&mut buf, "s", 0, &row("after", &[Value::I64(7)])).unwrap();

        let mut r = SegmentReader::new(&buf, MAX, one_key).unwrap();
        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "before");

        let fault = r.next_frame().unwrap().unwrap_err();
        assert_eq!(fault.offset(), poison_at);
        assert!(!fault.is_terminal());
        assert!(matches!(fault, FrameFault::BadRecord { err, .. } if err.is_corrupt()));

        // ...and the reader resynchronised exactly one frame later.
        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "after");
        assert!(r.next_frame().is_none());
    }

    #[test]
    fn an_unknown_epoch_is_skipped_not_guessed() {
        let buf = segment(1, &[("s", 0, "known", 1), ("s", 9, "stale", 1)]);
        let mut r = SegmentReader::new(&buf, MAX, |_, epoch| (epoch == 0).then_some(1)).unwrap();

        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "known");
        let fault = r.next_frame().unwrap().unwrap_err();
        assert!(matches!(
            fault,
            FrameFault::UnknownSchema {
                series: "s",
                epoch: 9,
                ..
            }
        ));
        assert!(!fault.is_terminal());
        assert!(r.next_frame().is_none());
    }

    #[test]
    fn a_torn_tail_stops_the_scan_at_the_last_good_offset() {
        let buf = segment(1, &[("s", 0, "whole", 1), ("s", 0, "torn", 1)]);
        for cut in 1..20 {
            let truncated = &buf[..buf.len() - cut];
            let mut r = SegmentReader::new(truncated, MAX, one_key).unwrap();
            let first = r.next_frame().unwrap().unwrap();
            assert_eq!(first.record.id, "whole");
            let good_upto = r.offset();

            let fault = r.next_frame().unwrap().unwrap_err();
            assert!(fault.is_terminal(), "cut of {cut}: {fault:?}");
            assert_eq!(fault.offset(), good_upto);
            // Terminal means terminal: the reader does not try to resynchronise.
            assert!(r.next_frame().is_none());
            assert_eq!(r.offset(), good_upto);
        }
    }

    #[test]
    fn a_flipped_crc_byte_is_terminal_because_len_is_no_longer_trustworthy() {
        let mut buf = segment(1, &[("s", 0, "good", 1), ("s", 0, "bad", 1)]);
        let last = buf.len() - 1;
        buf[last] ^= 0x01;

        let mut r = SegmentReader::new(&buf, MAX, one_key).unwrap();
        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "good");
        let fault = r.next_frame().unwrap().unwrap_err();
        assert!(fault.is_terminal());
        assert!(matches!(fault, FrameFault::Undecodable { err, .. } if err.is_corrupt()));
    }

    #[test]
    fn a_zero_padded_tail_stops_the_scan() {
        // What a sparse write or a filesystem that zero-fills leaves behind.
        let mut buf = segment(1, &[("s", 0, "good", 1)]);
        let good_upto = buf.len();
        buf.resize(good_upto + 4096, 0);

        let mut r = SegmentReader::new(&buf, MAX, one_key).unwrap();
        assert_eq!(r.next_frame().unwrap().unwrap().record.id, "good");
        let fault = r.next_frame().unwrap().unwrap_err();
        assert!(fault.is_terminal());
        assert_eq!(fault.offset(), good_upto);
    }

    #[test]
    fn a_bad_header_is_the_one_fatal_failure() {
        let mut buf = segment(1, &[("s", 0, "x", 1)]);
        buf[0] ^= 0xff;
        assert!(
            SegmentReader::new(&buf, MAX, one_key)
                .unwrap_err()
                .is_corrupt()
        );

        assert!(
            SegmentReader::new(&buf[..3], MAX, one_key)
                .unwrap_err()
                .is_truncated()
        );

        // A header and nothing else is a legitimate empty segment.
        let empty = segment(5, &[]);
        let mut r = SegmentReader::new(&empty, MAX, one_key).unwrap();
        assert_eq!(r.segment_id(), 5);
        assert!(r.next_frame().is_none());
    }

    #[test]
    fn read_segment_reads_what_the_writer_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.wal");
        let buf = segment(3, &[("s", 0, "x", 1)]);
        std::fs::write(&path, &buf).unwrap();
        assert_eq!(read_segment(&path).unwrap(), buf);
    }
}
