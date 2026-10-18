//! Reading a segment back, one frame at a time, for the flush.
//!
//! A bad record is a yielded *value*, never an error that ends iteration, so one
//! poison frame can't stall the flush forever:
//!
//! - [`FrameFault::BadRecord`], [`FrameFault::UnknownSchema`]: CRC is good, so
//!   `len` is trusted; skip this frame and continue.
//! - [`FrameFault::Undecodable`]: the frame can't be delimited; stop.
//!
//! Key counts come from a caller-supplied `(series, epoch)` lookup, since
//! [`crate::codec::decode`] needs the schema's arity.

use std::path::Path;

use crate::codec::{self, DecodeError, DecodeResult, SEGMENT_HEADER_BYTES};
use crate::error::Result;
use crate::record::{RecordRef, Value};

/// Read a whole segment file into memory. The flush uses [`read_segment_into`].
pub fn read_segment(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    Ok(std::fs::read(path)?)
}

/// [`read_segment`] into a reused buffer, replacing its contents. The flush
/// reads segments one by one; reuse keeps its footprint at one segment.
pub fn read_segment_into(path: impl AsRef<Path>, buf: &mut Vec<u8>) -> Result<()> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    buf.clear();
    let len = file.metadata().map_or(0, |m| m.len() as usize);
    buf.reserve(len);
    file.read_to_end(buf)?;
    Ok(())
}

/// One decoded frame and where it sits in the segment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Frame<'a> {
    pub record: RecordRef<'a>,
    /// Byte offset in the segment; with the segment id, the flush's arrival order.
    pub offset: usize,
    /// Total bytes the frame occupies, `len` and `crc32` included.
    pub len: usize,
}

/// A frame that could not be turned into a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFault<'a> {
    /// Intact frame whose contents don't decode under its epoch's schema.
    /// Skippable.
    BadRecord {
        offset: usize,
        len: usize,
        err: DecodeError,
    },

    /// Epoch absent from the manifest history (truncated or hand-edited
    /// manifest). Skippable.
    UnknownSchema {
        offset: usize,
        len: usize,
        series: &'a str,
        epoch: u32,
    },

    /// Not a delimitable frame; iteration stops (recovery truncates here).
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

/// A forward cursor over the frames of one segment. `L` maps `(series, epoch)`
/// to a key count; `None` yields [`FrameFault::UnknownSchema`].
pub struct SegmentReader<'a, L> {
    id: u64,
    buf: &'a [u8],
    at: usize,
    max_record_bytes: usize,
    key_count: L,
    stopped: bool,
    // Reused across frames.
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
    /// Parse the segment header (the one fatal failure) and position at the
    /// first frame.
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

    /// The id the segment's header declares; differs from the file name only if
    /// the file was renamed.
    pub fn segment_id(&self) -> u64 {
        self.id
    }

    /// Offset of the next frame; after iteration ends, of the first unusable byte.
    pub fn offset(&self) -> usize {
        self.at
    }

    /// Key values of the frame last returned by [`Self::next_frame`].
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

        // Copied out so records borrow the buffer, not `&mut self`.
        let buf = self.buf;
        let offset = self.at;
        let rest = &buf[offset..];

        // Prefix + CRC: gives the schema to decode against and the frame length.
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
            // CRC agreed, so `frame_len` is trusted: skip just this record.
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
        // Two epochs of one series, each decoded under its own arity.
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
        // Valid CRC and prefix, corrupt `keys`: what a bad client can get past ingest.
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
