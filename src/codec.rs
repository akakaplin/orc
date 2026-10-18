//! The frame codec: orc's byte layout, shared by the WAL and the ZeroMQ wire so
//! the server appends client bytes untranscoded. Changing any byte here is a
//! format break and needs a [`FORMAT_VERSION`] bump.
//!
//! ```text
//! wal segment header (once):  magic u32 | format_version u16 | segment_id u64
//! zmq batch header:           magic u32 | format_version u16 | count u32
//! frame (identical in both):
//!   len    u32   payload byte length -- everything after the crc field
//!   crc32  u32   over len + payload
//!   ts     u64   epoch microseconds, UTC
//!   epoch  u32   the series' schema epoch, as of encoding
//!   series u16 len + UTF-8 bytes
//!   id     u16 len + UTF-8 bytes (may be zero-length)
//!   keys   positional: 1-byte tag + value, in the order declared by `epoch`
//!   extra  u16 count, then (u16 len, bytes) name/value pairs, sorted by name
//!   data   u32 len + bytes
//! ```
//!
//! Little-endian; every read is bounds-checked. Invariants (each tested):
//! - `len` is checked against `max_record_bytes` before anything is read or reserved.
//! - [`DecodeError::Truncated`] (torn tail) vs [`DecodeError::Corrupt`] (bytes
//!   contradict themselves) is exact; recovery branches on it.
//! - Re-encoding a record is byte-identical (`extra` is sorted), so a replayed
//!   flush writes the same Parquet.
//! - Frames carry no key count: the caller supplies the arity of the frame's epoch.

use crate::error::{Error, FORMAT_VERSION, MAGIC, Result};
use crate::record::{RecordRef, Row, Value, tag};

/// `magic | format_version | segment_id`, written once at the head of a WAL file.
pub const SEGMENT_HEADER_BYTES: usize = 4 + 2 + 8;

/// `magic | format_version | count`, part 0 of a ZeroMQ ingest message.
pub const BATCH_HEADER_BYTES: usize = 4 + 2 + 4;

/// The `len` and `crc32` fields that precede every frame's payload.
pub const FRAME_HEADER_BYTES: usize = 4 + 4;

/// Smallest possible payload (zero keys, all strings empty). A smaller `len` is
/// corrupt, not truncated.
pub const MIN_PAYLOAD_BYTES: usize = 8 + 4 + 2 + 2 + 2 + 4;

/// Why a frame could not be decoded. `Truncated` is a torn WAL tail (recovery
/// truncates there); `Corrupt` is present bytes that contradict each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The buffer ends mid-frame; `needed` is the complete frame's length.
    Truncated { needed: usize, got: usize },

    /// CRC mismatch, bad length, unknown tag, invalid UTF-8 or misaligned sections.
    Corrupt { reason: &'static str },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Truncated { needed, got } => {
                write!(f, "truncated: frame needs {needed} bytes, buffer has {got}")
            }
            DecodeError::Corrupt { reason } => write!(f, "corrupt: {reason}"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl DecodeError {
    /// True for the "stop scanning, the tail is torn" case.
    pub fn is_truncated(&self) -> bool {
        matches!(self, DecodeError::Truncated { .. })
    }

    /// True for the "these bytes are wrong" case.
    pub fn is_corrupt(&self) -> bool {
        matches!(self, DecodeError::Corrupt { .. })
    }
}

/// Decode-side result: failures are a property of the bytes, not the engine.
pub type DecodeResult<T> = std::result::Result<T, DecodeError>;

const fn corrupt(reason: &'static str) -> DecodeError {
    DecodeError::Corrupt { reason }
}

/// The `Corrupt` reason for an unknown [`FORMAT_VERSION`]. Recovery matches on it:
/// a version mismatch refuses to start, real damage is quarantined.
pub const UNSUPPORTED_VERSION: &str = "unsupported format version";

/// Append a WAL segment header. Written once, before the first frame.
pub fn encode_segment_header(out: &mut Vec<u8>, segment_id: u64) {
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&segment_id.to_le_bytes());
}

/// Parse a WAL segment header, returning its segment id. Bad magic/version is `Corrupt`.
pub fn parse_segment_header(buf: &[u8]) -> DecodeResult<u64> {
    let after = parse_common_header(buf, SEGMENT_HEADER_BYTES)?;
    Ok(u64::from_le_bytes(after[..8].try_into().unwrap()))
}

/// Append a ZeroMQ batch header. `count` is how many frames follow.
pub fn encode_batch_header(out: &mut Vec<u8>, count: u32) {
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
}

/// Parse a ZeroMQ batch header, returning its frame count (a hint only: frames
/// are self-delimiting).
pub fn parse_batch_header(buf: &[u8]) -> DecodeResult<u32> {
    let after = parse_common_header(buf, BATCH_HEADER_BYTES)?;
    Ok(u32::from_le_bytes(after[..4].try_into().unwrap()))
}

/// The `magic | format_version` shared by both headers; returns the bytes after it.
fn parse_common_header(buf: &[u8], needed: usize) -> DecodeResult<&[u8]> {
    if buf.len() < needed {
        return Err(DecodeError::Truncated {
            needed,
            got: buf.len(),
        });
    }
    if u32::from_le_bytes(buf[..4].try_into().unwrap()) != MAGIC {
        return Err(corrupt("bad magic; not an orc segment or batch"));
    }
    if u16::from_le_bytes(buf[4..6].try_into().unwrap()) != FORMAT_VERSION {
        return Err(corrupt(UNSUPPORTED_VERSION));
    }
    Ok(&buf[6..needed])
}

/// Appends one frame to `out`; on error `out` is restored to its entry length.
///
/// `extra` is sorted by `(name, value)` and duplicate names collapse to the first
/// (a repeated MAP key makes the Parquet file unreadable). The canonical order
/// makes re-encoding byte-identical.
pub fn encode(out: &mut Vec<u8>, series: &str, epoch: u32, row: &Row) -> Result<()> {
    Encoder::default().encode(out, series, epoch, row)
}

/// Reusable encode scratch (the `extra` sort order).
#[derive(Debug, Default)]
pub struct Encoder {
    order: Vec<u32>,
}

impl Encoder {
    /// See [`encode`]; identical, but reuses this encoder's scratch.
    pub fn encode(&mut self, out: &mut Vec<u8>, series: &str, epoch: u32, row: &Row) -> Result<()> {
        let start = out.len();
        match self.encode_inner(out, series, epoch, row) {
            Ok(()) => Ok(()),
            Err(e) => {
                out.truncate(start);
                Err(e)
            }
        }
    }

    fn encode_inner(
        &mut self,
        out: &mut Vec<u8>,
        series: &str,
        epoch: u32,
        row: &Row,
    ) -> Result<()> {
        let start = out.len();
        out.extend_from_slice(&[0u8; FRAME_HEADER_BYTES]); // len + crc, backfilled below

        out.extend_from_slice(&row.ts.to_le_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        put_str16(out, series)?;
        put_str16(out, row.id)?;

        for v in row.keys {
            put_value(out, *v)?;
        }

        let n = u16::try_from(row.extra.len()).map_err(|_| Error::RecordTooLarge {
            size: row.extra.len(),
            limit: u16::MAX as usize,
        })?;
        if is_canonical(row.extra) {
            // Fast path: already canonical (always so when re-encoding a decoded frame).
            out.extend_from_slice(&n.to_le_bytes());
            for &(name, value) in row.extra {
                put_str16(out, name)?;
                put_str16(out, value)?;
            }
        } else {
            self.order.clear();
            self.order.extend(0..n as u32);
            self.order.sort_unstable_by_key(|&i| row.extra[i as usize]);
            // Collapse each run of equal names to its first member.
            self.order
                .dedup_by(|a, b| row.extra[*a as usize].0 == row.extra[*b as usize].0);
            let kept = u16::try_from(self.order.len()).expect("no more than we started with");
            out.extend_from_slice(&kept.to_le_bytes());
            for &i in &self.order {
                let (name, value) = row.extra[i as usize];
                put_str16(out, name)?;
                put_str16(out, value)?;
            }
        }

        put_str32(out, row.data)?;

        // Backfill len (bytes after the crc), then crc over len + payload.
        let payload_len = out.len() - start - FRAME_HEADER_BYTES;
        let len = u32::try_from(payload_len).map_err(|_| Error::RecordTooLarge {
            size: payload_len,
            limit: u32::MAX as usize,
        })?;
        out[start..start + 4].copy_from_slice(&len.to_le_bytes());

        let mut h = crc32fast::Hasher::new();
        h.update(&out[start..start + 4]);
        h.update(&out[start + FRAME_HEADER_BYTES..]);
        let crc = h.finalize();
        out[start + 4..start + FRAME_HEADER_BYTES].copy_from_slice(&crc.to_le_bytes());
        Ok(())
    }
}

/// Already canonical: names strictly increasing (sorted, no duplicates). Shared
/// with [`crate::flush::parquet::RowBuilder`] so the two agree.
pub(crate) fn is_canonical(extra: &[(&str, &str)]) -> bool {
    extra.windows(2).all(|w| w[0].0 < w[1].0)
}

/// Strings whose length the layout carries in a `u16`: the series name, the id,
/// and every `extra` name and value.
fn put_str16(out: &mut Vec<u8>, s: &str) -> Result<()> {
    let n = u16::try_from(s.len()).map_err(|_| Error::RecordTooLarge {
        size: s.len(),
        limit: u16::MAX as usize,
    })?;
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

/// Strings whose length the layout carries in a `u32`: `data`, and the payload of
/// a [`Value::Str`].
fn put_str32(out: &mut Vec<u8>, s: &str) -> Result<()> {
    let n = u32::try_from(s.len()).map_err(|_| Error::RecordTooLarge {
        size: s.len(),
        limit: u32::MAX as usize,
    })?;
    out.extend_from_slice(&n.to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

/// One positional key: tag byte, then value. `Str` takes a `u32` length so a key
/// is never capped below `max_record_bytes`.
fn put_value(out: &mut Vec<u8>, v: Value<'_>) -> Result<()> {
    out.push(v.tag());
    match v {
        Value::Null => {}
        Value::Bool(b) => out.push(u8::from(b)),
        Value::I64(i) => out.extend_from_slice(&i.to_le_bytes()),
        // Bit-cast: NaN payloads and signed zero round-trip.
        Value::F64(f) => out.extend_from_slice(&f.to_le_bytes()),
        Value::Str(s) => put_str32(out, s)?,
    }
    Ok(())
}

/// The fixed prefix of a frame, plus how far the frame extends.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Prefix<'a> {
    pub ts: u64,
    pub epoch: u32,
    pub series: &'a str,
    pub id: &'a str,
    /// Total frame bytes, including `len` and `crc32`.
    pub frame_len: usize,
}

/// Check `len` and the CRC, then read only the fixed prefix: the `append_raw` hot
/// path. `keys`/`extra`/`data` are not walked, so a frame passing here can still
/// fail [`decode`].
pub fn validate_prefix<'a>(buf: &'a [u8], max_record_bytes: usize) -> DecodeResult<Prefix<'a>> {
    let (payload, frame_len) = frame_payload(buf, max_record_bytes)?;
    let mut c = Cursor::new(payload);
    let ts = c.u64()?;
    let epoch = c.u32()?;
    let series = c.str16()?;
    let id = c.str16()?;
    Ok(Prefix {
        ts,
        epoch,
        series,
        id,
        frame_len,
    })
}

/// Decode one CRC-checked frame from the front of `buf`; returns it and its length.
///
/// `key_count` is the arity of the frame's epoch. `keys`/`extra` are cleared and
/// refilled (unspecified after an error). Allocates nothing and never panics.
pub fn decode<'a>(
    buf: &'a [u8],
    key_count: usize,
    max_record_bytes: usize,
    keys: &mut Vec<Value<'a>>,
    extra: &mut Vec<(&'a str, &'a str)>,
) -> DecodeResult<(RecordRef<'a>, usize)> {
    let (payload, frame_len) = frame_payload(buf, max_record_bytes)?;
    decode_payload(payload, frame_len, key_count, keys, extra)
}

/// [`decode`] without the CRC, for frames this process already verified (the flush
/// checks each frame once at segment read). Lengths and bounds are still checked.
pub fn decode_verified<'a>(
    buf: &'a [u8],
    key_count: usize,
    keys: &mut Vec<Value<'a>>,
    extra: &mut Vec<(&'a str, &'a str)>,
) -> DecodeResult<(RecordRef<'a>, usize)> {
    let (payload, frame_len) = frame_extent(buf, usize::MAX)?;
    decode_payload(payload, frame_len, key_count, keys, extra)
}

/// The flush merge's sort key: `(ts, raw id bytes, frame length)`. No CRC or
/// UTF-8 check (done at segment read); byte order on `id` equals `str` order.
pub fn verified_sort_key(buf: &[u8]) -> DecodeResult<(u64, &[u8], usize)> {
    let (payload, frame_len) = frame_extent(buf, usize::MAX)?;
    let mut c = Cursor::new(payload);
    let ts = c.u64()?;
    let _epoch = c.u32()?;
    let series_len = c.u16()? as usize;
    c.take(series_len)?;
    let id_len = c.u16()? as usize;
    let id = c.take(id_len)?;
    Ok((ts, id, frame_len))
}

/// The shared body of [`decode`] and [`decode_verified`].
fn decode_payload<'a>(
    payload: &'a [u8],
    frame_len: usize,
    key_count: usize,
    keys: &mut Vec<Value<'a>>,
    extra: &mut Vec<(&'a str, &'a str)>,
) -> DecodeResult<(RecordRef<'a>, usize)> {
    keys.clear();
    extra.clear();

    let mut c = Cursor::new(payload);
    let ts = c.u64()?;
    let epoch = c.u32()?;
    let series = c.str16()?;
    let id = c.str16()?;

    for _ in 0..key_count {
        keys.push(c.value()?);
    }

    let n = c.u16()?;
    for _ in 0..n {
        let name = c.str16()?;
        let value = c.str16()?;
        extra.push((name, value));
    }

    let data = c.str32()?;

    // Leftover bytes mean the sections disagree with `len` (usually a wrong key
    // count); never return a record built from misaligned bytes.
    if !c.is_empty() {
        return Err(corrupt("trailing bytes after data section"));
    }

    Ok((
        RecordRef {
            ts,
            epoch,
            series,
            id,
            data,
        },
        frame_len,
    ))
}

/// Split off `len` and `crc32`, returning the CRC-verified payload and frame size.
/// `len` is bounded by `max_record_bytes` before the buffer is consulted, so a
/// bad length is `Corrupt`, never a request to wait for gigabytes.
fn frame_payload(buf: &[u8], max_record_bytes: usize) -> DecodeResult<(&[u8], usize)> {
    let (payload, frame_len) = frame_extent(buf, max_record_bytes)?;
    let mut h = crc32fast::Hasher::new();
    h.update(&buf[..4]);
    h.update(payload);
    if h.finalize() != u32::from_le_bytes(buf[4..8].try_into().unwrap()) {
        return Err(corrupt("crc32 mismatch"));
    }
    Ok((payload, frame_len))
}

/// The length checks of [`frame_payload`], without the CRC.
fn frame_extent(buf: &[u8], max_record_bytes: usize) -> DecodeResult<(&[u8], usize)> {
    if buf.len() < FRAME_HEADER_BYTES {
        return Err(DecodeError::Truncated {
            needed: FRAME_HEADER_BYTES,
            got: buf.len(),
        });
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if len < MIN_PAYLOAD_BYTES {
        return Err(corrupt("length below the smallest possible frame"));
    }
    if len > max_record_bytes {
        return Err(corrupt("length over max_record_bytes"));
    }

    // `len` is bounded, so this cannot overflow.
    let frame_len = FRAME_HEADER_BYTES + len;
    if buf.len() < frame_len {
        return Err(DecodeError::Truncated {
            needed: frame_len,
            got: buf.len(),
        });
    }

    Ok((&buf[FRAME_HEADER_BYTES..frame_len], frame_len))
}

/// A bounds-checked reader over one payload. Overruns are `Corrupt`: the payload
/// length is known, so running out means the contents contradict it.
#[derive(Debug)]
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn is_empty(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn take(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        // Checked: `n` comes from disk and may wrap on 32-bit targets.
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| corrupt("section length overflows the payload"))?;
        if end > self.buf.len() {
            return Err(corrupt("section runs past the end of the frame"));
        }
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> DecodeResult<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> DecodeResult<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn str16(&mut self) -> DecodeResult<&'a str> {
        let n = self.u16()? as usize;
        self.utf8(n)
    }

    fn str32(&mut self) -> DecodeResult<&'a str> {
        let n = self.u32()? as usize;
        self.utf8(n)
    }

    fn utf8(&mut self, n: usize) -> DecodeResult<&'a str> {
        let bytes = self.take(n)?;
        std::str::from_utf8(bytes).map_err(|_| corrupt("string is not valid UTF-8"))
    }

    fn value(&mut self) -> DecodeResult<Value<'a>> {
        let t = self.u8()?;
        Ok(match t {
            tag::NULL => Value::Null,
            tag::BOOL => match self.u8()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                // Only 0/1, so each record has exactly one encoding.
                _ => return Err(corrupt("bool value is neither 0 nor 1")),
            },
            tag::I64 => Value::I64(self.i64()?),
            tag::F64 => Value::F64(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            tag::STR => Value::Str(self.str32()?),
            _ => return Err(corrupt("unknown value tag")),
        })
    }
}

/// Recompute a frame's CRC after a test forged its bytes (a "poison frame" that
/// passes `append_raw` but fails [`decode`]).
#[cfg(test)]
pub(crate) fn reseal(frame: &mut [u8]) {
    let mut h = crc32fast::Hasher::new();
    h.update(&frame[..4]);
    h.update(&frame[FRAME_HEADER_BYTES..]);
    frame[4..8].copy_from_slice(&h.finalize().to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 16 * 1024;

    /// Encode one row into a fresh buffer, for tests that want just the frame.
    fn frame(series: &str, epoch: u32, row: &Row) -> Vec<u8> {
        let mut out = Vec::new();
        encode(&mut out, series, epoch, row).unwrap();
        out
    }

    fn row<'a>(
        ts: u64,
        id: &'a str,
        keys: &'a [Value<'a>],
        extra: &'a [(&'a str, &'a str)],
        data: &'a str,
    ) -> Row<'a> {
        Row {
            ts,
            id,
            keys,
            extra,
            data,
        }
    }

    #[test]
    fn round_trips_every_value_variant() {
        let keys = [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::I64(i64::MIN),
            Value::I64(i64::MAX),
            Value::F64(-0.0),
            Value::F64(1.5e300),
            Value::Str(""),
            Value::Str("AAPL"),
            Value::Str("unicode: ✓ ünïcödé 🚀"),
        ];
        let extra = [("feed", "itch"), ("zone", "us-east")];
        let r = row(
            1_786_147_200_000_000,
            "itch-8841203",
            &keys,
            &extra,
            r#"{"px":193.4,"sz":100}"#,
        );
        let buf = frame("trades", 7, &r);

        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        let (rec, used) = decode(&buf, keys.len(), MAX, &mut ks, &mut ex).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(rec.ts, r.ts);
        assert_eq!(rec.epoch, 7);
        assert_eq!(rec.series, "trades");
        assert_eq!(rec.id, r.id);
        assert_eq!(rec.data, r.data);
        assert_eq!(ks, keys);
        assert_eq!(ex, extra);
    }

    #[test]
    fn float_bits_survive_exactly() {
        // `Value`'s PartialEq can't see NaN payloads or -0.0, so compare bits.
        for bits in [
            f64::NAN.to_bits(),
            (-f64::NAN).to_bits(),
            0x7ff8_0000_dead_beef, // NaN with a payload
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::MIN_POSITIVE.to_bits(),
        ] {
            let keys = [Value::F64(f64::from_bits(bits))];
            let buf = frame("s", 1, &row(1, "", &keys, &[], ""));
            let (mut ks, mut ex) = (Vec::new(), Vec::new());
            decode(&buf, 1, MAX, &mut ks, &mut ex).unwrap();
            match ks[0] {
                Value::F64(f) => assert_eq!(f.to_bits(), bits),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn round_trips_the_empty_shapes() {
        // Every optional section at its minimum.
        let buf = frame("s", 0, &row(0, "", &[], &[], ""));
        assert_eq!(buf.len(), FRAME_HEADER_BYTES + MIN_PAYLOAD_BYTES + 1); // +1 for "s"

        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        let (rec, used) = decode(&buf, 0, MAX, &mut ks, &mut ex).unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(rec.id, "");
        assert_eq!(rec.data, "");
        assert!(ks.is_empty());
        assert!(ex.is_empty());
    }

    /// `extra` becomes a Parquet MAP, where a repeated key is malformed.
    #[test]
    fn duplicate_extra_names_collapse_to_the_first_in_canonical_order() {
        let decode_extra = |frame: &[u8]| -> Vec<(String, String)> {
            let mut keys = Vec::new();
            let mut extra = Vec::new();
            decode(frame, 0, 1 << 20, &mut keys, &mut extra).unwrap();
            extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };

        // Sorted by (name, value), so "a" -> "1" is the first of its run.
        let dupes = [("a", "2"), ("b", "9"), ("a", "1"), ("a", "3")];
        let got = decode_extra(&frame("s", 1, &row(1, "i", &[], &dupes, "d")));
        assert_eq!(
            got,
            [
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "9".to_string())
            ]
        );

        // The canonical fast path must agree with the slow one.
        let sorted = [("a", "1"), ("a", "3"), ("b", "9")];
        assert_eq!(
            decode_extra(&frame("s", 1, &row(1, "i", &[], &sorted, "d"))),
            got
        );

        // ...and the collapsed form re-encodes identically.
        let once = frame("s", 1, &row(1, "i", &[], &dupes, "d"));
        let pairs: Vec<(&str, &str)> = [("a", "1"), ("b", "9")].to_vec();
        assert_eq!(once, frame("s", 1, &row(1, "i", &[], &pairs, "d")));
    }

    #[test]
    fn extra_is_canonically_sorted_whatever_the_input_order() {
        let sorted = [("a", "1"), ("b", "2"), ("m", "3"), ("z", "4")];
        let canonical = frame("s", 1, &row(1, "i", &[], &sorted, "d"));

        for permuted in [
            [("z", "4"), ("m", "3"), ("b", "2"), ("a", "1")],
            [("m", "3"), ("a", "1"), ("z", "4"), ("b", "2")],
            [("b", "2"), ("z", "4"), ("a", "1"), ("m", "3")],
        ] {
            let got = frame("s", 1, &row(1, "i", &[], &permuted, "d"));
            assert_eq!(
                got, canonical,
                "permutation {permuted:?} encoded differently"
            );
        }

        // Equal names are ordered by value, so the multiset determines the bytes.
        let dup_a = [("k", "2"), ("k", "1")];
        let dup_b = [("k", "1"), ("k", "2")];
        assert_eq!(
            frame("s", 1, &row(1, "i", &[], &dup_a, "d")),
            frame("s", 1, &row(1, "i", &[], &dup_b, "d")),
        );

        // UTF-8 byte order is decode order, so decode/re-encode is a fixed point.
        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        decode(&canonical, 0, MAX, &mut ks, &mut ex).unwrap();
        let reencoded = frame("s", 1, &row(1, "i", &ks, &ex, "d"));
        assert_eq!(reencoded, canonical);
    }

    #[test]
    fn encoding_is_deterministic_across_a_shared_buffer() {
        // No state leaks between calls.
        let r = row(9, "id", &[Value::I64(1)], &[("b", "2"), ("a", "1")], "d");
        let mut enc = Encoder::default();
        let mut both = Vec::new();
        enc.encode(&mut both, "s", 3, &r).unwrap();
        enc.encode(&mut both, "s", 3, &r).unwrap();

        let one = frame("s", 3, &r);
        assert_eq!(both, [one.clone(), one].concat());
    }

    #[test]
    fn a_failed_encode_leaves_the_buffer_untouched() {
        let mut out = Vec::new();
        encode(&mut out, "s", 1, &row(1, "ok", &[], &[], "d")).unwrap();
        let good = out.clone();

        let huge = "x".repeat(u16::MAX as usize + 1);
        let extra = [("k", huge.as_str())];
        let err = encode(&mut out, "s", 1, &row(2, "", &[], &extra, "")).unwrap_err();
        assert!(matches!(err, Error::RecordTooLarge { .. }), "got {err:?}");
        assert_eq!(out, good);

        // ...and the buffer is still usable afterwards.
        encode(&mut out, "s", 1, &row(3, "next", &[], &[], "")).unwrap();
        assert!(out.len() > good.len());
    }

    #[test]
    fn headers_round_trip_and_reject_foreign_bytes() {
        let mut seg = Vec::new();
        encode_segment_header(&mut seg, 42);
        assert_eq!(seg.len(), SEGMENT_HEADER_BYTES);
        assert_eq!(parse_segment_header(&seg).unwrap(), 42);

        let mut batch = Vec::new();
        encode_batch_header(&mut batch, 1024);
        assert_eq!(batch.len(), BATCH_HEADER_BYTES);
        assert_eq!(parse_batch_header(&batch).unwrap(), 1024);

        for (name, buf) in [("segment", &seg), ("batch", &batch)] {
            for cut in 0..buf.len() {
                assert!(
                    match name {
                        "segment" => parse_segment_header(&buf[..cut]),
                        _ => parse_batch_header(&buf[..cut]).map(u64::from),
                    }
                    .unwrap_err()
                    .is_truncated(),
                    "{name} header of {cut} bytes should be truncated"
                );
            }

            let mut bad_magic = buf.to_vec();
            bad_magic[0] ^= 0xff;
            let mut bad_version = buf.to_vec();
            bad_version[4] ^= 0xff;
            for b in [bad_magic, bad_version] {
                let e = match name {
                    "segment" => parse_segment_header(&b),
                    _ => parse_batch_header(&b).map(u64::from),
                };
                assert!(e.unwrap_err().is_corrupt(), "{name}: {e:?}");
            }
        }
    }

    #[test]
    fn validate_prefix_matches_decode_and_skips_the_tail() {
        let keys = [Value::Str("AAPL"), Value::I64(7)];
        let extra = [("feed", "itch")];
        let r = row(123, "abc", &keys, &extra, "payload");
        let buf = frame("trades", 5, &r);

        let p = validate_prefix(&buf, MAX).unwrap();
        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        let (rec, used) = decode(&buf, keys.len(), MAX, &mut ks, &mut ex).unwrap();
        assert_eq!(
            (p.ts, p.epoch, p.series, p.id),
            (rec.ts, rec.epoch, rec.series, rec.id)
        );
        assert_eq!(p.frame_len, used);

        // Poison frame: a corrupt `keys` section under a valid CRC passes the prefix.
        let mut poison = buf.clone();
        let keys_at = FRAME_HEADER_BYTES + 8 + 4 + 2 + "trades".len() + 2 + r.id.len();
        poison[keys_at] = 0x7f; // not a known tag
        reseal(&mut poison);
        assert!(validate_prefix(&poison, MAX).is_ok());
        assert!(
            decode(&poison, keys.len(), MAX, &mut ks, &mut ex)
                .unwrap_err()
                .is_corrupt()
        );

        // But it does verify the CRC.
        let mut bad = buf.clone();
        *bad.last_mut().unwrap() ^= 0x01;
        assert!(validate_prefix(&bad, MAX).unwrap_err().is_corrupt());
    }

    #[test]
    fn wrong_key_count_is_corrupt_never_a_plausible_record() {
        let keys = [Value::I64(1), Value::I64(2), Value::I64(3)];
        let buf = frame("s", 1, &row(1, "i", &keys, &[("a", "b")], "d"));
        let (mut ks, mut ex) = (Vec::new(), Vec::new());

        // The trailing-bytes check turns a wrong arity into an error.
        for n in [0usize, 1, 2, 4, 100] {
            let e = decode(&buf, n, MAX, &mut ks, &mut ex);
            assert!(
                e.is_err_and(|e| e.is_corrupt()),
                "key count {n} must be rejected for a frame written with 3"
            );
        }
        assert!(decode(&buf, 3, MAX, &mut ks, &mut ex).is_ok());
    }

    // ---- safety: the length cap ------------------------------------------

    #[test]
    fn an_implausible_length_is_rejected_before_anything_is_reserved() {
        // 8 bytes claiming a 4 GiB payload: Corrupt, not Truncated.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());

        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        let e = decode(&buf, 0, MAX, &mut ks, &mut ex).unwrap_err();
        assert!(e.is_corrupt(), "got {e:?}");
        assert!(validate_prefix(&buf, MAX).unwrap_err().is_corrupt());

        // Nothing was reserved.
        assert_eq!(ks.capacity(), 0);
        assert_eq!(ex.capacity(), 0);

        // Also for a length merely over the configured limit.
        let real = frame("s", 1, &row(1, "i", &[], &[], "0123456789"));
        assert!(decode(&real, 0, MAX, &mut ks, &mut ex).is_ok());
        let tight = real.len() - FRAME_HEADER_BYTES - 1;
        assert!(
            decode(&real, 0, tight, &mut ks, &mut ex)
                .unwrap_err()
                .is_corrupt()
        );
    }

    #[test]
    fn a_length_below_the_minimum_frame_is_corrupt() {
        for len in 0..MIN_PAYLOAD_BYTES as u32 {
            let mut buf = Vec::new();
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.resize(FRAME_HEADER_BYTES + len as usize, 0);
            let (mut ks, mut ex) = (Vec::new(), Vec::new());
            assert!(
                decode(&buf, 0, MAX, &mut ks, &mut ex)
                    .unwrap_err()
                    .is_corrupt(),
                "len {len} should be corrupt, not truncated"
            );
        }
    }

    // ---- fuzz-style: no input may panic ----------------------------------

    /// One record and everything needed to check a decode against it.
    struct Case {
        series: &'static str,
        epoch: u32,
        ts: u64,
        id: &'static str,
        keys: Vec<Value<'static>>,
        extra: Vec<(&'static str, &'static str)>,
        data: &'static str,
    }

    /// Every frame shares this key arity, so decoding needs no schema.
    const CORPUS_KEYS: usize = 5;

    fn corpus() -> (Vec<u8>, Vec<Case>) {
        let cases = vec![
            Case {
                series: "trades",
                epoch: 7,
                ts: 1_786_147_200_000_000,
                id: "itch-8841203",
                keys: vec![
                    Value::Str("AAPL"),
                    Value::I64(-42),
                    Value::F64(193.4),
                    Value::Bool(true),
                    Value::Null,
                ],
                extra: vec![("feed", "itch"), ("zone", "us-east-1")],
                data: r#"{"px":193.4,"sz":100}"#,
            },
            Case {
                series: "q",
                epoch: 0,
                ts: 0,
                id: "",
                keys: vec![Value::Null; CORPUS_KEYS],
                extra: vec![],
                data: "",
            },
            Case {
                series: "unicode-✓",
                epoch: u32::MAX,
                // The codec carries any `ts`; the accept window judges it.
                ts: u64::MAX,
                id: "ünïcödé id",
                keys: vec![
                    Value::Str(""),
                    Value::I64(i64::MAX),
                    Value::F64(-0.0),
                    Value::Bool(false),
                    Value::Str("🚀"),
                ],
                extra: vec![("a", ""), ("b", "2"), ("c", "3"), ("d", "4")],
                data: "tail",
            },
        ];

        let mut buf = Vec::new();
        for c in &cases {
            encode(
                &mut buf,
                c.series,
                c.epoch,
                &Row {
                    ts: c.ts,
                    id: c.id,
                    keys: &c.keys,
                    extra: &c.extra,
                    data: c.data,
                },
            )
            .unwrap();
        }
        (buf, cases)
    }

    /// Decode as far as the bytes allow, checking every record that comes back
    /// against the corpus. Returns how many frames decoded and how it stopped.
    fn scan(buf: &[u8], cases: &[Case]) -> (usize, Option<DecodeError>) {
        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        let mut at = 0;
        let mut n = 0;
        while at < buf.len() {
            match decode(&buf[at..], CORPUS_KEYS, MAX, &mut ks, &mut ex) {
                Ok((rec, used)) => {
                    // A successful decode must return the right record.
                    assert!(n < cases.len(), "decoded more frames than were encoded");
                    let c = &cases[n];
                    assert_eq!(rec.ts, c.ts);
                    assert_eq!(rec.epoch, c.epoch);
                    assert_eq!(rec.series, c.series);
                    assert_eq!(rec.id, c.id);
                    assert_eq!(rec.data, c.data);
                    assert_eq!(ks, c.keys);
                    assert_eq!(ex, c.extra);
                    at += used;
                    n += 1;
                }
                Err(e) => return (n, Some(e)),
            }
        }
        (n, None)
    }

    #[test]
    fn every_prefix_is_truncated_and_never_panics() {
        let (buf, cases) = corpus();
        assert_eq!(scan(&buf, &cases), (cases.len(), None));

        for cut in 0..buf.len() {
            let (n, err) = scan(&buf[..cut], &cases);
            match err {
                // A cut inside a frame must be Truncated...
                Some(e) => assert!(e.is_truncated(), "prefix of {cut} bytes gave {e:?}"),
                // ...unless the cut fell exactly on a frame boundary.
                None => assert!(n < cases.len() || cut == buf.len()),
            }
        }
    }

    #[test]
    fn every_single_byte_corruption_is_caught() {
        let (clean, cases) = corpus();

        for i in 0..clean.len() {
            // CRC-32 catches every burst up to 32 bits, so some frame must fail.
            let mut buf = clean.clone();
            buf[i] ^= 0xff;
            let (n, err) = scan(&buf, &cases);
            let e = err.unwrap_or_else(|| panic!("byte {i} flipped but the stream decoded clean"));
            assert!(
                e.is_corrupt() || e.is_truncated(),
                "byte {i}: unexpected {e:?}"
            );
            assert!(n < cases.len(), "byte {i} flipped but every frame decoded");

            // Every single-bit flip too: the case CRC-32 guarantees to catch.
            for bit in 0..8 {
                let mut buf = clean.clone();
                buf[i] ^= 1 << bit;
                let (n, err) = scan(&buf, &cases);
                assert!(err.is_some(), "bit {bit} of byte {i} went undetected");
                assert!(n < cases.len());
            }
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        // Insurance that the decoder is total: a panic is an out-of-bounds read.
        let mut rng = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let (clean, _) = corpus();
        for round in 0..2_000 {
            // Per round: the scratch borrows from this round's buffer.
            let (mut ks, mut ex) = (Vec::new(), Vec::new());
            let mut buf = if round % 2 == 0 {
                clean.clone()
            } else {
                vec![0u8; (next() % 64) as usize]
            };
            // Splatter a handful of random bytes over random positions.
            for _ in 0..1 + next() % 8 {
                if buf.is_empty() {
                    break;
                }
                let at = (next() % buf.len() as u64) as usize;
                buf[at] = next() as u8;
            }

            let mut at = 0;
            while at < buf.len() {
                match decode(&buf[at..], CORPUS_KEYS, MAX, &mut ks, &mut ex) {
                    Ok((_, used)) => {
                        assert!(used > 0, "a zero-length frame would loop forever");
                        at += used;
                    }
                    Err(_) => break,
                }
            }
            let _ = validate_prefix(&buf, MAX);
            let _ = parse_segment_header(&buf);
            let _ = parse_batch_header(&buf);
        }
    }
}
