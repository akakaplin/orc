//! The frame codec — the single definition of orc's byte layout.
//!
//! The WAL and the ZeroMQ wire use the *same* frame, which is what lets the
//! server append the bytes a client sent with no transcoding. That makes this
//! module the on-disk format: every byte written here is a compatibility
//! commitment, and changing one means bumping [`FORMAT_VERSION`].
//!
//! ```text
//! wal segment header (once):  magic u32 | format_version u16 | segment_id u64
//! zmq batch header:           magic u32 | format_version u16 | count u32
//! frame (identical in both):
//!   len    u32   payload byte length -- everything after the crc field
//!   crc32  u32   over len + payload
//!   ts     i64   epoch microseconds, UTC
//!   epoch  u32   the series' schema epoch, as of encoding
//!   series u16 len + UTF-8 bytes
//!   id     u16 len + UTF-8 bytes (may be zero-length)
//!   keys   positional: 1-byte tag + value, in the order declared by `epoch`
//!   extra  u16 count, then (u16 len, bytes) name/value pairs, sorted by name
//!   data   u32 len + bytes
//! ```
//!
//! Little-endian throughout, and every multi-byte read goes through
//! `from_le_bytes` on a bounds-checked slice, so nothing here assumes alignment
//! or host endianness.
//!
//! Three properties are load-bearing and each has a dedicated test:
//!
//! - **`len` is validated against `max_record_bytes` before anything is read or
//!   reserved.** A corrupt four-byte length is otherwise a request for 4 GiB.
//! - **Truncated and corrupt are different answers.** [`DecodeError::Truncated`]
//!   means "the buffer ends mid-frame, stop scanning here" — recovery truncates
//!   the segment at that offset. [`DecodeError::Corrupt`] means the bytes that
//!   *are* present disagree with themselves. Recovery logs them differently and
//!   the flush counts them separately, so the split must be exact.
//! - **Re-encoding a logical record is byte-identical**, which is what makes a
//!   crash-recovered flush produce the same Parquet file as the run it replaced.
//!   `extra` is sorted at encode time to get there.
//!
//! # Key counts are not in the frame
//!
//! A frame stores key *values* with no count: the arity comes from the series'
//! schema for the frame's `epoch`, which the frame names. So [`decode`] takes the
//! expected key count from the caller, and the caller learns which schema to ask
//! for by reading the cheap fixed prefix first:
//!
//! ```ignore
//! while !rest.is_empty() {
//!     let p = codec::validate_prefix(rest, max_record_bytes)?;   // ts, epoch, series, id
//!     let n = schema_history.key_count(p.series, p.epoch)?;      // caller's lookup
//!     let (rec, used) = codec::decode(rest, n, max_record_bytes, &mut keys, &mut extra)?;
//!     rest = &rest[used..];
//! }
//! ```
//!
//! The alternative — storing a key count in every frame — would let a decoder
//! walk a frame with no schema at all, but it would also make the count a second
//! source of truth that can disagree with the manifest, and pay 1-2 bytes per
//! record forever to carry the disagreement. The prefix re-read costs a handful
//! of loads at fixed offsets.

use crate::error::{Error, FORMAT_VERSION, MAGIC, Result};
use crate::record::{RecordRef, Row, Value, tag};

/// `magic | format_version | segment_id`, written once at the head of a WAL file.
pub const SEGMENT_HEADER_BYTES: usize = 4 + 2 + 8;

/// `magic | format_version | count`, part 0 of a ZeroMQ ingest message.
pub const BATCH_HEADER_BYTES: usize = 4 + 2 + 4;

/// The `len` and `crc32` fields that precede every frame's payload.
pub const FRAME_HEADER_BYTES: usize = 4 + 4;

/// The smallest payload any frame can have: `ts` + `epoch` + two empty strings +
/// an empty `extra` + an empty `data`, with zero declared keys.
///
/// A `len` below this is impossible rather than merely short, so it is reported
/// as corruption without waiting to see whether the buffer could supply it.
pub const MIN_PAYLOAD_BYTES: usize = 8 + 4 + 2 + 2 + 2 + 4;

/// Why a frame could not be decoded.
///
/// The two variants are not two flavours of the same thing. `Truncated` is the
/// expected end of a torn WAL segment — a crash mid-`write()` — and recovery
/// responds by truncating the file at that offset. `Corrupt` means bytes that
/// are all present contradict each other, which recovery logs loudly and the
/// flush path turns into one rejected record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The buffer ends before the frame does. `needed` is the total byte length
    /// this frame will occupy once complete, so a streaming reader knows exactly
    /// how much more to pull.
    #[error("truncated: frame needs {needed} bytes, buffer has {got}")]
    Truncated { needed: usize, got: usize },

    /// The frame is self-inconsistent: CRC mismatch, an implausible length, an
    /// unknown type tag, invalid UTF-8, or a section that does not end where the
    /// declared length says it must.
    #[error("corrupt: {reason}")]
    Corrupt { reason: &'static str },
}

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

/// Result alias for the decode side, which never returns [`Error`]: decoding
/// failures are a property of the bytes, not of the engine.
pub type DecodeResult<T> = std::result::Result<T, DecodeError>;

const fn corrupt(reason: &'static str) -> DecodeError {
    DecodeError::Corrupt { reason }
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/// Append a WAL segment header. Written once, before the first frame.
pub fn encode_segment_header(out: &mut Vec<u8>, segment_id: u64) {
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&segment_id.to_le_bytes());
}

/// Parse a WAL segment header, returning its segment id.
///
/// A wrong magic or version is reported as [`DecodeError::Corrupt`] rather than a
/// variant of its own: to a reader that cannot interpret the bytes, "this is not
/// an orc segment" and "this segment is damaged" call for the same response, and
/// a third variant would blur the one distinction recovery actually branches on.
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

/// Parse a ZeroMQ batch header, returning the frame count it declares.
///
/// The count is a hint for the ingest loop's bookkeeping, not a length: the
/// frames themselves are self-delimiting, so a lying count cannot desynchronise
/// the reader.
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
        return Err(corrupt("unsupported format version"));
    }
    Ok(&buf[6..needed])
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Appends one frame to `out`.
///
/// `out` is the caller's reusable buffer — the WAL writer's thread-local scratch
/// or the client's batch buffer — so the steady-state path allocates only when
/// that buffer grows.
///
/// On any error `out` is restored to the length it had on entry. A half-written
/// frame left in a WAL buffer would be indistinguishable from a real one to the
/// next writer, so partial output is never an acceptable failure mode here.
///
/// `extra` is sorted by `(name, value)` at encode time. Sorting by name alone
/// would leave two pairs with equal names in input order, which is enough to make
/// the same logical record encode to two different byte strings — and the
/// byte-identical re-encode is what makes a crash-recovered flush reproducible.
/// Duplicate names are preserved, not rejected: ingest never turns away a record
/// for the shape of its `extra`.
pub fn encode(out: &mut Vec<u8>, series: &str, epoch: u32, row: &Row) -> Result<()> {
    Encoder::default().encode(out, series, epoch, row)
}

/// A reusable encode-side scratch.
///
/// The only allocation the frame layout forces is the ordering of `extra`, and
/// this holds it across calls. It stores *indices*, not borrowed pairs, so the
/// scratch outlives the rows it sorts without borrowing from any of them.
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
        out.extend_from_slice(&n.to_le_bytes());
        if is_canonical(row.extra) {
            // The common case: the caller already handed us sorted pairs (a
            // decoded frame being re-encoded always is), so skip the scratch.
            for &(name, value) in row.extra {
                put_str16(out, name)?;
                put_str16(out, value)?;
            }
        } else {
            self.order.clear();
            self.order.extend(0..n as u32);
            self.order.sort_unstable_by_key(|&i| row.extra[i as usize]);
            for &i in &self.order {
                let (name, value) = row.extra[i as usize];
                put_str16(out, name)?;
                put_str16(out, value)?;
            }
        }

        put_str32(out, row.data)?;

        // Backfill len over the payload, then crc over len + payload. `len`
        // counts everything after the crc field, so the frame occupies
        // FRAME_HEADER_BYTES + len bytes.
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

/// Already in the canonical `(name, value)` order the encoder would produce.
fn is_canonical(extra: &[(&str, &str)]) -> bool {
    extra.windows(2).all(|w| w[0] <= w[1])
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

/// One positional key: its tag byte, then its value.
///
/// `Value::Str` takes a `u32` length rather than the `u16` used for `extra`,
/// deliberately. A declared key is a column value, not a label, and a `u16` here
/// would put a 64 KiB cliff in the format that a `max_record_bytes` above 64 KiB
/// could walk a caller off — an encodable record the config permits but the frame
/// cannot represent. `extra` keeps its `u16` because the layout specifies it, and
/// because labels that large are pathological by construction.
fn put_value(out: &mut Vec<u8>, v: Value<'_>) -> Result<()> {
    out.push(v.tag());
    match v {
        Value::Null => {}
        Value::Bool(b) => out.push(u8::from(b)),
        Value::I64(i) => out.extend_from_slice(&i.to_le_bytes()),
        // `to_le_bytes` is a bit-cast, so NaN payloads and signed zero survive a
        // round trip. Any float-to-float conversion here would not.
        Value::F64(f) => out.extend_from_slice(&f.to_le_bytes()),
        Value::Str(s) => put_str32(out, s)?,
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// The fixed prefix of a frame, plus how far the frame extends.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Prefix<'a> {
    pub ts: i64,
    pub epoch: u32,
    pub series: &'a str,
    pub id: &'a str,
    /// Total bytes this frame occupies, including `len` and `crc32`. Advance the
    /// cursor by this to reach the next frame.
    pub frame_len: usize,
}

/// Verify a frame's integrity and read only its fixed prefix.
///
/// This is the server's `append_raw` hot path. It checks `len` against
/// `max_record_bytes` and verifies the CRC — so the frame is known to be intact
/// and self-delimiting — then reads `ts`, `epoch`, `series` and `id` and stops.
/// It never walks `keys`, `extra` or `data`, which is the whole point: those are
/// variable-length and structurally rich, and validating them per record would
/// put work on remote ingest that the hourly flush can do once, amortised.
///
/// A frame that passes here can still fail [`decode`] later — a malformed `keys`
/// section survives a correct CRC if it was encoded wrong in the first place.
/// That is why the flush treats a decode failure as one rejected record rather
/// than as an impossibility.
pub fn validate_prefix<'a>(buf: &'a [u8], max_record_bytes: usize) -> DecodeResult<Prefix<'a>> {
    let (payload, frame_len) = frame_payload(buf, max_record_bytes)?;
    let mut c = Cursor::new(payload);
    let ts = c.i64()?;
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

/// Decode one frame from the front of `buf`.
///
/// `key_count` is the arity of the series' schema for this frame's epoch — see
/// the module docs for why it comes from the caller. `keys` and `extra` are
/// caller-owned scratch: they are cleared and refilled, so a decode loop
/// allocates only until they reach their high-water mark. Their contents after
/// an error are unspecified.
///
/// Returns the record and the number of bytes consumed.
///
/// The decoder allocates nothing itself. Every read is bounds-checked against the
/// payload slice, which is why no input — however hostile — can make it panic or
/// read past the buffer.
pub fn decode<'a>(
    buf: &'a [u8],
    key_count: usize,
    max_record_bytes: usize,
    keys: &mut Vec<Value<'a>>,
    extra: &mut Vec<(&'a str, &'a str)>,
) -> DecodeResult<(RecordRef<'a>, usize)> {
    let (payload, frame_len) = frame_payload(buf, max_record_bytes)?;
    keys.clear();
    extra.clear();

    let mut c = Cursor::new(payload);
    let ts = c.i64()?;
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

    // The frame declared its own length, so leftover bytes mean the sections
    // inside disagree with it -- most often a key count that does not match the
    // schema the frame was written under. Silently ignoring the tail would hand
    // the caller a plausible record built from the wrong bytes.
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

/// Split `len` and `crc32` off the front of `buf`, returning the verified payload
/// and the frame's total size.
///
/// The order of checks here is the safety property, not an implementation
/// detail. `len` is bounded against `max_record_bytes` **before** the buffer is
/// consulted for that many bytes and before anything is reserved, so a corrupt
/// four-byte length is rejected as corruption instead of becoming a demand for
/// gigabytes. It is also why an over-large `len` is `Corrupt` rather than
/// `Truncated`: "wait for more bytes" is exactly the wrong answer to a length
/// the format can never produce.
fn frame_payload(buf: &[u8], max_record_bytes: usize) -> DecodeResult<(&[u8], usize)> {
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

    // `len` is now known to be small, so this addition cannot overflow and the
    // buffer check that follows cannot be tricked by a wrapped total.
    let frame_len = FRAME_HEADER_BYTES + len;
    if buf.len() < frame_len {
        return Err(DecodeError::Truncated {
            needed: frame_len,
            got: buf.len(),
        });
    }

    let payload = &buf[FRAME_HEADER_BYTES..frame_len];
    let mut h = crc32fast::Hasher::new();
    h.update(&buf[..4]);
    h.update(payload);
    if h.finalize() != u32::from_le_bytes(buf[4..8].try_into().unwrap()) {
        return Err(corrupt("crc32 mismatch"));
    }
    Ok((payload, frame_len))
}

/// A bounds-checked forward reader over one frame's payload.
///
/// Every overrun is [`DecodeError::Corrupt`], never `Truncated`: the payload is
/// exactly as long as the frame said and the CRC has already agreed, so running
/// out of bytes inside it means the contents contradict the length — a short
/// *buffer* was ruled out before this cursor existed.
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
        // Checked addition, not `pos + n`: `n` comes from a length field on
        // disk, so on a 32-bit target it can be large enough to wrap.
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
                // Accepting any non-zero byte as `true` would let two different
                // frames mean the same record, breaking the byte-identical
                // re-encode this format depends on.
                _ => return Err(corrupt("bool value is neither 0 nor 1")),
            },
            tag::I64 => Value::I64(self.i64()?),
            tag::F64 => Value::F64(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            tag::STR => Value::Str(self.str32()?),
            _ => return Err(corrupt("unknown value tag")),
        })
    }
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
        ts: i64,
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
        // NaN payloads and signed zero are only preserved by a bit-cast, and
        // `Value`'s PartialEq cannot see the difference -- so compare bits.
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
        // Empty id, no keys, no extra, empty data -- every optional section at
        // its minimum at once.
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

        // Sorting is by UTF-8 byte order, which is what a decoded frame comes
        // back in -- so decode/re-encode is a fixed point.
        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        decode(&canonical, 0, MAX, &mut ks, &mut ex).unwrap();
        let reencoded = frame("s", 1, &row(1, "i", &ks, &ex, "d"));
        assert_eq!(reencoded, canonical);
    }

    #[test]
    fn encoding_is_deterministic_across_a_shared_buffer() {
        // Two frames appended to one buffer must be byte-identical to the same
        // two encoded separately: no state leaks between calls.
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
        // A half-written frame in a WAL buffer is indistinguishable from a real
        // one, so a rejected record must not leave a trace.
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

        // The poison frame: a corrupt `keys` section under a *valid* CRC, which
        // is what an ill-behaved client can produce. `append_raw` accepts it --
        // it never looks past the prefix -- and the flush is where it surfaces,
        // as one rejected record rather than a failed flush.
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

    /// Recompute a mutated frame's CRC, so a test can forge bytes that are
    /// structurally wrong but pass the integrity check.
    fn reseal(frame: &mut [u8]) {
        let mut h = crc32fast::Hasher::new();
        h.update(&frame[..4]);
        h.update(&frame[FRAME_HEADER_BYTES..]);
        frame[4..8].copy_from_slice(&h.finalize().to_le_bytes());
    }

    #[test]
    fn wrong_key_count_is_corrupt_never_a_plausible_record() {
        let keys = [Value::I64(1), Value::I64(2), Value::I64(3)];
        let buf = frame("s", 1, &row(1, "i", &keys, &[("a", "b")], "d"));
        let (mut ks, mut ex) = (Vec::new(), Vec::new());

        // Decoding under the wrong arity walks the sections out of step, which
        // is exactly how a frame from another epoch would be misread. The
        // trailing-bytes check is what turns that into an error instead of a
        // record assembled from the wrong offsets.
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
        // The whole point: 8 bytes of buffer claiming a 4 GiB payload. If the
        // cap were checked after the buffer-length check we would answer
        // "Truncated" and a streaming reader would dutifully try to buffer 4 GiB.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());

        let (mut ks, mut ex) = (Vec::new(), Vec::new());
        let e = decode(&buf, 0, MAX, &mut ks, &mut ex).unwrap_err();
        assert!(e.is_corrupt(), "got {e:?}");
        assert!(validate_prefix(&buf, MAX).unwrap_err().is_corrupt());

        // Nothing was reserved: the decoder allocates only by pushing values it
        // has already read, so a rejected frame cannot grow the scratch at all.
        assert_eq!(ks.capacity(), 0);
        assert_eq!(ex.capacity(), 0);

        // The same holds for a length that is merely over the configured limit,
        // and the limit is honoured even when the buffer could satisfy it.
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
        ts: i64,
        id: &'static str,
        keys: Vec<Value<'static>>,
        extra: Vec<(&'static str, &'static str)>,
        data: &'static str,
    }

    /// A corpus of frames covering every section at both its empty and its
    /// populated shape. Every frame carries the same key arity so a decode loop
    /// needs no schema lookup.
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
                ts: i64::MIN,
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
                    // A decode that succeeds must be *right*. Returning a
                    // plausible-but-wrong record is the failure this whole test
                    // exists to rule out.
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
                // A cut inside a frame must say "truncated" -- that is the
                // signal recovery uses to truncate the segment and carry on.
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
            // XOR 0xff is a 8-bit burst; CRC-32 detects every burst up to 32
            // bits, so *some* frame must fail. Which one, and whether the
            // damaged length reads as truncated or corrupt, is not fixed -- but
            // "decoded everything successfully" would mean a silent wrong read.
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
        // Not a substitute for the structured tests above -- insurance that the
        // decoder is total. Any panic here is an out-of-bounds read that the
        // release build would turn into something worse.
        let mut rng = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let (clean, _) = corpus();
        for round in 0..2_000 {
            // Declared inside the loop only because the scratch borrows from
            // the buffer it decoded out of, which changes every round.
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
