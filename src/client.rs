//! ZeroMQ ingest client: a PUSH socket for records, a REQ socket for the
//! handshake.
//!
//! Encodes with the same [`crate::codec`] the WAL uses, so the server appends
//! the bytes it receives without transcoding.
//!
//! **Ingest is fire-and-forget.** A successful `send` means the bytes reached
//! the local ZeroMQ queue, not that they are durable. Backpressure still works:
//! [`OnFull::Block`] is libzmq's behaviour at the send high-water mark, so a
//! saturated server slows producers instead of dropping silently.

use std::collections::HashMap;

use crate::codec::{self, Encoder};
use crate::config::KeyDef;
use crate::error::{Error, Result};
use crate::protocol::{ControlRequest, ControlResponse, SeriesSchema};
use crate::record::Row;

/// What to do when the send queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnFull {
    /// Block until there is room. Slows the producer, loses nothing.
    #[default]
    Block,
    /// Discard and count. Never blocks, loses the newest records.
    Drop,
}

/// A series resolved against the server's schema.
///
/// Holds the epoch to stamp and the key order to encode against, both fixed at
/// handshake time. If the server's config changes afterwards this handle keeps
/// encoding under its original epoch — which is safe, because every frame
/// declares the epoch it was built with and the server decodes it accordingly.
#[derive(Debug, Clone)]
pub struct RemoteSeries {
    name: String,
    epoch: u32,
    keys: Vec<KeyDef>,
}

impl RemoteSeries {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn epoch(&self) -> u32 {
        self.epoch
    }
    pub fn keys(&self) -> &[KeyDef] {
        &self.keys
    }
}

/// Counters for what a fire-and-forget socket cannot tell you.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientStats {
    pub records_sent: u64,
    pub records_dropped: u64,
    pub batches_sent: u64,
    pub bytes_sent: u64,
}

/// Builder for [`Client`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    ingest: String,
    control: Option<String>,
    batch: usize,
    sndhwm: i32,
    linger_ms: i32,
    on_full: OnFull,
    control_timeout_ms: i32,
    max_batch_bytes: usize,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            ingest: "tcp://127.0.0.1:5555".to_string(),
            control: None,
            batch: 1024,
            sndhwm: 100_000,
            // A fallback: the handshake replaces it with the server's own.
            max_batch_bytes: crate::config::DEFAULT_MAX_BATCH_BYTES as usize,
            // Non-zero so a clean exit flushes rather than discarding whatever
            // libzmq still holds. Zero here is a classic silent-loss bug.
            linger_ms: 2_000,
            on_full: OnFull::Block,
            // Fine for ping, stats and the schema handshake. Not for `Flush`,
            // which is synchronous and proportional to the backlog -- see
            // [`Client::control`], which raises it for that one request.
            control_timeout_ms: 5_000,
        }
    }
}

impl ClientBuilder {
    pub fn ingest(mut self, endpoint: &str) -> Self {
        self.ingest = endpoint.to_string();
        self
    }
    /// Control endpoint for the schema handshake. Defaults to the ingest
    /// endpoint's port + 1, matching the server's own defaults.
    pub fn control(mut self, endpoint: &str) -> Self {
        self.control = Some(endpoint.to_string());
        self
    }
    pub fn batch(mut self, records: usize) -> Self {
        self.batch = records.max(1);
        self
    }
    pub fn sndhwm(mut self, hwm: i32) -> Self {
        self.sndhwm = hwm;
        self
    }
    pub fn linger_ms(mut self, ms: i32) -> Self {
        self.linger_ms = ms;
        self
    }
    pub fn on_full(mut self, on_full: OnFull) -> Self {
        self.on_full = on_full;
        self
    }
    /// How long to wait for a control reply. Defaults to 5s.
    ///
    /// Does not apply to `Flush`, which the server handles synchronously over
    /// however much WAL has accumulated; [`Client::control`] gives that one its
    /// own, much longer bound.
    pub fn control_timeout_ms(mut self, ms: i32) -> Self {
        self.control_timeout_ms = ms;
        self
    }

    /// Largest message this client will put on the wire, in bytes.
    ///
    /// Rarely worth setting: the handshake learns the server's
    /// `server.max_batch_bytes` and the client takes the smaller of the two, so
    /// setting this higher cannot raise the limit the server enforces.
    pub fn max_batch_bytes(mut self, bytes: usize) -> Self {
        self.max_batch_bytes = bytes.max(codec::BATCH_HEADER_BYTES + 1);
        self
    }

    /// Connect both sockets and perform the schema handshake.
    pub fn connect(self) -> Result<Client> {
        let ctx = zmq::Context::new();
        let push = ctx.socket(zmq::PUSH).map_err(zmq_err)?;
        push.set_sndhwm(self.sndhwm).map_err(zmq_err)?;
        push.set_linger(self.linger_ms).map_err(zmq_err)?;
        push.connect(&self.ingest)
            .map_err(|e| Error::Config(format!("connecting to {}: {e}", self.ingest)))?;

        let control_endpoint = match &self.control {
            Some(c) => c.clone(),
            None => default_control_endpoint(&self.ingest)?,
        };
        let req = ctx.socket(zmq::REQ).map_err(zmq_err)?;
        req.set_linger(self.linger_ms).map_err(zmq_err)?;
        // Without a timeout a dead server makes `recv` hang forever, which
        // turns "the server is down" into "my process is wedged".
        req.set_rcvtimeo(self.control_timeout_ms).map_err(zmq_err)?;
        req.set_sndtimeo(self.control_timeout_ms).map_err(zmq_err)?;
        // A plain REQ socket is a strict send/recv state machine, so a timed-out
        // `recv` leaves it still expecting a reply and every later `send` fails
        // with EFSM -- permanently, with no way to reset it. One slow flush would
        // brick the socket for the life of the process. RELAXED lets a new
        // request replace the abandoned one; CORRELATE is what makes that safe,
        // by tagging replies so a late one from the abandoned request is
        // discarded instead of being read as the answer to the new one.
        req.set_req_relaxed(true).map_err(zmq_err)?;
        req.set_req_correlate(true).map_err(zmq_err)?;
        req.connect(&control_endpoint)
            .map_err(|e| Error::Config(format!("connecting to {control_endpoint}: {e}")))?;

        let mut client = Client {
            push,
            req,
            schema: HashMap::new(),
            buf: Vec::with_capacity(64 * 1024),
            pending: 0,
            batch: self.batch,
            on_full: self.on_full,
            stats: ClientStats::default(),
            encoder: Encoder::default(),
            control_timeout_ms: self.control_timeout_ms,
            max_batch_bytes: self.max_batch_bytes,
            configured_max_batch_bytes: self.max_batch_bytes,
        };
        client.refresh_schema()?;
        Ok(client)
    }
}

fn zmq_err(e: zmq::Error) -> Error {
    Error::Config(format!("zeromq: {e}"))
}

/// Receive timeout for a `Flush`, which the server answers only once the whole
/// backlog is on disk. Ten minutes: long enough for a real hour of WAL, short
/// enough that a genuinely wedged server is still eventually reported.
const FLUSH_TIMEOUT_MS: i32 = 600_000;

/// `tcp://host:5555` -> `tcp://host:5556`, matching the server's defaults.
fn default_control_endpoint(ingest: &str) -> Result<String> {
    let (head, port) = ingest.rsplit_once(':').ok_or_else(|| {
        Error::Config(format!(
            "cannot derive a control endpoint from {ingest:?}; pass one explicitly"
        ))
    })?;
    let port: u16 = port.parse().map_err(|_| {
        Error::Config(format!(
            "cannot derive a control endpoint from {ingest:?}; pass one explicitly"
        ))
    })?;
    // 65535 has no successor. Unchecked, that is a panic in a debug build and a
    // silent `:0` in release -- which connects nowhere and reports it as
    // something else entirely.
    let control = port.checked_add(1).ok_or_else(|| {
        Error::Config(format!(
            "cannot derive a control endpoint from {ingest:?}: port {port} has no successor; \
             pass one explicitly"
        ))
    })?;
    Ok(format!("{head}:{control}"))
}

/// A connected ingest client.
pub struct Client {
    push: zmq::Socket,
    req: zmq::Socket,
    schema: HashMap<String, RemoteSeries>,
    buf: Vec<u8>,
    pending: usize,
    batch: usize,
    on_full: OnFull,
    stats: ClientStats,
    encoder: Encoder,
    /// Kept so `control` can restore it after raising it for a `Flush`.
    control_timeout_ms: i32,
    /// The cap actually in force: `min(configured, whatever the server said)`.
    max_batch_bytes: usize,
    /// What the builder was given, so a re-handshake recomputes the minimum from
    /// the caller's intent rather than from an already-lowered value.
    configured_max_batch_bytes: usize,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("series", &self.schema.len())
            .field("pending", &self.pending)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Ask the server for every series' epoch and key order.
    ///
    /// Required before anything can be encoded: keys travel positionally, so
    /// without the declared order the client cannot build a frame at all.
    pub fn refresh_schema(&mut self) -> Result<()> {
        match self.control(ControlRequest::Schema)? {
            ControlResponse::Schema {
                series,
                max_batch_bytes,
            } => {
                self.schema = series
                    .into_iter()
                    .map(|SeriesSchema { name, epoch, keys }| {
                        (name.clone(), RemoteSeries { name, epoch, keys })
                    })
                    .collect();
                // The smaller of the two: the server's is the one libzmq
                // enforces, and a server too old to report one leaves ours.
                self.max_batch_bytes = match max_batch_bytes {
                    Some(server) => self
                        .configured_max_batch_bytes
                        .min(usize::try_from(server).unwrap_or(usize::MAX)),
                    None => self.configured_max_batch_bytes,
                };
                Ok(())
            }
            ControlResponse::Error { message } => Err(Error::Config(message)),
            other => Err(Error::Config(format!(
                "unexpected reply to a schema request: {other:?}"
            ))),
        }
    }

    /// Send a control request and wait for the reply.
    ///
    /// `Flush` gets its own receive timeout. The server runs it synchronously —
    /// decoding, sorting and writing Parquet for every pending segment before it
    /// replies — so the time it takes is proportional to the backlog, and the
    /// default control timeout would give up on a flush that is working fine and
    /// report it as a failure while the server went on to complete it.
    pub fn control(&self, req: ControlRequest) -> Result<ControlResponse> {
        let slow = matches!(req, ControlRequest::Flush);
        if slow {
            self.req.set_rcvtimeo(FLUSH_TIMEOUT_MS).map_err(zmq_err)?;
        }
        let result = self.control_inner(req);
        if slow {
            // Restore even on failure: leaving the long timeout in place would
            // make a later `stats` against a dead server hang for minutes.
            let restore = self.req.set_rcvtimeo(self.control_timeout_ms);
            if let Err(e) = restore {
                tracing::warn!(error = %e, "could not restore the control timeout");
            }
        }
        result
    }

    fn control_inner(&self, req: ControlRequest) -> Result<ControlResponse> {
        self.req.send(req.to_bytes()?, 0).map_err(zmq_err)?;
        let reply = self.req.recv_bytes(0).map_err(zmq_err)?;
        Ok(ControlResponse::from_bytes(&reply)?)
    }

    /// Resolve a series by name. Do this once, outside the hot loop.
    pub fn series(&self, name: &str) -> Result<RemoteSeries> {
        self.schema
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownSeries(name.to_string()))
    }

    /// Every series the server declared.
    pub fn series_names(&self) -> impl Iterator<Item = &str> {
        self.schema.keys().map(String::as_str)
    }

    /// Buffer one record, sending the batch once it is full — by record count or
    /// by size, whichever comes first.
    ///
    /// The size bound is not a tuning knob. libzmq discards a message over the
    /// receiver's `ZMQ_MAXMSGSIZE` *below* the application, so the server cannot
    /// count, log or reject it and `send` still reports success — the records
    /// just vanish. Batching by record count alone reached that with 1024
    /// records of 8 KiB against the 8 MiB default.
    ///
    /// A record too large for a batch of its own is [`Error::BatchTooLarge`]:
    /// no split will ever deliver it. A record that merely does not fit
    /// alongside what is buffered sends those first and starts a new batch.
    pub fn send(&mut self, series: &RemoteSeries, row: &Row<'_>) -> Result<()> {
        let before = self.buf.len();
        self.encoder
            .encode(&mut self.buf, &series.name, series.epoch, row)?;
        let frame = self.buf.len() - before;

        if codec::BATCH_HEADER_BYTES + frame > self.max_batch_bytes {
            // `encode` restores the buffer on its own errors; this one is ours.
            self.buf.truncate(before);
            return Err(Error::BatchTooLarge {
                size: codec::BATCH_HEADER_BYTES + frame,
                limit: self.max_batch_bytes,
            });
        }
        if before > 0 && codec::BATCH_HEADER_BYTES + self.buf.len() > self.max_batch_bytes {
            // Lift this frame out, send what it would have overflowed, put it
            // back as the first record of the next batch.
            let frame = self.buf.split_off(before);
            self.flush()?;
            self.buf.extend_from_slice(&frame);
        }

        self.pending += 1;
        if self.pending >= self.batch {
            self.flush()?;
        }
        Ok(())
    }

    /// The largest message this client will send, in bytes.
    pub fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    /// Send whatever is buffered now.
    ///
    /// A no-op when empty, so calling it on a timer is free.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        // Unreachable via `send`, but this is the one place bytes reach the
        // socket, and an oversized message here is lost with no diagnostic.
        let size = codec::BATCH_HEADER_BYTES + self.buf.len();
        if size > self.max_batch_bytes {
            return Err(Error::BatchTooLarge {
                size,
                limit: self.max_batch_bytes,
            });
        }
        let mut msg = Vec::with_capacity(codec::BATCH_HEADER_BYTES + self.buf.len());
        codec::encode_batch_header(&mut msg, self.pending as u32);
        msg.extend_from_slice(&self.buf);

        let flags = match self.on_full {
            OnFull::Block => 0,
            OnFull::Drop => zmq::DONTWAIT,
        };
        match self.push.send(&msg, flags) {
            Ok(()) => {
                self.stats.records_sent += self.pending as u64;
                self.stats.batches_sent += 1;
                self.stats.bytes_sent += msg.len() as u64;
            }
            // Only reachable under `OnFull::Drop`: the queue is full and we
            // chose not to wait. Counted, because it is otherwise invisible.
            Err(zmq::Error::EAGAIN) => {
                self.stats.records_dropped += self.pending as u64;
                tracing::warn!(
                    records = self.pending,
                    "dropped a batch: send queue full and on_full is Drop"
                );
            }
            Err(e) => return Err(zmq_err(e)),
        }
        self.buf.clear();
        self.pending = 0;
        Ok(())
    }

    /// Records buffered but not yet sent.
    pub fn pending(&self) -> usize {
        self.pending
    }

    pub fn stats(&self) -> ClientStats {
        self.stats
    }
}

impl Drop for Client {
    /// Flush what is buffered before the socket goes away.
    ///
    /// Without this, a clean exit would silently discard a partial batch — the
    /// most surprising possible loss, because nothing went wrong.
    fn drop(&mut self) {
        if self.pending > 0
            && let Err(e) = self.flush()
        {
            tracing::error!(error = %e, records = self.pending, "flushing on drop");
        }
    }
}
