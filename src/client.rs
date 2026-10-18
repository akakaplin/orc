//! ZeroMQ ingest client: PUSH for records (WAL [`crate::codec`] frames), REQ for control.
//! Fire-and-forget: a successful `send` means queued locally, not durable.

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

/// A series resolved at handshake time; frames carry its epoch, so later
/// server config changes are safe.
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
            // Replaced by the server's value at handshake.
            max_batch_bytes: crate::config::DEFAULT_MAX_BATCH_BYTES as usize,
            // Non-zero so a clean exit delivers what libzmq still holds.
            linger_ms: 2_000,
            on_full: OnFull::Block,
            // `Flush` gets a longer one in `control`.
            control_timeout_ms: 5_000,
        }
    }
}

impl ClientBuilder {
    pub fn ingest(mut self, endpoint: &str) -> Self {
        self.ingest = endpoint.to_string();
        self
    }
    /// Control endpoint. Defaults to the ingest port + 1.
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
    /// Control reply timeout (default 5s); `Flush` has its own, see [`Client::control`].
    pub fn control_timeout_ms(mut self, ms: i32) -> Self {
        self.control_timeout_ms = ms;
        self
    }

    /// Largest message to send; the handshake lowers it to the server's limit.
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
        // So a dead server can't hang `recv` forever.
        req.set_rcvtimeo(self.control_timeout_ms).map_err(zmq_err)?;
        req.set_sndtimeo(self.control_timeout_ms).map_err(zmq_err)?;
        // After a timed-out `recv`, plain REQ fails every later send (EFSM).
        // RELAXED allows a new request; CORRELATE discards late replies.
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
            control_endpoint,
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

/// Receive timeout for a synchronous `Flush`: long enough for an hour of WAL.
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
    // 65535 has no successor.
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
    /// Restored by `control` after a `Flush`.
    control_timeout_ms: i32,
    /// Named in timeout errors.
    control_endpoint: String,
    /// In force: `min(configured, server's)`.
    max_batch_bytes: usize,
    /// The builder's value, so a re-handshake recomputes the minimum from it.
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

    /// Fetch every series' epoch and key order; required before encoding.
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

    /// Send a control request and wait for the reply. `Flush` is synchronous on
    /// the server, so it waits up to `FLUSH_TIMEOUT_MS`.
    pub fn control(&self, req: ControlRequest) -> Result<ControlResponse> {
        let slow = matches!(req, ControlRequest::Flush);
        let waited = if slow {
            self.req.set_rcvtimeo(FLUSH_TIMEOUT_MS).map_err(zmq_err)?;
            FLUSH_TIMEOUT_MS
        } else {
            self.control_timeout_ms
        };
        let result = self.control_inner(req, waited);
        if slow {
            // Restore even on failure.
            let restore = self.req.set_rcvtimeo(self.control_timeout_ms);
            if let Err(e) = restore {
                tracing::warn!(error = %e, "could not restore the control timeout");
            }
        }
        result
    }

    fn control_inner(&self, req: ControlRequest, waited: i32) -> Result<ControlResponse> {
        self.req
            .send(req.to_bytes()?, 0)
            .map_err(|e| self.control_err(e, "send to", self.control_timeout_ms))?;
        let reply = self
            .req
            .recv_bytes(0)
            .map_err(|e| self.control_err(e, "get a reply from", waited))?;
        Ok(ControlResponse::from_bytes(&reply)?)
    }

    /// Turn libzmq's `EAGAIN` timeout into an error naming the endpoint.
    fn control_err(&self, e: zmq::Error, what: &str, waited: i32) -> Error {
        match e {
            zmq::Error::EAGAIN => Error::Config(format!(
                "could not {what} the control socket at {} within {waited} ms; \
                 is an orc server running there?",
                self.control_endpoint
            )),
            other => zmq_err(other),
        }
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

    /// Buffer one record; send the batch when full by count or size. The size
    /// bound matters: libzmq silently drops messages over the server's limit.
    /// A record that can't fit even alone is [`Error::BatchTooLarge`].
    pub fn send(&mut self, series: &RemoteSeries, row: &Row<'_>) -> Result<()> {
        let before = self.buf.len();
        self.encoder
            .encode(&mut self.buf, &series.name, series.epoch, row)?;
        let frame = self.buf.len() - before;

        if codec::BATCH_HEADER_BYTES + frame > self.max_batch_bytes {
            self.buf.truncate(before);
            return Err(Error::BatchTooLarge {
                size: codec::BATCH_HEADER_BYTES + frame,
                limit: self.max_batch_bytes,
            });
        }
        if before > 0 && codec::BATCH_HEADER_BYTES + self.buf.len() > self.max_batch_bytes {
            // Send the earlier records; this frame starts the next batch.
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

    /// Send whatever is buffered; a no-op when empty.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        // Unreachable via `send`; guards the one place bytes reach the socket.
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
            // `OnFull::Drop` with a full queue.
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
    /// Flush a partial batch so a clean exit doesn't lose it.
    fn drop(&mut self) {
        if self.pending > 0
            && let Err(e) = self.flush()
        {
            tracing::error!(error = %e, records = self.pending, "flushing on drop");
        }
    }
}
