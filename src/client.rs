//! ZeroMQ ingest client: a PUSH socket for records, a REQ socket for the
//! handshake.
//!
//! Encodes with the same [`crate::codec`] the WAL uses, so the server appends
//! the bytes it receives without transcoding.
//!
//! ```ignore
//! let mut client = Client::builder()
//!     .ingest("tcp://host:5555")
//!     .control("tcp://host:5556")
//!     .batch(1024)
//!     .connect()?;
//! let trades = client.series("trades")?;
//! client.send(&trades, &row)?;
//! client.flush()?;
//! ```
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
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            ingest: "tcp://127.0.0.1:5555".to_string(),
            control: None,
            batch: 1024,
            sndhwm: 100_000,
            // Non-zero so a clean exit flushes rather than discarding whatever
            // libzmq still holds. Zero here is a classic silent-loss bug.
            linger_ms: 2_000,
            on_full: OnFull::Block,
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
        req.set_rcvtimeo(5_000).map_err(zmq_err)?;
        req.set_sndtimeo(5_000).map_err(zmq_err)?;
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
        };
        client.refresh_schema()?;
        Ok(client)
    }
}

fn zmq_err(e: zmq::Error) -> Error {
    Error::Config(format!("zeromq: {e}"))
}

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
    Ok(format!("{head}:{}", port + 1))
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
            ControlResponse::Schema { series } => {
                self.schema = series
                    .into_iter()
                    .map(|SeriesSchema { name, epoch, keys }| {
                        (name.clone(), RemoteSeries { name, epoch, keys })
                    })
                    .collect();
                Ok(())
            }
            ControlResponse::Error { message } => Err(Error::Config(message)),
            other => Err(Error::Config(format!(
                "unexpected reply to a schema request: {other:?}"
            ))),
        }
    }

    /// Send a control request and wait for the reply.
    pub fn control(&self, req: ControlRequest) -> Result<ControlResponse> {
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

    /// Buffer one record, sending the batch once it is full.
    pub fn send(&mut self, series: &RemoteSeries, row: &Row<'_>) -> Result<()> {
        self.encoder
            .encode(&mut self.buf, &series.name, series.epoch, row)?;
        self.pending += 1;
        if self.pending >= self.batch {
            self.flush()?;
        }
        Ok(())
    }

    /// Send whatever is buffered now.
    ///
    /// A no-op when empty, so calling it on a timer is free.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
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
