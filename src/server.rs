//! ZeroMQ ingest server: a PULL socket for records, a REP socket for control.
//!
//! Sharing [`crate::codec`] with the client pays off here: a received batch is
//! already in WAL frame format, so it is validated and copied straight in,
//! crossing the process boundary exactly once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::codec;
use crate::config::Config;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::protocol::{ControlRequest, ControlResponse, FlushPayload, SeriesSchema, StatsPayload};

/// How long a poll waits before looping. Bounds shutdown latency without
/// spinning: the loop is otherwise entirely blocked in `zmq_poll`.
const POLL_TIMEOUT_MS: i64 = 100;

/// Batches taken from the ingest socket per poll wakeup.
///
/// An unbounded drain to `EAGAIN` never returns under a producer faster than the
/// WAL can absorb, so the control socket is never polled again and `ping`,
/// `stats`, `flush` and `shutdown` all hang. A bounded burst keeps ingest batched
/// while giving control a turn.
const MAX_BATCHES_PER_POLL: usize = 1024;

/// How long `shutdown` keeps draining the ingest queue before giving up.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

fn zmq_err(e: zmq::Error) -> Error {
    Error::Config(format!("zeromq: {e}"))
}

/// A bound server. Owns the engine for the duration.
pub struct Server {
    engine: Arc<Engine>,
    ingest: zmq::Socket,
    control: zmq::Socket,
    stop: Arc<AtomicBool>,
    batches: AtomicU64,
    max_record_bytes: usize,
    /// Reported in the schema handshake, so a client can bound its batches by
    /// the limit libzmq will actually enforce against it.
    max_batch_bytes: u64,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // zmq::Socket is not Debug.
        f.debug_struct("Server")
            .field("batches", &self.batches.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Server {
    /// Bind both sockets.
    ///
    /// Loopback by default. The ingest socket has no authentication, so binding a
    /// routable interface is a deliberate decision — with a private network or
    /// libzmq's CURVE in front of it.
    pub fn bind(engine: Arc<Engine>, config: &Config) -> Result<Server> {
        let ctx = zmq::Context::new();

        let ingest = ctx.socket(zmq::PULL).map_err(zmq_err)?;
        // A high-water mark is backpressure, not a leak: when it fills, PUSH
        // clients block instead of the server buffering without bound.
        ingest.set_rcvhwm(config.server.rcv_hwm).map_err(zmq_err)?;
        // The high-water mark bounds message *count*, not size, and libzmq's
        // size default is unlimited -- so one hostile message would be allocated
        // in full before a frame inside it was looked at, then written to a
        // single segment for the flush to read whole.
        ingest
            .set_maxmsgsize(config.server.max_batch_bytes)
            .map_err(zmq_err)?;
        ingest.bind(&config.server.ingest_endpoint).map_err(|e| {
            Error::Config(format!("binding {}: {e}", config.server.ingest_endpoint))
        })?;

        let control = ctx.socket(zmq::REP).map_err(zmq_err)?;
        control.bind(&config.server.control_endpoint).map_err(|e| {
            Error::Config(format!("binding {}: {e}", config.server.control_endpoint))
        })?;

        tracing::info!(
            ingest = %config.server.ingest_endpoint,
            control = %config.server.control_endpoint,
            "orc-server listening"
        );

        Ok(Server {
            max_record_bytes: config.limits.max_record_bytes,
            max_batch_bytes: config.server.max_batch_bytes.max(0) as u64,
            engine,
            ingest,
            control,
            stop: Arc::new(AtomicBool::new(false)),
            batches: AtomicU64::new(0),
        })
    }

    /// A handle that asks the loop to stop at the next poll.
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    /// Serve until a `Shutdown` request or a `stop_handle` flip.
    ///
    /// A socket error ends the loop but never skips [`Server::shutdown`]: a `?`
    /// straight out of the loop body would take the drain and the engine close
    /// with it, discarding whatever ZeroMQ had buffered. The loop's error is
    /// still what the caller gets; shutdown's only surfaces if there is no
    /// earlier one to lose.
    pub fn run(&self) -> Result<()> {
        let outcome = self.serve();
        let closed = self.shutdown();
        match (outcome, closed) {
            (Err(e), _) => Err(e),
            (Ok(()), closed) => closed,
        }
    }

    fn serve(&self) -> Result<()> {
        while !self.stop.load(Ordering::Relaxed) {
            let mut items = [
                self.ingest.as_poll_item(zmq::POLLIN),
                self.control.as_poll_item(zmq::POLLIN),
            ];
            // EINTR is not an error here; a signal during poll just means loop.
            match zmq::poll(&mut items, POLL_TIMEOUT_MS) {
                Ok(_) => {}
                Err(zmq::Error::EINTR) => continue,
                Err(e) => return Err(zmq_err(e)),
            }
            if items[0].is_readable() {
                self.drain_ingest()?;
            }
            if items[1].is_readable() {
                self.serve_control()?;
            }
        }
        Ok(())
    }

    /// Read up to [`MAX_BATCHES_PER_POLL`] queued batches, not just one.
    ///
    /// One message per wakeup would cap throughput; an unbounded drain would
    /// starve the control socket, including the `Shutdown` that would stop it.
    fn drain_ingest(&self) -> Result<()> {
        for _ in 0..MAX_BATCHES_PER_POLL {
            match self.ingest.recv_bytes(zmq::DONTWAIT) {
                Ok(bytes) => {
                    self.batches.fetch_add(1, Ordering::Relaxed);
                    self.ingest_batch(&bytes);
                }
                Err(zmq::Error::EAGAIN) => return Ok(()),
                Err(zmq::Error::EINTR) => continue,
                Err(e) => return Err(zmq_err(e)),
            }
        }
        Ok(())
    }

    /// Empty the ingest queue on the way out, but not forever.
    ///
    /// Drained because the loop may have been woken by the control socket with
    /// records still buffered, and closing would discard them unannounced.
    /// Deadlined because producers do not stop when the server decides to.
    fn drain_for_shutdown(&self) -> Result<()> {
        let deadline = Instant::now() + SHUTDOWN_DRAIN;
        loop {
            match self.ingest.recv_bytes(zmq::DONTWAIT) {
                Ok(bytes) => {
                    self.batches.fetch_add(1, Ordering::Relaxed);
                    self.ingest_batch(&bytes);
                }
                Err(zmq::Error::EAGAIN) => return Ok(()),
                Err(zmq::Error::EINTR) => continue,
                Err(e) => return Err(zmq_err(e)),
            }
            if Instant::now() >= deadline {
                // Anything still queued is dropped, which is worth saying out
                // loud -- those senders got no acknowledgement either way, but
                // an operator reading the log should not have to infer it.
                tracing::warn!(
                    "ingest was still arriving after {SHUTDOWN_DRAIN:?} of draining; \
                     closing with records left in the queue"
                );
                return Ok(());
            }
        }
    }

    /// Validate a batch header and hand the frames to the engine.
    ///
    /// Never fatal: ingest is fire-and-forget, so there is nobody to return an
    /// error to and one bad client must not stop the server.
    fn ingest_batch(&self, bytes: &[u8]) {
        let (count, rest) = match codec::parse_batch_header(bytes) {
            Ok(count) => (count, &bytes[codec::BATCH_HEADER_BYTES..]),
            Err(e) => {
                tracing::warn!(error = %e, len = bytes.len(), "dropped a batch with a bad header");
                return;
            }
        };
        match self.engine.append_raw(rest) {
            Ok(res) => {
                // `count` is a hint, not a length -- but it is the only
                // statement of how many records the sender thought it sent, so a
                // shortfall is worth naming. Without it a batch cut short at its
                // first frame reports "1 rejected" for a thousand lost records.
                let seen = res.accepted + res.rejected;
                if res.rejected > 0 || seen < count as usize {
                    tracing::warn!(
                        accepted = res.accepted,
                        rejected = res.rejected,
                        discarded_bytes = res.discarded_bytes,
                        declared = count,
                        unaccounted = (count as usize).saturating_sub(seen),
                        "batch partially rejected"
                    );
                }
            }
            Err(e) => tracing::error!(error = %e, "appending a batch"),
        }
    }

    fn serve_control(&self) -> Result<()> {
        let msg = match self.control.recv_bytes(0) {
            Ok(m) => m,
            Err(zmq::Error::EINTR) => return Ok(()),
            Err(e) => return Err(zmq_err(e)),
        };
        let reply = match ControlRequest::from_bytes(&msg) {
            Ok(req) => self.handle(req),
            // REQ/REP is lockstep: a reply is mandatory even for garbage, or the
            // client blocks forever waiting for one.
            Err(e) => ControlResponse::error(format_args!("unparseable request: {e}")),
        };
        let bytes = reply
            .to_bytes()
            .unwrap_or_else(|e| format!(r#"{{"status":"error","message":"{e}"}}"#).into_bytes());
        self.control.send(&bytes, 0).map_err(zmq_err)
    }

    fn handle(&self, req: ControlRequest) -> ControlResponse {
        match req {
            ControlRequest::Ping => ControlResponse::Pong {
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            ControlRequest::Stats => {
                let s = self.engine.stats();
                ControlResponse::Stats(Box::new(StatsPayload {
                    appended: s.appended,
                    rejected_ts: s.rejected_ts,
                    rejected_size: s.rejected_size,
                    rejected_series: s.rejected_series,
                    rejected_frames: s.rejected_frames,
                    flush_failures: s.flush_failures,
                    rows_flushed: s.rows_flushed,
                    rows_deduplicated: s.rows_deduplicated,
                    wal_bytes: s.wal_bytes,
                    wal_total_bytes: s.wal_total_bytes,
                    segment: s.segment,
                    batches_received: self.batches.load(Ordering::Relaxed),
                }))
            }
            ControlRequest::Flush => match self.engine.flush() {
                Ok(o) => ControlResponse::Flushed(FlushPayload {
                    segments_consumed: o.segments_consumed,
                    rows_written: o.rows_written,
                    rows_deduplicated: o.rows_deduplicated,
                    frames_rejected: o.frames_rejected,
                }),
                Err(e) => ControlResponse::error(e),
            },
            ControlRequest::Schema => ControlResponse::Schema {
                series: self.schema(),
                max_batch_bytes: Some(self.max_batch_bytes),
            },
            // Not implemented rather than silently accepted: adopting a config
            // means rebuilding the handle table, and pretending to succeed would
            // leave clients encoding against a schema the server never took.
            ControlRequest::ReloadConfig => ControlResponse::error(
                "reload-config is not implemented yet; restart the server to adopt a new config",
            ),
            ControlRequest::Shutdown => {
                self.stop.store(true, Ordering::Relaxed);
                ControlResponse::ShuttingDown
            }
        }
    }

    /// Every series' epoch and positional key list — the client handshake.
    fn schema(&self) -> Vec<SeriesSchema> {
        let mut out: Vec<SeriesSchema> = self
            .engine
            .series_handles()
            .map(|h| SeriesSchema {
                name: h.name().to_string(),
                epoch: h.epoch(),
                keys: h.keys().to_vec(),
            })
            .collect();
        // Stable order so a client diffing two handshakes sees real changes.
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Drain what ZeroMQ already buffered, then close cleanly. Deadlined, since a
    /// live producer would otherwise keep refilling the queue.
    fn shutdown(&self) -> Result<()> {
        tracing::info!("draining the ingest queue before exit");
        self.drain_for_shutdown()?;
        self.engine.close()?;
        tracing::info!(
            batches = self.batches.load(Ordering::Relaxed),
            "orc-server stopped"
        );
        Ok(())
    }

    /// Batches received since start.
    pub fn batches_received(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }

    /// The bound ingest endpoint, resolved. Useful when binding to port 0.
    pub fn ingest_endpoint(&self) -> Result<String> {
        self.ingest
            .get_last_endpoint()
            .map_err(zmq_err)
            .map(|r| r.unwrap_or_else(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// The bound control endpoint, resolved.
    pub fn control_endpoint(&self) -> Result<String> {
        self.control
            .get_last_endpoint()
            .map_err(zmq_err)
            .map(|r| r.unwrap_or_else(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// Records accepted so far, for tests and diagnostics.
    pub fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }
}
