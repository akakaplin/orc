//! ZeroMQ ingest server: PULL for records, REP for control. Batches arrive in
//! WAL frame format ([`crate::codec`]), so they are validated and copied straight in.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::codec;
use crate::config::Config;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::protocol::{ControlRequest, ControlResponse, FlushPayload, SeriesSchema, StatsPayload};

/// Poll timeout; bounds shutdown latency.
const POLL_TIMEOUT_MS: i64 = 100;

/// Batches read per poll wakeup; bounded so a fast producer can't starve control.
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
    /// Reported in the schema handshake.
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
    /// Bind both sockets. There is no authentication, so keep them on loopback
    /// or a private network.
    pub fn bind(engine: Arc<Engine>, config: &Config) -> Result<Server> {
        let ctx = zmq::Context::new();

        let ingest = ctx.socket(zmq::PULL).map_err(zmq_err)?;
        // Backpressure: a full queue blocks PUSH clients.
        ingest.set_rcvhwm(config.server.rcv_hwm).map_err(zmq_err)?;
        // Bounds message size (the HWM only bounds count). libzmq drops
        // oversized messages silently, hence the limit is sent in the handshake.
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

    /// Serve until `Shutdown` or a `stop_handle` flip. Always drains and closes,
    /// even after a socket error; the loop's error takes precedence.
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

    /// Read up to [`MAX_BATCHES_PER_POLL`] queued batches.
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

    /// Drain queued ingest before closing, up to [`SHUTDOWN_DRAIN`].
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
                tracing::warn!(
                    "ingest was still arriving after {SHUTDOWN_DRAIN:?} of draining; \
                     closing with records left in the queue"
                );
                return Ok(());
            }
        }
    }

    /// Validate a batch header and append its frames. Never fatal: one bad
    /// client must not stop the server.
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
                // `count` is the sender's claim; report any shortfall.
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
            // REQ/REP needs a reply even for garbage.
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
            ControlRequest::Stats => ControlResponse::Stats(Box::new(StatsPayload {
                engine: self.engine.stats(),
                batches_received: self.batches.load(Ordering::Relaxed),
            })),
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
            // Refused rather than faked: clients would encode against a schema never adopted.
            ControlRequest::ReloadConfig => ControlResponse::error(
                "reload-config is not implemented yet; restart the server to adopt a new config",
            ),
            ControlRequest::Shutdown => {
                self.stop.store(true, Ordering::Relaxed);
                ControlResponse::ShuttingDown
            }
        }
    }

    /// Every series' epoch and key order — the client handshake.
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
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Drain what ZeroMQ buffered, then close the engine.
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

    /// The resolved ingest endpoint (useful with port 0).
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

    /// The configured `limits.max_record_bytes`.
    pub fn max_record_bytes(&self) -> usize {
        self.max_record_bytes
    }
}
