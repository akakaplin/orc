//! Client and server over real ZeroMQ sockets.
//!
//! The engine tests prove the write path; these prove the wire. What is worth
//! testing here is specifically the claim that makes sharing the codec
//! worthwhile: a record encoded by the client is appended by the server
//! *without transcoding*, and comes out of Parquet identical to one appended
//! in-process.
#![cfg(feature = "net")]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orc::client::{Client, OnFull};
use orc::config::Config;
use orc::engine::Engine;
use orc::protocol::{ControlRequest, ControlResponse};
use orc::record::{Row, Value};
use orc::server::Server;

const T13: u64 = 1_786_194_000_000_000;

fn config_json(dir: &Path) -> String {
    config_json_with(dir, 8_388_608, 16_384)
}

fn config_json_with(dir: &Path, max_batch_bytes: i64, max_record_bytes: usize) -> String {
    format!(
        r#"{{
          "data_dir": {},
          "server": {{ "ingest_endpoint": "tcp://127.0.0.1:*",
                       "control_endpoint": "tcp://127.0.0.1:*",
                       "max_batch_bytes": {max_batch_bytes} }},
          "limits": {{ "max_record_bytes": {max_record_bytes} }},
          "flush": {{ "on_startup": false }},
          "wal": {{ "fsync_interval_ms": 1 }},
          "series": [
            {{ "name": "trades",
               "keys": [ {{"name": "symbol", "type": "string"}},
                         {{"name": "size", "type": "i64", "nullable": true}} ] }}
          ]
        }}"#,
        serde_json::to_string(&dir.to_string_lossy()).unwrap()
    )
}

/// A running server plus the endpoints it actually bound.
struct Harness {
    ingest: String,
    control: String,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Harness {
    fn start(dir: &Path) -> Harness {
        Harness::start_with(&config_json(dir))
    }

    fn start_with(config_json: &str) -> Harness {
        let config: Config = serde_json::from_str(config_json).unwrap();
        let engine = Arc::new(Engine::open(config.clone()).unwrap());
        let server = Server::bind(engine, &config).unwrap();

        // Bound with `*`, so the real ports are only knowable after binding.
        let ingest = server.ingest_endpoint().unwrap();
        let control = server.control_endpoint().unwrap();

        let thread = std::thread::spawn(move || {
            if let Err(e) = server.run() {
                eprintln!("server stopped: {e}");
            }
        });
        Harness {
            ingest,
            control,
            thread: Some(thread),
        }
    }

    fn client(&self) -> Client {
        Client::builder()
            .ingest(&self.ingest)
            .control(&self.control)
            .batch(16)
            .on_full(OnFull::Block)
            .connect()
            .expect("client connects")
    }

    fn stop(mut self, client: &Client) {
        let _ = client.control(ControlRequest::Shutdown);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Poll a condition until it holds. Ingest is asynchronous by design, so there
/// is no synchronous point at which a sent record is known to have landed.
fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {what}");
}

fn appended(client: &Client) -> u64 {
    match client.control(ControlRequest::Stats) {
        Ok(ControlResponse::Stats(s)) => s.engine.appended,
        other => panic!("unexpected stats reply: {other:?}"),
    }
}

#[test]
fn the_handshake_reports_every_series() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::start(dir.path());
    let client = h.client();

    let trades = client.series("trades").expect("series is known");
    assert_eq!(
        trades.keys().len(),
        2,
        "positional key order comes from here"
    );
    assert_eq!(trades.keys()[0].name, "symbol");
    assert_eq!(trades.keys()[1].name, "size");
    assert!(client.series("nope").is_err());

    h.stop(&client);
}

#[test]
fn records_sent_over_the_wire_reach_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::start(dir.path());
    let mut client = h.client();
    let trades = client.series("trades").unwrap();

    for i in 0..50 {
        client
            .send(
                &trades,
                &Row {
                    ts: T13 + i,
                    id: &format!("w-{i}"),
                    keys: &[Value::Str("AAPL"), Value::I64(i as i64)],
                    extra: &[("feed", "itch")],
                    data: r#"{"px":1.5}"#,
                },
            )
            .unwrap();
    }
    client.flush().unwrap();

    eventually("the server to accept every record", || {
        appended(&client) == 50
    });

    match client.control(ControlRequest::Flush).unwrap() {
        ControlResponse::Flushed(f) => {
            assert_eq!(f.rows_written, 50);
            assert_eq!(f.frames_rejected, 0, "nothing may be rejected");
        }
        other => panic!("unexpected flush reply: {other:?}"),
    }

    h.stop(&client);
}

#[test]
fn a_malformed_batch_does_not_stop_the_server() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::start(dir.path());
    let mut client = h.client();
    let trades = client.series("trades").unwrap();

    // Garbage straight at the ingest socket, bypassing the client's encoder.
    let ctx = orc::zmq::Context::new();
    let raw = ctx.socket(orc::zmq::PUSH).unwrap();
    raw.connect(&h.ingest).unwrap();
    raw.send(&b"this is not a batch header"[..], 0).unwrap();
    raw.send(&[0u8; 4][..], 0).unwrap();

    // The server must still be serving afterwards.
    client
        .send(
            &trades,
            &Row {
                ts: T13,
                id: "after-garbage",
                keys: &[Value::Str("AAPL"), Value::Null],
                extra: &[],
                data: "",
            },
        )
        .unwrap();
    client.flush().unwrap();

    eventually("the server to survive garbage and keep ingesting", || {
        appended(&client) == 1
    });
    assert!(matches!(
        client.control(ControlRequest::Ping).unwrap(),
        ControlResponse::Pong { .. }
    ));

    h.stop(&client);
}

#[test]
fn an_unparseable_control_request_still_gets_a_reply() {
    // REQ/REP is lockstep: no reply means the client blocks forever. Garbage
    // must produce an error response, not silence.
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::start(dir.path());
    let client = h.client();

    let ctx = orc::zmq::Context::new();
    let req = ctx.socket(orc::zmq::REQ).unwrap();
    req.set_rcvtimeo(5_000).unwrap();
    req.connect(&h.control).unwrap();
    req.send(&b"{\"op\":\"not-a-real-op\"}"[..], 0).unwrap();

    let reply = req.recv_bytes(0).expect("a reply is mandatory");
    match ControlResponse::from_bytes(&reply).unwrap() {
        ControlResponse::Error { message } => assert!(!message.is_empty()),
        other => panic!("expected an error reply, got {other:?}"),
    }

    h.stop(&client);
}

/// libzmq refuses a message over the receiver's `ZMQ_MAXMSGSIZE` and drops it
/// *below* the application: the server never sees it, so it cannot count it, log
/// it or reject it, and the sender's `send` returns success. Batching purely by
/// record count made that reachable with nothing exotic, and the records simply
/// disappeared — the client reported them sent, the server reported none
/// received, and no log line on either side mentioned it.
///
/// So the client bounds itself by bytes, and the two cases are handled
/// differently: a record too large to ever be delivered is an error, and a
/// record that merely does not fit alongside what is buffered starts a new
/// batch.
#[test]
fn a_batch_too_large_for_the_server_is_refused_rather_than_lost() {
    let dir = tempfile::tempdir().unwrap();
    // 4 KiB batches carrying 1 KiB records: a batch of 200 would be ~47 KiB.
    let h = Harness::start_with(&config_json_with(dir.path(), 4096, 1024));
    let mut client = Client::builder()
        .ingest(&h.ingest)
        .control(&h.control)
        .batch(200)
        .on_full(OnFull::Block)
        .connect()
        .unwrap();

    // The cap came from the handshake, not from the client's own default.
    assert_eq!(
        client.max_batch_bytes(),
        4096,
        "the client must adopt the limit the server actually enforces"
    );

    let trades = client.series("trades").unwrap();
    let payload = "x".repeat(200);
    let row = |ts: u64| Row {
        ts,
        id: "",
        keys: &[Value::Str("AAPL"), Value::Null],
        extra: &[],
        data: &payload,
    };

    // 200 records that would have been one 47 KiB message. Every one of them
    // must arrive, in whatever number of batches that takes.
    for i in 0..200u64 {
        client.send(&trades, &row(T13 + i)).unwrap();
    }
    client.flush().unwrap();

    eventually("every record to arrive despite the batch cap", || {
        appended(&client) == 200
    });
    assert_eq!(
        client.stats().records_dropped,
        0,
        "nothing was dropped to get there"
    );
    assert!(
        client.stats().batches_sent > 1,
        "which means the client split the batch rather than sending one oversized message"
    );
    // And every message it sent was actually within the cap.
    assert!(client.stats().bytes_sent >= 200 * 200);

    // A single record that cannot fit in a batch of its own can never be
    // delivered, however the batch is split. That is an error, not a split.
    let huge = "y".repeat(8_000);
    let err = client
        .send(
            &trades,
            &Row {
                ts: T13,
                id: "",
                keys: &[Value::Str("AAPL"), Value::Null],
                extra: &[],
                data: &huge,
            },
        )
        .unwrap_err();
    assert!(
        matches!(err, orc::Error::BatchTooLarge { .. }),
        "expected BatchTooLarge, got {err:?}"
    );
    assert!(err.to_string().contains("silently discarded"), "{err}");

    // The refusal must leave the client usable, not wedge its buffer.
    assert_eq!(client.pending(), 0, "the refused record left no residue");
    client.send(&trades, &row(T13 + 500)).unwrap();
    client.flush().unwrap();
    eventually("the client to keep working after a refusal", || {
        appended(&client) == 201
    });

    h.stop(&client);
}

#[test]
fn a_dropped_client_flushes_what_it_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::start(dir.path());
    let probe = h.client();
    {
        let mut client = h.client();
        let trades = client.series("trades").unwrap();
        // Fewer than the batch size, so nothing is sent until Drop runs.
        for i in 0..5 {
            client
                .send(
                    &trades,
                    &Row {
                        ts: T13 + i,
                        id: "",
                        keys: &[Value::Str("AAPL"), Value::Null],
                        extra: &[],
                        data: "",
                    },
                )
                .unwrap();
        }
        assert_eq!(client.pending(), 5, "still buffered, batch not full");
    }

    eventually("Drop to flush the partial batch", || appended(&probe) == 5);
    h.stop(&probe);
}

/// libzmq reports a control-socket timeout as `EAGAIN` — "Resource temporarily
/// unavailable" — which sends the reader looking for a resource when the cause
/// is that nothing is listening. This is the first failure a newcomer running
/// the examples hits, so it has to name the endpoint it actually tried.
#[test]
fn a_dead_control_endpoint_is_named_rather_than_reported_as_eagain() {
    let err = Client::builder()
        .ingest("tcp://127.0.0.1:1")
        .control("tcp://127.0.0.1:2")
        // Short, so the test does not wait out the 5s default.
        .control_timeout_ms(200)
        .connect()
        .expect_err("nothing is listening on port 2")
        .to_string();

    assert!(
        err.contains("tcp://127.0.0.1:2"),
        "must name the endpoint: {err}"
    );
    assert!(
        err.contains("orc server running"),
        "must say what to check: {err}"
    );
    assert!(
        !err.contains("Resource temporarily unavailable"),
        "the raw errno is what this replaces: {err}"
    );
}
