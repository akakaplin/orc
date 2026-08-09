//! A separate process writing into someone else's engine over ZeroMQ.
//!
//! The counterpart to `embedded_server`: it never touches the data directory,
//! only the socket. Records are encoded here in the same frame format the WAL
//! uses, so the server appends the bytes it receives without transcoding them.
//!
//! ```sh
//! cargo run --features net --example remote_writer -- tcp://127.0.0.1:5655
//! ```
//!
//! Arguments, both optional: `<ingest_endpoint> <control_endpoint>`. The control
//! endpoint defaults to the ingest port + 1, which is what the server defaults
//! to as well.

use std::time::{SystemTime, UNIX_EPOCH};

use orc::client::{Client, OnFull};
use orc::protocol::{ControlRequest, ControlResponse};
use orc::record::Row;

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after 1970")
        .as_micros() as u64
}

fn main() -> orc::Result<()> {
    let mut args = std::env::args().skip(1);
    let ingest = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:5655".to_string());

    let mut builder = Client::builder()
        .ingest(&ingest)
        // Records per message. The client also splits on size, because a message
        // over the server's `max_batch_bytes` is dropped by libzmq below the
        // application, where neither side can report it.
        .batch(8)
        // Block at the send high-water mark rather than dropping: a saturated
        // server slows this process down instead of losing records silently.
        .on_full(OnFull::Block);
    if let Some(control) = args.next() {
        builder = builder.control(&control);
    }

    // Connecting performs the schema handshake, which is not optional: keys
    // travel positionally, so there is no frame to build until the server has
    // said what order they go in and which epoch to stamp.
    let mut client = builder.connect()?;
    let declared: Vec<&str> = client.series_names().collect();
    println!("connected to {ingest}; server declares {declared:?}");

    let pulse = client.series("pulse")?;
    for i in 0..5 {
        let id = format!("remote-{i}");
        client.send(
            &pulse,
            &Row {
                ts: now_us(),
                id: &id,
                keys: &[],
                extra: &[],
                data: "",
            },
        )?;
    }
    // Ingest is fire-and-forget: this only puts the buffered batch on the wire.
    client.flush()?;
    println!("sent {} records", client.stats().records_sent);

    // Which is why the flush is a separate, synchronous request. Without it the
    // records sit in the WAL until the server's own interval timer fires.
    match client.control(ControlRequest::Flush)? {
        ControlResponse::Flushed(f) => println!(
            "server flushed {} row(s) from segment(s) {:?}",
            f.rows_written, f.segments_consumed
        ),
        other => println!("unexpected reply to flush: {other:?}"),
    }
    Ok(())
}
