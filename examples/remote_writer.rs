//! Writes to `embedded_server` over ZeroMQ only. Optional args:
//! `<ingest_endpoint> <control_endpoint>` (control defaults to ingest port + 1).
//! ```sh
//! cargo run --features net --example remote_writer -- tcp://127.0.0.1:5655
//! ```

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

fn main() -> std::process::ExitCode {
    // Not `main() -> Result`, which would print the error with `Debug`.
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("remote_writer: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> orc::Result<()> {
    let mut args = std::env::args().skip(1);
    let ingest = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:5655".to_string());

    let mut builder = Client::builder()
        .ingest(&ingest)
        // Records per message (also split by size).
        .batch(8)
        // Block rather than drop when the queue is full.
        .on_full(OnFull::Block);
    if let Some(control) = args.next() {
        builder = builder.control(&control);
    }

    // Connecting performs the schema handshake.
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
    // Fire-and-forget: only puts the batch on the wire.
    client.flush()?;
    println!("sent {} records", client.stats().records_sent);

    // A synchronous flush; otherwise records wait for the server's interval.
    match client.control(ControlRequest::Flush)? {
        ControlResponse::Flushed(f) => println!(
            "server flushed {} row(s) from segment(s) {:?}",
            f.rows_written, f.segments_consumed
        ),
        other => println!("unexpected reply to flush: {other:?}"),
    }
    Ok(())
}
