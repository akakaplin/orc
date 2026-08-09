//! One process that is both the database and a writer.
//!
//! It opens the engine, appends to it directly from a background thread, and at
//! the same time binds a ZeroMQ server so other processes can write to the same
//! data directory. Both writers land in the same WAL and the same Parquet files.
//!
//! ```sh
//! cargo run --features net --example embedded_server -- ./data-example
//! ```
//!
//! Arguments, all optional: `<data_dir> <ingest_endpoint> <control_endpoint>`.
//! It serves until someone sends a `shutdown` on the control socket:
//!
//! ```sh
//! cargo run --features cli --bin orc-cli -- --ingest tcp://127.0.0.1:5655 shutdown
//! ```
//!
//! `examples/two_processes.sh` drives the whole thing end to end.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use orc::config::{Config, SeriesConfig, ServerConfig};
use orc::engine::Engine;
use orc::record::Row;
use orc::server::Server;

/// Records carry epoch **microseconds**, UTC — not milliseconds, which is the
/// mistake the accept window exists to catch.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after 1970")
        .as_micros() as u64
}

fn main() -> orc::Result<()> {
    let mut args = std::env::args().skip(1);
    let data_dir = args.next().unwrap_or_else(|| "./data-example".to_string());
    let ingest = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:5655".to_string());
    let control = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:5656".to_string());

    // Built in code rather than read from `config.json`, so the example is one
    // file. A real deployment writes the same fields to `<data_dir>/config.json`
    // and calls `Config::load`.
    let config = Config {
        data_dir: data_dir.clone().into(),
        server: ServerConfig {
            ingest_endpoint: ingest,
            control_endpoint: control,
            ..ServerConfig::default()
        },
        // No declared keys: this series carries nothing but a timestamp and an
        // id. Anything undeclared a writer sends would land in `extra`.
        series: vec![SeriesConfig {
            name: "pulse".to_string(),
            keys: Vec::new(),
        }],
        ..Config::default()
    };

    // One engine, shared. The server holds a reference and so does the writer
    // thread below -- appending in-process and appending over the socket are the
    // same operation, and neither needs to know about the other.
    let engine = Arc::new(Engine::open(config.clone())?);
    let server = Server::bind(Arc::clone(&engine), &config)?;

    // Resolved after binding, because a config may ask for port 0 or `*`.
    println!("data_dir {data_dir}");
    println!("ingest   {}", server.ingest_endpoint()?);
    println!("control  {}", server.control_endpoint()?);
    println!("writing five timestamps in-process, then serving until shutdown");

    let local = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || -> orc::Result<()> {
            // Resolve once, outside the loop: this is the hash lookup that
            // `append` is designed never to repeat.
            let pulse = engine.series("pulse")?;
            for i in 0..5 {
                let id = format!("embedded-{i}");
                engine.append(
                    &pulse,
                    &Row {
                        ts: now_us(),
                        id: &id,
                        keys: &[],
                        extra: &[],
                        data: "",
                    },
                )?;
                std::thread::sleep(Duration::from_millis(10));
            }
            println!("in-process writer: 5 records appended");
            Ok(())
        })
    };

    // Blocks until a `shutdown` request. On the way out it drains whatever
    // ZeroMQ still had queued and closes the engine, which flushes the WAL.
    let served = server.run();

    match local.join() {
        Ok(r) => r?,
        Err(_) => eprintln!("the in-process writer panicked"),
    }
    served
}
