//! One process that appends in-process and serves ZeroMQ for other writers.
//! Optional args: `<data_dir> <ingest_endpoint> <control_endpoint>`; stop with `orc-cli shutdown`.
//! ```sh
//! cargo run --features net --example embedded_server -- ./data-example
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use orc::config::{Config, SeriesConfig, ServerConfig};
use orc::engine::Engine;
use orc::record::Row;
use orc::server::Server;

/// Timestamps are epoch **microseconds**, UTC.
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
            eprintln!("embedded_server: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> orc::Result<()> {
    let mut args = std::env::args().skip(1);
    let data_dir = args.next().unwrap_or_else(|| "./data-example".to_string());
    let ingest = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:5655".to_string());
    let control = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:5656".to_string());

    // Normally `Config::load("<data_dir>/config.json")`.
    let config = Config {
        data_dir: data_dir.clone().into(),
        server: ServerConfig {
            ingest_endpoint: ingest,
            control_endpoint: control,
            ..ServerConfig::default()
        },
        // No declared keys: just timestamp and id.
        series: vec![SeriesConfig {
            name: "pulse".to_string(),
            keys: Vec::new(),
        }],
        ..Config::default()
    };

    // One engine shared by the server and the in-process writer.
    let engine = Arc::new(Engine::open(config.clone())?);
    let server = Server::bind(Arc::clone(&engine), &config)?;

    println!("data_dir {data_dir}");
    println!("ingest   {}", server.ingest_endpoint()?);
    println!("control  {}", server.control_endpoint()?);
    println!("writing five timestamps in-process, then serving until shutdown");

    let local = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || -> orc::Result<()> {
            // Resolve once, outside the loop.
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

    // Blocks until `shutdown`, then drains the queue and closes the engine.
    let served = server.run();

    match local.join() {
        Ok(r) => r?,
        Err(_) => eprintln!("the in-process writer panicked"),
    }
    served
}
