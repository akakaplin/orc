//! `orc-server` — run the engine behind a ZeroMQ ingest socket.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use orc::config::Config;
use orc::engine::Engine;
use orc::server::Server;

#[derive(Parser, Debug)]
#[command(name = "orc-server", version, about)]
struct Args {
    /// Data directory. Overrides `data_dir` in the config file.
    #[arg(long, default_value = "./data")]
    data: PathBuf,

    /// Config path. Defaults to `<data>/config.json`.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Take over a lock held by a process that is stuck but alive.
    #[arg(long)]
    force_unlock: bool,
}

fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt().with_max_level(log_level()).init();

    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "orc-server failed");
            std::process::ExitCode::FAILURE
        }
    }
}

/// `RUST_LOG` as a bare level: `error`, `warn`, `info`, `debug` or `trace`.
///
/// Not `EnvFilter`, which parses per-target directives like `orc=debug,zmq=warn`
/// and costs five crates to do it. One binary logging from one crate has no
/// targets to discriminate between, so a level is the whole of what the setting
/// ever meant here. Anything else is reported rather than silently ignored --
/// a filter that quietly does nothing is worse than no filter.
fn log_level() -> tracing::Level {
    use std::str::FromStr;

    match std::env::var("RUST_LOG") {
        Err(_) => tracing::Level::INFO,
        Ok(raw) => match tracing::Level::from_str(raw.trim()) {
            Ok(level) => level,
            Err(_) => {
                eprintln!(
                    "orc-server: RUST_LOG={raw:?} is not a level \
                     (error|warn|info|debug|trace); using info"
                );
                tracing::Level::INFO
            }
        },
    }
}

fn run() -> orc::Result<()> {
    let args = Args::parse();
    let config_path = args.config.unwrap_or_else(|| args.data.join("config.json"));

    let mut config = Config::load(&config_path)?;
    // `--data` wins over the file's own `data_dir`. That field is self-
    // referential -- it lives inside the directory it names -- so honouring it
    // over the path we were actually pointed at is how a server ends up reading
    // one directory's config while writing another's.
    config.data_dir = args.data.clone();

    let engine = Arc::new(Engine::open_with(config.clone(), args.force_unlock)?);
    let server = Server::bind(Arc::clone(&engine), &config)?;
    server.run()
}
