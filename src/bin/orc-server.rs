//! `orc-server` — run the engine behind a ZeroMQ ingest socket.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use orc::config::Config;
use orc::engine::Engine;
use orc::server::Server;

/// Where to look for `config.json` when neither `--config` nor `--data` says.
const DEFAULT_DATA_DIR: &str = "./data";

#[derive(Parser, Debug)]
#[command(name = "orc-server", version, about)]
struct Args {
    /// Data directory. Overrides `data_dir` in the config file.
    ///
    /// `Option`, not a `default_value`, so "not passed" differs from "passed the
    /// default". With a default the override fires unconditionally, and
    /// `--config /etc/orc/config.json` silently ignores that file's own
    /// `data_dir`.
    #[arg(long)]
    data: Option<PathBuf>,

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
/// Not `EnvFilter`: per-target directives cost five crates, and one binary
/// logging from one crate has no targets to discriminate between. Anything else
/// is reported rather than ignored — a filter that quietly does nothing is worse
/// than no filter.
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
    let config_path = args.config.clone().unwrap_or_else(|| {
        args.data
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR))
            .join("config.json")
    });

    let mut config = Config::load(&config_path)?;
    // `--data` wins over the file's own `data_dir`, which is self-referential:
    // honouring it over the path we were pointed at is how a server reads one
    // directory's config and writes another's. Absent the flag the file decides,
    // which is the only way `--config` alone can work.
    if let Some(data) = &args.data {
        if data != &config.data_dir {
            tracing::info!(
                config = %config.data_dir.display(),
                using = %data.display(),
                "--data overrides the config file's data_dir"
            );
        }
        config.data_dir = data.clone();
    }

    let engine = Arc::new(Engine::open_with(config.clone(), args.force_unlock)?);
    let server = Server::bind(Arc::clone(&engine), &config)?;
    server.run()
}
