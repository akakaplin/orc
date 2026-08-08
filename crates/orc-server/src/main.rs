//! `orc-server` — ZeroMQ PULL ingest over an [`orc_core`] engine.
//!
//! Binds a PULL socket for ingest and a REP socket for control (`ping`, `stats`,
//! `flush`, `schema`, `reload-config`). Received batches are handed straight to
//! `append_raw`, so a batch is copied exactly once: from the ZeroMQ buffer into
//! the WAL.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "orc-server", version, about)]
struct Args {
    /// Data directory. Overrides `data_dir` in the config.
    #[arg(long, default_value = "./data")]
    data: String,

    /// Config path. Defaults to `<data>/config.json`.
    #[arg(long)]
    config: Option<String>,

    /// Take over a LOCK held by a live-but-stuck process.
    #[arg(long)]
    force_unlock: bool,
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config = args
        .config
        .unwrap_or_else(|| format!("{}/config.json", args.data.trim_end_matches('/')));
    tracing::info!(data = %args.data, config = %config, "orc-server: not yet implemented (M6)");
}
