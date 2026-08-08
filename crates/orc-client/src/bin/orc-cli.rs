//! `orc-cli` — manual ingest and control against a running `orc-server`.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "orc-cli", version, about)]
struct Args {
    /// Ingest endpoint (PUSH -> server PULL).
    #[arg(long, default_value = "tcp://127.0.0.1:5555")]
    ingest: String,

    /// Control endpoint (REQ -> server REP).
    #[arg(long, default_value = "tcp://127.0.0.1:5556")]
    control: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Send newline-delimited JSON records from stdin.
    Send {
        /// Series name; must exist in the server config.
        series: String,
    },
    /// Ask the server to flush now.
    Flush,
    /// Print server statistics.
    Stats,
    /// Check that the server is alive.
    Ping,
}

fn main() {
    let args = Args::parse();
    eprintln!("orc-cli: not yet implemented (M7): {:?}", args.cmd);
}
