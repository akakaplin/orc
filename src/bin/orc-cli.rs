//! `orc-cli` — drive a running `orc-server` by hand.
//!
//! `send` reads newline-delimited JSON on stdin, one record per line:
//!
//! ```text
//! {"ts": 1786194000000000, "id": "a1", "keys": ["AAPL", 100],
//!  "extra": {"feed": "itch"}, "data": "{\"px\":1.5}"}
//! ```

use clap::{Parser, Subcommand};
use orc::client::{Client, OnFull};
use orc::protocol::ControlRequest;
use orc::record::{Row, Value};

#[derive(Parser, Debug)]
#[command(name = "orc-cli", version, about)]
struct Args {
    /// Ingest endpoint (PUSH -> server PULL).
    #[arg(long, default_value = "tcp://127.0.0.1:5555")]
    ingest: String,

    /// Control endpoint (REQ -> server REP). Defaults to the ingest port + 1.
    ///
    /// `Option`, not a `default_value`: a default is always present, so it would
    /// suppress the client's port+1 derivation and `--ingest tcp://prod:5555`
    /// would send records to prod while handshaking against localhost. Keys
    /// travel positionally, so two hosts declaring a series in a different order
    /// means silently swapped columns.
    #[arg(long)]
    control: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Check the server is alive.
    Ping,
    /// Print server counters.
    Stats,
    /// Ask the server to flush now.
    Flush,
    /// Print every series' epoch and key order.
    Schema,
    /// Ask the server to drain and exit.
    Shutdown,
    /// Send newline-delimited JSON records from stdin.
    Send {
        /// Series name; must exist in the server's config.
        series: String,
        /// Records per batch.
        #[arg(long, default_value_t = 1024)]
        batch: usize,
    },
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("orc-cli: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> orc::Result<()> {
    let args = Args::parse();
    // Batch size is fixed at connect, so it has to be known before we build.
    let batch = match args.cmd {
        Cmd::Send { batch, .. } => batch,
        _ => 1,
    };
    let mut builder = Client::builder()
        .ingest(&args.ingest)
        .batch(batch)
        .on_full(OnFull::Block);
    if let Some(control) = &args.control {
        builder = builder.control(control);
    }
    let mut client = builder.connect()?;

    match args.cmd {
        Cmd::Ping => print_reply(client.control(ControlRequest::Ping)?),
        Cmd::Stats => print_reply(client.control(ControlRequest::Stats)?),
        Cmd::Flush => print_reply(client.control(ControlRequest::Flush)?),
        Cmd::Schema => print_reply(client.control(ControlRequest::Schema)?),
        Cmd::Shutdown => print_reply(client.control(ControlRequest::Shutdown)?),
        Cmd::Send { series, .. } => send(&mut client, &series)?,
    }
    Ok(())
}

fn print_reply(reply: orc::protocol::ControlResponse) {
    match serde_json::to_string_pretty(&reply) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("orc-cli: could not render reply: {e}"),
    }
}

/// One JSON object per line, in the shape the module docs show.
fn send(client: &mut Client, series_name: &str) -> orc::Result<()> {
    use std::io::BufRead;

    // Returns an owned handle, so the borrow ends here and `send` can take
    // `&mut client` freely below.
    let series = client.series(series_name)?;

    let stdin = std::io::stdin();
    let mut line_no = 0usize;
    for line in stdin.lock().lines() {
        let line = line.map_err(orc::Error::Io)?;
        line_no += 1;
        if line.trim().is_empty() {
            continue;
        }
        let parsed: JsonRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                // One bad line must not abandon the rest of the stream.
                eprintln!("orc-cli: line {line_no}: {e}");
                continue;
            }
        };
        // One bad line must not abandon the rest of the stream, same as a parse
        // failure above.
        let keys: Vec<Value<'_>> = match parsed.keys.iter().map(json_to_value).collect() {
            Ok(keys) => keys,
            Err(why) => {
                eprintln!("orc-cli: line {line_no}: {why}");
                continue;
            }
        };
        let extra: Vec<(&str, &str)> = parsed
            .extra
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        client.send(
            &series,
            &Row {
                ts: parsed.ts,
                id: &parsed.id,
                keys: &keys,
                extra: &extra,
                data: &parsed.data,
            },
        )?;
    }
    client.flush()?;
    let s = client.stats();
    eprintln!(
        "sent {} record(s) in {} batch(es), {} byte(s); {} dropped",
        s.records_sent, s.batches_sent, s.bytes_sent, s.records_dropped
    );
    Ok(())
}

#[derive(serde::Deserialize)]
struct JsonRecord {
    ts: u64,
    #[serde(default)]
    id: String,
    #[serde(default)]
    keys: Vec<serde_json::Value>,
    #[serde(default)]
    extra: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    data: String,
}

/// One JSON scalar as a key value.
///
/// Arrays and objects have no declared-key representation and are named as
/// errors rather than coerced. They used to become `Value::Str("")` on the
/// theory that the server would reject the type — but `""` is a perfectly good
/// value for a `string` column, so an object in a string key was stored as an
/// empty string the producer never wrote.
fn json_to_value(v: &serde_json::Value) -> Result<Value<'_>, &'static str> {
    Ok(match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) if n.is_i64() => Value::I64(n.as_i64().unwrap_or_default()),
        serde_json::Value::Number(n) => Value::F64(n.as_f64().unwrap_or_default()),
        serde_json::Value::String(s) => Value::Str(s),
        serde_json::Value::Array(_) => return Err("a key value may not be an array"),
        serde_json::Value::Object(_) => return Err("a key value may not be an object"),
    })
}
