use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use tim_ws::hub::Hub;
use tim_ws::record::Recorder;
use tim_ws::{replay, sensor, server};

/// Streams SICK TiM LiDAR scans to WebSocket clients.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Scanner address (CoLa A), e.g. 192.168.0.1:2111.
    #[arg(long, required_unless_present = "replay", conflicts_with = "replay")]
    sensor: Option<String>,

    /// Replay a JSONL recording instead of connecting to a scanner.
    #[arg(long, value_name = "FILE")]
    replay: Option<PathBuf>,

    /// Replay speed factor (0.01–1000); 0 = as fast as possible.
    #[arg(long, default_value_t = 1.0, requires = "replay", value_parser = parse_speed)]
    speed: f64,

    /// Restart the replay at the end of the file.
    #[arg(long = "loop", requires = "replay")]
    repeat: bool,

    /// WebSocket listen address. Use 0.0.0.0:9002 to serve the network.
    #[arg(long, default_value = "127.0.0.1:9002")]
    listen: String,

    /// Name for this scanner in all messages (default: the address).
    #[arg(long)]
    sensor_id: Option<String>,

    /// Record every raw telegram to this new JSONL file.
    #[arg(long, value_name = "FILE", conflicts_with = "replay")]
    record: Option<PathBuf>,

    /// Stop recording after this many MiB.
    #[arg(long, default_value_t = 256)]
    record_max_mib: u64,

    /// Raw distances below this (mm at scale 1) become null. Values below 16 are
    /// always status codes (no echo, dazzled, implausible); raise it to e.g. 50 to
    /// also drop readings under the TiM551's 0.05 m working range.
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u16).range(16..))]
    invalid_below: u16,

    /// Maximum simultaneous WebSocket clients, including pending handshakes.
    #[arg(long, default_value_t = 32, value_parser = clap::value_parser!(u32).range(1..=4096))]
    max_clients: u32,

    /// Browser Origin allowed to connect, e.g. http://192.168.178.41:8000 (repeatable).
    /// Clients without an Origin header and http(s) origins on localhost are always allowed.
    #[arg(long = "allow-origin", value_name = "ORIGIN")]
    allow_origins: Vec<String>,

    /// Reconnect when no telegram arrives for this long while streaming.
    #[arg(long, default_value_t = 1000)]
    watchdog_ms: u64,

    /// Debug logging.
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(if args.verbose { tracing::Level::DEBUG } else { tracing::Level::INFO })
        .with_writer(std::io::stderr)
        .init();
    match run(args).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), String> {
    let source = args.sensor.clone().unwrap_or_else(|| args.replay.as_ref().unwrap().display().to_string());
    let sensor_id = args.sensor_id.clone().unwrap_or_else(|| source.clone());

    let listener = TcpListener::bind(&args.listen).await.map_err(|e| format!("listen on {}: {e}", args.listen))?;
    tracing::info!("serving ws://{}/ (JSON) and ws://{}/?format=binary", args.listen, args.listen);

    let cancel = CancellationToken::new();
    let (events, events_rx) = mpsc::channel(256);
    let channels = Hub::new(sensor_id.clone(), args.invalid_below).spawn(events_rx);

    let mut writer = None;
    let source_task = if let Some(path) = args.replay.clone() {
        let cfg = replay::ReplayConfig { path, speed: args.speed, repeat: args.repeat };
        tokio::spawn(replay::run(cfg, events, cancel.clone()))
    } else {
        let recorder = match &args.record {
            Some(path) => {
                let (recorder, handle) =
                    Recorder::start(path.clone(), args.record_max_mib * 1024 * 1024, sensor_id.clone())
                        .await
                        .map_err(|e| format!("recording {}: {e}", path.display()))?;
                writer = Some(handle);
                Some(recorder)
            }
            None => None,
        };
        let cfg = sensor::SensorConfig {
            addr: source,
            connect_timeout: Duration::from_secs(3),
            watchdog: Duration::from_millis(args.watchdog_ms),
        };
        tokio::spawn(sensor::run(cfg, events, recorder, cancel.clone()))
    };
    let server_cfg = server::ServerConfig {
        max_clients: args.max_clients as usize,
        allowed_origins: args.allow_origins.clone(),
        ..Default::default()
    };
    let mut server_task = tokio::spawn(server::serve(listener, channels, server_cfg, cancel.clone()));
    let mut source_task = source_task;

    // The source only ends on its own if something failed; never keep serving stale state.
    let mut result = tokio::select! {
        _ = shutdown_signal() => Ok(()),
        r = &mut source_task => Err(match r {
            Ok(Ok(())) => "data source stopped unexpectedly".to_string(),
            Ok(Err(e)) => e,
            Err(e) => format!("data source crashed: {e}"),
        }),
    };
    tracing::info!("shutting down");
    cancel.cancel();
    if !source_task.is_finished() && tokio::time::timeout(Duration::from_secs(3), &mut source_task).await.is_err() {
        tracing::warn!("data source did not stop in time; aborting it");
        source_task.abort();
        let _ = source_task.await;
    }
    if tokio::time::timeout(Duration::from_secs(1), &mut server_task).await.is_err() {
        server_task.abort();
    }
    if let Some(w) = writer {
        // The source task is gone, so its recorder is dropped; let the writer flush.
        let err = match tokio::time::timeout(Duration::from_secs(3), w).await {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(e))) => Some(format!("recording failed: {e}")),
            Ok(Err(e)) => Some(format!("recording task crashed: {e}")),
            Err(_) => Some("recording did not finish flushing in time".to_string()),
        };
        if let Some(e) = err {
            tracing::error!("{e}");
            result = result.and(Err(e));
        }
    }
    result
}

fn parse_speed(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|e| format!("{e}"))?;
    if v == 0.0 || (0.01..=1000.0).contains(&v) {
        Ok(v)
    } else {
        Err("speed must be 0 or between 0.01 and 1000".into())
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
