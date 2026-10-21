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

    /// Replay speed factor; 0 = as fast as possible.
    #[arg(long, default_value_t = 1.0, requires = "replay")]
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

    /// Raw distance values below this count as "no echo" (null).
    #[arg(long, default_value_t = 1)]
    invalid_below: u16,

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
    let server_task = tokio::spawn(server::serve(listener, channels, cancel.clone()));

    shutdown_signal().await;
    tracing::info!("shutting down");
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), source_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), server_task).await;
    if let Some(w) = writer {
        // The sensor task has dropped its recorder; let the writer flush.
        let _ = tokio::time::timeout(Duration::from_secs(2), w).await;
    }
    Ok(())
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
