//! Live connection to the scanner: connect, identify, subscribe, stream,
//! and reconnect with backoff. A watchdog treats silence as a failure, so a
//! dead link never looks like an empty room.

use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::clock::now_utc_us;
use crate::cola::ColaA;
use crate::frame::Pipeline;
use crate::hub::{Event, State};
use crate::record::{Dir, Recorder};
use crate::scan::{is_scan_telegram, parse_scan};

#[derive(Debug, Clone)]
pub struct SensorConfig {
    pub addr: String,
    pub connect_timeout: Duration,
    /// Maximum silence while streaming before reconnecting.
    pub watchdog: Duration,
}

/// Read-only identification queries sent after connecting.
const IDENTIFY: &[&str] = &["DeviceIdent", "DItype", "DIornr", "SCdevicestate"];

const BACKOFF_MIN: Duration = Duration::from_millis(250);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

pub async fn run(
    cfg: SensorConfig,
    events: mpsc::Sender<Event>,
    recorder: Option<Recorder>,
    cancel: CancellationToken,
) {
    let mut pipeline = Pipeline::new(false);
    let mut backoff = BACKOFF_MIN;
    loop {
        let state = |state, detail: String| Event::State { state, detail };
        if events.send(state(State::Connecting, cfg.addr.clone())).await.is_err() {
            return;
        }
        pipeline.reset();
        let mut session = Session { cfg: &cfg, events: &events, recorder: recorder.as_ref(), scans: 0 };
        let result = session.run(&mut pipeline, &cancel).await;
        if session.scans > 0 {
            backoff = BACKOFF_MIN;
        }
        match result {
            Ok(()) => {
                let _ = events.send(state(State::Stopped, "shutdown".into())).await;
                return;
            }
            Err(reason) => {
                tracing::warn!("sensor {}: {reason}; retrying in {backoff:?}", cfg.addr);
                if events.send(state(State::Disconnected, reason)).await.is_err() {
                    return;
                }
            }
        }
        tokio::select! {
            _ = sleep(backoff) => {}
            _ = cancel.cancelled() => {
                let _ = events.send(state(State::Stopped, "shutdown".into())).await;
                return;
            }
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

struct Session<'a> {
    cfg: &'a SensorConfig,
    events: &'a mpsc::Sender<Event>,
    recorder: Option<&'a Recorder>,
    scans: u64,
}

type Io = Framed<TcpStream, ColaA>;

impl Session<'_> {
    /// Ok(()) means a clean shutdown; Err carries the reason to reconnect.
    async fn run(&mut self, pipeline: &mut Pipeline, cancel: &CancellationToken) -> Result<(), String> {
        let stream = timeout(self.cfg.connect_timeout, TcpStream::connect(&self.cfg.addr))
            .await
            .map_err(|_| "connect timed out".to_string())?
            .map_err(|e| format!("connect failed: {e}"))?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        let mut io = Framed::new(stream, ColaA::default());

        let mut identity = Vec::new();
        for name in IDENTIFY {
            if let Some(answer) = self.request(&mut io, &format!("sRN {name}"), &format!("sRA {name} ")).await? {
                identity.push((name.to_string(), answer));
            }
        }
        self.send_event(Event::Identity(identity)).await?;

        if self.request(&mut io, "sEN LMDscandata 1", "sEA LMDscandata 1").await?.is_none() {
            return Err("scanner did not confirm the scan subscription".into());
        }
        self.send_event(Event::State { state: State::Streaming, detail: self.cfg.addr.clone() }).await?;

        loop {
            tokio::select! {
                next = timeout(self.cfg.watchdog, io.next()) => {
                    let payload = match next {
                        Err(_) => return Err(format!("no telegram for {} ms", self.cfg.watchdog.as_millis())),
                        Ok(None) => return Err("connection closed by scanner".into()),
                        Ok(Some(Err(e))) => return Err(format!("read failed: {e}")),
                        Ok(Some(Ok(p))) => p,
                    };
                    let t = now_utc_us();
                    self.record(Dir::Rx, t, &payload);
                    self.handle(pipeline, payload, t).await?;
                }
                _ = cancel.cancelled() => {
                    // Leave the scanner unsubscribed for the next client.
                    let stop = self.request(&mut io, "sEN LMDscandata 0", "sEA LMDscandata 0");
                    match timeout(Duration::from_millis(500), stop).await {
                        Ok(Ok(Some(_))) => tracing::info!("scan subscription stopped"),
                        _ => tracing::warn!("scanner did not confirm unsubscribing"),
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn handle(&mut self, pipeline: &mut Pipeline, payload: Bytes, t: i64) -> Result<(), String> {
        if is_scan_telegram(&payload) {
            match parse_scan(&payload) {
                Ok(scan) => {
                    self.scans += 1;
                    self.send_event(Event::Scan(Box::new(pipeline.ingest(scan, t)))).await
                }
                Err(e) => self.send_event(Event::BadTelegram(e.to_string())).await,
            }
        } else {
            if payload.starts_with(b"sFA") {
                tracing::warn!("scanner error reply: {}", String::from_utf8_lossy(&payload));
            }
            Ok(())
        }
    }

    async fn send(&self, io: &mut Io, command: &str) -> Result<(), String> {
        self.record(Dir::Tx, now_utc_us(), &Bytes::copy_from_slice(command.as_bytes()));
        io.send(command).await.map_err(|e| format!("write failed: {e}"))
    }

    /// Sends `command` and waits up to 2 s for a reply starting with `expect`.
    /// Scan telegrams from an older subscription are skipped meanwhile.
    async fn request(&self, io: &mut Io, command: &str, expect: &str) -> Result<Option<String>, String> {
        self.send(io, command).await?;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let payload = match tokio::time::timeout_at(deadline, io.next()).await {
                Err(_) => return Ok(None),
                Ok(None) => return Err("connection closed by scanner".into()),
                Ok(Some(Err(e))) => return Err(format!("read failed: {e}")),
                Ok(Some(Ok(p))) => p,
            };
            self.record(Dir::Rx, now_utc_us(), &payload);
            if payload.starts_with(expect.as_bytes()) {
                let text = String::from_utf8_lossy(&payload[expect.len()..]).trim().to_string();
                return Ok(Some(text));
            }
            if payload.starts_with(b"sFA") {
                tracing::warn!("{command}: scanner error reply {}", String::from_utf8_lossy(&payload));
                return Ok(None);
            }
        }
    }

    fn record(&self, dir: Dir, t: i64, payload: &Bytes) {
        if let Some(r) = self.recorder {
            r.record(dir, t, payload);
        }
    }

    async fn send_event(&self, ev: Event) -> Result<(), String> {
        self.events.send(ev).await.map_err(|_| "hub stopped".to_string())
    }
}
