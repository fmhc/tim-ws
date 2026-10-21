//! Fan-out: a single task owns the stream state, serialises each message once
//! and broadcasts it. Slow clients lag and skip frames; they never slow down
//! the sensor connection.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde_json::json;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_tungstenite::tungstenite::Utf8Bytes;

use crate::clock::{host_clock, now_utc_us, ClockStatus};
use crate::frame::ScanFrame;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Connecting,
    Streaming,
    Disconnected,
    ReplayEnded,
    Stopped,
}

#[derive(Debug)]
pub enum Event {
    State { state: State, detail: String },
    Identity(Vec<(String, String)>),
    Scan(Box<ScanFrame>),
    BadTelegram(String),
}

/// One serialised message, shared by all clients.
#[derive(Debug)]
pub struct Outgoing {
    pub text: Utf8Bytes,
    /// Binary encoding for clients that asked for it; status stays text.
    pub binary: Option<Bytes>,
}

impl Outgoing {
    fn text(s: String) -> Arc<Self> {
        Arc::new(Self { text: s.into(), binary: None })
    }
}

#[derive(Clone)]
pub struct Channels {
    pub broadcast: broadcast::Sender<Arc<Outgoing>>,
    /// Latest status, sent to every client right after connecting.
    pub status: watch::Receiver<Arc<Outgoing>>,
}

pub struct Hub {
    sensor: String,
    invalid_below: u16,
    state: State,
    detail: String,
    state_since_utc_us: i64,
    identity: Vec<(String, String)>,
    scans: u64,
    bad_telegrams: u64,
    counter_gaps: u64,
    last_scan_utc_us: Option<i64>,
    clock: Option<ClockStatus>,
}

impl Hub {
    pub fn new(sensor: String, invalid_below: u16) -> Self {
        Self {
            sensor,
            invalid_below,
            state: State::Connecting,
            detail: String::new(),
            state_since_utc_us: now_utc_us(),
            identity: Vec::new(),
            scans: 0,
            bad_telegrams: 0,
            counter_gaps: 0,
            last_scan_utc_us: None,
            clock: host_clock(),
        }
    }

    fn status(&self) -> Arc<Outgoing> {
        let now = now_utc_us();
        let identity: serde_json::Map<_, _> = self.identity.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
        Outgoing::text(
            json!({
                "type": "status",
                "sensor": self.sensor,
                "state": self.state,
                "detail": self.detail,
                "state_since_utc_us": self.state_since_utc_us,
                "now_utc_us": now,
                "last_scan_utc_us": self.last_scan_utc_us,
                "last_scan_age_ms": self.last_scan_utc_us.map(|t| (now - t) / 1000),
                "scans": self.scans,
                "counter_gaps": self.counter_gaps,
                "bad_telegrams": self.bad_telegrams,
                "host_clock": self.clock,
                "identity": identity,
            })
            .to_string(),
        )
    }

    /// Spawns the hub task; it runs until every event sender is dropped.
    pub fn spawn(mut self, mut events: mpsc::Receiver<Event>) -> Channels {
        let (tx, _) = broadcast::channel(64);
        let (status_tx, status_rx) = watch::channel(self.status());
        let channels = Channels { broadcast: tx.clone(), status: status_rx };
        let ret = channels.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    ev = events.recv() => match ev {
                        Some(ev) => self.apply(ev, &tx, &status_tx),
                        None => break,
                    },
                    _ = tick.tick() => {
                        self.clock = host_clock();
                        let s = self.status();
                        status_tx.send_replace(s.clone());
                        let _ = tx.send(s);
                    }
                }
            }
        });
        ret
    }

    fn apply(&mut self, ev: Event, tx: &broadcast::Sender<Arc<Outgoing>>, status_tx: &watch::Sender<Arc<Outgoing>>) {
        match ev {
            Event::Scan(frame) => {
                match (frame.to_json(&self.sensor, self.invalid_below), frame.to_binary(self.invalid_below)) {
                    (Ok(text), Ok(bin)) => {
                        self.scans += 1;
                        self.counter_gaps += u64::from(frame.counter_gap);
                        self.last_scan_utc_us = Some(frame.host_recv_utc_us);
                        let _ = tx.send(Arc::new(Outgoing { text: text.into(), binary: Some(bin) }));
                    }
                    _ => self.bad_telegrams += 1,
                }
                return;
            }
            Event::BadTelegram(reason) => {
                self.bad_telegrams += 1;
                tracing::debug!("bad telegram: {reason}");
                return;
            }
            Event::Identity(id) => self.identity = id,
            Event::State { state, detail } => {
                if state != self.state {
                    self.state_since_utc_us = now_utc_us();
                }
                self.state = state;
                self.detail = detail;
            }
        }
        let s = self.status();
        status_tx.send_replace(s.clone());
        let _ = tx.send(s);
    }
}
