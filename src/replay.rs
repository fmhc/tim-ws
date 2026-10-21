//! Replays recorded telegrams as if they came from the scanner.
//!
//! Accepts tim-ws recordings (`frame` / `frame_hex`) and line formats that
//! carry `frame_hex` with STX/ETX included. Pacing follows the device clock in
//! the telegrams, so no host timestamps are needed; frames are restamped with
//! the current time and marked `replay: true`.

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::clock::now_utc_us;
use crate::cola::{ETX, STX};
use crate::frame::Pipeline;
use crate::hub::{Event, State};
use crate::scan::{is_scan_telegram, parse_scan};

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub path: PathBuf,
    /// 1.0 = real time, 0 = as fast as possible.
    pub speed: f64,
    pub repeat: bool,
}

/// Device clock gaps above this are not slept (recording pauses, restarts).
const MAX_PAUSE_US: u32 = 2_000_000;

/// Runs until `cancel` fires (Ok) or the file cannot be replayed (Err).
/// After a single pass it keeps serving the `replay_ended` status.
pub async fn run(cfg: ReplayConfig, events: mpsc::Sender<Event>, cancel: CancellationToken) -> Result<(), String> {
    let mut pipeline = Pipeline::new(true);
    let detail = format!("replay {}", cfg.path.display());
    loop {
        if events.send(Event::State { state: State::Streaming, detail: detail.clone() }).await.is_err() {
            return Err("hub stopped".into());
        }
        let end = tokio::select! {
            r = replay_file(&cfg, &mut pipeline, &events) => r,
            _ = cancel.cancelled() => Ok(()),
        };
        let failed = end.is_err();
        let (state, detail) = match end {
            Err(e) => (State::Stopped, format!("replay failed: {e}")),
            Ok(()) if cancel.is_cancelled() => (State::Stopped, "shutdown".to_string()),
            Ok(()) if cfg.repeat => continue,
            Ok(()) => (State::ReplayEnded, detail.clone()),
        };
        tracing::info!("{detail}");
        let _ = events.send(Event::State { state, detail: detail.clone() }).await;
        if failed {
            return Err(detail);
        }
        if state == State::ReplayEnded {
            // Keep serving the final status until shutdown.
            cancel.cancelled().await;
        }
        return Ok(());
    }
}

async fn replay_file(cfg: &ReplayConfig, pipeline: &mut Pipeline, events: &mpsc::Sender<Event>) -> std::io::Result<()> {
    let file = tokio::fs::File::open(&cfg.path).await?;
    let mut lines = BufReader::new(file).lines();
    let mut last_device_us: Option<u32> = None;
    while let Some(line) = lines.next_line().await? {
        let Some(payload) = payload_of(&line) else { continue };
        if !is_scan_telegram(&payload) {
            continue;
        }
        let scan = match parse_scan(&payload) {
            Ok(s) => s,
            Err(e) => {
                if events.send(Event::BadTelegram(e.to_string())).await.is_err() {
                    return Ok(());
                }
                continue;
            }
        };
        if let Some(prev) = last_device_us {
            let gap = scan.time_since_startup_us.wrapping_sub(prev);
            match pause(gap, cfg.speed) {
                Some(d) => sleep(d).await,
                None => tokio::task::yield_now().await,
            }
        }
        last_device_us = Some(scan.time_since_startup_us);
        if events.send(Event::Scan(Box::new(pipeline.ingest(scan, now_utc_us())))).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Sleep for a device clock gap at `speed`; None means do not wait.
/// Non-finite or out-of-range speeds never panic, they just do not pace.
fn pause(gap_us: u32, speed: f64) -> Option<Duration> {
    if gap_us > MAX_PAUSE_US || !(speed.is_finite() && speed > 0.0) {
        return None;
    }
    Duration::try_from_secs_f64(f64::from(gap_us) / 1e6 / speed).ok().filter(|d| d.as_secs() <= 60)
}

/// Received payload of one JSON line, without STX/ETX; None for tx or metadata.
fn payload_of(line: &str) -> Option<Vec<u8>> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let dir = v.get("dir").or_else(|| v.get("direction")).and_then(|d| d.as_str());
    if dir != Some("rx") {
        return None;
    }
    let mut bytes = if let Some(s) = v.get("frame").and_then(|f| f.as_str()) {
        s.as_bytes().to_vec()
    } else {
        unhex(v.get("frame_hex")?.as_str()?)?
    };
    if bytes.first() == Some(&STX) && bytes.last() == Some(&ETX) && bytes.len() >= 2 {
        bytes.pop();
        bytes.remove(0);
    }
    Some(bytes)
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_never_panics() {
        assert_eq!(pause(66_667, 1.0), Some(Duration::from_micros(66_667)));
        assert_eq!(pause(66_667, 0.0), None);
        assert_eq!(pause(66_667, 1e-300), None);
        assert_eq!(pause(66_667, f64::NAN), None);
        assert_eq!(pause(66_667, f64::INFINITY), None);
        assert_eq!(pause(3_000_000, 1.0), None);
    }

    #[test]
    fn reads_both_line_formats() {
        assert_eq!(payload_of(r#"{"dir":"rx","frame":"sEA LMDscandata 1"}"#).unwrap(), b"sEA LMDscandata 1");
        assert_eq!(payload_of(r#"{"direction":"rx","frame_hex":"02734541203103"}"#).unwrap(), b"sEA 1");
        assert_eq!(payload_of(r#"{"dir":"tx","frame":"sRN DIornr"}"#), None);
        assert_eq!(payload_of(r#"{"type":"meta"}"#), None);
        assert_eq!(payload_of("not json"), None);
    }
}
