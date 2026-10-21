//! Raw telegram recording as JSON lines, written off the hot path.
//!
//! The first line is metadata; each further line is one telegram:
//! `{"t_utc_us":…,"dir":"rx","frame":"sSN LMDscandata …"}`. Payloads that are
//! not printable ASCII are stored as `frame_hex` instead. Recording stops at
//! the size limit; an existing file is never overwritten.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::clock::{host_clock, now_utc_us};

#[derive(Debug, Clone, Copy)]
pub enum Dir {
    Rx,
    Tx,
}

#[derive(Clone)]
pub struct Recorder {
    tx: mpsc::Sender<(i64, Dir, Bytes)>,
    pub dropped: Arc<AtomicU64>,
}

impl Recorder {
    /// Creates the file and spawns the writer task. The task flushes and ends
    /// once every `Recorder` clone is dropped; await the handle before exiting.
    pub async fn start(
        path: PathBuf,
        max_bytes: u64,
        sensor: String,
    ) -> std::io::Result<(Self, tokio::task::JoinHandle<()>)> {
        let file = tokio::fs::OpenOptions::new().write(true).create_new(true).open(&path).await?;
        let mut out = tokio::io::BufWriter::new(file);
        let meta = json!({
            "type": "meta",
            "tool": concat!("tim-ws ", env!("CARGO_PKG_VERSION")),
            "sensor": sensor,
            "started_utc_us": now_utc_us(),
            "host_clock": host_clock(),
            "framing": "CoLa A, STX/ETX stripped",
        });
        out.write_all(format!("{meta}\n").as_bytes()).await?;
        out.flush().await?;

        let (tx, mut rx) = mpsc::channel::<(i64, Dir, Bytes)>(1024);
        let writer = tokio::spawn(async move {
            let mut written = 0u64;
            while let Some((t, dir, payload)) = rx.recv().await {
                let dir = match dir {
                    Dir::Rx => "rx",
                    Dir::Tx => "tx",
                };
                let line = match std::str::from_utf8(&payload) {
                    Ok(s) if s.bytes().all(|b| (0x20..0x7F).contains(&b)) => {
                        json!({"t_utc_us": t, "dir": dir, "frame": s})
                    }
                    _ => json!({"t_utc_us": t, "dir": dir, "frame_hex": hex(&payload)}),
                };
                let line = format!("{line}\n");
                written += line.len() as u64;
                if written > max_bytes {
                    tracing::warn!(
                        "recording limit of {max_bytes} bytes reached, recording stopped: {}",
                        path.display()
                    );
                    break;
                }
                if let Err(e) = out.write_all(line.as_bytes()).await {
                    tracing::error!("recording failed: {e}");
                    break;
                }
                if rx.is_empty() {
                    let _ = out.flush().await;
                }
            }
            let _ = out.flush().await;
        });
        Ok((Self { tx, dropped: Arc::new(AtomicU64::new(0)) }, writer))
    }

    /// Never blocks; counts telegrams dropped when the writer falls behind.
    pub fn record(&self, dir: Dir, t_utc_us: i64, payload: &Bytes) {
        if self.tx.try_send((t_utc_us, dir, payload.clone())).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
