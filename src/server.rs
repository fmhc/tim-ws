//! WebSocket endpoint. `ws://host:port/` streams JSON; `/?format=binary`
//! sends scans in the binary layout and status messages as JSON text.
//!
//! Clients only receive. The server bounds what a client can cost: a global
//! connection limit (pending handshakes included), a handshake deadline,
//! small read buffers and message limits, and a send timeout. Browser
//! clients must come from an allowed Origin, because WebSockets are not
//! covered by CORS and any web page could otherwise read the scans.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::hub::Channels;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Simultaneous clients, including pending handshakes.
    pub max_clients: usize,
    pub handshake_timeout: Duration,
    /// A client that cannot take a message within this time is disconnected.
    pub send_timeout: Duration,
    /// Allowed browser Origins in addition to localhost (exact match, e.g. `http://host:8000`).
    pub allowed_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_clients: 32,
            handshake_timeout: Duration::from_secs(5),
            send_timeout: Duration::from_secs(5),
            allowed_origins: Vec::new(),
        }
    }
}

/// Clients send nothing useful; keep their buffers and messages tiny.
fn ws_config() -> WebSocketConfig {
    WebSocketConfig::default().read_buffer_size(4096).max_message_size(Some(4096)).max_frame_size(Some(4096))
}

pub async fn serve(listener: TcpListener, channels: Channels, cfg: ServerConfig, cancel: CancellationToken) {
    let slots = Arc::new(Semaphore::new(cfg.max_clients));
    let cfg = Arc::new(cfg);
    let clients = tokio_util::task::TaskTracker::new();
    loop {
        let (stream, peer) = tokio::select! {
            r = listener.accept() => match r {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = cancel.cancelled() => break,
        };
        let Ok(slot) = slots.clone().try_acquire_owned() else {
            tracing::warn!("client {peer} rejected: {} clients connected", cfg.max_clients);
            drop(stream);
            continue;
        };
        let (channels, cancel, cfg) = (channels.clone(), cancel.clone(), cfg.clone());
        clients.spawn(async move {
            let _slot = slot;
            match client(stream, peer, channels, &cfg, cancel).await {
                Ok(()) => tracing::info!("client {peer} disconnected"),
                Err(e) => tracing::info!("client {peer} dropped: {e}"),
            }
        });
    }
    clients.close();
    if tokio::time::timeout(Duration::from_secs(1), clients.wait()).await.is_err() {
        tracing::warn!("some clients did not close in time");
    }
}

/// Origins that may connect: none (non-browser clients), localhost, or configured.
pub fn origin_allowed(origin: Option<&str>, allowed: &[String]) -> bool {
    let Some(origin) = origin else { return true };
    if allowed.iter().any(|a| a == origin) {
        return true;
    }
    let Some(rest) = origin.strip_prefix("http://").or_else(|| origin.strip_prefix("https://")) else {
        return false;
    };
    let host = match rest.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => rest.split(':').next().unwrap_or(""),
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn forbidden() -> ErrorResponse {
    let mut resp = ErrorResponse::new(Some("origin not allowed".into()));
    *resp.status_mut() = StatusCode::FORBIDDEN;
    resp
}

// The handshake callback type is fixed by tungstenite.
#[allow(clippy::result_large_err)]
async fn client(
    stream: TcpStream,
    peer: SocketAddr,
    channels: Channels,
    cfg: &ServerConfig,
    cancel: CancellationToken,
) -> Result<(), String> {
    let _ = stream.set_nodelay(true);
    let mut binary = false;
    let handshake = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |req: &Request, resp: Response| {
            let origin = req.headers().get("origin").map(|v| v.to_str().unwrap_or("<invalid>"));
            if !origin_allowed(origin, &cfg.allowed_origins) {
                tracing::warn!("client {peer} rejected: origin {origin:?} not allowed");
                return Err(forbidden());
            }
            binary = req.uri().query().is_some_and(|q| q.split('&').any(|kv| kv == "format=binary"));
            Ok(resp)
        },
        Some(ws_config()),
    );
    let ws = tokio::select! {
        r = tokio::time::timeout(cfg.handshake_timeout, handshake) => r
            .map_err(|_| "handshake timed out".to_string())?
            .map_err(|e| format!("handshake: {e}"))?,
        _ = cancel.cancelled() => return Ok(()),
    };
    tracing::info!("client {peer} connected ({})", if binary { "binary" } else { "json" });

    let (mut sink, mut incoming) = ws.split();
    let mut rx = channels.broadcast.subscribe();
    let first = channels.status.borrow().text.clone();
    send(&mut sink, Message::Text(first), cfg.send_timeout).await?;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(RecvError::Lagged(skipped)) => {
                        let note = json!({"type": "lagged", "skipped": skipped}).to_string();
                        send(&mut sink, Message::Text(note.into()), cfg.send_timeout).await?;
                        continue;
                    }
                    Err(RecvError::Closed) => return Ok(()),
                };
                let out = match (&msg.binary, binary) {
                    (Some(b), true) => Message::Binary(b.clone()),
                    _ => Message::Text(msg.text.clone()),
                };
                send(&mut sink, out, cfg.send_timeout).await?;
            }
            // Reading drives ping/pong and notices disconnects; content is ignored.
            m = incoming.next() => match m {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return Ok(()),
                Some(Ok(_)) => {}
            },
            _ = cancel.cancelled() => {
                let _ = send(&mut sink, Message::Close(None), Duration::from_millis(500)).await;
                return Ok(());
            }
        }
    }
}

async fn send<S>(sink: &mut S, msg: Message, limit: Duration) -> Result<(), String>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(limit, sink.send(msg)).await {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err("send timed out".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::origin_allowed;

    #[test]
    fn origins() {
        let extra = vec!["http://192.168.178.41:8000".to_string()];
        assert!(origin_allowed(None, &[]));
        assert!(origin_allowed(Some("http://localhost:8765"), &[]));
        assert!(origin_allowed(Some("http://127.0.0.1"), &[]));
        assert!(origin_allowed(Some("http://[::1]:8000"), &[]));
        assert!(origin_allowed(Some("http://192.168.178.41:8000"), &extra));
        assert!(!origin_allowed(Some("https://untrusted.example"), &extra));
        assert!(!origin_allowed(Some("http://localhost.evil.example"), &[]));
        assert!(!origin_allowed(Some("http://127.0.0.1.evil.example"), &[]));
        assert!(!origin_allowed(Some("null"), &[]));
        assert!(!origin_allowed(Some("http://192.168.178.41:8001"), &extra));
    }
}
