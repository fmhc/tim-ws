//! WebSocket endpoint. `ws://host:port/` streams JSON; `/?format=binary`
//! sends scans in the binary layout and status messages as JSON text.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::hub::Channels;

/// A client that cannot take a message within this time is disconnected.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn serve(listener: TcpListener, channels: Channels, cancel: CancellationToken) {
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
            _ = cancel.cancelled() => return,
        };
        let channels = channels.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            match client(stream, peer, channels, cancel).await {
                Ok(()) => tracing::info!("client {peer} disconnected"),
                Err(e) => tracing::info!("client {peer} dropped: {e}"),
            }
        });
    }
}

// The handshake callback type is fixed by tungstenite.
#[allow(clippy::result_large_err)]
async fn client(
    stream: TcpStream,
    peer: SocketAddr,
    channels: Channels,
    cancel: CancellationToken,
) -> Result<(), String> {
    let _ = stream.set_nodelay(true);
    let mut binary = false;
    let ws = tokio_tungstenite::accept_hdr_async(stream, |req: &Request, resp: Response| {
        binary = req.uri().query().is_some_and(|q| q.split('&').any(|kv| kv == "format=binary"));
        Ok(resp)
    })
    .await
    .map_err(|e| format!("handshake: {e}"))?;
    tracing::info!("client {peer} connected ({})", if binary { "binary" } else { "json" });

    let (mut sink, mut incoming) = ws.split();
    let mut rx = channels.broadcast.subscribe();
    let first = channels.status.borrow().text.clone();
    send(&mut sink, Message::Text(first)).await?;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(RecvError::Lagged(skipped)) => {
                        let note = json!({"type": "lagged", "skipped": skipped}).to_string();
                        send(&mut sink, Message::Text(note.into())).await?;
                        continue;
                    }
                    Err(RecvError::Closed) => return Ok(()),
                };
                let out = match (&msg.binary, binary) {
                    (Some(b), true) => Message::Binary(b.clone()),
                    _ => Message::Text(msg.text.clone()),
                };
                send(&mut sink, out).await?;
            }
            // Reading drives ping/pong and notices disconnects; content is ignored.
            m = incoming.next() => match m {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return Ok(()),
                Some(Ok(_)) => {}
            },
            _ = cancel.cancelled() => {
                let _ = sink.send(Message::Close(None)).await;
                return Ok(());
            }
        }
    }
}

async fn send<S>(sink: &mut S, msg: Message) -> Result<(), String>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(SEND_TIMEOUT, sink.send(msg)).await {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err("send timed out".into()),
    }
}
