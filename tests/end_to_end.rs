//! Mock scanner → tim-ws → WebSocket client, over real sockets.
//! Synthetic telegrams only: this checks the plumbing, not detection.

use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use tim_ws::hub::Hub;
use tim_ws::{sensor, server};

fn scan_telegram(counter: u16, startup_us: u32) -> Vec<u8> {
    format!(
        "\x02sSN LMDscandata 1 1 ABCDEF 0 0 {c:X} {c:X} {s:X} {t:X} 0 0 1 0 0 5DC 36 0 \
         1 DIST1 3F800000 00000000 FFF92230 2710 3 0 B0 717 0 0 1 B not defined 0 0 0\x03",
        c = counter,
        s = startup_us,
        t = startup_us + 2814,
    )
    .into_bytes()
}

/// Reads one STX…ETX command from the client.
async fn read_command(sock: &mut TcpStream) -> String {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        sock.read_exact(&mut b).await.unwrap();
        match b[0] {
            0x02 => out.clear(),
            0x03 => return String::from_utf8(out).unwrap(),
            x => out.push(x),
        }
    }
}

/// Answers identification and the subscription, then sends `counters` as
/// scans in awkward fragments and closes the connection.
async fn mock_session(sock: &mut TcpStream, counters: &[u16]) {
    loop {
        let cmd = read_command(sock).await;
        if cmd == "sEN LMDscandata 1" {
            sock.write_all(b"\x02sEA LMDscandata 1\x03").await.unwrap();
            break;
        }
        let name = cmd.strip_prefix("sRN ").unwrap();
        let answer = if name == "DItype" { "E TIM551-2050001".to_string() } else { "1".to_string() };
        sock.write_all(format!("\x02sRA {name} {answer}\x03").as_bytes()).await.unwrap();
    }
    let mut stream = Vec::new();
    for &c in counters {
        stream.extend(scan_telegram(c, 1_000_000 + u32::from(c) * 66_667));
    }
    for (i, chunk) in stream.chunks(37).enumerate() {
        sock.write_all(chunk).await.unwrap();
        if i % 5 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }
    sleep(Duration::from_millis(50)).await;
}

async fn next_json(
    ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
) -> Value {
    loop {
        let msg = timeout(Duration::from_secs(5), ws.next()).await.expect("ws timeout").unwrap().unwrap();
        if let Message::Text(t) = msg {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn next_of(
    ws: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    pred: impl Fn(&Value) -> bool,
) -> Value {
    loop {
        let v = next_json(ws).await;
        if pred(&v) {
            return v;
        }
    }
}

#[tokio::test]
async fn streams_fragmented_scans_and_reconnects() {
    let scanner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scanner_addr = scanner.local_addr().unwrap().to_string();
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();

    let cancel = CancellationToken::new();
    let (events, events_rx) = mpsc::channel(64);
    let channels = Hub::new("mock".into(), 1).spawn(events_rx);
    tokio::spawn(server::serve(ws_listener, channels, server::ServerConfig::default(), cancel.clone()));

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{ws_addr}/")).await.unwrap();
    assert_eq!(next_json(&mut ws).await["type"], "status");

    let cfg = sensor::SensorConfig {
        addr: scanner_addr,
        connect_timeout: Duration::from_secs(1),
        watchdog: Duration::from_millis(500),
    };
    let sensor_task = tokio::spawn(sensor::run(cfg, events, None, cancel.clone()));

    // First session: counter 11 is missing.
    let (mut sock, _) = scanner.accept().await.unwrap();
    let mock = tokio::spawn(async move {
        mock_session(&mut sock, &[10, 12, 13]).await;
    });

    let streaming = next_of(&mut ws, |v| v["state"] == "streaming").await;
    assert_eq!(streaming["identity"]["DItype"], "E TIM551-2050001");
    let first = next_of(&mut ws, |v| v["type"] == "scan").await;
    assert_eq!(first["range_m"], serde_json::json!([null, 0.176, 1.815]));
    assert_eq!(first["device"]["scan_counter"], 10);
    assert_eq!(first["angle"]["start"], -45.0);
    let second = next_of(&mut ws, |v| v["type"] == "scan").await;
    assert_eq!(second["counter_gap"], 1);
    let est = second["scan_utc_us_est"].as_i64().unwrap();
    let recv = second["host_recv_utc_us"].as_i64().unwrap();
    assert!((0..1_000_000).contains(&(recv - est)), "estimate {est} vs receive {recv}");
    next_of(&mut ws, |v| v["type"] == "scan").await;
    mock.await.unwrap();

    // Mock closed the socket: the bridge must report it and reconnect.
    let down = next_of(&mut ws, |v| v["state"] == "disconnected").await;
    assert_eq!(down["scans"], 3);
    assert_eq!(down["counter_gaps"], 1);
    let (mut sock, _) = timeout(Duration::from_secs(3), scanner.accept()).await.unwrap().unwrap();
    let mock = tokio::spawn(async move {
        mock_session(&mut sock, &[20]).await;
        // Stay silent: the watchdog must fire.
        sleep(Duration::from_secs(2)).await;
    });
    next_of(&mut ws, |v| v["state"] == "streaming").await;
    let again = next_of(&mut ws, |v| v["type"] == "scan").await;
    assert_eq!(again["device"]["scan_counter"], 20);
    assert_eq!(again["counter_gap"], 0, "counters restart after reconnect");
    let silent = next_of(&mut ws, |v| v["state"] == "disconnected").await;
    assert!(silent["detail"].as_str().unwrap().contains("no telegram"), "{silent}");

    cancel.cancel();
    timeout(Duration::from_secs(2), sensor_task).await.unwrap().unwrap().unwrap();
    mock.abort();
}

#[tokio::test]
async fn binary_clients_get_binary_scans() {
    let scanner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scanner_addr = scanner.local_addr().unwrap().to_string();
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();

    let cancel = CancellationToken::new();
    let (events, events_rx) = mpsc::channel(64);
    let channels = Hub::new("mock".into(), 1).spawn(events_rx);
    tokio::spawn(server::serve(ws_listener, channels, server::ServerConfig::default(), cancel.clone()));
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{ws_addr}/?format=binary")).await.unwrap();

    let cfg = sensor::SensorConfig {
        addr: scanner_addr,
        connect_timeout: Duration::from_secs(1),
        watchdog: Duration::from_millis(500),
    };
    tokio::spawn(sensor::run(cfg, events, None, cancel.clone()));
    let (mut sock, _) = scanner.accept().await.unwrap();
    tokio::spawn(async move { mock_session(&mut sock, &[1]).await });

    let bin = loop {
        match timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap() {
            Message::Binary(b) => break b,
            _ => continue,
        }
    };
    assert_eq!(&bin[..4], b"TIM1");
    assert_eq!(bin.len(), 52 + 6);
    assert_eq!(u16::from_le_bytes([bin[54], bin[55]]), 0xB0);
    cancel.cancel();
}

fn sensor_cfg(addr: String) -> sensor::SensorConfig {
    sensor::SensorConfig { addr, connect_timeout: Duration::from_secs(1), watchdog: Duration::from_millis(500) }
}

/// Cancel while the scanner ignores the subscription request: the bridge must
/// still try to unsubscribe and stop promptly.
#[tokio::test]
async fn cancel_before_subscription_ack_still_unsubscribes() {
    let scanner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = scanner.local_addr().unwrap().to_string();
    let cancel = CancellationToken::new();
    let (events, mut events_rx) = mpsc::channel(64);
    tokio::spawn(async move { while events_rx.recv().await.is_some() {} });
    let task = tokio::spawn(sensor::run(sensor_cfg(addr), events, None, cancel.clone()));

    let (mut sock, _) = scanner.accept().await.unwrap();
    loop {
        let cmd = read_command(&mut sock).await;
        if cmd == "sEN LMDscandata 1" {
            break; // never acknowledged
        }
        let name = cmd.strip_prefix("sRN ").unwrap();
        sock.write_all(format!("\x02sRA {name} 1\x03").as_bytes()).await.unwrap();
    }
    cancel.cancel();
    let next = timeout(Duration::from_secs(1), read_command(&mut sock)).await.expect("no unsubscribe sent");
    assert_eq!(next, "sEN LMDscandata 0");
    timeout(Duration::from_secs(1), task).await.expect("task did not stop").unwrap().unwrap();
}

/// Cancel during identification stops at once instead of finishing the queries.
#[tokio::test]
async fn cancel_during_identification_stops_promptly() {
    let scanner = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = scanner.local_addr().unwrap().to_string();
    let cancel = CancellationToken::new();
    let (events, mut events_rx) = mpsc::channel(64);
    tokio::spawn(async move { while events_rx.recv().await.is_some() {} });
    let task = tokio::spawn(sensor::run(sensor_cfg(addr), events, None, cancel.clone()));
    let (mut sock, _) = scanner.accept().await.unwrap();
    assert_eq!(read_command(&mut sock).await, "sRN DeviceIdent");
    cancel.cancel();
    timeout(Duration::from_millis(300), task).await.expect("task did not stop").unwrap().unwrap();
}

#[tokio::test]
async fn foreign_origin_is_rejected_and_client_limit_holds() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let (_events, events_rx) = mpsc::channel(8);
    let channels = Hub::new("mock".into(), 16).spawn(events_rx);
    let cfg = server::ServerConfig { max_clients: 1, ..Default::default() };
    tokio::spawn(server::serve(ws_listener, channels, cfg, cancel.clone()));

    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{ws_addr}/").into_client_request().unwrap();
    req.headers_mut().insert("origin", "https://untrusted.example".parse().unwrap());
    let err = tokio_tungstenite::connect_async(req).await.expect_err("foreign origin accepted");
    assert!(err.to_string().contains("403"), "{err}");

    let mut req = format!("ws://{ws_addr}/").into_client_request().unwrap();
    req.headers_mut().insert("origin", "http://localhost:8765".parse().unwrap());
    let (mut first, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(next_json(&mut first).await["type"], "status");
    // The only slot is taken: a second client is turned away.
    assert!(tokio_tungstenite::connect_async(format!("ws://{ws_addr}/")).await.is_err());
    cancel.cancel();
}

/// Silent TCP peers occupy a slot only until the handshake deadline.
#[tokio::test]
async fn idle_handshake_is_dropped() {
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws_listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let (_events, events_rx) = mpsc::channel(8);
    let channels = Hub::new("mock".into(), 16).spawn(events_rx);
    let cfg =
        server::ServerConfig { max_clients: 1, handshake_timeout: Duration::from_millis(200), ..Default::default() };
    tokio::spawn(server::serve(ws_listener, channels, cfg, cancel.clone()));
    let mut idle = TcpStream::connect(ws_addr).await.unwrap();
    let mut buf = [0u8; 1];
    let n = timeout(Duration::from_secs(2), idle.read(&mut buf)).await.expect("idle peer kept").unwrap_or(0);
    assert_eq!(n, 0, "server should close the idle connection");
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{ws_addr}/")).await.unwrap();
    assert_eq!(next_json(&mut ws).await["type"], "status");
    cancel.cancel();
}
