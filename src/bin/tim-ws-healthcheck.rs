//! Check fresh streaming status through the bridge, never through the scanner.
use std::process::ExitCode;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;

fn healthy(status: &Value) -> bool {
    status["state"] == "streaming"
        && status["last_scan_age_ms"].as_f64().is_some_and(|age| (0.0..1500.0).contains(&age))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let url = std::env::args().nth(1).unwrap_or_else(|| "ws://127.0.0.1:9002/".into());
    let check = async {
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.ok()?;
        while let Some(Ok(message)) = ws.next().await {
            if let Ok(text) = message.to_text() {
                if let Ok(status) = serde_json::from_str::<Value>(text) {
                    if status["type"] == "status" {
                        let result = healthy(&status);
                        let _ = ws.close(None).await;
                        return Some(result);
                    }
                }
            }
        }
        None
    };
    match tokio::time::timeout(Duration::from_secs(3), check).await {
        Ok(Some(true)) => ExitCode::SUCCESS,
        _ => ExitCode::FAILURE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_missing_stale_and_disconnected_status() {
        assert!(healthy(&json!({"state":"streaming", "last_scan_age_ms":67})));
        assert!(!healthy(&json!({"state":"streaming", "last_scan_age_ms":1500})));
        assert!(!healthy(&json!({"state":"streaming", "last_scan_age_ms":null})));
        assert!(!healthy(&json!({"state":"disconnected", "last_scan_age_ms":12})));
    }
}
