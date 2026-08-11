use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use tokio::sync::mpsc;

static ENABLED: AtomicBool = AtomicBool::new(false);
static CHANNEL: OnceLock<mpsc::UnboundedSender<String>> = OnceLock::new();

/// Call once at startup.
pub fn init(webapp_url: &str, device_id: &str) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let url = format!("{}/api/debug-log", webapp_url.trim_end_matches('/'));
    let did = device_id.to_string();

    tokio::spawn(async move {
        let mut batch: Vec<String> = Vec::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    if let Some(m) = msg { batch.push(m); }
                    else { break; }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() && ENABLED.load(Ordering::Relaxed) {
                        let entries: Vec<_> = batch.iter().map(|m|
                            serde_json::json!({"level":"info","category":"conn","msg":m})
                        ).collect();
                        let body = serde_json::json!({"device_id": &did, "entries": entries});
                        let _ = post_json(&url, body.to_string()).await;
                    }
                    batch.clear();
                }
            }
        }
    });
    CHANNEL.set(tx).ok();
}

pub fn enable() { ENABLED.store(true, Ordering::Relaxed); }
pub fn disable() { ENABLED.store(false, Ordering::Relaxed); }

pub fn push(msg: &str) {
    if ENABLED.load(Ordering::Relaxed) {
        if let Some(tx) = CHANNEL.get() { let _ = tx.send(msg.to_string()); }
    }
}

async fn post_json(url: &str, body: String) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    client.post(url).header("Content-Type", "application/json").body(body).send().await?;
    Ok(())
}