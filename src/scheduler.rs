use anyhow::Result;

use crate::metrics;

const PINGS_URL: &str = "https://scheduler.testnet.subsquid.io/workers/pings";

#[derive(Debug, serde::Deserialize)]
pub struct WorkerStatus {
    pub peer_id: String,
    pub address: String,
    pub last_ping: u64,
    pub version: Option<String>,
    pub jailed: bool,
    pub jail_reason: Option<String>,
    pub assigned_units: Vec<String>,
    pub assigned_bytes: u64,
    // pub stored_ranges: Vec<StoredRange>,
    pub stored_bytes: u64,
    pub num_missing_chunks: u64,
    pub last_assignment: u64,
    pub unreachable_since: Option<u64>,
    pub last_dial_time: u64,
    pub last_dial_ok: bool,
}

async fn request_pings_status() -> Result<Vec<WorkerStatus>> {
    Ok(reqwest::get(PINGS_URL).await?.json().await?)
}

pub async fn collect_scheduler_metrics() -> Result<()> {
    let pings_status = request_pings_status().await?;
    for worker in pings_status {
        metrics::report_scheduler_status(worker);
    }
    Ok(())
}
