use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::{counter::Counter, family::Family, gauge::Gauge};
use prometheus_client::registry::Registry;
use subsquid_network_transport::PeerId;

use crate::scheduler::WorkerStatus;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WorkerLabels {
    peer_id: Option<String>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientLabels {
    client_id: Option<String>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PingLabels {
    peer_id: String,
    version: String,
}

lazy_static::lazy_static! {
    static ref INVALID_MESSAGES: Family<WorkerLabels, Counter> = Default::default();
    static ref PING: Family<PingLabels, Counter> = Default::default();
    static ref STORED_BYTES: Family<WorkerLabels, Gauge> = Default::default();
    static ref STORED_CHUNKS: Family<WorkerLabels, Gauge> = Default::default();
    static ref LAST_COLLECTED_LOG: Family<WorkerLabels, Gauge> = Default::default();
    static ref QUERIES_OK: Family<WorkerLabels, Counter> = Default::default();
    static ref QUERIES_BAD_REQUEST: Family<WorkerLabels, Counter> = Default::default();
    static ref QUERIES_SERVER_ERROR: Family<WorkerLabels, Counter> = Default::default();
    static ref LAST_EXECUTED_QUERY: Family<WorkerLabels, Gauge> = Default::default();
    static ref QUERIES_BY_CLIENT: Family<ClientLabels, Counter> = Default::default();

    static ref NUM_MISSING_CHUNKS: Family<WorkerLabels, Gauge> = Default::default();
    static ref ASSIGNED_UNITS: Family<WorkerLabels, Gauge> = Default::default();
    static ref ASSIGNED_BYTES: Family<WorkerLabels, Gauge> = Default::default();
    static ref LAST_ASSIGNMENT: Family<WorkerLabels, Gauge> = Default::default();
    static ref SCHEDULER_STORED_BYTES: Family<WorkerLabels, Gauge> = Default::default();
}

pub fn invalid_message(peer_id: Option<PeerId>) {
    INVALID_MESSAGES
        .get_or_create(&WorkerLabels {
            peer_id: peer_id.map(|id| id.to_string()),
        })
        .inc();
}

pub fn report_ping(peer_id: PeerId, version: String, stored_bytes: u64, total_chunks: u64) {
    let peer_id = peer_id.to_string();
    let worker = WorkerLabels {
        peer_id: Some(peer_id.clone()),
    };
    PING.get_or_create(&PingLabels { peer_id, version }).inc();
    STORED_BYTES.get_or_create(&worker).set(stored_bytes as i64);
    STORED_CHUNKS
        .get_or_create(&worker)
        .set(total_chunks as i64);
}

pub fn report_logs_collected(peer_id: String, seq_no: u64) {
    let worker = WorkerLabels {
        peer_id: Some(peer_id),
    };
    LAST_COLLECTED_LOG.get_or_create(&worker).set(seq_no as i64);
}

pub fn report_scheduler_status(status: WorkerStatus) {
    let worker = WorkerLabels {
        peer_id: Some(status.peer_id),
    };
    NUM_MISSING_CHUNKS
        .get_or_create(&worker)
        .set(status.num_missing_chunks as i64);
    ASSIGNED_UNITS
        .get_or_create(&worker)
        .set(status.assigned_units.len() as i64);
    ASSIGNED_BYTES
        .get_or_create(&worker)
        .set(status.assigned_bytes as i64);
    LAST_ASSIGNMENT
        .get_or_create(&worker)
        .set(status.last_assignment as i64);
    SCHEDULER_STORED_BYTES
        .get_or_create(&worker)
        .set(status.stored_bytes as i64);
}

pub fn register_metrics(registry: &mut Registry) {
    registry.register(
        "invalid_messages",
        "Number of messages that could not be parsed",
        INVALID_MESSAGES.clone(),
    );
    registry.register("ping", "Ping received from a worker", PING.clone());
    registry.register(
        "stored_bytes",
        "Total bytes used on worker's machine",
        STORED_BYTES.clone(),
    );
    registry.register(
        "stored_chunks",
        "Total chunks stored on worker's machine",
        STORED_CHUNKS.clone(),
    );
    registry.register(
        "last_collected_log",
        "Seq no of the last log saved by logs collector",
        LAST_COLLECTED_LOG.clone(),
    );
    registry.register(
        "queries_ok",
        "Number of queries executed successfully",
        QUERIES_OK.clone(),
    );
    registry.register(
        "queries_bad_request",
        "Number of queries executed with bad request",
        QUERIES_BAD_REQUEST.clone(),
    );
    registry.register(
        "queries_server_error",
        "Number of queries executed with server error",
        QUERIES_SERVER_ERROR.clone(),
    );
    registry.register(
        "last_executed_query",
        "Seq no of the last query executed by the worker",
        LAST_EXECUTED_QUERY.clone(),
    );
    registry.register(
        "queries_by_client",
        "Number of processed queries from a given client",
        QUERIES_BY_CLIENT.clone(),
    );
    registry.register(
        "num_missing_chunks",
        "Number of missing chunks",
        NUM_MISSING_CHUNKS.clone(),
    );
    registry.register(
        "assigned_units",
        "Number of assigned units to the worker",
        ASSIGNED_UNITS.clone(),
    );
    registry.register(
        "assigned_bytes",
        "Number of bytes assigned to the worker",
        ASSIGNED_BYTES.clone(),
    );
    registry.register(
        "last_assignment",
        "Timestamp of the last assignment",
        LAST_ASSIGNMENT.clone(),
    );
    registry.register(
        "scheduler_stored_bytes",
        "Total bytes stored by the worker as reported by scheduler",
        SCHEDULER_STORED_BYTES.clone(),
    );
}
