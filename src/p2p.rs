use anyhow::Result;
use futures::{Stream, StreamExt};
use subsquid_messages::signatures::SignedMessage;
use subsquid_messages::DatasetRanges;
use subsquid_network_transport::{
    ObserverConfig, ObserverEvent, ObserverTransportHandle, P2PTransportBuilder, PeerId,
    TransportArgs,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::metrics;

pub struct Observer<EventStream> {
    raw_event_stream: Option<EventStream>,
    _transport_handle: ObserverTransportHandle,
}

pub async fn create_observer(
    args: TransportArgs,
    _scheduler_id: PeerId,
    logs_collector_id: PeerId,
) -> Result<Observer<impl Stream<Item = ObserverEvent>>> {
    let transport_builder = P2PTransportBuilder::from_cli(args).await?;
    let (event_stream, _transport_handle) =
        transport_builder.build_observer(ObserverConfig::new(logs_collector_id))?;
    Ok(Observer {
        raw_event_stream: Some(event_stream),
        _transport_handle,
    })
}

impl<EventStream: Stream<Item = ObserverEvent>> Observer<EventStream> {
    pub async fn run(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        let event_stream = self.raw_event_stream.take().unwrap();
        event_stream
            .take_until(cancellation_token.cancelled_owned())
            .for_each(|ev| async { self.handle_event(ev) })
            .await;
        Ok(())
    }

    fn handle_event(&self, ev: ObserverEvent) {
        match ev {
            ObserverEvent::Ping { peer_id, ping } => {
                let stored_bytes = ping.stored_bytes();
                let version = match ping.version {
                    Some(ver) => ver,
                    None => {
                        warn!("ping without version from {peer_id}");
                        metrics::invalid_message(Some(peer_id));
                        return;
                    }
                };
                let total_chunks = total_chunks(&ping.stored_ranges);
                metrics::report_ping(peer_id, version, stored_bytes, total_chunks);
            }
            ObserverEvent::LogsCollected(logs) => {
                for (peer_id, seq_no) in logs.sequence_numbers {
                    metrics::report_logs_collected(peer_id, seq_no + 1);
                }
            }
            ObserverEvent::WorkerQueryLogs {
                peer_id,
                query_logs,
            } => {
                let worker_id = peer_id.to_base58();
                for mut log in query_logs.queries_executed {
                    if !(log.verify_signature(&peer_id) && log.worker_id == worker_id) {
                        warn!("Invalid query log from {peer_id}");
                        continue;
                    }
                    let (result, seq_no) = match (log.result, log.seq_no) {
                        (Some(res), Some(seq_no)) => (res, seq_no),
                        _ => {
                            warn!("Invalid query log from {peer_id}");
                            continue;
                        }
                    };
                    metrics::report_query_executed(
                        worker_id.clone(),
                        result,
                        seq_no,
                        log.client_id,
                    );
                }
            }
        }
    }
}

fn total_chunks(ranges: &[DatasetRanges]) -> u64 {
    ranges
        .iter()
        .map(|dataset_ranges| {
            dataset_ranges
                .ranges
                .iter()
                .map(|r| (r.end - r.begin + 1) as u64)
                .sum::<u64>()
        })
        .sum()
}
