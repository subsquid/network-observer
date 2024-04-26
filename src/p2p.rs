use anyhow::{anyhow, bail, Result};
use futures::{Stream, StreamExt};
use subsquid_messages::{envelope::Msg, signatures::SignedMessage, DatasetRanges, ProstMsg};
use subsquid_network_transport::{
    cli::TransportArgs,
    transport::{P2PTransportBuilder, P2PTransportHandle},
    PeerId,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::metrics;

type MsgContent = Vec<u8>;
type Message = subsquid_network_transport::Message<MsgContent>;
const PING_TOPIC: &str = "worker_ping";
const LOGS_TOPIC: &str = "worker_query_logs";
const CONCURRENT_MESSAGES: usize = 32;

pub struct Observer<MsgStream> {
    raw_msg_stream: Option<MsgStream>,
    transport_handle: P2PTransportHandle<MsgContent>,
    logs_collector_id: PeerId,
}

pub async fn create_observer(
    args: TransportArgs,
    _scheduler_id: PeerId,
    logs_collector_id: PeerId,
) -> Result<Observer<impl Stream<Item = Message>>> {
    let transport_builder = P2PTransportBuilder::from_cli(args).await?;

    let (msg_receiver, transport_handle) = transport_builder.run().await?;
    transport_handle.subscribe(PING_TOPIC).await?;
    transport_handle.subscribe(LOGS_TOPIC).await?;
    Ok(Observer {
        raw_msg_stream: Some(msg_receiver),
        logs_collector_id,
        transport_handle,
    })
}

impl<MsgStream: Stream<Item = Message>> Observer<MsgStream> {
    pub async fn run(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        let msg_stream = self.raw_msg_stream.take().unwrap();
        let this = &*self;
        msg_stream
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(CONCURRENT_MESSAGES, |msg: Message| async move {
                let envelope = match subsquid_messages::Envelope::decode(msg.content.as_slice()) {
                    Ok(envelope) => envelope,
                    Err(e) => {
                        warn!("Couldn't parse p2p message: {e}");
                        metrics::invalid_message(None);
                        return;
                    }
                };
                let peer_id = match msg.peer_id {
                    Some(peer_id) => peer_id,
                    None => {
                        warn!("Received p2p message without peer_id: '{:?}'", msg);
                        metrics::invalid_message(None);
                        return;
                    }
                };
                if let Err(e) = this.handle_msg(peer_id, envelope.msg).await {
                    warn!("Invalid message: {e:?}");
                    metrics::invalid_message(Some(peer_id));
                }
            })
            .await;
        Ok(())
    }

    async fn handle_msg(&self, peer_id: PeerId, message: Option<Msg>) -> Result<()> {
        match message {
            Some(Msg::Ping(mut ping)) => {
                if !ping.verify_signature(&peer_id) {
                    bail!("wrong ping signature from {}", peer_id);
                }
                if ping.worker_id != Some(peer_id.to_string()) {
                    bail!(
                        "wrong worker_id: {:?}, peer id: {}",
                        ping.worker_id,
                        peer_id
                    );
                }
                let stored_bytes = ping.stored_bytes();
                let version = ping
                    .version
                    .ok_or_else(|| anyhow!("ping without version from {}", peer_id))?;
                let total_chunks = total_chunks(&ping.stored_ranges);
                metrics::report_ping(peer_id, version, stored_bytes, total_chunks);
            }
            Some(Msg::LogsCollected(collected)) => {
                if peer_id != self.logs_collector_id {
                    bail!("wrong LogsCollected message origin: {peer_id}");
                }
                for (peer_id, seq_no) in collected.sequence_numbers {
                    metrics::report_logs_collected(peer_id, seq_no + 1);
                }
            }
            Some(Msg::QueryLogs(logs)) => {
                for mut query in logs.queries_executed {
                    if !query.verify_signature(&peer_id) {
                        bail!("wrong query signature from {}", peer_id);
                    }
                    if query.worker_id != peer_id.to_string() {
                        bail!(
                            "wrong worker_id: {:?}, peer id: {}",
                            query.worker_id,
                            peer_id
                        );
                    }
                    let result = query
                        .result
                        .ok_or_else(|| anyhow!("query without result from {}", peer_id))?;
                    metrics::report_query_executed(
                        peer_id,
                        result,
                        query
                            .seq_no
                            .ok_or_else(|| anyhow!("query without seq_no from {}", peer_id))?,
                        query.client_id,
                    );
                }
            }
            _ => {
                tracing::warn!("Unexpected message: {:?}", message);
            }
        }
        Ok(())
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
