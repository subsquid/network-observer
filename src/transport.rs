use std::{collections::VecDeque, task::Poll, time::Duration};

use anyhow::Result;
use futures::StreamExt;
use libp2p::{
    identity::Keypair,
    metrics::{Metrics as Libp2pMetrics, Recorder},
    swarm::SwarmEvent,
    Multiaddr, PeerId, SwarmBuilder,
};

use sqd_contract_client::Network;
use sqd_network_transport::{
    get_agent_info,
    protocol::{self, dht_protocol},
    util::get_keypair,
    AgentInfo,
};

use crate::cli::Cli;

pub struct Transport {
    swarm: libp2p::Swarm<Behaviour>,
    events: VecDeque<Event>,
    libp2p_metrics: Libp2pMetrics,
}

pub enum Event {
    // Gossipsub(GossipsubMessage),
    PeerSeen(PeerSeen),
    // TODO: reuse base behavior to poll statuses from workers
    #[allow(dead_code)]
    WorkerHeartbeat(WorkerHeartbeat),
    Ping(libp2p::ping::Event),
}

pub struct PeerSeen {
    pub peer_id: PeerId,
    pub address: Multiaddr,
}

pub struct WorkerHeartbeat {
    pub peer_id: Option<PeerId>,
    pub heartbeat: sqd_messages::Heartbeat,
}

impl Transport {
    pub async fn build(args: Cli, libp2p_metrics: Libp2pMetrics) -> Result<Self> {
        let keypair = get_keypair(Some(args.key.clone())).await?;

        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic()
            .with_dns()?
            .with_behaviour(|key| Behaviour::new(key, args.network))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
            .build();

        for addr in args.p2p_listen_addrs {
            swarm.listen_on(addr)?;
        }
        for public_addr in args.p2p_public_addrs {
            log::info!("Adding public address {public_addr}");
            swarm.add_external_address(public_addr);
        }

        for node in args.boot_nodes {
            log::info!("Adding bootnode {node:?}");
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&node.peer_id, node.address);
            swarm.dial(node.peer_id)?;
        }

        Ok(Self {
            swarm,
            events: Default::default(),
            libp2p_metrics,
        })
    }

    pub fn poll_event(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Event> {
        while self.events.is_empty() {
            match futures::ready!(self.swarm.poll_next_unpin(cx)).unwrap() {
                SwarmEvent::NewListenAddr { address, .. } => log::info!("Listening on {address:?}"),
                SwarmEvent::Behaviour(event) => match event {
                    BehaviourEvent::Ping(e) => self.on_ping(e),
                    BehaviourEvent::Identify(e) => self.on_identify(e),
                    BehaviourEvent::Kademlia(e) => self.on_kademlia(e),
                },
                _ => {}
            };
        }
        Poll::Ready(self.events.pop_front().unwrap())
    }

    fn on_ping(&mut self, event: libp2p::ping::Event) {
        log::trace!("Ping event: {event:?}");
        self.libp2p_metrics.record(&event);
        self.events.push_back(Event::Ping(event));
    }

    #[allow(clippy::needless_pass_by_value)]
    fn on_identify(&mut self, event: libp2p::identify::Event) {
        log::debug!("Identify event: {event:?}");
        self.libp2p_metrics.record(&event);
    }

    fn on_kademlia(&mut self, event: libp2p::kad::Event) {
        log::debug!("Kademlia event: {event:?}");
        self.libp2p_metrics.record(&event);
        match event {
            libp2p::kad::Event::RoutingUpdated {
                peer, addresses, ..
            } => {
                for address in addresses.into_vec() {
                    self.events.push_back(Event::PeerSeen(PeerSeen {
                        peer_id: peer,
                        address,
                    }))
                }
            }
            libp2p::kad::Event::RoutablePeer { peer, address } => {
                self.events.push_back(Event::PeerSeen(PeerSeen {
                    peer_id: peer,
                    address,
                }));
            }
            _ => {}
        }
    }
}

impl futures::Stream for Transport {
    type Item = Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_event(cx).map(Some)
    }
}

impl futures::stream::FusedStream for Transport {
    fn is_terminated(&self) -> bool {
        false
    }
}

#[derive(libp2p_swarm_derive::NetworkBehaviour)]
struct Behaviour {
    ping: libp2p::ping::Behaviour,
    identify: libp2p::identify::Behaviour,
    kademlia: libp2p::kad::Behaviour<libp2p::kad::store::MemoryStore>,
}

impl Behaviour {
    fn new(key: &Keypair, network: Network) -> Self {
        let local_peer_id = PeerId::from(key.public());

        let agent_info: AgentInfo = get_agent_info!();
        let identify_config =
            libp2p::identify::Config::new(protocol::ID_PROTOCOL.to_owned(), key.public())
                .with_agent_version(agent_info.to_string());

        Self {
            ping: Default::default(),
            identify: libp2p::identify::Behaviour::new(identify_config),
            kademlia: libp2p::kad::Behaviour::with_config(
                local_peer_id,
                libp2p::kad::store::MemoryStore::new(local_peer_id),
                libp2p::kad::Config::new(dht_protocol(network)),
            ),
        }
    }
}
