use clap::Parser;
use subsquid_network_transport::{PeerId, TransportArgs};

#[derive(Parser)]
#[command(version)]
pub struct Args {
    /// Port to listen on
    #[clap(short, long, env, default_value_t = 8000)]
    pub port: u16,

    /// Peer ID of the scheduler
    #[clap(long, env)]
    pub scheduler_id: PeerId,

    /// Peer ID of the logs collector
    #[clap(long, env)]
    pub logs_collector_id: PeerId,

    #[command(flatten)]
    pub transport: TransportArgs,

    #[clap(env, hide(true))]
    pub sentry_dsn: Option<String>,
}
