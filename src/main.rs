use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;

mod cli;
mod http_server;
mod metrics;
mod p2p;
mod scheduler;

fn setup_tracing() -> Result<()> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
    let fmt = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry().with(fmt).try_init()?;
    Ok(())
}

fn create_cancellation_token() -> Result<CancellationToken> {
    use tokio::signal::unix::{signal, SignalKind};

    let token = CancellationToken::new();
    let copy = token.clone();
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select!(
            _ = sigint.recv() => {
                copy.cancel();
            },
            _ = sigterm.recv() => {
                copy.cancel();
            },
        );
    });
    Ok(token)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = cli::Args::parse();
    setup_tracing()?;
    let cancellation_token = create_cancellation_token()?;

    let mut metrics_registry = Default::default();
    metrics::register_metrics(&mut metrics_registry);

    let mut observer =
        p2p::create_observer(args.transport, args.scheduler_id, args.logs_collector_id).await?;
    let cancellation_token_child = cancellation_token.child_token();
    let observer_task = tokio::spawn(async move { observer.run(cancellation_token_child).await });

    let http_server = http_server::Server::new(metrics_registry);
    let server_task = tokio::spawn(http_server.run(args.port, cancellation_token.child_token()));

    let (observer_result, server_result) = tokio::join!(observer_task, server_task);
    observer_result??;
    server_result??;

    Ok(())
}
