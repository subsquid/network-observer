# SQD Network observer

Observability service for [SQD Network](https://sqd.dev/network). It connects to the SQD Network peer-to-peer network as a libp2p node, observes peers and workers, collects metrics, and exposes them in Prometheus format.

This is the `observer` binary. It was moved here from the `crates/observer` directory of the [subsquid/sqd-network](https://github.com/subsquid/sqd-network) repository.

## What it does

The observer joins the network as a libp2p node (QUIC transport) and runs the following behaviours:

- **Kademlia DHT**: discovers peers in the network. Each discovered peer address is recorded with a `last_seen` timestamp.
- **Ping**: pings peers and records the ping count and the duration of the last ping.
- **Identify**: exchanges peer identity and agent information.

It also handles worker heartbeat messages to record per-worker storage usage, the number of missing chunks, and the assignment timestamp from the latest heartbeat.

Collected metrics are served over HTTP at `/metrics` in OpenMetrics / Prometheus text format, alongside the standard libp2p metrics.

### Metrics

Custom metrics registered by the observer, in addition to libp2p's built-in metrics (final names follow OpenMetrics unit suffixing):

| Metric | Type | Description |
|---|---|---|
| `last_seen` | gauge | Timestamp of the last message from the given peer |
| `worker_missing_chunks` | gauge | Number of chunks missing from the worker |
| `worker_storage_bytes` | gauge | Storage used by the worker, in bytes |
| `worker_assignment_timestamp_seconds` | gauge | Timestamp of the assignment referenced by the worker's last heartbeat |
| `pings_total` | counter | Number of pings sent to the worker |
| `last_ping_seconds` | gauge | Duration of the last ping, in seconds |

Metrics carry `peer_id` and `network` labels (and an `addr` label for `last_seen`).

## Build

Requires a Rust toolchain and the Protocol Buffers compiler (`protoc`).

```bash
cargo build --release
```

The binary is produced at `target/release/observer`.

A `Dockerfile` is included. It uses `cargo-chef` for layer caching and installs `protobuf-compiler` during the build.

## Usage

```bash
observer [OPTIONS] --key <KEY>
```

Options (most are also settable via environment variables):

| Flag | Env | Default | Description |
|---|---|---|---|
| `-p, --port <PORT>` | | `8000` | HTTP port for the metrics endpoint |
| `-k, --key <KEY>` | `KEY_PATH` | | Path to the libp2p key file |
| `--p2p-listen-addrs <ADDRS>` | `P2P_LISTEN_ADDRS` | | Addresses the p2p node listens on (comma-separated multiaddrs) |
| `--p2p-public-addrs <ADDRS>` | `P2P_PUBLIC_ADDRS` | | Public addresses the p2p node can be reached at |
| `--boot-nodes <NODES>` | `BOOT_NODES` | | Boot nodes to connect to, each as `<peer_id> <address>` (comma-separated) |
| `--network <NETWORK>` | `NETWORK` | `mainnet` | Network to connect to: `mainnet` or `tethys` (testnet) |

Once running, scrape metrics from `http://<host>:<port>/metrics`.

The repository includes an `.env` file with example values (testnet scheduler, logs collector, listen address, and boot nodes) that can be sourced for local runs.

## Documentation

- SQD Network: https://docs.sqd.dev/en/network
- SQD docs: https://docs.sqd.dev
