use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

pub const ALIGNED_CHUNK_SIZE: usize = 64 << 20;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "engineering-challenge",
    about = "Collect every open Polymarket CLOB order book into an aligned SBE file"
)]
pub struct Config {
    /// Destination SBE file. It is replaced when the process starts.
    #[arg(long, default_value = "data/orderbooks.sbe")]
    pub output_file: PathBuf,

    /// Maximum outcome tokens assigned to one WebSocket.
    #[arg(long, default_value_t = 250)]
    pub symbols_per_socket: usize,

    /// Independent copies of every WebSocket shard.
    #[arg(long, default_value_t = 2)]
    pub redundancy: usize,

    /// Seconds between complete market catalog reconciliations.
    #[arg(long, default_value_t = 30)]
    pub discovery_interval: u64,

    /// Seconds between locally generated full-book snapshots.
    #[arg(long, default_value_t = 60)]
    pub snapshot_interval: u64,

    /// Reconnect a socket after this many seconds without any frame.
    #[arg(long, default_value_t = 30)]
    pub socket_stale: u64,

    /// Refresh an outcome after this many seconds without a book message.
    #[arg(long, default_value_t = 300)]
    pub symbol_stale: u64,

    /// Seconds in each per-symbol activity-rate measurement window.
    #[arg(long, default_value_t = 60)]
    pub activity_window: u64,

    /// Refresh when observed activity falls below this fraction of expected activity.
    #[arg(long, default_value_t = 0.25)]
    pub activity_ratio: f64,

    /// Do not classify a rate as low until at least this many events are expected.
    #[arg(long, default_value_t = 10)]
    pub activity_min_events: u64,

    /// Consecutive low-rate windows required before refreshing a subscription.
    #[arg(long, default_value_t = 2)]
    pub activity_low_windows: u32,

    /// Rotate every socket after this many seconds.
    #[arg(long, default_value_t = 1800)]
    pub reconnect_interval: u64,

    /// Maximum queued feed events before producers apply backpressure.
    #[arg(long, default_value_t = 100_000)]
    pub queue_size: usize,

    #[arg(long, default_value = "https://gamma-api.polymarket.com")]
    pub gamma_url: String,

    #[arg(
        long,
        default_value = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    )]
    pub websocket_url: String,
}

impl Config {
    pub fn parse_and_validate() -> anyhow::Result<Self> {
        let config = Self::parse();
        anyhow::ensure!(
            config.symbols_per_socket > 0,
            "symbols-per-socket must be positive"
        );
        anyhow::ensure!(config.redundancy > 0, "redundancy must be positive");
        anyhow::ensure!(config.queue_size > 0, "queue-size must be positive");
        anyhow::ensure!(
            config.discovery_interval > 0,
            "discovery-interval must be positive"
        );
        anyhow::ensure!(
            config.snapshot_interval > 0,
            "snapshot-interval must be positive"
        );
        anyhow::ensure!(config.socket_stale > 0, "socket-stale must be positive");
        anyhow::ensure!(config.symbol_stale > 0, "symbol-stale must be positive");
        anyhow::ensure!(
            config.activity_window > 0,
            "activity-window must be positive"
        );
        anyhow::ensure!(
            config.activity_ratio > 0.0 && config.activity_ratio < 1.0,
            "activity-ratio must be strictly between zero and one"
        );
        anyhow::ensure!(
            config.activity_min_events > 0,
            "activity-min-events must be positive"
        );
        anyhow::ensure!(
            config.activity_low_windows > 0,
            "activity-low-windows must be positive"
        );
        anyhow::ensure!(
            config.reconnect_interval > 0,
            "reconnect-interval must be positive"
        );
        Ok(config)
    }

    pub fn discovery_interval(&self) -> Duration {
        Duration::from_secs(self.discovery_interval)
    }

    pub fn snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.snapshot_interval)
    }

    pub fn socket_stale(&self) -> Duration {
        Duration::from_secs(self.socket_stale)
    }

    pub fn symbol_stale(&self) -> Duration {
        Duration::from_secs(self.symbol_stale)
    }

    pub fn activity_window(&self) -> Duration {
        Duration::from_secs(self.activity_window)
    }

    pub fn reconnect_interval(&self) -> Duration {
        Duration::from_secs(self.reconnect_interval)
    }
}
