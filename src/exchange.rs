use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::Request;

use crate::Side;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asset {
    pub asset_id: String,
    pub market_id: String,
    pub condition_id: String,
    pub outcome: String,
    pub protocol_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    pub asset_id: String,
    pub market_id: String,
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
    pub hash: Option<String>,
    pub exchange_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelUpdate {
    pub asset_id: String,
    pub market_id: String,
    pub side: Side,
    pub price: i64,
    pub quantity: i64,
    pub hash: Option<String>,
    pub exchange_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookMessage {
    Snapshot(BookSnapshot),
    LevelUpdate(LevelUpdate),
}

/// Everything the exchange-independent collector needs from a WebSocket venue.
/// A second exchange can be added by implementing this trait; sharding, books,
/// redundancy, monitoring, failover, and SBE output remain unchanged.
#[async_trait]
pub trait ExchangeAdapter: Clone + Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn websocket_request(&self) -> anyhow::Result<Request<()>>;

    async fn initial_assets(&self) -> anyhow::Result<HashMap<String, Asset>>;
    async fn all_assets(&self) -> anyhow::Result<HashMap<String, Asset>>;

    fn initial_subscription(&self, symbols: &HashSet<String>) -> Message;
    fn subscribe(&self, symbols: &HashSet<String>) -> Message;
    fn unsubscribe(&self, symbols: &HashSet<String>) -> Message;
    fn heartbeat(&self) -> Message;
    fn decode_text(&self, raw: &str) -> anyhow::Result<Vec<BookMessage>>;
}
