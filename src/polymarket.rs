use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Request;

use crate::exchange::{Asset, BookMessage, ExchangeAdapter};
use crate::markets::MarketDiscovery;
use crate::protocol::decode_frame;

#[derive(Clone)]
pub struct PolymarketAdapter {
    discovery: MarketDiscovery,
    websocket_url: String,
}

impl PolymarketAdapter {
    pub fn new(
        gamma_url: impl Into<String>,
        websocket_url: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            discovery: MarketDiscovery::new(gamma_url)?,
            websocket_url: websocket_url.into(),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for PolymarketAdapter {
    fn name(&self) -> &'static str {
        "Polymarket"
    }

    fn websocket_request(&self) -> anyhow::Result<Request<()>> {
        Ok(self.websocket_url.as_str().into_client_request()?)
    }

    async fn initial_assets(&self) -> anyhow::Result<HashMap<String, Asset>> {
        Ok(self.discovery.first_page().await?.assets)
    }

    async fn all_assets(&self) -> anyhow::Result<HashMap<String, Asset>> {
        self.discovery.open_assets().await
    }

    fn initial_subscription(&self, symbols: &HashSet<String>) -> Message {
        text_message(json!({
            "assets_ids": sorted(symbols),
            "type": "market",
            "custom_feature_enabled": true,
        }))
    }

    fn subscribe(&self, symbols: &HashSet<String>) -> Message {
        operation_message("subscribe", symbols)
    }

    fn unsubscribe(&self, symbols: &HashSet<String>) -> Message {
        operation_message("unsubscribe", symbols)
    }

    fn heartbeat(&self) -> Message {
        Message::Text("PING".into())
    }

    fn decode_text(&self, raw: &str) -> anyhow::Result<Vec<BookMessage>> {
        decode_frame(raw)
    }
}

fn operation_message(operation: &str, symbols: &HashSet<String>) -> Message {
    text_message(json!({"assets_ids": sorted(symbols), "operation": operation}))
}

fn text_message(value: serde_json::Value) -> Message {
    Message::Text(value.to_string().into())
}

fn sorted(symbols: &HashSet<String>) -> Vec<&str> {
    let mut symbols: Vec<_> = symbols.iter().map(String::as_str).collect();
    symbols.sort_unstable();
    symbols
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_owns_exchange_specific_wire_messages() {
        let adapter = PolymarketAdapter::new("http://example.test", "ws://example.test").unwrap();
        let symbols = HashSet::from(["b".to_owned(), "a".to_owned()]);

        assert_eq!(
            adapter.websocket_request().unwrap().uri(),
            "ws://example.test/"
        );

        let Message::Text(initial) = adapter.initial_subscription(&symbols) else {
            panic!("expected text subscription");
        };
        let initial: serde_json::Value = serde_json::from_str(&initial).unwrap();
        assert_eq!(initial["assets_ids"], json!(["a", "b"]));
        assert_eq!(initial["type"], "market");

        let Message::Text(change) = adapter.unsubscribe(&symbols) else {
            panic!("expected text unsubscription");
        };
        let change: serde_json::Value = serde_json::from_str(&change).unwrap();
        assert_eq!(change["operation"], "unsubscribe");
    }
}
