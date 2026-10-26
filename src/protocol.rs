use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::Side;
use crate::book::decimal_to_fixed;
use crate::exchange::{BookMessage, BookSnapshot, LevelUpdate};

pub fn decode_frame(raw: &str) -> anyhow::Result<Vec<BookMessage>> {
    if raw == "PONG" {
        return Ok(Vec::new());
    }
    let payload: Value = serde_json::from_str(raw).context("invalid WebSocket JSON")?;
    let messages = match payload {
        Value::Array(messages) => messages,
        message => vec![message],
    };
    let mut decoded = Vec::new();
    for message in messages {
        let Some(object) = message.as_object() else {
            continue;
        };
        match object.get("event_type").and_then(Value::as_str) {
            Some("price_change") => decoded.extend(decode_updates(&message)?),
            Some("book") | None if object.contains_key("asset_id") => {
                decoded.push(BookMessage::Snapshot(decode_snapshot(&message)?));
            }
            _ => {}
        }
    }
    Ok(decoded)
}

fn decode_snapshot(value: &Value) -> anyhow::Result<BookSnapshot> {
    Ok(BookSnapshot {
        asset_id: required_string(value, "asset_id")?,
        market_id: optional_string(value, "market").unwrap_or_default(),
        bids: decode_levels(value.get("bids"), "bids")?,
        asks: decode_levels(value.get("asks"), "asks")?,
        hash: optional_string(value, "hash"),
        exchange_timestamp: decode_timestamp(value.get("timestamp"))?,
    })
}

fn decode_updates(value: &Value) -> anyhow::Result<Vec<BookMessage>> {
    let market_id = optional_string(value, "market").unwrap_or_default();
    let exchange_timestamp = decode_timestamp(value.get("timestamp"))?;
    let changes = value
        .get("price_changes")
        .and_then(Value::as_array)
        .context("price_changes must be an array")?;
    changes
        .iter()
        .map(|change| {
            let side = match required_string(change, "side")?.as_str() {
                "BUY" => Side::Buy,
                "SELL" => Side::Sell,
                side => anyhow::bail!("invalid order-book side {side}"),
            };
            Ok(BookMessage::LevelUpdate(LevelUpdate {
                asset_id: required_string(change, "asset_id")?,
                market_id: market_id.clone(),
                side,
                price: decimal_field(change, "price")?,
                quantity: decimal_field(change, "size")?,
                hash: optional_string(change, "hash"),
                exchange_timestamp,
            }))
        })
        .collect()
}

fn decode_levels(value: Option<&Value>, name: &str) -> anyhow::Result<Vec<(i64, i64)>> {
    value
        .and_then(Value::as_array)
        .with_context(|| format!("{name} must be an array"))?
        .iter()
        .map(|level| {
            Ok((
                decimal_field(level, "price")?,
                decimal_field(level, "size")?,
            ))
        })
        .collect()
}

fn decimal_field(value: &Value, name: &str) -> anyhow::Result<i64> {
    let raw = value.get(name).with_context(|| format!("missing {name}"))?;
    let text = match raw {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => anyhow::bail!("{name} must be a decimal"),
    };
    decimal_to_fixed(&text).with_context(|| format!("invalid {name}"))
}

fn required_string(value: &Value, name: &str) -> anyhow::Result<String> {
    optional_string(value, name).with_context(|| format!("missing {name}"))
}

fn optional_string(value: &Value, name: &str) -> Option<String> {
    value.get(name).and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn decode_timestamp(value: Option<&Value>) -> anyhow::Result<DateTime<Utc>> {
    let milliseconds: i64 = match value.context("missing exchange timestamp")? {
        Value::String(value) => value.parse().context("invalid exchange timestamp")?,
        Value::Number(value) => value.as_i64().context("invalid exchange timestamp")?,
        _ => anyhow::bail!("invalid exchange timestamp"),
    };
    DateTime::from_timestamp_millis(milliseconds).context("exchange timestamp out of range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_snapshot_and_all_level_updates() {
        let snapshot = decode_frame(
            r#"[{"event_type":"book","market":"0xabc","asset_id":"123","timestamp":"1000","hash":"h","bids":[{"price":"0.1","size":"2"}],"asks":[]}]"#,
        )
        .unwrap();
        assert!(
            matches!(&snapshot[0], BookMessage::Snapshot(book) if book.bids == vec![(100_000, 2_000_000)])
        );

        let updates = decode_frame(
            r#"{"event_type":"price_change","market":"0xabc","timestamp":"1100","price_changes":[{"asset_id":"123","price":"0.2","size":"1.25","side":"BUY","hash":"h1"},{"asset_id":"456","price":"0.8","size":"0","side":"SELL","hash":"h2"}]}"#,
        )
        .unwrap();
        assert_eq!(updates.len(), 2);
        assert!(
            matches!(&updates[1], BookMessage::LevelUpdate(update) if update.side == Side::Sell && update.quantity == 0)
        );
    }
}
