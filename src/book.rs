use std::collections::BTreeMap;

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::Side;
use crate::exchange::{BookSnapshot, LevelUpdate};

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub hash: Option<String>,
    pub initialized: bool,
    pub last_received_at: DateTime<Utc>,
    pub last_exchange_timestamp: DateTime<Utc>,
}

impl OrderBook {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            hash: None,
            initialized: false,
            last_received_at: now,
            last_exchange_timestamp: now,
        }
    }

    pub fn apply_snapshot(
        &mut self,
        snapshot: &BookSnapshot,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.bids = validated_levels(&snapshot.bids)?;
        self.asks = validated_levels(&snapshot.asks)?;
        self.hash.clone_from(&snapshot.hash);
        self.initialized = true;
        self.last_received_at = received_at;
        self.last_exchange_timestamp = snapshot.exchange_timestamp;
        Ok(())
    }

    pub fn apply_update(
        &mut self,
        update: &LevelUpdate,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<i64> {
        anyhow::ensure!(self.initialized, "level update received before snapshot");
        validate_level(update.price, update.quantity)?;
        let levels = match update.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let previous = levels.get(&update.price).copied().unwrap_or_default();
        if update.quantity == 0 {
            levels.remove(&update.price);
        } else {
            levels.insert(update.price, update.quantity);
        }
        self.hash.clone_from(&update.hash);
        self.last_received_at = received_at;
        self.last_exchange_timestamp = update.exchange_timestamp;
        Ok(update.quantity - previous)
    }
}

fn validated_levels(levels: &[(i64, i64)]) -> anyhow::Result<BTreeMap<i64, i64>> {
    let mut result = BTreeMap::new();
    for &(price, quantity) in levels {
        validate_level(price, quantity)?;
        if quantity != 0 {
            result.insert(price, quantity);
        }
    }
    Ok(result)
}

fn validate_level(price: i64, quantity: i64) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..1_000_000).contains(&price),
        "price must be strictly between zero and one USDC"
    );
    anyhow::ensure!(quantity >= 0, "quantity must not be negative");
    Ok(())
}

pub fn decimal_to_fixed(value: &str) -> anyhow::Result<i64> {
    let value = value.trim();
    anyhow::ensure!(!value.is_empty(), "empty decimal");
    anyhow::ensure!(!value.starts_with('-'), "negative decimal");
    anyhow::ensure!(
        !value.contains(['e', 'E']),
        "scientific notation is unsupported"
    );
    let mut parts = value.split('.');
    let whole = parts.next().context("missing whole part")?;
    let fraction = parts.next().unwrap_or("");
    anyhow::ensure!(parts.next().is_none(), "decimal has multiple points");
    anyhow::ensure!(
        !whole.is_empty() && whole.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid decimal whole part"
    );
    anyhow::ensure!(
        fraction.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid decimal fractional part"
    );
    anyhow::ensure!(
        fraction.len() <= 6 || fraction[6..].chars().all(|character| character == '0'),
        "decimal has precision beyond six places"
    );
    let whole: i64 = whole.parse().context("invalid decimal whole part")?;
    let kept = &fraction[..fraction.len().min(6)];
    let fraction_value: i64 = if kept.is_empty() {
        0
    } else {
        kept.parse().context("invalid decimal fractional part")?
    };
    let scale = 10_i64.pow((6 - kept.len()) as u32);
    whole
        .checked_mul(1_000_000)
        .and_then(|scaled| scaled.checked_add(fraction_value * scale))
        .context("fixed-point value overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_decimal_strings_exactly() {
        assert_eq!(decimal_to_fixed("0.42").unwrap(), 420_000);
        assert_eq!(decimal_to_fixed("12").unwrap(), 12_000_000);
        assert_eq!(decimal_to_fixed("1.0000000").unwrap(), 1_000_000);
        assert!(decimal_to_fixed("0.1234567").is_err());
        assert!(decimal_to_fixed("-1").is_err());
        assert!(decimal_to_fixed("0.\u{0661}").is_err());
    }

    #[test]
    fn applies_absolute_levels_and_removes_zero_quantity() {
        let now = DateTime::from_timestamp_millis(1_000).unwrap();
        let mut book = OrderBook::new(now);
        book.apply_snapshot(
            &BookSnapshot {
                asset_id: "asset".to_owned(),
                market_id: "market".to_owned(),
                bids: vec![(400_000, 2_000_000)],
                asks: vec![(600_000, 3_000_000)],
                hash: Some("snapshot".to_owned()),
                exchange_timestamp: now,
            },
            now,
        )
        .unwrap();

        let changed = LevelUpdate {
            asset_id: "asset".to_owned(),
            market_id: "market".to_owned(),
            side: Side::Buy,
            price: 400_000,
            quantity: 1_250_000,
            hash: Some("update".to_owned()),
            exchange_timestamp: now,
        };
        assert_eq!(book.apply_update(&changed, now).unwrap(), -750_000);
        assert_eq!(book.bids[&400_000], 1_250_000);

        let removed = LevelUpdate {
            quantity: 0,
            ..changed
        };
        assert_eq!(book.apply_update(&removed, now).unwrap(), -1_250_000);
        assert!(!book.bids.contains_key(&400_000));
    }
}
