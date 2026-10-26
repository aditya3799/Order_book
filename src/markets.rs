use std::collections::HashMap;

use anyhow::Context;
use serde_json::Value;

use crate::exchange::Asset;

#[derive(Debug)]
pub struct DiscoveryPage {
    pub assets: HashMap<String, Asset>,
    pub next_cursor: Option<String>,
    pub market_count: usize,
}

#[derive(Clone)]
pub struct MarketDiscovery {
    client: reqwest::Client,
    base_url: String,
}

impl MarketDiscovery {
    pub fn new(base_url: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            base_url: base_url.into().trim_end_matches('/').to_owned(),
        })
    }

    pub async fn first_page(&self) -> anyhow::Result<DiscoveryPage> {
        self.fetch_page(None).await
    }

    pub async fn open_assets(&self) -> anyhow::Result<HashMap<String, Asset>> {
        let mut cursor = None;
        let mut assets = HashMap::new();
        let mut pages = 0usize;
        let mut markets = 0usize;
        loop {
            let page = self.fetch_page(cursor.as_deref()).await?;
            pages += 1;
            markets += page.market_count;
            assets.extend(page.assets);
            if pages == 1 || pages.is_multiple_of(10) {
                eprintln!(
                    "market discovery: pages={pages} markets={markets} orderable_assets={}",
                    assets.len()
                );
            }
            match page.next_cursor {
                Some(next) if cursor.as_deref() != Some(next.as_str()) => cursor = Some(next),
                _ => break,
            }
        }
        eprintln!(
            "market discovery complete: pages={pages} markets={markets} orderable_assets={}",
            assets.len()
        );
        Ok(assets)
    }

    async fn fetch_page(&self, cursor: Option<&str>) -> anyhow::Result<DiscoveryPage> {
        let url = format!("{}/markets/keyset", self.base_url);
        let mut query = vec![
            ("closed", "false"),
            ("limit", "100"),
            ("version", "v1"),
            ("version", "v2"),
        ];
        if let Some(cursor) = cursor {
            query.push(("after_cursor", cursor));
        }
        let payload: Value = self
            .client
            .get(url)
            .query(&query)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let markets = payload
            .get("markets")
            .and_then(Value::as_array)
            .context("Gamma response is missing markets array")?;
        let mut assets = HashMap::new();
        for market in markets {
            for asset in assets_from_market(market)? {
                assets.insert(asset.asset_id.clone(), asset);
            }
        }
        Ok(DiscoveryPage {
            assets,
            next_cursor: payload
                .get("next_cursor")
                .and_then(Value::as_str)
                .filter(|cursor| !cursor.is_empty())
                .map(str::to_owned),
            market_count: markets.len(),
        })
    }
}

pub fn assets_from_market(market: &Value) -> anyhow::Result<Vec<Asset>> {
    if boolean(market, "closed") == Some(true)
        || boolean(market, "active") == Some(false)
        || boolean(market, "enableOrderBook") == Some(false)
        || boolean(market, "acceptingOrders") == Some(false)
    {
        return Ok(Vec::new());
    }
    let version = market
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("v1")
        .to_ascii_lowercase();
    let id_field = if version == "v2" {
        "positionIds"
    } else {
        "clobTokenIds"
    };
    let asset_ids = string_list(market.get(id_field))?;
    let outcomes = outcome_names(market.get("outcomes"))?;
    let market_id = scalar_string(market.get("id")).unwrap_or_default();
    let condition_id = scalar_string(
        market
            .get("conditionId")
            .or_else(|| market.get("condition_id")),
    )
    .unwrap_or_default();
    Ok(asset_ids
        .into_iter()
        .enumerate()
        .filter(|(_, asset_id)| !asset_id.is_empty())
        .map(|(index, asset_id)| Asset {
            asset_id,
            market_id: market_id.clone(),
            condition_id: condition_id.clone(),
            outcome: outcomes
                .get(index)
                .cloned()
                .unwrap_or_else(|| index.to_string()),
            protocol_version: version.clone(),
        })
        .collect())
}

fn boolean(value: &Value, name: &str) -> Option<bool> {
    value.get(name).and_then(Value::as_bool)
}

fn scalar_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn string_list(value: Option<&Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let parsed;
    let value = if let Value::String(encoded) = value {
        parsed = serde_json::from_str(encoded).context("invalid JSON-encoded list")?;
        &parsed
    } else {
        value
    };
    let Some(items) = value.as_array() else {
        return Ok(Vec::new());
    };
    Ok(items
        .iter()
        .filter_map(|item| scalar_string(Some(item)))
        .collect())
}

fn outcome_names(value: Option<&Value>) -> anyhow::Result<Vec<String>> {
    if let Some(Value::Object(outcomes)) = value {
        let mut names = Vec::new();
        for preferred in ["yes", "no"] {
            if outcomes.contains_key(preferred) {
                names.push(capitalize(preferred));
            }
        }
        for name in outcomes.keys() {
            if name != "yes" && name != "no" {
                names.push(capitalize(name));
            }
        }
        return Ok(names);
    }
    string_list(value)
}

fn capitalize(value: &str) -> String {
    let mut characters = value.chars();
    characters
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + characters.as_str())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_ctf_and_v2_assets() {
        let ctf = serde_json::json!({
            "id": "9",
            "version": "v1",
            "conditionId": "0xabc",
            "outcomes": "[\"Yes\",\"No\"]",
            "clobTokenIds": "[\"123\",\"456\"]",
            "closed": false,
            "active": true,
            "acceptingOrders": true,
            "enableOrderBook": true
        });
        let assets = assets_from_market(&ctf).unwrap();
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].asset_id, "123");
        assert_eq!(assets[1].outcome, "No");

        let disabled =
            serde_json::json!({"version":"v2","positionIds":["1"],"acceptingOrders":false});
        assert!(assets_from_market(&disabled).unwrap().is_empty());
    }
}
