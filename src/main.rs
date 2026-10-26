use std::fs;

use engineering_challenge::collector::Collector;
use engineering_challenge::config::{ALIGNED_CHUNK_SIZE, Config};
use engineering_challenge::polymarket::PolymarketAdapter;
use engineering_challenge::util::AlignedSbeWriter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse_and_validate()?;

    if let Some(parent) = config.output_file.parent() {
        fs::create_dir_all(parent)?;
    }

    let writer = AlignedSbeWriter::create(&config.output_file, ALIGNED_CHUNK_SIZE)?;
    let adapter = PolymarketAdapter::new(&config.gamma_url, &config.websocket_url)?;
    eprintln!(
        "writing aligned SBE records to {}",
        config.output_file.display()
    );
    Collector::new(config, writer, adapter)?.run().await
}
