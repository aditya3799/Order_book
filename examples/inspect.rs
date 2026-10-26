use std::collections::HashSet;
use std::path::PathBuf;

use clap::Parser;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Parser)]
struct Args {
    /// Aligned SBE file produced by the collector.
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&args.path)?;
    let mut snapshots = 0usize;
    let mut updates = 0usize;
    let mut symbols = HashSet::new();

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                snapshots += 1;
                symbols.insert(snapshot.symbol);
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                updates += 1;
                symbols.insert(update.symbol);
            }
            _ => {}
        }
    }

    println!("file: {}", args.path.display());
    println!("symbols: {}", symbols.len());
    println!("snapshots: {snapshots}");
    println!("price-level updates: {updates}");
    Ok(())
}
