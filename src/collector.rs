use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Context;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};

use crate::activity::ActivityMonitor;
use crate::book::OrderBook;
use crate::config::Config;
use crate::exchange::{Asset, BookMessage, BookSnapshot, ExchangeAdapter, LevelUpdate};
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use crate::worker::{WorkerCommand, WorkerEvent, WorkerHandle};

type WorkerKey = (usize, usize);
type BookKey = (usize, String);

pub struct Collector<A: ExchangeAdapter> {
    config: Config,
    adapter: A,
    writer: AlignedSbeWriter,
    events_tx: mpsc::Sender<WorkerEvent>,
    events_rx: mpsc::Receiver<WorkerEvent>,
    workers: HashMap<WorkerKey, WorkerHandle>,
    connected: HashSet<WorkerKey>,
    assets: HashMap<String, Asset>,
    symbol_shard: HashMap<String, usize>,
    books: HashMap<BookKey, OrderBook>,
    active_source: HashMap<String, usize>,
    last_symbol: HashMap<BookKey, Instant>,
    last_refresh: HashMap<BookKey, Instant>,
    activity: ActivityMonitor,
}

impl<A: ExchangeAdapter> Collector<A> {
    pub fn new(config: Config, writer: AlignedSbeWriter, adapter: A) -> anyhow::Result<Self> {
        let (events_tx, events_rx) = mpsc::channel(config.queue_size);
        let activity = ActivityMonitor::new(
            config.activity_window(),
            config.activity_ratio,
            config.activity_min_events,
            config.activity_low_windows,
        );
        Ok(Self {
            config,
            adapter,
            writer,
            events_tx,
            events_rx,
            workers: HashMap::new(),
            connected: HashSet::new(),
            assets: HashMap::new(),
            symbol_shard: HashMap::new(),
            books: HashMap::new(),
            active_source: HashMap::new(),
            last_symbol: HashMap::new(),
            last_refresh: HashMap::new(),
            activity,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        eprintln!("discovering open {} markets...", self.adapter.name());
        let first_assets = self.adapter.initial_assets().await?;
        anyhow::ensure!(
            !first_assets.is_empty(),
            "initial market discovery had no orderable assets"
        );
        eprintln!(
            "starting live feeds from the first page ({} assets) while full discovery continues",
            first_assets.len()
        );
        self.configure(first_assets).await?;

        let (discovery_tx, mut discovery_rx) = mpsc::channel(1);
        spawn_discovery(self.adapter.clone(), discovery_tx.clone());
        let mut discovery_running = true;
        let mut discovery_tick = tokio::time::interval(self.config.discovery_interval());
        discovery_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        discovery_tick.tick().await;
        let mut snapshot_tick = tokio::time::interval(self.config.snapshot_interval());
        snapshot_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        snapshot_tick.tick().await;
        let mut health_tick = tokio::time::interval(std::time::Duration::from_secs(5));
        health_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        health_tick.tick().await;

        let run_result: anyhow::Result<()> = async {
            loop {
                tokio::select! {
                    signal = tokio::signal::ctrl_c() => {
                        signal.context("install Ctrl-C handler")?;
                        eprintln!("shutdown requested");
                        break Ok(());
                    }
                    event = self.events_rx.recv() => {
                        let Some(event) = event else {
                            break Err(anyhow::anyhow!("all feed workers stopped"));
                        };
                        self.process_event(event).await?;
                    }
                    result = discovery_rx.recv() => {
                        discovery_running = false;
                        match result.context("discovery task stopped without a result")? {
                            Ok(assets) => self.configure(assets).await?,
                            Err(error) => eprintln!("market discovery failed: {error:#}"),
                        }
                    }
                    _ = discovery_tick.tick() => {
                        if !discovery_running {
                            spawn_discovery(self.adapter.clone(), discovery_tx.clone());
                            discovery_running = true;
                        }
                    }
                    _ = snapshot_tick.tick() => self.emit_periodic_snapshots()?,
                    _ = health_tick.tick() => self.monitor_subscriptions().await?,
                }
            }
        }
        .await;

        // Wake workers that may be applying backpressure on a full event queue.
        self.events_rx.close();
        let workers = std::mem::take(&mut self.workers);
        for (_, worker) in workers {
            worker.stop().await;
        }
        self.writer.flush()?;
        run_result
    }

    async fn configure(&mut self, assets: HashMap<String, Asset>) -> anyhow::Result<()> {
        let old_symbols: HashSet<_> = self.assets.keys().cloned().collect();
        let symbols: HashSet<_> = assets.keys().cloned().collect();
        let shards = stable_shards(
            &symbols,
            self.config.symbols_per_socket,
            &mut self.symbol_shard,
        );
        let wanted: HashSet<_> = (0..self.config.redundancy)
            .flat_map(|replica| shards.keys().map(move |&shard| (replica, shard)))
            .collect();

        for replica in 0..self.config.redundancy {
            for (&shard, shard_symbols) in &shards {
                let key = (replica, shard);
                if let Some(worker) = self.workers.get(&key) {
                    worker
                        .command
                        .send(WorkerCommand::Replace(shard_symbols.clone()))
                        .await?;
                } else {
                    self.workers.insert(
                        key,
                        WorkerHandle::spawn(
                            replica,
                            shard,
                            self.adapter.clone(),
                            shard_symbols.clone(),
                            self.events_tx.clone(),
                            self.config.socket_stale(),
                            self.config.reconnect_interval(),
                        ),
                    );
                }
            }
        }

        let obsolete: Vec<_> = self
            .workers
            .keys()
            .filter(|key| !wanted.contains(key))
            .copied()
            .collect();
        for key in obsolete {
            self.connected.remove(&key);
            if let Some(worker) = self.workers.remove(&key) {
                worker.stop().await;
            }
        }

        for symbol in old_symbols.difference(&symbols) {
            self.active_source.remove(symbol);
            for replica in 0..self.config.redundancy {
                let key = (replica, symbol.clone());
                self.books.remove(&key);
                self.last_symbol.remove(&key);
                self.last_refresh.remove(&key);
            }
        }
        let now = Instant::now();
        for symbol in &symbols {
            self.active_source.entry(symbol.clone()).or_insert(0);
            for replica in 0..self.config.redundancy {
                self.last_symbol
                    .entry((replica, symbol.clone()))
                    .or_insert(now);
            }
        }
        self.activity
            .reconcile(&symbols, self.config.redundancy, now);
        self.assets = assets;
        eprintln!(
            "tracking {} live {} assets across {} shard(s) x {} replica(s)",
            symbols.len(),
            self.adapter.name(),
            shards.len(),
            self.config.redundancy
        );
        Ok(())
    }

    async fn process_event(&mut self, event: WorkerEvent) -> anyhow::Result<()> {
        match event {
            WorkerEvent::Connection {
                replica,
                shard,
                connected,
                reason,
            } => {
                let key = (replica, shard);
                if connected {
                    self.connected.insert(key);
                } else {
                    self.connected.remove(&key);
                }
                eprintln!(
                    "replica={replica} shard={shard} {}{}",
                    if connected {
                        "connected"
                    } else {
                        "disconnected"
                    },
                    reason
                        .as_deref()
                        .map(|reason| format!(": {reason}"))
                        .unwrap_or_default()
                );
                if !connected {
                    let affected: Vec<_> = self
                        .assets
                        .keys()
                        .filter(|symbol| {
                            self.symbol_shard.get(*symbol) == Some(&shard)
                                && self.active_source.get(*symbol) == Some(&replica)
                        })
                        .cloned()
                        .collect();
                    for symbol in affected {
                        self.failover(&symbol, replica)?;
                    }
                }
            }
            WorkerEvent::Feed {
                replica,
                shard,
                received_at,
                received_instant,
                message,
            } => {
                let symbol = match &message {
                    BookMessage::Snapshot(snapshot) => &snapshot.asset_id,
                    BookMessage::LevelUpdate(update) => &update.asset_id,
                };
                if !self.assets.contains_key(symbol) {
                    return Ok(());
                }
                self.last_symbol
                    .insert((replica, symbol.clone()), received_instant);
                self.activity.record(replica, symbol, received_instant);
                match message {
                    BookMessage::Snapshot(snapshot) => {
                        self.apply_snapshot(replica, shard, received_at, snapshot)?;
                    }
                    BookMessage::LevelUpdate(update) => {
                        self.apply_update(replica, received_at, update).await?;
                    }
                }
            }
        }
        Ok(())
    }

    fn apply_snapshot(
        &mut self,
        replica: usize,
        shard: usize,
        received_at: chrono::DateTime<Utc>,
        snapshot: BookSnapshot,
    ) -> anyhow::Result<()> {
        let symbol = snapshot.asset_id.clone();
        self.books
            .entry((replica, symbol.clone()))
            .or_insert_with(|| OrderBook::new(received_at))
            .apply_snapshot(&snapshot, received_at)?;
        let current = self.active_source.get(&symbol).copied();
        let current_ready = current.is_some_and(|source| {
            self.connected.contains(&(source, shard))
                && self
                    .books
                    .get(&(source, symbol.clone()))
                    .is_some_and(|book| book.initialized)
        });
        if current == Some(replica) || !current_ready {
            self.active_source.insert(symbol.clone(), replica);
            self.emit_snapshot(&symbol, replica, received_at)?;
        }
        Ok(())
    }

    async fn apply_update(
        &mut self,
        replica: usize,
        received_at: chrono::DateTime<Utc>,
        update: LevelUpdate,
    ) -> anyhow::Result<()> {
        let symbol = update.asset_id.clone();
        let key = (replica, symbol.clone());
        let result = self
            .books
            .entry(key.clone())
            .or_insert_with(|| OrderBook::new(received_at))
            .apply_update(&update, received_at);
        if let Err(error) = result {
            if let Some(book) = self.books.get_mut(&key) {
                book.initialized = false;
            }
            eprintln!("book invalid for {symbol}; refreshing: {error:#}");
            self.request_refresh(replica, &symbol).await;
            if self.active_source.get(&symbol) == Some(&replica) {
                self.failover(&symbol, replica)?;
            }
            return Ok(());
        }
        if self.active_source.get(&symbol) == Some(&replica) {
            self.writer.write_msg(&PriceLevelUpdateDomain {
                backtest_timestamp: received_at,
                exchange_timestamp: update.exchange_timestamp,
                price_level: update.price,
                quantity: update.quantity,
                side: update.side,
                symbol,
            })?;
        }
        Ok(())
    }

    fn emit_snapshot(
        &mut self,
        symbol: &str,
        replica: usize,
        received_at: chrono::DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let book = self
            .books
            .get(&(replica, symbol.to_owned()))
            .context("snapshot source book disappeared")?;
        anyhow::ensure!(book.initialized, "snapshot source is uninitialized");
        self.writer.write_msg(&SnapshotDomain {
            received_at,
            exchange_timestamp: book.last_exchange_timestamp,
            bids: book
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: book
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        })?;
        Ok(())
    }

    fn emit_periodic_snapshots(&mut self) -> anyhow::Result<()> {
        let now = Utc::now();
        let sources: Vec<_> = self
            .active_source
            .iter()
            .filter_map(|(symbol, &replica)| {
                self.books
                    .get(&(replica, symbol.clone()))
                    .filter(|book| book.initialized)
                    .map(|_| (symbol.clone(), replica))
            })
            .collect();
        for (symbol, replica) in sources {
            self.emit_snapshot(&symbol, replica, now)?;
        }
        Ok(())
    }

    async fn monitor_subscriptions(&mut self) -> anyhow::Result<()> {
        let now = Instant::now();
        let stale_after = self.config.symbol_stale();
        let mut refreshes = HashMap::<BookKey, String>::new();
        for alert in self.activity.evaluate(now) {
            let Some(&shard) = self.symbol_shard.get(&alert.symbol) else {
                continue;
            };
            if self.connected.contains(&(alert.replica, shard)) {
                refreshes.insert(
                    (alert.replica, alert.symbol),
                    format!(
                        "activity rate dropped: observed={} expected={}",
                        alert.observed, alert.expected
                    ),
                );
            }
        }
        for symbol in self.assets.keys() {
            for replica in 0..self.config.redundancy {
                let key = (replica, symbol.clone());
                let last = self.last_symbol.get(&key).copied();
                let last_refresh = self.last_refresh.get(&key).copied();
                if last.is_none_or(|last| now.duration_since(last) > stale_after)
                    && last_refresh.is_none_or(|last| now.duration_since(last) > stale_after)
                {
                    refreshes
                        .entry(key)
                        .or_insert_with(|| "no symbol activity before stale deadline".to_owned());
                }
            }
        }
        for ((replica, symbol), reason) in refreshes {
            eprintln!("refreshing replica={replica} symbol={symbol}: {reason}");
            if let Some(book) = self.books.get_mut(&(replica, symbol.clone())) {
                book.initialized = false;
            }
            self.request_refresh(replica, &symbol).await;
            self.last_refresh.insert((replica, symbol.clone()), now);
            if self.active_source.get(&symbol) == Some(&replica) {
                self.failover(&symbol, replica)?;
            }
        }
        Ok(())
    }

    async fn request_refresh(&self, replica: usize, symbol: &str) {
        let Some(&shard) = self.symbol_shard.get(symbol) else {
            return;
        };
        let Some(worker) = self.workers.get(&(replica, shard)) else {
            return;
        };
        let _ = worker
            .command
            .send(WorkerCommand::Refresh(HashSet::from([symbol.to_owned()])))
            .await;
    }

    fn failover(&mut self, symbol: &str, old_replica: usize) -> anyhow::Result<()> {
        let Some(&shard) = self.symbol_shard.get(symbol) else {
            return Ok(());
        };
        for replica in 0..self.config.redundancy {
            if replica == old_replica || !self.connected.contains(&(replica, shard)) {
                continue;
            }
            if self
                .books
                .get(&(replica, symbol.to_owned()))
                .is_some_and(|book| book.initialized)
            {
                self.active_source.insert(symbol.to_owned(), replica);
                self.emit_snapshot(symbol, replica, Utc::now())?;
                break;
            }
        }
        Ok(())
    }
}

fn spawn_discovery<A: ExchangeAdapter>(
    adapter: A,
    results: mpsc::Sender<anyhow::Result<HashMap<String, Asset>>>,
) {
    tokio::spawn(async move {
        let _ = results.send(adapter.all_assets().await).await;
    });
}

fn stable_shards(
    symbols: &HashSet<String>,
    capacity: usize,
    assignments: &mut HashMap<String, usize>,
) -> BTreeMap<usize, HashSet<String>> {
    assignments.retain(|symbol, _| symbols.contains(symbol));
    let mut shards: BTreeMap<usize, HashSet<String>> = BTreeMap::new();
    for (symbol, &shard) in assignments.iter() {
        shards.entry(shard).or_default().insert(symbol.clone());
    }
    let mut unassigned: Vec<_> = symbols
        .iter()
        .filter(|symbol| !assignments.contains_key(*symbol))
        .cloned()
        .collect();
    unassigned.sort_unstable();
    for symbol in unassigned {
        let shard = shards
            .iter()
            .filter(|(_, members)| members.len() < capacity)
            .min_by_key(|(shard, members)| (members.len(), **shard))
            .map(|(&shard, _)| shard)
            .unwrap_or_else(|| {
                (0..=shards.len())
                    .find(|candidate| !shards.contains_key(candidate))
                    .unwrap()
            });
        shards.entry(shard).or_default().insert(symbol.clone());
        assignments.insert(symbol, shard);
    }
    shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_sharding_is_bounded_and_preserves_existing_symbols() {
        let mut assignments = HashMap::new();
        let first = HashSet::from_iter(["A", "B", "C"].map(str::to_owned));
        stable_shards(&first, 2, &mut assignments);
        let original = assignments.clone();
        let second = HashSet::from_iter(["B", "C", "D"].map(str::to_owned));
        let shards = stable_shards(&second, 2, &mut assignments);
        assert_eq!(assignments["B"], original["B"]);
        assert_eq!(assignments["C"], original["C"]);
        assert!(shards.values().all(|members| members.len() <= 2));
        assert_eq!(shards.values().map(HashSet::len).sum::<usize>(), 3);
    }
}
