# Polymarket order-book collector

Rust implementation of the Voluntas engineering challenge using the permitted Polymarket
alternative. The service discovers open, orderable CLOB outcomes, maintains full-depth books, and
writes snapshots and absolute price-level updates in the starter repository's aligned FIX SBE
format. It uses public endpoints and does not place orders.

## Run

Windows PowerShell:

```powershell
cd D:\VoluntasOrderbookCollector\engineering-challenge-template
cargo run --release -- --output-file data\orderbooks.sbe
```

Linux or WSL:

```bash
cd /mnt/d/VoluntasOrderbookCollector/engineering-challenge-template
cargo run --release -- --output-file data/orderbooks.sbe
```

Ctrl-C sends close frames to the WebSockets, stops the workers, and flushes the final 64 MiB SBE
chunk. Starting the collector replaces an existing output file.

Important options:

```text
--symbols-per-socket 250   maximum assets assigned to one WebSocket
--redundancy 2             independent replicas of each shard
--discovery-interval 30    seconds between market-catalog reconciliations
--snapshot-interval 60     seconds between full local snapshots
--socket-stale 30          socket-wide no-frame timeout
--symbol-stale 300         per-symbol no-message timeout
--activity-window 60       per-symbol activity measurement window
--activity-ratio 0.25      low-rate threshold relative to peers/history
--activity-min-events 10   expected events required to enable rate alerts
--activity-low-windows 2   consecutive low-rate windows before recovery
--reconnect-interval 1800  scheduled whole-socket rotation
--queue-size 100000        bounded event queue capacity
```

Inspect a generated file with the supplied SBE reader:

```powershell
cargo run --release --example inspect -- data\orderbooks.sbe
```

## Architecture

- `exchange.rs` defines `ExchangeAdapter`, covering discovery, authenticated WebSocket handshake
  construction, subscriptions, heartbeats, and frame decoding.
- `polymarket.rs`, `markets.rs`, and `protocol.rs` contain Polymarket-specific behavior.
- `worker.rs` manages sharded connections, reconnects, heartbeats, refreshes, and rotation.
- `book.rs` maintains an independent book for every `(replica, symbol)` using exact fixed-point
  decimal conversion.
- `activity.rs` compares rolling per-symbol rates with redundant peers and an EWMA baseline.
- `collector.rs` owns subscription reconciliation, source selection, failover, and SBE output.

Symbols keep stable shard assignments. Each shard has two replicas by default, but only one
initialized source emits for a symbol. A source change always emits a snapshot before updates from
the replacement source. The bounded event queue applies backpressure instead of dropping data.

The receive timestamp is captured immediately after a WebSocket frame arrives, before decoding or
book mutation. A `price_change.size` replaces the absolute aggregate quantity at its level; zero
removes the level.

## Recovery and extensions

- **Extension 1:** redundant WebSockets, independent books, failover, and snapshot-on-source-change.
- **Extension 2:** per-symbol activity rates, peer/history comparison, quiet-symbol refresh,
  socket-stall detection, and scheduled connection rotation.
- **Extension 3:** exchange-independent collector and workers behind `ExchangeAdapter`.
- **Extension 4:** not claimed. Infrastructure-dependent region, source-port, and connection-racing
  experiments were not performed.

When activity remains below the configured threshold for consecutive windows, the collector
invalidates that replica's book, unsubscribe/resubscribes the symbol, and switches sources when a
healthy replica is available. A separate timeout handles symbols without enough history for rate
comparison.

## Output

The generated `SnapshotDomain` and `PriceLevelUpdateDomain` types are written through the supplied
`AlignedSbeWriter`. The schema, DTOs, and writer are unchanged. The reader only has equivalent
Windows and Unix positional-read implementations.

- `Snapshot.received_at` and `PriceLevelUpdate.backtest_timestamp` are local receive times.
- `exchange_timestamp` comes from the exchange message.
- prices use `1_000_000 = 1 USDC`.
- quantities are absolute and use `1_000_000 = 1 share/contract`.
- `Buy` is bid and `Sell` is ask.
- snapshot bids are descending; asks are ascending.
- files use 64 MiB aligned chunks expected by the supplied reader.

The starter helper writes the standard SBE stream to a file. Runtime diagnostics use stderr.

Polymarket does not provide a monotonically increasing sequence number on this public stream, so
losslessness cannot be proven from one connection. Redundant books, snapshots, activity monitoring,
refreshes, and socket rotation reduce this risk.

## Verification

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

Tests cover decimal conversion, absolute-level semantics, protocol decoding, v1/v2 discovery,
adapter wire isolation, activity-rate monitoring, stable sharding, and SBE round trips.
