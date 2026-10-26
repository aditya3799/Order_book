# Polymarket order-book collector

This is a Rust implementation of the Voluntas engineering challenge. It discovers every open,
orderable Polymarket CLOB outcome, maintains local full-depth books from the public market
WebSocket, and writes snapshots plus absolute price-level updates using the starter repository's
aligned FIX SBE format.

No API key, wallet, or account is required. The process is read-only and never places orders.

## Run

The collector and the supplied aligned SBE reader work on both Windows and Linux. From PowerShell:

```powershell
cd D:\VoluntasOrderbookCollector\engineering-challenge-template
cargo run --release -- --output-file data\orderbooks.sbe
```

On Linux or WSL:

```bash
cd /mnt/d/VoluntasOrderbookCollector/engineering-challenge-template
cargo run --release -- --output-file data/orderbooks.sbe
```

Stop with Ctrl-C. The collector stops its workers and flushes the final zero-padded 64 MiB SBE
chunk before exiting. A newly opened output file is truncated, matching `AlignedSbeWriter`'s
contract.

Useful controls:

```text
--symbols-per-socket 250   maximum outcome assets on each WebSocket
--redundancy 2             independent replicas of every WebSocket shard
--discovery-interval 30    seconds between full market-catalog reconciliations
--snapshot-interval 60     seconds between locally generated full snapshots
--socket-stale 30          reconnect a socket after this many seconds without a frame
--symbol-stale 300         refresh an outcome after this many seconds without a book message
--activity-window 60       seconds in each per-symbol rate measurement
--activity-ratio 0.25      low-rate threshold relative to peers/history
--activity-min-events 10   minimum expected events before rate alerts are enabled
--activity-low-windows 2   consecutive low-rate windows required before recovery
--reconnect-interval 1800  rotate each connection after this many seconds
--queue-size 100000        bounded central event queue capacity
```

To confirm that a produced file decodes with the supplied reader:

```bash
cargo run --release --example inspect -- data/orderbooks.sbe
```

In PowerShell, use `data\orderbooks.sbe` for the path instead.

The inspector reports the distinct symbol, snapshot, and price-level-update counts. This is the
submission's lightweight demonstration; a graphical UI is intentionally omitted because it would
not improve collector correctness or throughput.

## Design

`markets.rs` cursor-paginates Gamma's `/markets/keyset` catalog for both CTF (`v1`) and Protocol V2
markets. It filters inactive, closed, disabled-book, and non-orderable markets. Each outcome token
ID is a schema `symbol` (`clobTokenIds` for v1, `positionIds` for v2). Discovery starts one page of
feeds immediately, then completes the catalog in the background and reconciles it periodically.

`exchange.rs` defines the exchange adapter contract: discovery, WebSocket handshake construction
(including exchange-specific authentication headers), subscription encoding, heartbeat encoding,
and frame decoding. `polymarket.rs` is the only implementation that knows Polymarket wire
messages. The collector, workers, order books, monitoring, failover, and SBE output are generic
over that adapter, so adding a WebSocket exchange does not change common logic.

`worker.rs` owns reconnecting WebSocket shards. Symbols retain stable shard assignments, each shard
has two replicas by default, and a bounded Tokio channel applies backpressure. The local receive
timestamp is captured immediately after `recv`, before decoding or book mutation. Workers use the
adapter for all wire messages, detect socket stalls, support per-symbol refresh, rotate
connections, reconnect with bounded exponential backoff, and send an explicit close frame during
shutdown.

`book.rs` stores independent books for every `(replica, symbol)`. Decimal strings are converted
exactly into the schema's 1,000,000 fixed-point scale without floating-point arithmetic. A
`price_change.size` replaces the aggregate quantity at that price; zero removes the level.

`collector.rs` is the single owner of `AlignedSbeWriter`. Only one initialized replica emits for a
symbol. A source failure promotes a healthy replica and writes a full snapshot before subsequent
updates, so consumers never have to combine two replica streams. Exchange snapshots are written
when received and complete active books are also written every minute by default.

`activity.rs` counts data points for every `(replica, symbol)` in rolling one-minute windows. It
compares each rate with both an EWMA historical baseline and redundant peers. A configurable
minimum avoids false alerts on naturally quiet markets; a sustained configurable decline
invalidates the affected book, unsubscribe/resubscribes the symbol, and fails over the active
source. A separate absolute-silence deadline catches subscriptions without enough history, while
periodic whole-socket rotation mitigates cumulative exchange or client degradation.

## Implemented extensions

- **Extension 1 — redundant WebSockets:** complete. Every shard has independent books per replica,
  automatic source failover, and a mandatory snapshot before the replacement source emits.
- **Extension 2 — subscription monitoring:** complete. Per-symbol activity windows, peer and
  historical comparisons, sustained-low-rate detection, quiet-symbol refresh, socket-stall
  detection, and scheduled whole-socket reconnects are implemented.
- **Extension 3 — generic exchange boundary:** complete. `ExchangeAdapter` isolates discovery and
  every exchange-specific WebSocket encode/decode operation from the reusable collection engine.
- **Extension 4 — latency experimentation:** intentionally not claimed. Receive timestamps are
  captured at the correct boundary, but region/source-port/connection-racing experiments require
  controlled infrastructure and responsible exchange-rate-limit coordination.

## Output contract

The repository's generated `SnapshotDomain` and `PriceLevelUpdateDomain` types are written directly
through `AlignedSbeWriter`. The schema, generated DTOs, and writer were not changed. The supplied
reader received only a platform adapter for equivalent positional reads on Windows and Unix.

- `Snapshot.received_at` and `PriceLevelUpdate.backtest_timestamp` are local receive times.
- `exchange_timestamp` comes from the Polymarket message.
- prices use `1_000_000 = 1 USDC`.
- quantities use `1_000_000 = 1 share/contract` and are absolute, including zero deletions.
- `Buy` means bid and `Sell` means ask.
- bid snapshots are highest-price first; asks are lowest-price first.
- files use the reader-compatible 64 MiB aligned chunk size.

Polymarket's public stream has no monotonically increasing sequence number, so a client cannot
prove losslessness from one connection. Independent books, redundant sockets, hashes retained in
memory, per-symbol refreshes, full-book checkpoints, and scheduled reconnects reduce and bound that
risk.

## Verification

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

Tests cover exact decimal conversion, absolute update and zero-delete semantics, WebSocket payload
decoding, v1/v2 asset discovery, generic-adapter wire isolation, adaptive activity monitoring,
stable bounded sharding, and an encode/decode round trip through the starter's aligned SBE writer
and reader.

The SBE schema is generated from
[`sbe_schemas/engineering_challenge/challenge_orderbook.yaml`](sbe_schemas/engineering_challenge/challenge_orderbook.yaml)
by the unchanged starter build script.
"# Order_book" 
