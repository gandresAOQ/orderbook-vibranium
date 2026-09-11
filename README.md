# Orderbook — Vibranium (MVP)

A limit order book that lets users place **buy** and **sell** orders for
**Vibranium** priced in Colombian pesos (COP), matches them by **price-time
priority**, and credits/debits wallets as trades execute — with full trade
traceability.

Built in **Go**. The domain core (order book, wallets, rules) is standard library
only; external dependencies live exclusively in the infrastructure adapters.

Thanks to the hexagonal design the same binary runs in **two modes**, chosen by
configuration alone:

| Mode | Command | Infrastructure |
|---|---|---|
| **Full** (default in Docker) | `make up` | Redpanda (event log), Postgres (wallets/trades/orders/journal), Redis (cache) |
| **In-memory** | `make run` or `make up-memory` | None: fully in-process, zero dependencies |

---

## Why this design

The challenge's key constraint is a paradox: **the order book cannot tolerate
concurrency** (matching must be strictly ordered and deterministic), yet
thousands of orders can arrive in the same millisecond. Real exchanges (LMAX,
Nasdaq) solve this by **serializing every order into an ordered log and running
a single-threaded matching engine**. That is exactly the shape here, and Go maps
onto it naturally:

- **One goroutine owns the order book.** All mutations flow through a single
  channel, so there are no locks and no data races on the book. Ordering is
  decided once — at the channel — then applied deterministically.
- **The ordered log is a port, not a channel.** One `EventLog` interface, two
  implementations: a Go channel in memory, and a Redpanda/Kafka partition keyed by
  symbol to preserve per-instrument order. Both are implemented and running.
- **The engine never touches money.** It only decides matches and emits events.
  A separate **settlement** consumer applies credits/debits and refunds. This
  keeps the hot path tiny and the money path auditable.

```
  Clients / bots
        │  POST /orders
        ▼
┌──────────────────┐  reserve funds (atomic)   ┌────────────────────────────┐
│   HTTP API       │──────────────────────────▶│  Wallet store (available /  │
│  (stateless,     │   row lock per user        │  locked split)              │
│   scalable)      │           place            │  memory | POSTGRES (+REDIS) │
└───────┬──────────┘─────────────┐             └────────────────────────────┘
        │                        ▼
        │              ┌────────────────────────┐  emits events
        │              │  Matching Engine        │───────────────┐
        │              │  1 goroutine, owns book │                │
        │              │  price-time priority    │                ▼
        │              └────────────────────────┘     ┌──────────────────────┐
        │                                             │  Event log           │
        │                                             │  memory | REDPANDA   │
        │                                             │  1 partition/symbol  │
        │                                             └───────┬──────────────┘
        ▼                                                     ▼
  GET /book, /trades, /wallets              ┌────────────────────────────────┐
                                            │  Settlement consumer            │
                                            │  idempotency journal (event ID) │
                                            │  applies trades, refunds        │
                                            └───────┬────────────────────────┘
                                                    ├─▶ Wallet store
                                                    ├─▶ Trade history
                                                    └─▶ Order projection
                                                        memory | POSTGRES
```

Uppercase = the durable adapters that `make up` wires in. Everything is selected
by environment variables; the core code is identical in both modes.

## The correctness core: reserve / settle / release

A user's balance is split into **available** and **locked**. This is what
prevents double-spend when a bot fires many orders in the same millisecond.

| Event | COP | Vibranium |
|-------|-----|-----------|
| Place **BUY** `qty @ price` | `qty*price` moves available → locked | — |
| Place **SELL** `qty` | — | `qty` moves available → locked |
| Trade executes (`qty @ execPrice`) | buyer: locked → spent; seller: +available | seller: locked → delivered; buyer: +available |
| Buy over-reservation refund | `qty*(limit − execPrice)` locked → available | — |
| Cancel remainder | locked → available | locked → available |

The **execution price is the resting (maker) order's price**. A buy taker may
lock more than it spends (its limit was above the maker's ask), so the
difference is refunded — see `TestBuyOverReservationRefundedEndToEnd`.

The **money invariant**: `sum(available + locked)` for COP and for Vibranium is
constant across all users. The concurrency test and the load-test both assert it.

---

## Run it

Requirements: Docker (for the full stack) or Go 1.26+ (to run it bare).

### Full architecture — one command

```bash
make up          # docker compose up --build -d, waits until healthy
```

That starts four services and applies the DB schema automatically:

| Service | Role | Port |
|---|---|---|
| `orderbook` | API + matching engine + settlement | 3000 |
| `redpanda` | durable ordered event log (Kafka API) | 19092 |
| `postgres` | wallets, trades, orders, settlement journal | 5432 |
| `redis` | hot read cache for wallet lookups | 6379 |
| `console` | web UI to inspect the event log (optional) | 8080 |

```bash
make logs        # follow the app
make ps          # service health
make down        # stop, keep data
make reset       # stop and wipe volumes
```

### Zero infrastructure

Same binary, all adapters set to `memory`:

```bash
make run         # bare process on :3000
make up-memory   # single container, no dependencies
```

### Test

```bash
make test        # go test ./... -race
make loadtest    # 5000 pairs, then reconciles balances
```

## API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/wallets` | Seed/replace a wallet (testing; registration is out of scope) |
| `GET`  | `/wallets` | List all wallets (reconciliation) |
| `GET`  | `/wallets/{id}` | Get one wallet |
| `POST` | `/orders` | Place a buy/sell limit order |
| `DELETE` | `/orders/{id}` | Cancel a resting order |
| `GET`  | `/orders/{id}` | Order status |
| `GET`  | `/book?depth=N` | Aggregated order book depth |
| `GET`  | `/trades?limit=N` | Recent trades (traceability) |
| `GET`  | `/health` | Liveness + trade count |

### Example

```bash
# Seed two users
curl -s localhost:3000/wallets -d '{"userId":"alice","copAvailable":1000000,"vibraniumAvailable":0}'
curl -s localhost:3000/wallets -d '{"userId":"bob","copAvailable":0,"vibraniumAvailable":100}'

# Bob sells 50 @ 100, Alice buys 50 @ 100  -> a trade executes
curl -s localhost:3000/orders -d '{"userId":"bob","side":"SELL","price":100,"quantity":50}'
curl -s localhost:3000/orders -d '{"userId":"alice","side":"BUY","price":100,"quantity":50}'

# Inspect
curl -s localhost:3000/trades
curl -s localhost:3000/wallets/alice   # +50 Vibranium, -5000 COP
curl -s localhost:3000/wallets/bob     # -50 Vibranium, +5000 COP
```

## Load test (validates balances, like the evaluation)

```bash
make run &                                    # in one shell
go run ./scripts/loadtest -pairs 5000 -concurrency 200
```

It seeds 5000 buyer/seller pairs, fires all orders concurrently, prints the
throughput, then reconciles: total COP and total Vibranium must be unchanged.

---

## Failure modes (what happens when a component fails)

These are **verified behaviours** of the running stack, not aspirations:

- **Settlement replay / duplicate delivery** — the event log is at-least-once, so
  the same event can arrive twice. Every event carries a unique ID
  (`<producerID>-<sequence>`) and settlement records it in a Postgres
  **idempotency journal** before applying it. Rewinding the consumer group to
  offset 0 and re-consuming the entire log leaves balances **byte-identical**:
  ```bash
  make replay-test    # rewinds to offset 0, proves money is not duplicated
  ```
- **App crash / restart** — wallets, trades, orders and the journal live in
  Postgres, so state survives. `docker compose restart orderbook` keeps every
  balance and the full trade history.
- **Wallet store down** — the API **fails closed**: `POST /orders` returns `503`
  (`wallet store unavailable`) and no order enters the book, because no order can
  enter without a successful reservation. `/health` reports `degraded`. When
  Postgres returns, the pool reconnects and trading resumes with no restart.
- **API failure after reserving funds** — a compensating `release` runs on a
  detached context (the caller's context may already be cancelled), so funds are
  never stranded in `locked`.
- **Broker/DB slow at startup** — every adapter retries until
  `INFRA_WAIT_TIMEOUT`, and startup **refuses to boot** rather than silently
  degrading a ledger to non-durable memory.
- **Backpressure** — `Publish` hands events to an internal queue drained by a
  producer goroutine, so broker latency never blocks the single-writer engine. If
  the queue saturates the engine blocks: intentional backpressure, not silent loss.

**Known limitation, stated plainly:** the in-memory book itself is not rebuilt on
restart. Recovery of resting orders requires replaying the log into a fresh book
(plus periodic snapshots to bound replay time). The log, the event IDs and the
journal are all in place to make that possible; the replay routine itself is
deliberately out of MVP scope.

## Scaling path

- **API tier**: stateless, scale horizontally behind a load balancer. Because
  reservations take a **row-level lock in Postgres** rather than an in-process
  mutex, the no-double-spend guarantee survives multiple API replicas.
- **One asset (Vibranium)**: a single partition and one engine already exceed the
  target — measured **~14,900 orders/sec (~7,400 trades/sec)** on a laptop
  against the full Redpanda+Postgres+Redis stack, with the money invariant
  holding.
- **More assets**: shard by **symbol** across partitions/engines — linear scaling
  per instrument. A single hot symbol is the hard ceiling; you cannot parallelize
  one book. That is a property of order books, not a flaw here.
- **Next bottleneck** is settlement's per-event round trips to Postgres, which
  batching (or a per-trade unit-of-work transaction) would relieve.

## Observability

Structured JSON logs via `slog`, including the selected adapters at startup.
`/health` surfaces dependency failure instead of lying `ok`. Redpanda Console
(`:8080`) shows the raw event log for traceability demos, and `make groups` shows
settlement consumer lag.

Metrics to add next: orders/sec, match latency, book depth, log lag, settlement
latency, and a periodic **reconciliation job** that proves the money invariant in
production.

## Project layout (hexagonal / ports & adapters)

The code follows a hexagonal architecture: a pure core (domain + use cases)
surrounded by ports (interfaces) and adapters (infrastructure). Dependencies
point **inward** — adapters depend on the core, never the reverse.

```
cmd/orderbook                     composition root: wires adapters to services, graceful shutdown

internal/core/                    THE APPLICATION CORE (no transport/storage deps)
  domain/                         entities + rules: order, trade, wallet, events, the pure order book
  port/                           interfaces: inbound (driving) + outbound (driven) ports
  service/                        use cases: trading, settlement, market data, wallets

internal/adapter/                 INFRASTRUCTURE (implements the ports)
  driving/rest/                   HTTP adapter (net/http, stateless) -> calls inbound ports
  driven/matching/                single-goroutine matching engine  -> MatchingEngine
  driven/memstore/                in-memory repos + journal          -> repositories
  driven/memlog/                  in-memory ordered event log        -> EventLog
  driven/postgres/                durable repos + idempotency journal (row locks, schema.sql)
  driven/kafkalog/                Redpanda/Kafka ordered event log
  driven/rediscache/              Redis read-cache DECORATOR over any WalletRepository

internal/bootstrap                picks the adapters from configuration (the only
                                  place that knows both "memory" and "postgres" exist)
internal/config                   env-based configuration
scripts/loadtest                  concurrent bot client + balance-invariant checker
```

**Ports** live in `internal/core/port`:
- *Inbound (driving):* `TradingService`, `MarketDataService`, `WalletService` —
  what the app offers; implemented by `internal/core/service`, called by the REST adapter.
- *Outbound (driven):* `MatchingEngine`, `WalletRepository`, `TradeRepository`,
  `OrderRepository`, `EventLog`, `EventJournal` — what the app needs; implemented
  by `internal/adapter/driven/*`.

Every adapter pair implements the same port, so infrastructure is a **configuration
choice, not a code change**:

| Port | `memory` | real |
|---|---|---|
| `EventLog` | Go channel | **Redpanda/Kafka**, 1 partition per symbol |
| `WalletRepository` | `RWMutex` map | **Postgres** + `SELECT … FOR UPDATE` |
| `TradeRepository` | slice | **Postgres** append-only |
| `OrderRepository` | map | **Postgres** (monotonic by sequence) |
| `EventJournal` | set | **Postgres** `processed_events` |
| cache | — | **Redis** decorator over the above |

The Redis adapter is worth calling out: it is a **decorator**, implementing
`WalletRepository` and wrapping the durable one. Caching is composed in at the
composition root, and neither the core nor Postgres knows it exists.

## Scope

**Built and verified:** limit buy/sell, cancel, price-time matching,
reserve/settle/release, trade history, one asset, the ordered-log architecture,
tests, load test, and the **real infrastructure adapters** (Redpanda + Postgres +
Redis) with an idempotency journal, durability across restarts, and fail-closed
behaviour when the database is down.

**Out of scope** (intentionally, to avoid overengineering): GUI,
auth/registration, market orders, and **rebuilding the book by replaying the log
on startup** (plus snapshots) — the known limitation described under Failure
modes.
