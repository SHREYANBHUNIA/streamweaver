# StreamWeaver

**StreamWeaver** is a Go-based incremental transaction-processing engine. It consumes continuous transaction events from Kafka, applies filtering and mapping operators, assigns event-time windows, persists state in RocksDB, checkpoints progress, and exposes a monitoring API. The included transaction demo preserves the contract **SUM(amount)** over **10-second windows**, emitting an alert when the **configured alert threshold** is exceeded.

## Processing Contract

| Concern | Implemented behavior |
| --- | --- |
| Incremental aggregation | Each transaction updates only its assigned window aggregate; the engine never recomputes the full input history. |
| Event time | Tumbling and sliding assignments use `eventTime`; a watermark advances from maximum observed event time less allowed lateness. |
| Late events | `drop`, `side_output`, and `accumulate` policies are modeled. The demo uses `side_output` with 2 seconds of allowed lateness. |
| Backpressure | A bounded queue rejects submissions when full, reporting an explicit producer-slow-down error instead of allocating an unbounded backlog. |
| Durable state | Aggregate values and the processed-event ledger are written to RocksDB in the same write batch. |
| Recovery | State is committed first, then `latest.json` is atomically replaced; Kafka offsets are committed only after the engine acknowledges that checkpoint. Persisted event IDs make Kafka redelivery idempotent. |
| Alerts | A window aggregate crossing the threshold yields an authenticated owner-alert callback. Recovery or checkpoint write failures also emit an alert payload. |

## Local Runtime

Install Docker with the Compose plugin, then run the full Kafka, Go engine, RocksDB volume, dashboard/API, and transaction-generator topology.

```bash
cp config/compose.settings.example .env
docker compose up --build
```

Open [http://localhost:3000](http://localhost:3000) for the React monitoring dashboard. The Go API is available at [http://localhost:8080/v1/status](http://localhost:8080/v1/status), while [http://localhost:8080/v1/health](http://localhost:8080/v1/health) provides a small health response. The demo producer publishes a transaction every 700 milliseconds and includes a deterministic spike every twelfth event, so a 10-second window can cross the default `$10,000` threshold.

The Compose runtime includes a self-contained, network-internal alert bridge. The Go engine posts a bearer-authenticated alert to the dashboard service, which calls the project owner notification channel when it is available. The Compose token is deliberately local-only. If the worker is deployed outside the Compose network, replace it with a project secret and configure `OWNER_ALERT_WEBHOOK_URL` and `OWNER_ALERT_TOKEN` for the external endpoint.

| Service | Address | Purpose |
| --- | --- | --- |
| React monitoring dashboard | `http://localhost:3000` | Live health, throughput, backpressure, checkpoints, windows, late events, and alerts. |
| Go HTTP API | `http://localhost:8080` | Engine health, stream status, window results, alerts, and demo transaction intake. |
| Kafka | `kafka:9092` inside Compose | Transaction transport for the engine and demo producer. |
| RocksDB volume | `streamweaver_state` | Durable operator state and atomic checkpoint manifests. |

## API Surface

| Method and route | Description |
| --- | --- |
| `GET /v1/health` | Liveness response for the Go API. |
| `GET /v1/status` | Pipeline status, metrics, checkpoint, windows, late events, alerts, and recovery contract. |
| `GET /v1/pipeline` | Current pipeline ID, windowing policy, bounded queue capacity, and alert threshold. |
| `PUT /v1/pipeline` | Updates the configured `alertThreshold` without replaying historical data. |
| `GET /v1/windows` | Current RocksDB-backed window aggregate view. |
| `GET /v1/alerts` | In-memory active alert feed since the current process start. |
| `POST /v1/transactions` | Submits a normalized transaction event through the bounded engine queue. |

`POST /v1/transactions` accepts an event such as the following. The API fills the event ID only when the caller omits it; Kafka consumers should preserve a stable source ID so duplicate deliveries remain idempotent.

```json
{
  "id": "txn-1001",
  "stream": "transactions",
  "key": "merchant-orbit",
  "amount": 425.50,
  "eventTime": "2026-08-26T12:00:05Z"
}
```

## Verification and Benchmarks

The test suite covers filter/map execution, tumbling and sliding assignment, late-event detection, `SUM(amount)` threshold alerting, event-ID idempotency, checkpoint recovery, bounded backpressure, the pipeline-configuration API, owner-alert authentication, dashboard formatting, and telemetry provenance. Run it with the commands below.

```bash
go test ./...
go test -bench=. ./benchmarks
pnpm test
pnpm check
```

## Continuous-Worker Deployment Choices

The React dashboard can be hosted as a conventional web application, but the Go Kafka consumer must stay online, retain a writable RocksDB volume, and run a custom Go/RocksDB runtime. Therefore, use one of the following operational models for the engine rather than a request-scoped serverless process.

| Approach | Tradeoffs | Cost | Setup complexity |
| --- | --- | --- |
| Run the included Compose stack on an existing always-on Linux server | Full Docker and volume control; you operate monitoring, upgrades, and backups. | Uses your existing infrastructure. | Moderate. |
| Run the consumer on a managed always-on instance and retain the dashboard separately | A managed persistent process can host lightweight consumers, but it still requires Go/RocksDB runtime support and a durable volume. | Usage-based; resource and volume charges depend on the selected service. | Moderate. |
| Use a dedicated persistent server for the complete Compose topology | Best fit when the Kafka worker, local RocksDB state, Docker runtime, and operations need to live together independently of a browser session. | A persistent cloud server is a paid runtime; size it to your expected Kafka rate and RocksDB growth. | Moderate to high. |

> **Operational note:** A request-scoped, autoscaling web process is not an appropriate host for the long-lived consumer. It can pause between requests, while this engine needs a continuously running loop and durable local state. Keep the dashboard separate from the worker if you want independent web scaling.
