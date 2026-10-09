# CLAUDE.md — Project context for Claude Code

## What this project is

A Go proof-of-concept for receiving and storing [NetBird traffic event logs](https://docs.netbird.io/how-to/traffic-events-logging).
NetBird v0.53.0 exposes flow events (connections, bytes, packets) over gRPC, but only on the paid cloud plan.
This project self-hosts that capability and adds an ETL layer that aggregates raw events into human-readable sessions.

The repo contains two independent programs that share a single PostgreSQL database:

1. **flow server** (`server.go` + supporting files in root) — long-running gRPC receiver
2. **ETL job** (`go-etl/`) — scheduled short-lived process that aggregates events into sessions

Module path: `github.com/ValarMarkhulis/traffic-event-logger`
Go version: 1.23.7

---

## Repository layout

```
/                           flow server (package main)
  server.go                 gRPC server, worker pool, FlowEventToDatabaseEvent
  circuit_breaker.go        CircuitBreaker (Closed → Open → HalfOpen)
  retry_buffer.go           In-memory retry buffer for open-circuit batches
  server_test.go            Unit tests for server (uses mockDB)

database/
  database.go               Database interface, DB struct (GORM), DBInit/DBConnect,
                            connection pool config, CreateEventLog/Batch (with context)
  session.go                SessionRecord, ETLWatermark, UpsertSession, GetOpenSession,
                            GetAllOpenSessions, GetWatermark/SetWatermark,
                            CloseInactiveSessions, TryAdvisoryLock/ReleaseAdvisoryLock
  netflow_types.go          Protocol / Direction enums and their String() methods

go-etl/                     ETL job (package main)
  data/session.go           In-memory Session type shared by transformer and pipeline
  main.go                   CLI entry point — --incremental / --db / --seed / CSV modes
  pipeline/
    incremental_pipeline.go RunIncremental: advisory lock → watermark → query →
                            pipeline → upsert → watermark → CloseInactiveSessions
    db_pipeline.go          RunFromDBWithCount / RunFromDB (full-scan, backfill)
    csv_to_database_client.go SeedFromCSV
    incremental_pipeline_test.go Advisory lock unit tests
    db_pipeline_test.go     sessionToRecord unit tests
  transformer/
    extract.go / extract_db.go  CSV and DB extraction
    filter.go               FilterByProtocol (TCP/UDP/ICMP → ingress/egress)
    group.go                GroupByPeers (correlate ingress ↔ egress)
    session_split.go        SplitSessions + mergeSessions (full-scan path)
    incremental.go          IncrementalPipeline + applyGroupIncremental
    transformer.go          ETLPipeline / ETLPipelineFromDB (full-scan wrappers)
    enrichment.go / load.go / strings.go  Supporting helpers

client/                     Test gRPC client (sends synthetic flow events)
  csv-replay/               Replays a CSV fixture over gRPC to a running flow server
docker/
  Dockerfile                flow-server image (includes grpc_health_probe)
  Dockerfile.etl            ETL image
  docker-compose.testing.yml    Local test env (Postgres + flow-server + ETL)
  docker-compose.production.yml Production env (adds Caddy reverse proxy)
  docker-compose.test-postgres-db.yml  Postgres-only (for running server locally)
  Caddyfile                 TLS termination + gRPC proxying on port 443
tests/                      CSV test fixtures + expected outputs
```

---

## Key design decisions

### Flow server

- **Worker pool**: 4 goroutines (`eventWorkers`) read from a buffered channel (`eventChannelBuffer = 10_000`) and flush batches of 100 events (`batchSize`) every 500 ms (`batchFlushInterval`) or when full.
- **Batch insert with dedup**: `CreateEventLogBatch` uses `ON CONFLICT DO NOTHING` on `event_logs.event_id` (unique index). Retransmitted events are silently ignored.
- **Batch fallback**: If a batch insert fails for any reason other than duplication, each event is retried individually so only the bad row is dropped.
- **Circuit breaker**: After 5 consecutive DB failures the circuit opens. Writes are rejected immediately and the gRPC health status flips to `NOT_SERVING`. Resets after 30 s cooldown.
- **In-memory retry buffer**: While the circuit is open, batches are buffered (max 50 batches = 5 000 events). A background goroutine retries them every 5 s once the circuit recovers. Buffer is flushed synchronously on clean shutdown. **Known trade-off**: buffer is lost on unclean restart.
- **DB write timeout**: Every DB call is wrapped in `context.WithTimeout(5s)` (`dbWriteTimeout`) to prevent hung Postgres connections from blocking all workers.
- **Authentication**: gRPC stream interceptor verifies `Authorization: Bearer <sig>.<payload>` where payload = Unix expiry timestamp and sig = `base64(HMAC-SHA256(payload, sha256(NB_FLOW_SECRET)))`. If `NB_FLOW_SECRET` is unset, verification is skipped (dev mode only).
- **Graceful shutdown**: `SIGTERM`/`SIGINT` → `GracefulStop()` → `close(events)` → workers drain and flush → exit. If `DBInit` fails at startup, `GracefulStop()` is called immediately so the server doesn't accept connections with no consumer.
- **Health endpoint**: `grpc.health.v1.Health` — `SERVING` when circuit closed, `NOT_SERVING` on DB failure.

### ETL job

- **Incremental mode** (`--incremental`, recommended for production): Reads only events since the last watermark. O(new events) per run.
  - Acquires a Postgres session-level advisory lock (`pg_try_advisory_lock(1)`) to prevent concurrent double-counting. Lock auto-releases on connection close (crash-safe).
  - Loads **all** open sessions in a single query (`GetAllOpenSessions`) before the pipeline loop to avoid N+1 per peer group.
  - Upserts resulting sessions, advances watermark, then closes sessions inactive for 4+ hours via a single `UPDATE`.
- **DB mode** (`--db`, backfill only): Full-scan over a fixed lookback window. No advisory lock, no watermark.
- **CSV mode**: Reads a CSV export, prints sessions to stdout. No DB writes.
- **Seed mode** (`--seed`): Inserts CSV events into DB preserving original timestamps.
- All three DB modes use `DBInit()` (runs GORM auto-migration on startup).

### Session model

A session represents one logical connection: `(sender_peer, routing_peer, dest_port, start_time)` is the unique key in `session_records`.

- `is_open = true` while the connection is ongoing
- `is_open = false` after: split threshold fires, `TYPE_END` received cleanly, or 4-hour inactivity timeout
- Sessions are built by correlating **egress** events (sender's perspective) with **ingress** events (router's perspective) matched by source+dest port (TCP/UDP) or source+dest IP + ICMP type/code (ICMP).
- Split threshold (default 0.5): if a `TYPE_END` event carries > 50% of cumulative traffic AND the next `TYPE_START` is > 2 s later → close current session, start new one.

---

## Database schema (GORM auto-migrated)

```
event_logs         — raw flow events; event_id has UNIQUE index
                     event_time = when it happened on the peer (FlowEvent timestamp; required,
                     indexed). created_at = when the flow server stored it. Peers report in
                     batches, so created_at lags event_time by up to the report interval; the
                     ETL watermark uses created_at (ingest order), sessions use event_time.
session_records    — aggregated sessions; unique on (sender_peer, routing_peer, dest_port, start_time)
etl_watermarks     — single row (id=1) tracking last processed event timestamp
```

Connection pool: `MaxOpenConns=25`, `MaxIdleConns=5`, `ConnMaxLifetime=5m`.

---

## Build and run

```bash
# Root (flow server + client)
make build-s          # → ./flow-server
make build-c          # → ./flow-client
make test             # go test -v ./...
make run-server       # build + run flow-server (needs DB)

# go-etl
cd go-etl
make build            # → ./etl_runner
make unit-test        # go test ./...
make test             # integration: run against CSV fixtures in ../tests/

# Docker environments
make run-test-env     # docker-compose.testing.yml (Postgres + flow-server --debug + ETL --incremental)
make run-production   # docker-compose.production.yml (adds Caddy, no --debug)
make run-db           # Postgres only
make export-data      # dump event_logs to test.csv
```

---

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `NB_FLOW_SECRET` | Recommended | Shared HMAC-SHA256 secret. Unset = no auth (dev only) |
| `POSTGRES_USER` | Yes | DB user |
| `POSTGRES_PASSWORD` | Yes | DB password |
| `DATABASE_ADDR` | Yes | DB host |
| `POSTGRES_PORT` | Yes | DB port |
| `POSTGRES_DB` | Yes | DB name |
| `POSTGRES_SSL` | Yes | `disable` / `require` etc. |
| `DB_LOG_MODE` | No | GORM log level: `silent` (default), `error`, `warn`, `info` |
| `ETL_INTERVAL` | ETL only | Sleep interval between incremental runs (seconds, default 300) |

### flow-server flags

| Flag | Default | Description |
|---|---|---|
| `--port` | 9000 | gRPC listen port |
| `--debug` | false | Enable debug-level structured logging |

### etl_runner flags

| Flag | Default | Description |
|---|---|---|
| `--incremental` | false | Incremental watermark mode (production) |
| `--db` | false | Full-scan mode (backfill) |
| `--window` | 48h | Lookback window for `--db` mode |
| `--threshold` | 0.5 | Session split threshold |
| `--seed` | false | Insert CSV events into DB |
| `--debug` | false | Print parsed flags and extra debug output |

---


## Testing approach

- **Unit tests** in root package (`server_test.go`) use `mockDB` that implements the full `database.Database` interface. Tests cover: batch success, fallback on batch error, bad-row isolation, circuit-open buffering, retry-buffer-full drop, circuit recovery flush.
- **Unit tests** in `go-etl/pipeline/` cover `sessionToRecord` field mapping and all advisory lock code paths in `RunIncremental`.
- **Unit tests** in `go-etl/transformer/` cover extract, filter, group, session split, strings, and incremental pipeline logic.
- **CSV integration tests** in `go-etl/` (`make test`) run `etl_runner` against fixture CSVs and diff against `.expected_output.txt` files in `tests/`.

---

## NetBird integration

The modified NetBird management service is the fork at https://github.com/ValarMarkhulis/netbird/tree/v0.53.0-traffic-event-logging-unlocked (NetBird v0.53.0 with flow unlocked). It adds:

- `NB_FlowEnabled=true` env var to enable flow event reporting on all clients
- `NB_FLOW_URL=https://domain.com:443` — URL of the flow server
- `NB_FlowIntervalInMinutes=10` — how often clients report
- `NB_FLOW_SECRET` — shared secret for HMAC token generation (must match server)

The management service generates tokens with format `<sig>.<unix_expiry>` where sig = `base64(HMAC-SHA256(expiry, sha256(NB_FLOW_SECRET)))`.
