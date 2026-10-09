# go-etl

The ETL (Extract, Transform, Load) program that processes raw NetBird flow events and aggregates them into human-readable **sessions** — one session representing a single logical connection between two peers.

It runs as a scheduled job alongside the flow server (see the `etl` service in `docker-compose.production.yml`) and writes derived session records back to the database.

---

## What is a session?

NetBird emits raw flow events for each connection. A single RDP session, for example, produces many individual flow events over its lifetime — one `TYPE_START` when the connection opens, one `TYPE_END` when it closes, and potentially more if the connection is long-lived or idle periods cause intermediate closes and reopens.

The ETL aggregates these raw events into a **session**, which represents:

- **Who** connected to whom (sender peer, routing peer)
- **What** service was used (destination port / protocol)
- **When** the session started and ended
- **How much** data was transferred (bytes and packets, combined across all flows)
- Which flow IDs, source ports, source IPs, and destination IPs were involved
- Which policy rule and network resource were matched

---

## Pipeline stages

```
Raw events (CSV or DB)
        │
        ▼
   1. Extract        — parse CSV or query the DB for events in the time window
        │
        ▼
   2. Filter         — keep TCP / UDP / ICMP; split into ingress and egress sets
        │
        ▼
   3. Group          — correlate ingress ↔ egress by port match (TCP/UDP)
        │              or IP + ICMP type/code match (ICMP)
        │              key: (senderPeer, routingPeer, destPort/protocol)
        ▼
   4. Split          — detect session boundaries using a traffic-volume threshold:
        │              if a TYPE_END event carries > threshold% of cumulative traffic
        │              AND there is a gap > 2 s before the next TYPE_START → new session
        ▼
   5. Merge          — merge adjacent sessions that share a source port or flow ID
        │
        ▼
   6. Enrich         — placeholder for future derived fields
        │
        ▼
   7. Load           — print sessions to stdout
        │
        ▼
   8. Upsert (DB mode only) — write sessions to the session_records table
```

### Session split threshold

The `--threshold` flag (default `0.5`) controls how sessions are split. When a `TYPE_END` event arrives and the traffic it carries exceeds `threshold × cumulative_traffic`, the algorithm looks for the next `TYPE_START`. If the gap is greater than 2 seconds, the current session is closed and a new one is started.

A threshold of `0.5` means: if a single `TYPE_END` carries more than 50% of all traffic seen so far, it likely marks a real disconnection rather than a mid-session partial close.

---

## Running modes

### Incremental mode (production — recommended)
Processes only events that arrived since the last run, using a **watermark** stored in the `etl_watermarks` table. Each run is O(new events) regardless of how much historical data exists, making it efficient to run frequently.

On the very first run (no watermark) it processes all events in the database, equivalent to a full scan. Every subsequent run fetches only new events, merges them into existing open sessions, and advances the watermark.

```bash
etl_runner --incremental
```

This is the mode used by the `etl` Docker service. The binary reads `ETL_INTERVAL` (seconds) from the environment and loops internally — no shell wrapper is needed:

```yaml
environment:
  - ETL_INTERVAL=300   # run every 5 minutes
```

If `ETL_INTERVAL` is unset or zero the binary runs once and exits, which is useful for one-shot backfills or CI.

#### How the watermark works

The watermark is the `created_at` timestamp of the latest event processed by the previous run. Each incremental run:

1. Acquires a Postgres advisory lock — if another instance is already running, this run exits immediately (see [Concurrency safety](#concurrency-safety))
2. Reads the watermark from the `etl_watermarks` table (`id = 1`)
3. Queries `event_logs WHERE created_at > watermark`
4. Runs the incremental pipeline on the new events only
5. Upserts the resulting sessions (updating open ones, closing split-off ones)
6. Updates the watermark to the latest `created_at` in the batch
7. Closes any sessions that have been open without activity for more than 4 hours
8. Deletes `event_logs` rows older than the retention window (default 48 h) — see [Event log retention](#event-log-retention)
9. Releases the advisory lock

#### Open vs. closed sessions

Sessions in the `session_records` table have an `is_open` flag:
- `is_open = true` — the session is still accumulating events (connection ongoing)
- `is_open = false` — the session has been closed (split threshold triggered, connection ended, or inactivity timeout)

When an incremental run processes new events for a peer group, it loads the existing open session from the DB (if any) and merges the new events into it. If the split threshold fires, the current session is closed and a new open one is started.

#### Inactivity timeout

A session can get stuck as `is_open = true` if a connection drops without sending a clean `TYPE_END` event (e.g. network failure, process crash). To guard against this, at the end of every incremental run the ETL issues a single SQL `UPDATE` that sets `is_open = false` on any session whose `updated_at` is older than **4 hours**:

```sql
UPDATE session_records SET is_open = false
WHERE is_open = true AND updated_at < now() - interval '4 hours'
```

This is a lightweight, O(stuck sessions) operation — typically touching zero rows during normal operation.

#### Event log retention

Raw `event_logs` rows accumulate quickly in high-traffic environments. Once the ETL has processed a batch of events and advanced the watermark, those rows are no longer needed for session building and can be deleted.

At the end of every incremental run the ETL hard-deletes `event_logs` rows whose `created_at` is older than the retention window:

```sql
DELETE FROM event_logs WHERE created_at < now() - $retention
```

The retention window is controlled by `--retention` (default `48h`). Pass `--retention 0` to disable pruning entirely.

```bash
etl_runner --incremental --retention 7d    # keep one week of raw events
etl_runner --incremental --retention 0     # never prune
```

The prune runs **after** sessions are upserted and the watermark is advanced, so processed events are never deleted before they have been aggregated.

#### Concurrency safety

The `etl` service loops internally, sleeping `ETL_INTERVAL` seconds between runs. If a run takes longer than `ETL_INTERVAL`, or if the service is accidentally started twice, two instances could overlap and process the same events — doubling traffic counts in sessions.

To prevent this, `RunIncremental` acquires a **Postgres session-level advisory lock** (`pg_try_advisory_lock`) before touching any data. The lock is non-blocking: if it is already held by another session, the new run logs a message and exits immediately without processing anything. The lock is automatically released when the run completes (or when the DB connection closes on process exit).

```
Run A starts  → pg_try_advisory_lock → acquired → processes events
Run B starts  → pg_try_advisory_lock → NOT acquired → "skipping this run" → exits
Run A finishes → pg_advisory_unlock
```

---

### DB mode (full scan — useful for backfills)
Reads all events over a fixed lookback window, derives sessions from scratch, and upserts them. Reprocesses the entire window on every run — use only for one-off backfills or debugging.

```bash
etl_runner --db --window 48h
```

### CSV mode (development / testing)
Reads events from a CSV export and prints derived sessions to stdout. Does not write to the database.

```bash
etl_runner events.csv
etl_runner events.csv 0.3   # custom threshold
```

Export from PostgreSQL:
```bash
docker exec -t netbird_traffic_postgres psql -U netbird -d netbird_traffic_event_logging \
  -c "COPY (SELECT * FROM event_logs) TO STDOUT DELIMITER ',' CSV HEADER;" > events.csv
```

### Seed mode
Inserts events from a CSV file into the database, preserving their original timestamps. Useful for replaying historical data before running the incremental ETL.

```bash
etl_runner --seed events.csv
```

---

## CLI flags

| Flag | Default | Description |
|---|---|---|
| `--incremental` | false | Process only new events since the last watermark (recommended for production) |
| `--db` | false | Full-scan mode: process all events in the lookback window |
| `--window` | `48h` | Lookback window for `--db` mode |
| `--retention` | `48h` | How long to keep raw `event_logs` rows after processing. Pass `0` to disable pruning. Incremental mode only. |
| `--threshold` | `0.5` | Session split threshold (0–1) |
| `--seed` | false | Insert CSV events into the database |
| `--debug` | false | Print parsed flags and extra debug output |

The loop interval for daemon mode is set via the `ETL_INTERVAL` environment variable (seconds). If unset or zero, the binary runs once and exits.

---

## Output

Sessions are printed to stdout in this format:

```
Session 1: <senderPeer> connected to port 3389/TCP via <routingPeer> as routing peer.
  Flow IDs: abc123, def456
  Source ports used: 51234, 51300
  Source IPs: 100.64.0.1
  Destination IPs: 192.168.1.10
  Start: 2026-01-15 09:01:00.000000
  End:   2026-01-15 09:45:22.000000
  RX Packets: 45210
  TX Packets: 38900
  RX Bytes:   12345678
  TX Bytes:   8901234
```

Sessions are also upserted to the `session_records` table keyed by `(sender_peer, routing_peer, dest_port, start_time)`.
