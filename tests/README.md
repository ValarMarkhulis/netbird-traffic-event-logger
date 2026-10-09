# Tests Folder

This folder contains the artifacts used for testing the ETL pipeline:

- **CSV test fixtures** — raw traffic event data exported from a real Netbird PostgreSQL database
- **Expected output files** — the reference output that the ETL pipeline should produce for each fixture

The CSV replay client that sends these fixtures to the gRPC server lives in [`client/csv-replay/`](../client/csv-replay/main.go).

---

## Project Goal

The goal of this project is to receive raw Netbird flow traffic events (via a GRPC server), store them in a PostgreSQL database, and then run an ETL pipeline that transforms the low-level per-flow events into meaningful **sessions**: human-readable records of which peer connected to what, when, for how long, and how much data was transferred.

A single user session (e.g. an RDP connection) generates many individual flow events. The ETL pipeline groups, filters, and aggregates these into one session record per logical connection.

---

## CSV Test Fixtures

Each `.csv` file is a real export from the `event_logs` table of the Netbird traffic event PostgreSQL database. The CSV schema matches the database schema:

```
id, created_at, updated_at, deleted_at, event_id, public_key, flow_id, type,
rule_id, source_resource_id, dest_resource_id, direction, protocol,
source_ip, dest_ip, conn_source_port, conn_dest_port,
icmp_icmp_type, icmp_icmp_code,
rx_packets, tx_packets, rx_bytes, tx_bytes
```

To export a new fixture from a running database:
```bash
docker exec -t netbird_traffic_postgres psql -U netbird -d netbird_traffic_event_logging \
  -c "COPY (select * from event_logs) To STDOUT DELIMITER ',' CSV HEADER;" > tests/my-test.csv
```

### Fixture Descriptions

| File | Description |
|------|-------------|
| `2025-12-06-RDP-test-v0.53.0.csv` | Single RDP session from one Windows client (NetBird v0.53.0) |
| `2025-12-06-RDP-test2-v0.53.0.csv` | Second isolated RDP test session, same client version |
| `2025-12-06-RDP-test-v0.53.0-2-sessions-from-same-pc.csv` | Two consecutive RDP sessions initiated from the same PC |
| `2025-12-06-ping-ssh-rdp-test-v0.53.0.csv` | Mixed traffic: ICMP ping, SSH, and RDP in a single capture |
| `2025-12-06-ping-ssh-rdp-test.csv` | Earlier version of the mixed traffic test (pre-v0.53.0 schema) |
| `2025-12-07-client-v0530-3-rdp-session-after-each-other-long-test.csv` | Three sequential RDP sessions, one after the other |
| `2025-12-07-client-v0530-long-rdp-session-that-ended-test.csv` | A long RDP session with a clean TYPE_END event |
| `2025-12-07-client-v0530-and-linux-dual-rdp-session-that-ended-test.csv` | Two simultaneous RDP sessions from Windows and Linux clients |
| `2025-12-07-client-v0530-and-linux-v0530-3-rdp-session-disconnecting-each-other-on-second-session-then-normal-long-test.csv` | Overlapping RDP sessions where a new connection forcibly disconnects the previous one |
| `2025-12-07-linux-client-rdping-that-endedand-windows-spamming-ping-file-download-test.csv` | Linux RDP session mixed with Windows ICMP and file download traffic |
| `linux-client-test.csv` | Basic connectivity test from a Linux NetBird client (internet traffic only, so the expected output is intentionally empty) |
| `1gbdownload.csv` | ICMP ping plus a ~1 GB HTTP download on port 8090 through a routing peer |
| `linux-chaos-test.csv` | Stress test with irregular and overlapping events from a Linux client |

### Expected Output Files

Each `.expected_output.txt` file contains the human-readable session output that the ETL pipeline is expected to produce for the corresponding CSV fixture. Example:

```
Session 1: <sender_public_key> connected to port 3389 via <router_public_key> as routing peer.
  Flow IDs: <uuid>, <uuid>, ...
  Source ports used: 60814, 65015, 65017
  Source IPs: 100.67.146.229
  Destination IPs: 192.168.4.2
  Start: 2025-12-06 13:53:57.739350
  End:   2025-12-06 13:54:27.785622
  RX Packets: 153
  TX Packets: 138
  RX Bytes:   15482
  TX Bytes:   39633
```

These files serve as regression references. `make test` in `go-etl/` runs `etl_runner` on every fixture that has a matching `.expected_output.txt` and diffs the result.

To regenerate an expected output after an intended change in ETL behaviour:

```bash
cd go-etl && make build
./etl_runner ../tests/<fixture>.csv > ../tests/<fixture>.expected_output.txt
```

---

## CSV replay client (`client/csv-replay`)

This is a standalone Go program (compiled separately from the main ETL pipeline) that acts as a **CSV-driven GRPC client**. It replays events from a CSV fixture file row-by-row, sending each one as a live `proto.FlowEvent` to the GRPC server running on `tcp://localhost:9000` — exactly as a real NetBird peer would.

### Purpose

Its role in the project is to enable **end-to-end integration testing** of the full pipeline without needing real NetBird clients:

1. A CSV fixture captures a known set of real traffic events from the database.
2. The replay client replays those events into the GRPC server.
3. The server stores the events in the database.
4. The ETL pipeline then processes the database entries and produces session output.
5. That output is compared against the corresponding `.expected_output.txt` file.

This makes it possible to reproduce specific traffic scenarios (e.g. overlapping RDP sessions, ICMP floods, dual-client connections) deterministically, without needing a live NetBird network.

### How to run

First ensure the GRPC server is running locally on port 9000, then from the repo root:

```bash
go run ./client/csv-replay <path-to-fixture.csv>

# Example:
go run ./client/csv-replay tests/2025-12-06-RDP-test-v0.53.0.csv
```

The client will:
1. Open the CSV file and skip the header row.
2. Parse each row into a `proto.FlowEvent` protobuf message.
3. Send each event to the server and wait for an acknowledgement before sending the next.
4. Log progress and any send/ack errors to stdout.

### Relationship to `go-etl/pipeline/csv_to_database_client.go`

The ETL pipeline also has a CSV reader at [go-etl/pipeline/csv_to_database_client.go](../go-etl/pipeline/csv_to_database_client.go), but it works differently:

| | `client/csv-replay` | `go-etl/pipeline/csv_to_database_client.go` (`SeedFromCSV`) |
|---|---|---|
| **Transport** | Sends events over GRPC (live network protocol) | Writes directly to the database via GORM |
| **Use case** | End-to-end integration testing of the GRPC server | Seeding the database for ETL pipeline development and testing |
| **Timestamps** | Events arrive with server-assigned `created_at` times | Preserves original `created_at` timestamps from the CSV |
| **Schema** | Reads the raw database export CSV column layout | Reads the same CSV format |

`SeedFromCSV` is the faster path for ETL development: it bypasses the GRPC layer and loads fixture data directly into the database with timestamps intact, so the ETL pipeline can be tested and iterated on independently of the server.
