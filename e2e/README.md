# NetBird traffic events — local e2e lab

Runs the whole self-hosted traffic-events stack on localhost for manual
end-to-end testing: this repo's flow-server and ETL, the modified NetBird
management, signal, relay and dashboard, an OIDC provider, and a set of peers
with access policies and a routed network that generate traffic.

It builds from source, so check out the other two repos next to this one:

```
<parent>/
├── netbird-traffic-event-logger/      this repo — flow-server, ETL (this folder: e2e/)
├── netbird/                           branch v0.53.0-traffic-event-logging-unlocked
│                                      — management, signal, relay, peer clients
└── management-integrations-flow-enabled/
                                       — replaces the pinned integrations module in management
```

```bash
git clone git@github.com:ValarMarkhulis/netbird.git -b v0.53.0-traffic-event-logging-unlocked
git clone git@github.com:ValarMarkhulis/management-integrations-flow-enabled.git
```

Override the netbird and integrations paths with `NETBIRD_SRC` and
`INTEGRATIONS_SRC` in `.env`. Whatever is checked out, including uncommitted
changes, is what gets built.

Requirements: Docker with Compose v2, `make`, `curl`, `jq`, and a kernel with
WireGuard, TUN and SCTP support (a standard Linux or WSL2 kernel works).

## Quick start

```bash
make e2e          # build + start everything, set up the lab, start peers, generate traffic
```

Then open **http://localhost:8080** and log in with `admin@netbird.local` / `password`.

Step by step:

```bash
make up           # control plane + flow-server + ETL + Postgres
make peers        # bootstrap the lab (groups, policies, network, setup keys) and start the peers
make traffic      # allowed, blocked and routed traffic between the peers (ROUNDS=n for more)
make check-events # verify what the protocol probes reported (waits for the next flow report)
make seed         # or skip the waiting: load recorded events (see below)
make flow-events  # raw event_logs rows
make sessions     # session_records produced by the ETL
```

Events show up after the client report interval (1 min) and the next ETL run (60 s).

## Seeded data instead of live traffic

`seed/event_logs.csv` holds a recorded run of `make traffic` (518 events, 303
flows: allowed, dropped, routed, SCTP/GRE, ~45 s long-lived flows). Load it
instead of waiting for peers to report:

```bash
make up
make peers   # once after `make clean`: registers the peers; they can be stopped afterwards
make seed    # replaces the flow DB's events with the fixture, newest event = now
```

The fixture is lab-independent: `make seed-export` writes peer hostnames,
`peer:<hostname>` for NetBird IPs, `<policy>/<rule>` names and resource names
instead of keys and IDs, and `make seed` maps them onto the current lab
(public keys come from management's store). Events get fresh IDs, all times
shift so the newest event arrived just now (the batch lag between
`event_time` and `created_at` is kept), and the ETL is restarted to rebuild
sessions. It fails, leaving the DB untouched, if a fixture peer isn't
registered. Run `make seed-export` after a traffic run to refresh the fixture.

## The lab

`scripts/bootstrap.sh` replaces the allow-all `Default` policy with role-based
policies, so traffic produces every kind of event: allowed (with the policy
name), dropped, and routed through a NetBird Network.

| Peer | Group | Listens on |
|---|---|---|
| dev-laptop | developers | – |
| ops-laptop | ops | – |
| web-server | web-servers | HTTP 8000, 8443 |
| db-server | databases | TCP 5432, UDP 514, SCTP 3868 |
| kernel-server | kernel-servers | TCP 22, SCTP 3868 — **kernel WireGuard**, all others are userspace |
| lan-router | routers | routing peer for **Office LAN** (172.30.0.0/24) |
| lan-app | – (no NetBird) | HTTP 80 at 172.30.0.10, reachable only via lan-router |

| Policy (all one-way) | Protocol |
|---|---|
| developers → web-servers | TCP 8000 |
| ops → web-servers, databases | all |
| web-servers → databases | TCP 5432 |
| ops → kernel-servers | all |
| developers → kernel-servers | TCP 22 |
| developers → office-lan (Office LAN resource) | TCP 80 |
| ops → office-lan | all |

`make traffic` runs 15 probes (e.g. dev → web:8000 allowed, dev → web:8443
dropped, web → db UDP 514 dropped, dev → lan-app ping dropped at the router)
and flags any result that disagrees with the policies.

Protocols, by where the event comes from (NetBird v0.53.0 client):

| Protocol | Userspace peer (uspfilter) | Kernel peer (kernel conntrack) |
|---|---|---|
| TCP, UDP, ICMP | start / end / drop | start / end, never drops |
| SCTP, GRE, any other IP protocol | nothing: inbound packets are dropped silently before any policy check, so even allowed SCTP fails | start / end; SCTP with ports, others stored by number (e.g. `47`) |

`make traffic` sends SCTP and raw GRE between the peers, and `make check-events`
checks the dashboard API for both the events that must appear and the ones
that must not. Not covered: `ProtocolUnknown (0)` (userspace only reports it
for blocked 6in4 packets), and other protocol numbers than GRE — the kernel
keeps a port-less conntrack entry for 10 minutes, so repeat runs report nothing new.
The ETL only builds sessions from TCP/UDP/ICMP/ICMPv6; other protocols appear
in Traffic Events but not in `session_records`.

Things that shape which events appear:

- Peers run userspace WireGuard (`NB_WG_KERNEL_DISABLED=true`). Only NetBird's
  userspace firewall reports `TYPE_DROP`; in kernel/iptables mode denied
  packets vanish without an event.
- `lan-router` sets `NB_FORCE_USERSPACE_ROUTER=true` for the same reason on
  routed traffic. Without it a denied routed ping shows up as a normal
  start/end flow, because the kernel drops it after NetBird has recorded it.
- Traffic between peers with no policy at all (e.g. dev → db) never enters a
  tunnel — the peers don't know each other — so it only shows as an egress
  `TYPE_START` on the sender.
- Peers report flow events in batches (every `FLOW_INTERVAL_MINUTES`).
  Timestamps in Traffic Events come from the peer's own event time
  (`event_logs.event_time`), not the batch's arrival time (`created_at`).
  `make traffic` also runs ~45 s flows (`LONG_SECONDS`) so durations are visible.
- UDP 53 and 5353 (NetBird's DNS forwarder port) are excluded from flow logs
  unless DNS collection is enabled, so don't use them for test listeners.

## What runs

```
browser ─► dashboard :8080 ─► management :8081 ─► flow-postgres (NB_NETWORK_TRAFFIC_POSTGRES_DSN)
   └────► dex :5556                 │                     ▲
                                    │ flow config + token │ sessions
lab peers ◄── tunnels ──► lab peers ┘                     │
   └──── flow events (gRPC) ──► flow-server :9000 ──► event_logs ──► etl
```

| Service | Host port | Notes |
|---|---|---|
| dashboard | 8080 | `netbirdio/dashboard:${DASHBOARD_TAG:-v2.80.0}`, `NETBIRD_LICENSED=true` |
| management | 8081 | HTTP API and gRPC on the same port |
| dex | 5556 | OIDC provider, user `admin@netbird.local` / `password` |
| flow-postgres | 5433 | `netbird` / `netbird`, db `netbird_traffic_event_logging` |
| flow-server | 9000 | usable with the logger repo's `client/csv-replay` |
| signal, relay | – | internal only |

Peers reach everything by its docker DNS name, so the flow URL handed to
clients is `http://flow-server:9000`. Management and flow-server share
`FLOW_SECRET`, so the HMAC token path is exercised.

## Rebuilding after code changes

```bash
docker compose up -d --build management                # netbird management or integrations change
docker compose up -d --build flow-server etl           # traffic-event-logger change
docker compose --profile peers up -d --build          # client change
```

## Knobs (`.env`)

| Variable | Default | |
|---|---|---|
| `FLOW_INTERVAL_MINUTES` | 1 | client flow report interval |
| `ETL_INTERVAL` | 60 | seconds between ETL runs |
| `FLOW_SECRET` | `local-e2e-flow-secret` | shared by management and flow-server |
| `DASHBOARD_TAG` | `v2.80.0` | dashboard image tag |
| `MGMT_LOG_LEVEL` / `PEER_LOG_LEVEL` | debug / info | |

## Other commands

`make logs s=management`, `make status`, `make psql`, `make ps`, `make down`
(keeps data), `make clean` (wipes the account, peers, flow DB and setup keys).

## Notes

- Dex's issuer is `http://localhost:5556/dex` (the URL the browser uses);
  management fetches the signing keys from `http://dex:5556/dex/keys` over the
  docker network instead, so `OIDCConfigEndpoint` is left empty on purpose.
- There is no IdP manager (`ManagerType: none`), so the dashboard doesn't
  show user names or emails.
- Per-account flow settings live only in memory in the integrations module.
  A management restart resets them to `NB_FlowEnabled=true`.
- Dashboard v2.80.0 over plain HTTP: the image's init script writes the CSP
  into nginx's config after nginx has started and never reloads it (only the
  Let's Encrypt path does), leaving an https-only CSP that blocks Dex and the
  API ("Error: Unauthenticated"). The compose file runs the init script before
  supervisord and adds `NETBIRD_CSP=http://localhost:5556`. `make login-check`
  verifies the login in headless Chromium.
- Run `make` from this folder, or `docker compose` from this folder. From
  anywhere else, set `E2E_DIR` to this folder's absolute path: Compose resolves
  the NetBird Dockerfile relative to the netbird checkout, so it needs it.
- Every secret in `config/` and `docker-compose.yml` (Dex password hash, store
  encryption key, relay and flow secrets) is a lab-only value, committed on
  purpose. Don't reuse them anywhere real.
