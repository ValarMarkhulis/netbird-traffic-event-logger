SHELL := /bin/bash
# docker-compose.yml needs this folder's absolute path for netbird.Dockerfile.
export E2E_DIR := $(CURDIR)
COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T flow-postgres psql -U netbird -d netbird_traffic_event_logging

.PHONY: up build bootstrap peers e2e traffic check-events seed seed-export login-check status flow-events sessions psql logs ps down clean

## up: build and start the control plane + traffic event logger
up:
	$(COMPOSE) up -d --build --wait

## build: rebuild images from the local repos without starting
build:
	$(COMPOSE) --profile peers build

## bootstrap: create the account and a setup key for the peers (idempotent)
bootstrap:
	./scripts/bootstrap.sh

## peers: start the lab peers (dev-laptop, ops-laptop, web-server, db-server, lan-router) and lan-app
peers: bootstrap
	$(COMPOSE) --profile peers up -d --build

## e2e: everything — up, bootstrap, peers, then generate traffic
e2e: up peers
	@echo "Waiting 30s for peers to connect..."; sleep 30
	./scripts/traffic.sh

## traffic: generate allowed/blocked/routed traffic between the lab peers (ROUNDS=n)
traffic:
	./scripts/traffic.sh

## check-events: verify what the protocol probes of the last traffic run reported (waits for the next flow report)
check-events:
	./scripts/check-events.sh

## seed: replace the flow DB's events with seed/event_logs.csv, shifted to now (peers must be registered, not running)
seed:
	./scripts/seed.sh

## seed-export: write the flow DB's current events to seed/event_logs.csv
seed-export:
	./scripts/seed-export.sh

## login-check: log in to the dashboard with headless Chromium and report browser errors
login-check:
	docker run --rm --network host -v "$(CURDIR)/scripts:/scripts:ro" -w /tmp mcr.microsoft.com/playwright:v1.48.2-jammy \
		bash -c 'npm i -s playwright@1.48.2 >/dev/null 2>&1 && cp /scripts/login-check.mjs . && node login-check.mjs'

## status: NetBird connection summary for every lab peer
status:
	@for p in dev-laptop ops-laptop web-server db-server kernel-server lan-router; do \
		printf '%-11s ' $$p; $(COMPOSE) exec -T $$p netbird status | grep -E 'Management|Peers count' | tr '\n' ' '; echo; \
	done

## flow-events: latest raw events written by flow-server
flow-events:
	$(PSQL) -c "SELECT * FROM event_logs ORDER BY id DESC LIMIT 20;"

## sessions: sessions aggregated by the ETL
sessions:
	$(PSQL) -c "SELECT * FROM session_records ORDER BY updated_at DESC LIMIT 20;"

## psql: interactive shell on the flow-event database
psql:
	$(COMPOSE) exec flow-postgres psql -U netbird -d netbird_traffic_event_logging

## logs: follow logs (all, or one service with s=management)
logs:
	$(COMPOSE) --profile peers logs -f $(s)

ps:
	$(COMPOSE) --profile peers ps

## down: stop everything, keep data
down:
	$(COMPOSE) --profile peers down

## clean: stop everything and delete all data (account, peers, flow events, setup key)
clean:
	$(COMPOSE) --profile peers down -v
	sed -i '/^PEER_SETUP_KEY=/d; /^SETUP_KEY_/d' .env 2>/dev/null || true
