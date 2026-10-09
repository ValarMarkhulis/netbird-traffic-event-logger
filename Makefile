.PHONY: build-s build-c clean test run-db stop-db run-test-env stop-test-env run-production stop-production run-server run-client run-client-signature export-data help

# Build the flow-server binary
build-s:
	go build -o flow-server .

build-c:
	go build -o flow-client client/client.go

# Clean build artifacts
clean:
	rm -f flow-server flow-client

# Run tests
test:
	go test -v ./...

# Start the PostgreSQL database
run-db:
	cd docker && docker compose -f docker-compose.test-postgres-db.yml up -d

# Stop the PostgreSQL database
stop-db:
	cd docker && docker compose -f docker-compose.test-postgres-db.yml down

# Start testing environment with Docker Compose
run-test-env:
	cd docker && docker compose -f docker-compose.testing.yml up -d --build

# Stop testing environment
stop-test-env:
	cd docker && docker compose -f docker-compose.testing.yml down

# Start production environment with Docker Compose
run-production:
	cd docker && docker compose -f docker-compose.production.yml up -d --build 

# Stop production environment
stop-production:
	cd docker && docker compose -f docker-compose.production.yml down

# Run the server (requires database to be running)
run-server: build-s
	./flow-server

# Run the client (requires flow-server to be running)
run-client-signature: build-c
	FLOW_SERVER_ADDR=tcp://localhost:9000 NB_FLOW_SECRET=test-signature ./flow-client

# Run the client (requires flow-server to be running)
run-client: build-c
	./flow-client

# Export data from the database (requires a database to be running )
export-data:
	docker exec -t netbird_traffic_postgres psql -U netbird -d netbird_traffic_event_logging -c "COPY (select * from event_logs)  To STDOUT DELIMITER ',' CSV HEADER;" > test.csv

# Show help
help:
	@echo "Available commands:"
	@echo "  build-s              - Build the flow-server binary"
	@echo "  build-c              - Build the flow-client binary"
	@echo "  clean                - Clean build artifacts"
	@echo "  test                 - Run Go unit tests"
	@echo "  run-db               - Start PostgreSQL database only"
	@echo "  stop-db              - Stop PostgreSQL database"
	@echo "  run-test-env         - Start testing environment (Postgres + flow-server + ETL)"
	@echo "  stop-test-env        - Stop testing environment"
	@echo "  run-production       - Start production environment (Caddy + Postgres + flow-server + ETL)"
	@echo "  stop-production      - Stop production environment"
	@echo "  run-server           - Build and run the flow-server locally (needs DB)"
	@echo "  run-client           - Build and run the client without authentication"
	@echo "  run-client-signature - Build and run the client with NB_FLOW_SECRET=test-signature"
	@echo "  export-data          - Export event_logs table to test.csv"
	@echo "  help                 - Show this help message"