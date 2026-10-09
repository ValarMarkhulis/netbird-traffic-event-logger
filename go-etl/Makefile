.PHONY: build clean test run run-incremental run-db unit-test docker-build docker-run help

BINARY    := etl_runner
TESTS_DIR := ../tests

# Build the ETL binary
build:
	go build -o $(BINARY) .

# Remove build artifacts
clean:
	rm -f $(BINARY)

# Run all tests against expected outputs
test: build
	@pass=0; fail=0; tmp=$$(mktemp); \
	for csv in $(TESTS_DIR)/*.csv; do \
		expected="$${csv%.csv}.expected_output.txt"; \
		[ -f "$$expected" ] || continue; \
		name=$$(basename "$$csv"); \
		./$(BINARY) "$$csv" > "$$tmp" 2>&1; \
		if diff -q "$$tmp" "$$expected" > /dev/null 2>&1; then \
			echo "PASS  $$name"; pass=$$((pass+1)); \
		else \
			echo "FAIL  $$name"; \
			diff "$$tmp" "$$expected"; \
			fail=$$((fail+1)); \
		fi; \
	done; \
	rm -f "$$tmp"; \
	echo ""; \
	echo "Results: $$pass passed, $$fail failed"; \
	[ $$fail -eq 0 ]

# Run the ETL on a specific CSV file: make run CSV=../tests/foo.csv
run: build
	./$(BINARY) $(CSV)

run-db: build
	./$(BINARY) --db --window 48h

# Run ETL in incremental mode (requires Postgres env vars)
run-incremental: build
	./$(BINARY) --incremental

# Run Go unit tests
unit-test:
	go test ./...

docker-build:
	docker build -f ../docker/Dockerfile.etl -t go-etl:latest ..

docker-run:
	docker run --rm -e ETL_INTERVAL=300 -e ETL_WINDOW=172800 go-etl:latest

# Show help
help:
	@echo "Available targets:"
	@echo "  build              - Build the $(BINARY) binary"
	@echo "  clean              - Remove build artifacts"
	@echo "  unit-test          - Run Go unit tests"
	@echo "  test               - Run CSV integration tests against expected outputs in $(TESTS_DIR)"
	@echo "  run CSV=<path>     - Run the ETL on a specific CSV file (stdout only)"
	@echo "  run-incremental    - Run ETL in incremental watermark mode (requires Postgres env vars)"
	@echo "  run-db             - Run ETL in full-scan DB mode, window=48h (requires Postgres env vars)"
	@echo "  docker-build       - Build Docker image for go-etl"
	@echo "  docker-run         - Run go-etl Docker container"
	@echo "  help               - Show this help message"
