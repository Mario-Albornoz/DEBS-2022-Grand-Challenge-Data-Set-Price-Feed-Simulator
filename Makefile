.PHONY: build run test test-short test-throughput benchmark benchmark-all profile-cpu profile-mem clean help

build:
	@echo "Building simulator..."
	@go build -o bin/simulator cmd/simulator/main.go

run: build
	@echo "Starting simulator..."
	@./bin/simulator

test:
	@echo "Running tests..."
	@go test ./... -v

test-short:
	@echo "Running short tests..."
	@go test ./... -short

test-throughput:
	@echo "Running throughput validation tests..."
	@echo "This will take a few minutes..."
	@go test ./test/benchmark -run TestThroughput -v -timeout 5m

benchmark:
	@echo "Running benchmarks..."
	@go test ./test/benchmark/... -bench=. -benchmem

benchmark-all:
	@echo "Running all benchmarks..."
	@go test ./... -bench=. -benchmem

profile-cpu:
	@echo "Profiling CPU..."
	@go test ./test/benchmark/... -bench=. -cpuprofile=cpu.prof
	@echo "Opening pprof..."
	@go tool pprof cpu.prof

profile-mem:
	@echo "Profiling memory..."
	@go test ./test/benchmark/... -bench=. -memprofile=mem.prof
	@echo "Opening pprof..."
	@go tool pprof mem.prof

clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f *.prof
	@echo "Done"

help:
	@echo "Available targets:"
	@echo "  build           - Build the simulator binary"
	@echo "  run             - Build and run the simulator"
	@echo "  test            - Run all tests"
	@echo "  test-short      - Run short tests"
	@echo "  test-throughput - Run throughput validation tests"
	@echo "  benchmark       - Run benchmark tests"
	@echo "  benchmark-all   - Run all benchmarks"
	@echo "  profile-cpu     - Profile CPU usage"
	@echo "  profile-mem     - Profile memory usage"
	@echo "  clean           - Remove build artifacts"
	@echo "  help            - Show this help message"
