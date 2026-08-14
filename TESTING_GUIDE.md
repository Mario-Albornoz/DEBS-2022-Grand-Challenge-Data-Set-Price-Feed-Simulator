# Testing Guide - Price Feed Simulator

This guide covers all testing capabilities in the price-feed-simulator project.

## Quick Reference

```bash
# Unit tests (fast)
make test-short

# All tests including integration
make test

# Throughput validation (verifies performance targets)
make test-throughput

# Benchmarks (performance measurement)
make benchmark
```

---

## Test Types Overview

### 1. Unit Tests
**Purpose**: Test individual components in isolation  
**Speed**: Fast (< 1 second per package)  
**Location**: `internal/*/`

### 2. Integration Tests
**Purpose**: Test end-to-end workflows  
**Speed**: Medium (1-10 seconds)  
**Location**: `test/integration/`

### 3. Benchmarks
**Purpose**: Measure performance with Go's benchmarking framework  
**Speed**: Variable (depends on benchtime)  
**Location**: `test/benchmark/`

### 4. Throughput Validation Tests
**Purpose**: Verify actual performance targets are met  
**Speed**: Slow (1-2 minutes total)  
**Location**: `test/benchmark/throughput_validation_test.go`

---

## Throughput Validation Tests

These tests verify that the simulator meets its performance targets.

### Available Tests

#### 1. CSV Parser Throughput
```bash
go test ./test/benchmark -run TestCSVParserThroughput -v
```

**What it tests**: CSV parsing speed  
**Target**: 100,000+ ticks/second  
**Test data**: 100,000 rows

**Example Output**:
```
=== RUN   TestCSVParserThroughput
    Parsed 100000 ticks in 0.18 seconds
    Throughput: 569957 ticks/second
    Target: 100,000+ ticks/second for CSV parsing
    ✓ Throughput meets target (569957 >= 100000 ticks/sec)
--- PASS: TestCSVParserThroughput (0.18s)
```

#### 2. Channel Throughput
```bash
go test ./test/benchmark -run TestChannelThroughput -v
```

**What it tests**: Channel communication speed  
**Target**: 500,000+ ticks/second  
**Test data**: 1,000,000 messages

**Example Output**:
```
=== RUN   TestChannelThroughput
    Sent and received 1000000 ticks in 0.31 seconds
    Throughput: 3247445 ticks/second
    Target: 500,000+ ticks/second for channel operations
    ✓ Throughput meets target (3247445 >= 500000 ticks/sec)
--- PASS: TestChannelThroughput (0.31s)
```

#### 3. Full Pipeline Throughput
```bash
go test ./test/benchmark -run TestFullPipelineThroughput -v
```

**What it tests**: Complete data processing pipeline  
**Target**: 100,000+ ticks/second  
**Test data**: 50,000 rows through full pipeline

**Example Output**:
```
=== RUN   TestFullPipelineThroughput
    Full pipeline processed 50000 ticks in 0.08 seconds
    Throughput: 607145 ticks/second
    Target: 100,000+ ticks/second for full pipeline
    ✓ Pipeline throughput meets target (607145 >= 100000 ticks/sec)
--- PASS: TestFullPipelineThroughput (0.08s)
```

#### 4. Object Pool Performance
```bash
go test ./test/benchmark -run TestObjectPoolPerformance -v
```

**What it tests**: Memory pool efficiency  
**Compares**: With pool vs without pool

**Example Output**:
```
=== RUN   TestObjectPoolPerformance
=== RUN   TestObjectPoolPerformance/with_pool
    With pool: 45234567 operations/sec
=== RUN   TestObjectPoolPerformance/without_pool
    Without pool: 23456789 operations/sec
--- PASS: TestObjectPoolPerformance (4.42s)
```

### Running All Throughput Tests

```bash
# Using Makefile
make test-throughput

# Or directly with go test
go test ./test/benchmark -run TestThroughput -v -timeout 5m
```

### Interpreting Results

✅ **PASS** + "✓ Throughput meets target" = Performance is good  
❌ **FAIL** or warning = Performance needs optimization

---

## Benchmarks

Benchmarks measure performance using Go's built-in benchmarking framework.

### Running Benchmarks

```bash
# All benchmarks
make benchmark

# Specific benchmark
go test ./test/benchmark -bench=BenchmarkCSVParsing -benchmem

# With custom duration
go test ./test/benchmark -bench=. -benchtime=10s
```

### Available Benchmarks

| Benchmark | What It Measures |
|-----------|------------------|
| `BenchmarkCSVParsing` | Complete file parsing |
| `BenchmarkCSVParseRow` | Single row parsing |
| `BenchmarkObjectPooling` | Pool vs no pool |
| `BenchmarkChannelThroughput` | Channel operations |
| `BenchmarkFullPipeline` | End-to-end pipeline |
| `BenchmarkMemoryAllocations` | Memory allocations |

### Reading Benchmark Output

```
BenchmarkCSVParsing-8    1000    1234567 ns/op    5000 B/op    100 allocs/op
```

- `BenchmarkCSVParsing-8`: Benchmark name with 8 CPU cores
- `1000`: Number of iterations
- `1234567 ns/op`: Nanoseconds per operation (lower is better)
- `5000 B/op`: Bytes allocated per operation (lower is better)
- `100 allocs/op`: Allocations per operation (lower is better)

---

## Unit Tests

### Running Unit Tests

```bash
# All unit tests
go test ./internal/... -v

# Specific package
go test ./internal/config -v
go test ./internal/model -v
go test ./internal/parser -v
go test ./internal/publisher -v
go test ./internal/simulator -v

# With coverage
go test ./internal/... -cover
```

### Test Coverage by Package

```bash
# Generate coverage report
go test ./... -coverprofile=coverage.out

# View in browser
go tool cover -html=coverage.out

# View in terminal
go tool cover -func=coverage.out
```

---

## Integration Tests

### Running Integration Tests

```bash
# All integration tests
go test ./test/integration -v

# Specific test
go test ./test/integration -run TestEndToEndSimulation -v
```

### Available Integration Tests

| Test | Description |
|------|-------------|
| `TestEndToEndSimulation` | Complete CSV to Kafka flow |
| `TestMultipleFiles` | Processing multiple CSV files |
| `TestGracefulShutdown` | Clean shutdown with SIGTERM |
| `TestRealtimeMode` | Real-time timing simulation |

---

## Test Flags

### Common Flags

```bash
# Verbose output
go test ./... -v

# Run specific test
go test ./internal/parser -run TestParseRow

# Skip slow tests
go test ./... -short

# Timeout for long tests
go test ./... -timeout 10m

# Run tests multiple times
go test ./... -count=5

# Disable test cache
go test ./... -count=1

# Parallel execution
go test ./... -parallel 4
```

### Coverage Flags

```bash
# Basic coverage
go test ./... -cover

# Coverage profile
go test ./... -coverprofile=coverage.out

# Coverage by function
go tool cover -func=coverage.out

# HTML coverage report
go tool cover -html=coverage.out
```

---

## Performance Testing Workflow

### 1. Quick Check
```bash
make test-short
```

### 2. Full Validation
```bash
make test
```

### 3. Verify Throughput
```bash
make test-throughput
```

### 4. Detailed Benchmarks
```bash
make benchmark
```

### 5. Profile if Needed
```bash
make profile-cpu
make profile-mem
```

---

## CI/CD Integration

### Basic CI Pipeline

```bash
#!/bin/bash
# ci-test.sh

# Fast checks
go test ./... -short || exit 1

# Build verification
go build ./cmd/simulator || exit 1

# Full test suite
go test ./... || exit 1

# Throughput validation (optional, slow)
# go test ./test/benchmark -run TestThroughput -timeout 5m || exit 1

echo "All tests passed!"
```

---

## Troubleshooting Tests

### Tests Failing

```bash
# Clean test cache
go clean -testcache

# Verbose output
go test ./... -v

# Run specific failing test
go test ./internal/parser -run TestParseRow -v
```

### Throughput Tests Below Target

**Possible causes**:
1. CPU in power-saving mode
2. System under load
3. Disk I/O slow
4. Need to optimize code

**Solutions**:
```bash
# Check CPU governor (Linux)
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Close other applications
# Run tests again
make test-throughput

# If still failing, profile
make profile-cpu
```

### Benchmarks Inconsistent

```bash
# Run multiple times
go test ./test/benchmark -bench=BenchmarkCSVParsing -count=5

# Increase benchmark time
go test ./test/benchmark -bench=. -benchtime=10s

# Disable CPU frequency scaling
# (Linux) sudo cpupower frequency-set --governor performance
```

---

## Performance Targets Summary

| Component | Target | Test Command |
|-----------|--------|--------------|
| CSV Parser | 100k+ ticks/sec | `go test ./test/benchmark -run TestCSVParserThroughput -v` |
| Channels | 500k+ ticks/sec | `go test ./test/benchmark -run TestChannelThroughput -v` |
| Pipeline | 100k+ ticks/sec | `go test ./test/benchmark -run TestFullPipelineThroughput -v` |
| End-to-End | 700k+ ticks/sec | Run with real Kafka + data |

---

## Quick Commands Reference

```bash
# Development workflow
make test-short              # Quick validation
make build                   # Compile
make test                    # Full test suite
make test-throughput         # Verify performance

# Performance analysis
make benchmark               # Measure performance
make profile-cpu            # Find CPU bottlenecks
make profile-mem            # Find memory issues

# Specific tests
go test ./internal/parser -v                           # Parser tests
go test ./test/integration -v                          # Integration
go test ./test/benchmark -bench=. -benchmem           # All benchmarks
go test ./test/benchmark -run TestThroughput -v       # Throughput
```

---

For more details, see [DEVELOPER.md](DEVELOPER.md).
