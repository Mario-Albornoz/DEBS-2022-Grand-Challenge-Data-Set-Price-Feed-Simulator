# Price Feed Simulator

High-performance market data feed simulator designed to achieve real-time throughput for feed-handler aggregators. Reads DEBS 2022 Grand Challenge trading data from CSV files and publishes to Kafka with optimized batching and compression.

## Features

- **High Throughput**: Designed for 700k+ ticks/second sustained throughput
- **Multiple Replay Modes**: Real-time, accelerated, and full-speed simulation
- **Optimized Performance**: Custom CSV parsing, object pooling, and efficient batching
- **Configurable**: YAML-based configuration for all parameters
- **Production Ready**: Graceful shutdown, comprehensive logging, and statistics tracking

## Requirements

- Go 1.26.4 or later
- Kafka broker (default: localhost:9092)
- DEBS 2022 dataset CSV files

## Installation

```bash
git clone <repository-url>
cd price-feed-simulator
go mod download
make build
```

## Configuration

Edit `config/simulator.yaml` to configure the simulator:

```yaml
kafka:
  brokers:
    - "localhost:9092"
  topic: "raw-ticks"

publisher:
  batch_size: 500
  batch_timeout_ms: 15
  compression: "snappy"
  workers: 4

simulator:
  mode: "realtime"
  acceleration_factor: 1.0
  data_dir: "data"
  file_pattern: "debs2022-gc-trading-day-*.csv"

performance:
  csv_buffer_kb: 256
  parse_workers: 8
  channel_buffer: 10000
  use_mmap: false

logging:
  stats_interval_sec: 5
  level: "info"
```

### Simulation Modes

- **realtime**: Simulates market timing using CSV timestamps
- **accelerated**: Faster-than-real-time replay (use `acceleration_factor`)
- **fullspeed**: Publishes as fast as possible, no delays

## Usage

### Basic Usage

```bash
make run
```

### Custom Configuration

```bash
./bin/simulator -config path/to/config.yaml
```

### Development

```bash
# Run tests
make test

# Run benchmarks
make benchmark

# Profile CPU
make profile-cpu

# Profile memory
make profile-mem
```

## Data Format

The simulator expects DEBS 2022 Grand Challenge CSV files with the following columns:

- Column 0: ID (e.g., "RDSA.NL")
- Column 1: SecType ("E" for equity, "I" for index)
- Column 4: Ask price
- Column 6: Bid price
- Column 14: ISIN
- Column 23: Trading time
- Column 24: Total volume

Place CSV files in the directory specified by `simulator.data_dir` (default: `data/`).

## Output Message Format

Messages are published to Kafka as JSON:

```json
{
  "ID": "RDSA.NL",
  "Exchange": "NL",
  "SecType": "E",
  "ISIN": "NL0011267392",
  "Bid": 100.50,
  "Ask": 100.75,
  "TotalVolume": 12345.67,
  "TradingTime": "2021-11-08T09:30:45Z",
  "Date": "2021-11-08T00:00:00Z",
  "Time": "2021-11-08T09:30:45Z"
}
```

## Performance Targets

- **Throughput**: 700k+ ticks/second sustained
- **Latency**: Sub-millisecond per-tick processing
- **Memory**: Bounded usage with object pooling
- **CPU**: Multi-core utilization via goroutines

## Architecture

```
File Reader → CSV Parser → Timing Simulator → Kafka Publisher
(goroutine)    (N workers)   (rate limiter)      (M workers)
     ↓              ↓              ↓                   ↓
   Raw bytes    RawTick chan   Timed chan        Kafka batches
```

## Statistics

The simulator logs statistics at regular intervals:

```
[INFO] =====================================
[INFO] Statistics (5s interval):
[INFO]   Ticks read:      3,547,890
[INFO]   Ticks published: 3,547,890
[INFO]   Throughput:      709,578 ticks/sec
[INFO]   Errors:          0
[INFO]   Bytes read:      512.3 MB
[INFO] =====================================
```

## Graceful Shutdown

Send `SIGINT` (Ctrl+C) or `SIGTERM` to gracefully shut down:

1. Stops reading new CSV rows
2. Drains pipeline channels
3. Flushes Kafka producer buffers
4. Logs final statistics
5. Closes connections

## License

DEBS 2022 Dataset: CC BY-NC-SA 4.0

## Resources

- DEBS 2022 Dataset: https://doi.org/10.5281/zenodo.6382482
- Kafka-go Documentation: https://github.com/segmentio/kafka-go
