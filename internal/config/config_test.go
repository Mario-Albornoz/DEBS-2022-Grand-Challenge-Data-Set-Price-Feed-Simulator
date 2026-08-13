package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	if len(cfg.Kafka.Brokers) == 0 {
		t.Error("Expected default brokers to be set")
	}
	if cfg.Kafka.Topic != "raw-ticks" {
		t.Errorf("Expected topic 'raw-ticks', got %q", cfg.Kafka.Topic)
	}
	if cfg.Publisher.BatchSize != 500 {
		t.Errorf("Expected batch size 500, got %d", cfg.Publisher.BatchSize)
	}
	if cfg.Publisher.Workers != 4 {
		t.Errorf("Expected 4 workers, got %d", cfg.Publisher.Workers)
	}
	if cfg.Simulator.Mode != "realtime" {
		t.Errorf("Expected realtime mode, got %q", cfg.Simulator.Mode)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		wantError bool
	}{
		{
			name:      "valid config",
			config:    Default(),
			wantError: false,
		},
		{
			name: "empty brokers",
			config: Config{
				Kafka:       KafkaConfig{Brokers: []string{}, Topic: "test"},
				Publisher:   Default().Publisher,
				Simulator:   Default().Simulator,
				Performance: Default().Performance,
				Logging:     Default().Logging,
			},
			wantError: true,
		},
		{
			name: "empty topic",
			config: Config{
				Kafka:       KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: ""},
				Publisher:   Default().Publisher,
				Simulator:   Default().Simulator,
				Performance: Default().Performance,
				Logging:     Default().Logging,
			},
			wantError: true,
		},
		{
			name: "invalid batch size",
			config: Config{
				Kafka: Default().Kafka,
				Publisher: PublisherConfig{
					BatchSize:      0,
					BatchTimeoutMS: 15,
					Compression:    "snappy",
					Workers:        4,
				},
				Simulator:   Default().Simulator,
				Performance: Default().Performance,
				Logging:     Default().Logging,
			},
			wantError: true,
		},
		{
			name: "invalid mode",
			config: Config{
				Kafka:     Default().Kafka,
				Publisher: Default().Publisher,
				Simulator: SimulatorConfig{
					Mode:               "invalid",
					AccelerationFactor: 1.0,
					DataDir:            "data",
					FilePattern:        "*.csv",
				},
				Performance: Default().Performance,
				Logging:     Default().Logging,
			},
			wantError: true,
		},
		{
			name: "negative acceleration factor",
			config: Config{
				Kafka:     Default().Kafka,
				Publisher: Default().Publisher,
				Simulator: SimulatorConfig{
					Mode:               "accelerated",
					AccelerationFactor: -1.0,
					DataDir:            "data",
					FilePattern:        "*.csv",
				},
				Performance: Default().Performance,
				Logging:     Default().Logging,
			},
			wantError: true,
		},
		{
			name: "zero workers",
			config: Config{
				Kafka: Default().Kafka,
				Publisher: PublisherConfig{
					BatchSize:      500,
					BatchTimeoutMS: 15,
					Compression:    "snappy",
					Workers:        0,
				},
				Simulator:   Default().Simulator,
				Performance: Default().Performance,
				Logging:     Default().Logging,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Run("load valid config", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "test-config.yaml")

		validYAML := `
kafka:
  brokers:
    - "localhost:9092"
  topic: "test-topic"

publisher:
  batch_size: 100
  batch_timeout_ms: 10
  compression: "gzip"
  workers: 2

simulator:
  mode: "fullspeed"
  acceleration_factor: 1.0
  data_dir: "testdata"
  file_pattern: "*.csv"

performance:
  csv_buffer_kb: 128
  parse_workers: 4
  channel_buffer: 5000
  use_mmap: true

logging:
  stats_interval_sec: 10
  level: "debug"
`
		if err := os.WriteFile(configPath, []byte(validYAML), 0644); err != nil {
			t.Fatalf("Failed to write test config: %v", err)
		}

		cfg, err := Load(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		if cfg.Kafka.Topic != "test-topic" {
			t.Errorf("Expected topic 'test-topic', got %q", cfg.Kafka.Topic)
		}
		if cfg.Publisher.BatchSize != 100 {
			t.Errorf("Expected batch size 100, got %d", cfg.Publisher.BatchSize)
		}
		if cfg.Simulator.Mode != "fullspeed" {
			t.Errorf("Expected mode 'fullspeed', got %q", cfg.Simulator.Mode)
		}
		if !cfg.Performance.UseMmap {
			t.Error("Expected UseMmap to be true")
		}
	})

	t.Run("load non-existent file", func(t *testing.T) {
		_, err := Load("non-existent-file.yaml")
		if err == nil {
			t.Error("Expected error for non-existent file")
		}
	})

	t.Run("load invalid yaml", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "invalid.yaml")

		invalidYAML := `
this is not: valid: yaml: content
  - invalid
    nesting
`
		if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
			t.Fatalf("Failed to write test config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("Expected error for invalid YAML")
		}
	})

	t.Run("load config with validation errors", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "invalid-values.yaml")

		invalidYAML := `
kafka:
  brokers: []
  topic: ""

publisher:
  batch_size: -1
  batch_timeout_ms: 10
  compression: "snappy"
  workers: 1

simulator:
  mode: "realtime"
  acceleration_factor: 1.0
  data_dir: "data"
  file_pattern: "*.csv"

performance:
  csv_buffer_kb: 128
  parse_workers: 4
  channel_buffer: 5000
  use_mmap: false

logging:
  stats_interval_sec: 5
  level: "info"
`
		if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
			t.Fatalf("Failed to write test config: %v", err)
		}

		_, err := Load(configPath)
		if err == nil {
			t.Error("Expected validation error")
		}
	})
}

func TestBatchTimeout(t *testing.T) {
	cfg := Config{
		Publisher: PublisherConfig{
			BatchTimeoutMS: 20,
		},
	}

	timeout := cfg.BatchTimeout()
	expected := "20ms"
	if timeout.String() != expected {
		t.Errorf("Expected %s, got %s", expected, timeout)
	}
}

func TestStatsInterval(t *testing.T) {
	cfg := Config{
		Logging: LoggingConfig{
			StatsIntervalSec: 10,
		},
	}

	interval := cfg.StatsInterval()
	expected := "10s"
	if interval.String() != expected {
		t.Errorf("Expected %s, got %s", expected, interval)
	}
}
