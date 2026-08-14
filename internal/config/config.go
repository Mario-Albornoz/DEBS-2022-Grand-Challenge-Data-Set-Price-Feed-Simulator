// Package config provides configuration loading and validation for the price feed simulator.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultPath = "config/simulator.yaml"

// Config holds all configuration parameters for the simulator.
type Config struct {
	Kafka       KafkaConfig       `yaml:"kafka"`
	Publisher   PublisherConfig   `yaml:"publisher"`
	Simulator   SimulatorConfig   `yaml:"simulator"`
	Performance PerformanceConfig `yaml:"performance"`
	Logging     LoggingConfig     `yaml:"logging"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type PublisherConfig struct {
	BatchSize      int    `yaml:"batch_size"`
	BatchTimeoutMS int    `yaml:"batch_timeout_ms"`
	Compression    string `yaml:"compression"`
	Workers        int    `yaml:"workers"`
}

type SimulatorConfig struct {
	Mode               string  `yaml:"mode"`
	AccelerationFactor float64 `yaml:"acceleration_factor"`
	DataDir            string  `yaml:"data_dir"`
	FilePattern        string  `yaml:"file_pattern"`
}

type PerformanceConfig struct {
	CSVBufferKB  int  `yaml:"csv_buffer_kb"`
	ParseWorkers int  `yaml:"parse_workers"`
	ChannelBuffer int  `yaml:"channel_buffer"`
	UseMmap       bool `yaml:"use_mmap"`
}

type LoggingConfig struct {
	StatsIntervalSec int    `yaml:"stats_interval_sec"`
	Level            string `yaml:"level"`
}

// Load reads and parses the configuration file from the given path.
// If path is empty, DefaultPath is used. Returns error if file cannot be read or is invalid.
func Load(path string) (Config, error) {
	if path == "" {
		path = DefaultPath
	}

	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Default returns a Config with sensible default values.
func Default() Config {
	return Config{
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "raw-ticks",
		},
		Publisher: PublisherConfig{
			BatchSize:      500,
			BatchTimeoutMS: 15,
			Compression:    "snappy",
			Workers:        4,
		},
		Simulator: SimulatorConfig{
			Mode:               "realtime",
			AccelerationFactor: 1,
			DataDir:            "data",
			FilePattern:        "debs2022-gc-trading-day-*.csv",
		},
		Performance: PerformanceConfig{
			CSVBufferKB:  256,
			ParseWorkers: 8,
			ChannelBuffer: 10000,
			UseMmap:       false,
		},
		Logging: LoggingConfig{
			StatsIntervalSec: 5,
			Level:            "info",
		},
	}
}

// Validate checks if all required configuration fields are set and valid.
func (c Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("kafka.brokers must contain at least one broker")
	}
	if c.Kafka.Topic == "" {
		return errors.New("kafka.topic is required")
	}
	if c.Publisher.BatchSize <= 0 {
		return errors.New("publisher.batch_size must be greater than zero")
	}
	if c.Publisher.BatchTimeoutMS <= 0 {
		return errors.New("publisher.batch_timeout_ms must be greater than zero")
	}
	if c.Publisher.Workers <= 0 {
		return errors.New("publisher.workers must be greater than zero")
	}
	if c.Simulator.DataDir == "" {
		return errors.New("simulator.data_dir is required")
	}
	if c.Simulator.FilePattern == "" {
		return errors.New("simulator.file_pattern is required")
	}
	if c.Simulator.Mode != "realtime" && c.Simulator.Mode != "accelerated" && c.Simulator.Mode != "fullspeed" {
		return fmt.Errorf("simulator.mode must be realtime, accelerated, or fullspeed")
	}
	if c.Simulator.Mode == "accelerated" && c.Simulator.AccelerationFactor <= 0 {
		return errors.New("simulator.acceleration_factor must be greater than zero")
	}
	if c.Performance.CSVBufferKB <= 0 {
		return errors.New("performance.csv_buffer_kb must be greater than zero")
	}
	if c.Performance.ChannelBuffer <= 0 {
		return errors.New("performance.channel_buffer must be greater than zero")
	}
	if c.Logging.StatsIntervalSec <= 0 {
		return errors.New("logging.stats_interval_sec must be greater than zero")
	}
	return nil
}

// BatchTimeout converts BatchTimeoutMS to time.Duration.
func (c Config) BatchTimeout() time.Duration {
	return time.Duration(c.Publisher.BatchTimeoutMS) * time.Millisecond
}

// StatsInterval converts StatsIntervalSec to time.Duration.
func (c Config) StatsInterval() time.Duration {
	return time.Duration(c.Logging.StatsIntervalSec) * time.Second
}
