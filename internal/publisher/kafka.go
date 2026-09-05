// Package publisher provides high-throughput Kafka message publishing.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

type PublisherStats struct {
	Published uint64
	Failed    uint64
}

type KafkaPublisher struct {
	writer *kafka.Writer
	config *config.Config
	stats  PublisherStats
}

// NewKafkaPublisher creates a new Kafka publisher with optimized settings for throughput.
func NewKafkaPublisher(cfg *config.Config) (*KafkaPublisher, error) {
	compression := kafka.Snappy
	switch cfg.Publisher.Compression {
	case "gzip":
		compression = kafka.Gzip
	case "snappy":
		compression = kafka.Snappy
	case "none":
		compression = 0
	default:
		compression = kafka.Snappy
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Kafka.Brokers...),
		Topic:        cfg.Kafka.Topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: cfg.BatchTimeout(),
		BatchSize:    cfg.Publisher.BatchSize,
		RequiredAcks: kafka.RequireOne,
		WriteTimeout: 2 * time.Second,
		MaxAttempts:  3,
		Compression:  compression,
		Async:        true, // Enable async writes for much higher throughput
	}

	return &KafkaPublisher{
		writer: writer,
		config: cfg,
	}, nil
}

// Start begins publishing messages from the input channel using multiple workers.
// Blocks until context is cancelled or channel is closed.
func (p *KafkaPublisher) Start(ctx context.Context, input <-chan *model.RawTick) error {
	for i := 0; i < p.config.Publisher.Workers; i++ {
		go p.publishWorker(ctx, input)
	}

	<-ctx.Done()
	return p.Close()
}

func (p *KafkaPublisher) publishWorker(ctx context.Context, input <-chan *model.RawTick) {
	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-input:
			if !ok {
				return
			}

			if err := p.publish(ctx, tick); err != nil {
				atomic.AddUint64(&p.stats.Failed, 1)
			} else {
				atomic.AddUint64(&p.stats.Published, 1)
			}
		}
	}
}

func (p *KafkaPublisher) publish(ctx context.Context, tick *model.RawTick) error {
	data, err := json.Marshal(tick)
	if err != nil {
		return fmt.Errorf("marshal tick: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(tick.ID),
		Value: data,
		Time:  tick.TradingTime,
	}

	return p.writer.WriteMessages(ctx, msg)
}

// Close flushes pending messages and closes the Kafka writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

// GetStats returns the current publishing statistics.
func (p *KafkaPublisher) GetStats() PublisherStats {
	return PublisherStats{
		Published: atomic.LoadUint64(&p.stats.Published),
		Failed:    atomic.LoadUint64(&p.stats.Failed),
	}
}
