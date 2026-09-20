// Package publisher provides high-throughput Kafka message publishing.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

type PublisherStats struct {
	Published uint64 // messages handed to the writer
	Delivered uint64 // messages Kafka acknowledged (the writer is asynchronous)
	Failed    uint64 // messages that could not be marshalled, enqueued or delivered
}

// messageWriter is the part of kafka.Writer the publisher uses.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	writer messageWriter
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

	p := &KafkaPublisher{
		writer: writer,
		config: cfg,
	}

	// The writer is asynchronous, so WriteMessages never reports a delivery error; only
	// this callback does. Without it a lost message is invisible.
	writer.Completion = func(messages []kafka.Message, err error) {
		if err != nil {
			atomic.AddUint64(&p.stats.Failed, uint64(len(messages)))
			return
		}
		atomic.AddUint64(&p.stats.Delivered, uint64(len(messages)))
	}

	return p, nil
}

// Start begins publishing messages from the input channel using multiple workers.
// Blocks until context is cancelled or channel is closed.
//
// The messages of one instrument must reach Kafka in the order they were produced: the
// feed-handler's timestamp check, timing features and silence detection all assume it.
// Workers that pull from a shared channel break that (two consecutive ticks of one
// instrument can be picked up by different workers and enqueued in either order), so
// each tick is routed to the worker that owns its instrument, and every worker publishes
// its queue sequentially.
func (p *KafkaPublisher) Start(ctx context.Context, input <-chan *model.RawTick) error {
	workers := p.config.Publisher.Workers
	if workers < 1 {
		workers = 1
	}

	queues := make([]chan *model.RawTick, workers)
	for i := range queues {
		queues[i] = make(chan *model.RawTick, 1024)
		go p.publishWorker(ctx, queues[i])
	}

	go func() {
		defer func() {
			for _, q := range queues {
				close(q)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case tick, ok := <-input:
				if !ok {
					return
				}
				select {
				case queues[shardOf(tick.ID, workers)] <- tick:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	<-ctx.Done()
	return p.Close()
}

// shardOf maps an instrument to the worker that publishes it.
func shardOf(id string, workers int) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32() % uint32(workers))
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
		Delivered: atomic.LoadUint64(&p.stats.Delivered),
		Failed:    atomic.LoadUint64(&p.stats.Failed),
	}
}
