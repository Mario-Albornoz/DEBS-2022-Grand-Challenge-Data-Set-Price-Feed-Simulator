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
	Published uint64
	Delivered uint64
	Failed    uint64
}

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaPublisher struct {
	writer messageWriter
	config *config.Config
	stats  PublisherStats
}

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
		Async:        true,
	}

	p := &KafkaPublisher{
		writer: writer,
		config: cfg,
	}

	writer.Completion = func(messages []kafka.Message, err error) {
		if err != nil {
			atomic.AddUint64(&p.stats.Failed, uint64(len(messages)))
			return
		}
		atomic.AddUint64(&p.stats.Delivered, uint64(len(messages)))
	}

	return p, nil
}

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

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

func (p *KafkaPublisher) GetStats() PublisherStats {
	return PublisherStats{
		Published: atomic.LoadUint64(&p.stats.Published),
		Delivered: atomic.LoadUint64(&p.stats.Delivered),
		Failed:    atomic.LoadUint64(&p.stats.Failed),
	}
}
