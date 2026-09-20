package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/config"
	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

// recordingWriter records the order in which messages reach the writer. It sleeps a random
// moment before recording so that goroutine scheduling gets every chance to reorder.
type recordingWriter struct {
	mu   sync.Mutex
	seen map[string][]float64
	n    int
}

func (w *recordingWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	time.Sleep(time.Duration(rand.Intn(30)) * time.Microsecond)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range msgs {
		var tick struct {
			TotalVolume float64 // carries the sequence number
		}
		if err := json.Unmarshal(m.Value, &tick); err != nil {
			return err
		}
		w.seen[string(m.Key)] = append(w.seen[string(m.Key)], tick.TotalVolume)
		w.n++
	}
	return nil
}

func (w *recordingWriter) Close() error { return nil }

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

// Consecutive ticks of one instrument must stay in order however many workers publish.
func TestPublisherKeepsEachInstrumentsMessagesInOrder(t *testing.T) {
	cfg := config.Default()
	cfg.Publisher.Workers = 4
	w := &recordingWriter{seen: map[string][]float64{}}
	pub := &KafkaPublisher{writer: w, config: &cfg}

	const instruments, perInstrument = 20, 800
	input := make(chan *model.RawTick, 256)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Start(ctx, input)

	go func() {
		for i := 0; i < perInstrument; i++ {
			for k := 0; k < instruments; k++ {
				// the sequence number rides in the volume; publish() serialises the tick as JSON
				input <- &model.RawTick{ID: fmt.Sprintf("I%d.ETR", k), Exchange: "ETR", SecType: "E",
					TotalVolume: float64(i), TradingTime: time.Unix(int64(i), 0)}
			}
		}
		close(input)
	}()

	deadline := time.Now().Add(20 * time.Second)
	for w.count() < instruments*perInstrument && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if w.count() != instruments*perInstrument {
		t.Fatalf("expected %d messages, got %d", instruments*perInstrument, w.count())
	}

	for id, seqs := range w.seen {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] < seqs[i-1] {
				t.Fatalf("%s: message %v arrived after %v (out of order)", id, seqs[i], seqs[i-1])
			}
		}
	}
}

func TestShardOfIsStableAndInRange(t *testing.T) {
	for _, id := range []string{"A.ETR", "SAP.ETR", "RDSA.NL", ""} {
		first := shardOf(id, 4)
		if first < 0 || first >= 4 {
			t.Errorf("%q: shard %d out of range", id, first)
		}
		for i := 0; i < 10; i++ {
			if shardOf(id, 4) != first {
				t.Errorf("%q: shard is not stable", id)
			}
		}
	}
}

// The old design, kept as a check that this test can see the problem: workers pulling
// from one shared channel do reorder an instrument's messages.
func TestSharedChannelWorkersWouldReorder(t *testing.T) {
	cfg := config.Default()
	w := &recordingWriter{seen: map[string][]float64{}}
	pub := &KafkaPublisher{writer: w, config: &cfg}

	input := make(chan *model.RawTick, 256)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tick := range input {
				pub.publish(context.Background(), tick)
			}
		}()
	}
	for i := 0; i < 3000; i++ {
		input <- &model.RawTick{ID: "ONE.ETR", Exchange: "ETR", SecType: "E", TotalVolume: float64(i), TradingTime: time.Unix(int64(i), 0)}
	}
	close(input)
	wg.Wait()

	reordered := 0
	seqs := w.seen["ONE.ETR"]
	for i := 1; i < len(seqs); i++ {
		if seqs[i] < seqs[i-1] {
			reordered++
		}
	}
	t.Logf("shared-channel workers reordered %d of %d consecutive messages", reordered, len(seqs)-1)
	if reordered == 0 {
		t.Skip("no reordering happened this time (it is scheduling dependent); the sharded publisher does not depend on luck")
	}
}
