// Package kafkalog is the durable driven adapter for port.EventLog, backed by
// Kafka/Redpanda via franz-go.
//
// Why this exists
// ---------------
// In the MVP the event log is a Go channel: fast, but it dies with the process.
// Backing it with a real log makes the architecture's central claim true — the
// engine's output is a durable, ordered, replayable stream, and settlement is a
// consumer that can restart from its committed offset.
//
// Ordering
// --------
// Every event is keyed by SYMBOL. With one partition per symbol, Kafka
// guarantees per-symbol ordering, which is exactly the guarantee the order book
// needs (a single book must see a single total order). Sharding to more symbols
// means more partitions, which is the documented horizontal scaling path.
//
// Delivery semantics
// ------------------
// Consumption is at-least-once: offsets are committed for records already
// handed to the consumer, so a crash can redeliver. That is deliberate — losing
// a settled trade is far worse than replaying one — and it is safe because
// settlement guards every event with the idempotency journal
// (port.EventJournal). Exactly-once would require settlement to acknowledge
// offsets itself; that coupling is the documented next step, not the MVP.
package kafkalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/meli/orderbook/internal/core/domain"
)

// Config holds the broker settings for the log.
type Config struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	// Buffer sizes the in-process queues that decouple the engine goroutine
	// from network I/O.
	Buffer int
}

// Log is a Kafka/Redpanda-backed port.EventLog.
//
// Publish never performs network I/O inline: it hands the event to an internal
// queue drained by a producer goroutine. That keeps the single-writer engine
// goroutine free of broker latency, which is essential at thousands of
// events/second.
type Log struct {
	client *kgo.Client
	topic  string
	logger *slog.Logger

	in  chan domain.Event // engine -> producer goroutine
	out chan domain.Event // consumer goroutine -> settlement

	startConsumer sync.Once
	closeOnce     sync.Once
	producerDone  chan struct{}
	consumerStop  context.CancelFunc
	consumerDone  chan struct{}
}

// New connects to the brokers, ensures the topic exists, and starts the
// producer loop. It waits up to `wait` for the brokers to become reachable,
// since containers routinely start out of order.
func New(ctx context.Context, cfg Config, wait time.Duration, logger *slog.Logger) (*Log, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Buffer <= 0 {
		cfg.Buffer = 1 << 16
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		// Start from the beginning the first time this group runs, so no event
		// produced before settlement joined is missed.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Commit only what we explicitly mark as handed downstream.
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(time.Second),
		// Batch aggressively: throughput over per-event latency.
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	if err := waitForBrokers(ctx, client, wait); err != nil {
		client.Close()
		return nil, err
	}
	if err := ensureTopic(ctx, cfg.Brokers, cfg.Topic); err != nil {
		client.Close()
		return nil, err
	}

	l := &Log{
		client:       client,
		topic:        cfg.Topic,
		logger:       logger,
		in:           make(chan domain.Event, cfg.Buffer),
		out:          make(chan domain.Event, 1024),
		producerDone: make(chan struct{}),
		consumerDone: make(chan struct{}),
	}
	go l.produceLoop()
	return l, nil
}

// waitForBrokers polls until the cluster answers a metadata request.
func waitForBrokers(ctx context.Context, client *kgo.Client, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := client.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("kafka unreachable after %s: %w", wait, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ensureTopic creates the topic with a single partition if it does not exist.
//
// One partition is intentional: it is what guarantees a total order for this
// symbol's book. Relying on broker auto-creation could yield multiple
// partitions and silently break ordering.
func ensureTopic(ctx context.Context, brokers []string, topic string) error {
	admClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("create admin client: %w", err)
	}
	defer admClient.Close()

	adm := kadm.NewClient(admClient)
	createCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(createCtx, 1, 1, nil, topic)
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	for _, t := range resp {
		// TopicAlreadyExists is the expected outcome on every restart.
		if t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", topic, t.Err)
		}
	}
	return nil
}

// --- port.EventPublisher ---

// Publish queues an event for asynchronous production.
//
// It is called only from the engine goroutine. The send is a channel hand-off
// so the engine is never blocked on the broker; if the queue is saturated the
// engine does block, which is intentional backpressure rather than silent loss.
func (l *Log) Publish(ev domain.Event) {
	l.in <- ev
}

// produceLoop drains the queue and produces to Kafka. franz-go batches
// internally, so per-event overhead stays low.
func (l *Log) produceLoop() {
	defer close(l.producerDone)
	for ev := range l.in {
		payload, err := json.Marshal(ev)
		if err != nil {
			l.logger.Error("marshal event failed", "event", ev.ID, "err", err)
			continue
		}
		rec := &kgo.Record{
			Topic: l.topic,
			Key:   []byte(symbolOf(ev)),
			Value: payload,
		}
		l.client.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
			if err != nil {
				// The event is lost for this consumer generation. In production
				// this is a paging alert: the ledger's input stream is broken.
				l.logger.Error("produce event failed", "event", ev.ID, "err", err)
			}
		})
	}
}

// symbolOf extracts the partition key. All events for one instrument must share
// a key so they land on the same partition and stay ordered.
func symbolOf(ev domain.Event) string {
	switch {
	case ev.Trade != nil:
		return ev.Trade.Symbol
	case ev.Order != nil:
		return ev.Order.Symbol
	default:
		return "unknown"
	}
}

// --- port.EventLog ---

// Events returns the stream consumed by settlement. The consumer goroutine is
// started on first call.
func (l *Log) Events() <-chan domain.Event {
	l.startConsumer.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		l.consumerStop = cancel
		go l.consumeLoop(ctx)
	})
	return l.out
}

func (l *Log) consumeLoop(ctx context.Context) {
	defer close(l.consumerDone)
	defer close(l.out)

	for {
		if ctx.Err() != nil {
			return
		}
		fetches := l.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if ctx.Err() == nil {
				l.logger.Error("fetch error", "topic", topic, "partition", partition, "err", err)
			}
		})

		var stop bool
		fetches.EachRecord(func(rec *kgo.Record) {
			if stop {
				return
			}
			var ev domain.Event
			if err := json.Unmarshal(rec.Value, &ev); err != nil {
				l.logger.Error("unmarshal event failed", "offset", rec.Offset, "err", err)
				l.client.MarkCommitRecords(rec) // poison pill: skip it
				return
			}
			select {
			case l.out <- ev:
				// Mark only after the event is handed downstream. Settlement's
				// journal makes any redelivery harmless.
				l.client.MarkCommitRecords(rec)
			case <-ctx.Done():
				stop = true
			}
		})
	}
}

// Close stops accepting new events, flushes everything still in flight to the
// broker, then stops the consumer.
//
// Order matters: the producer must drain BEFORE the consumer stops, otherwise
// events produced during shutdown would never be settled in this run.
func (l *Log) Close() {
	l.closeOnce.Do(func() {
		// 1) Stop intake and let the producer goroutine drain the queue.
		close(l.in)
		<-l.producerDone

		// 2) Flush batches sitting in the client buffer.
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := l.client.Flush(flushCtx); err != nil {
			l.logger.Error("flush producer failed", "err", err)
		}
		cancel()

		// 3) Stop the consumer (if it was ever started) and commit offsets.
		if l.consumerStop != nil {
			l.consumerStop()
			<-l.consumerDone
		}
		commitCtx, cancelCommit := context.WithTimeout(context.Background(), 5*time.Second)
		if err := l.client.CommitMarkedOffsets(commitCtx); err != nil {
			l.logger.Error("commit offsets failed", "err", err)
		}
		cancelCommit()

		l.client.Close()
	})
}
