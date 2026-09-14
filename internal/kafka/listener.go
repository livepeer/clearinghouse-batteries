// Package kafka provides Kafka event-log integration for the accounting service.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/sqlite"
	"github.com/livepeer/clearinghouse/internal/store"
	kgo "github.com/segmentio/kafka-go"
)

type Listener struct {
	DB      *store.Store
	Brokers []string
	Topic   string
	Ready   atomic.Bool
}

// OpenBroker initializes persistent storage and binds the listener before returning.
// The caller must run Serve and close the broker, including on startup failure.
func OpenBroker(ctx context.Context, bind, topic, dir string) (*minikafka.Broker, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	backend, err := sqlite.Open(filepath.Join(dir, "broker.db"), sqlite.WithWAL(true), sqlite.WithSynchronous(sqlite.SyncFull), sqlite.WithBusyTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	// MiniKafka's SQLite implementation applies connection-local pragmas on initialization.
	// Serialize store operations to reuse that connection and prevent competing deferred writes.
	serialized := &serialStore{Store: backend, topic: topic}
	if err := serialized.Init(ctx); err != nil {
		backend.Close()
		return nil, err
	}
	broker, err := minikafka.Open(minikafka.Config{Addr: bind, Store: serialized})
	if err != nil {
		backend.Close()
		return nil, err
	}
	return broker, nil
}

func (l *Listener) Run(ctx context.Context) error {
	defer l.Ready.Store(false)
	if len(l.Brokers) == 0 {
		return errors.New("Kafka bootstrap brokers required")
	}
	next, _, _, err := l.DB.Checkpoint(ctx, "kafka", l.Topic)
	if err != nil {
		return err
	}
	first, last, err := l.offsets(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if next < first || next > last {
		return fmt.Errorf("Kafka checkpoint %d outside retained offsets [%d,%d]; broker data was lost or replaced", next, first, last)
	}
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:               l.Brokers,
		Topic:                 l.Topic,
		Partition:             0,
		MinBytes:              1,
		MaxBytes:              10 << 20,
		MaxWait:               100 * time.Millisecond,
		ReadLagInterval:       -1,
		OffsetOutOfRangeError: true,
	})
	defer reader.Close()
	if err := reader.SetOffset(next); err != nil {
		return err
	}
	l.Ready.Store(true)
	slog.Info("accounting ready", "brokers", l.Brokers, "topic", l.Topic, "next_offset", next)
	for {
		msg, err := reader.ReadMessage(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if msg.Offset != next {
			return fmt.Errorf("Kafka offset gap: expected %d, received %d", next, msg.Offset)
		}
		if err := l.DB.Ingest(ctx, l.Topic, msg.Offset, msg.Value); err != nil {
			return err
		}
		next++
	}
}

// Each bootstrap gets its own timeout so an unavailable address does not prevent
// probing the remaining brokers. DialLeader discovers the partition's current leader.
func (l *Listener) offsets(ctx context.Context) (int64, int64, error) {
	var failures []error
	for _, addr := range l.Brokers {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := kgo.DialLeader(probeCtx, "tcp", addr, l.Topic, 0)
		var first, last int64
		if err == nil {
			deadline, _ := probeCtx.Deadline()
			_ = conn.SetDeadline(deadline)
			first, last, err = conn.ReadOffsets()
			_ = conn.Close()
		}
		cancel()
		if err == nil {
			return first, last, nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", addr, err))
		if ctx.Err() != nil {
			break
		}
	}
	return 0, 0, errors.Join(failures...)
}

// These forwarding methods intentionally serialize all accesses, including startup and shutdown.
type serialStore struct {
	minikafka.Store
	mu          sync.Mutex
	topic       string
	initialized bool
	initErr     error
}

func (s *serialStore) Init(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialized {
		return s.initErr
	}
	defer func() { s.initialized, s.initErr = true, err }()
	if err := s.Store.Init(ctx); err != nil {
		return err
	}
	err = s.Store.CreateTopic(ctx, s.topic, minikafka.TopicOptions{})
	if errors.Is(err, minikafka.ErrTopicExists) {
		return nil
	}
	return err
}
func (s *serialStore) Close() error { s.mu.Lock(); defer s.mu.Unlock(); return s.Store.Close() }
func (s *serialStore) CreateTopic(c context.Context, t string, o minikafka.TopicOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.CreateTopic(c, t, o)
}
func (s *serialStore) DeleteTopic(c context.Context, t string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.DeleteTopic(c, t)
}
func (s *serialStore) Topic(c context.Context, t string) (minikafka.TopicMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Topic(c, t)
}
func (s *serialStore) ListTopics(c context.Context) ([]minikafka.TopicMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.ListTopics(c)
}
func (s *serialStore) Append(c context.Context, r minikafka.AppendRequest) (minikafka.AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Append(c, r)
}
func (s *serialStore) Fetch(c context.Context, r minikafka.FetchRequest) (minikafka.FetchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Fetch(c, r)
}
func (s *serialStore) CommitOffset(c context.Context, r minikafka.CommitOffsetRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.CommitOffset(c, r)
}
func (s *serialStore) FetchOffset(c context.Context, r minikafka.FetchOffsetRequest) (minikafka.FetchOffsetResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.FetchOffset(c, r)
}
func (s *serialStore) EarliestOffset(c context.Context, t string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.EarliestOffset(c, t)
}
func (s *serialStore) LatestOffset(c context.Context, t string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.LatestOffset(c, t)
}
func (s *serialStore) ApplyRetention(c context.Context, t string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.ApplyRetention(c, t)
}
