// Package kafka provides Kafka event-log integration for the accounting service.
package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/sqlite"
	"github.com/livepeer/clearinghouse/internal/store"
	kgo "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type Listener struct {
	DB     *store.Store
	Broker string
	Topic  string
	Dialer *kgo.Dialer
	Ready  atomic.Bool
}

// OpenBroker initializes persistent storage and binds the listener before returning.
// The caller must run Serve and close the broker, including on startup failure.
func OpenBroker(ctx context.Context, bind, topic, dir string, access *BrokerAccess) (*minikafka.Broker, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// A trailing separator tells minikafka this is a directory even when the
	// final path component is "." or contains a dot.
	root := filepath.Clean(dir) + string(os.PathSeparator)
	backend, err := sqlite.Open(root, sqlite.WithWAL(true), sqlite.WithSynchronous(sqlite.SyncFull), sqlite.WithBusyTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	if err := backend.Init(ctx); err != nil {
		backend.Close()
		return nil, err
	}
	if err := backend.CreateTopic(ctx, topic, minikafka.TopicOptions{}); err != nil && !errors.Is(err, minikafka.ErrTopicExists) {
		backend.Close()
		return nil, err
	}
	cfg := minikafka.Config{Addr: bind, Store: backend}
	if access != nil {
		cfg.SASL = &minikafka.SASLConfig{
			Mechanisms: []minikafka.SASLMechanism{minikafka.SASLPlain, minikafka.SASLSCRAMSHA512},
			Users:      map[string]string{access.Read.Username: access.Read.Password, access.Write.Username: access.Write.Password},
		}
		cfg.Authorization = &minikafka.AuthorizationConfig{Grants: []minikafka.TopicGrant{
			{User: access.Read.Username, Topic: topic, Action: minikafka.TopicRead},
			{User: access.Write.Username, Topic: topic, Action: minikafka.TopicWrite},
		}}
	}
	broker, err := minikafka.Open(cfg)
	if err != nil {
		backend.Close()
		return nil, err
	}
	return broker, nil
}

func (l *Listener) Run(ctx context.Context) error {
	defer l.Ready.Store(false)
	if l.Broker == "" {
		return errors.New("Kafka broker address required")
	}
	partitions, err := l.partitions(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	type partitionReader struct {
		reader *kgo.Reader
		next   int64
	}
	var readers []partitionReader
	// Validate every checkpoint before allowing any reader to ingest.
	for _, partition := range partitions {
		reader, next, err := l.openReader(ctx, partition)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		defer reader.Close()
		readers = append(readers, partitionReader{reader, next})
	}
	group, runCtx := errgroup.WithContext(ctx)
	for _, item := range readers {
		group.Go(func() error { return l.readPartition(runCtx, item.reader, item.next) })
	}
	l.Ready.Store(true)
	err = group.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (l *Listener) openReader(ctx context.Context, partition int) (*kgo.Reader, int64, error) {
	next, _, _, err := l.DB.Checkpoint(ctx, "kafka", store.KafkaStream(l.Topic, partition))
	if err != nil {
		return nil, 0, err
	}
	first, last, err := l.offsets(ctx, partition)
	if err != nil {
		return nil, 0, fmt.Errorf("Kafka topic %s partition %d offsets: %w", l.Topic, partition, err)
	}
	if next < first || next > last {
		return nil, 0, fmt.Errorf("Kafka topic %s partition %d checkpoint %d outside retained offsets [%d,%d]; broker data was lost or replaced", l.Topic, partition, next, first, last)
	}
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:               []string{l.Broker},
		Topic:                 l.Topic,
		Partition:             partition,
		Dialer:                l.Dialer,
		MinBytes:              1,
		MaxBytes:              10 << 20,
		MaxWait:               100 * time.Millisecond,
		ReadLagInterval:       -1,
		OffsetOutOfRangeError: true,
	})
	if err := reader.SetOffset(next); err != nil {
		reader.Close()
		return nil, 0, err
	}
	return reader, next, nil
}

func (l *Listener) readPartition(ctx context.Context, reader *kgo.Reader, next int64) error {
	partition := reader.Config().Partition
	slog.Info("accounting partition ready", "broker", l.Broker, "topic", l.Topic, "partition", partition, "next_offset", next)
	for {
		msg, err := reader.ReadMessage(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("Kafka topic %s partition %d: %w", l.Topic, partition, err)
		}
		if msg.Offset != next {
			return fmt.Errorf("Kafka offset gap for %s partition %d: expected %d, received %d", l.Topic, partition, next, msg.Offset)
		}
		if err := l.DB.Ingest(ctx, l.Topic, partition, msg.Offset, msg.Value); err != nil {
			return fmt.Errorf("Kafka topic %s partition %d: %w", l.Topic, partition, err)
		}
		next++
	}
}

func (l *Listener) partitions(ctx context.Context) ([]int, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := cmp.Or(l.Dialer, kgo.DefaultDialer).DialContext(probeCtx, "tcp", l.Broker)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", l.Broker, err)
	}
	deadline, _ := probeCtx.Deadline()
	_ = conn.SetDeadline(deadline)
	metadata, err := conn.ReadPartitions(l.Topic)
	_ = conn.Close()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", l.Broker, err)
	}
	var partitions []int
	for _, p := range metadata {
		if p.Topic == l.Topic && p.ID >= 0 {
			partitions = append(partitions, p.ID)
		}
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("Kafka topic %s has no partitions", l.Topic)
	}
	slices.Sort(partitions)
	return slices.Compact(partitions), nil
}

// DialLeader discovers the partition's current leader from the configured broker.
func (l *Listener) offsets(ctx context.Context, partition int) (int64, int64, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := cmp.Or(l.Dialer, kgo.DefaultDialer).DialLeader(probeCtx, "tcp", l.Broker, l.Topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("%s: %w", l.Broker, err)
	}
	deadline, _ := probeCtx.Deadline()
	_ = conn.SetDeadline(deadline)
	first, last, err := conn.ReadOffsets()
	_ = conn.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("%s: %w", l.Broker, err)
	}
	return first, last, nil
}
