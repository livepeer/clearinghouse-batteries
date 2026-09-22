// Package kafka provides Kafka event-log integration for the accounting service.
package kafka

import (
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
	broker, err := minikafka.Open(minikafka.Config{Addr: bind, Store: backend})
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
		Brokers:               l.Brokers,
		Topic:                 l.Topic,
		Partition:             partition,
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
	slog.Info("accounting partition ready", "brokers", l.Brokers, "topic", l.Topic, "partition", partition, "next_offset", next)
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
	var failures []error
	for _, addr := range l.Brokers {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := kgo.DialContext(probeCtx, "tcp", addr)
		var partitions []int
		if err == nil {
			deadline, _ := probeCtx.Deadline()
			_ = conn.SetDeadline(deadline)
			var metadata []kgo.Partition
			metadata, err = conn.ReadPartitions(l.Topic)
			_ = conn.Close()
			for _, p := range metadata {
				if p.Topic == l.Topic && p.ID >= 0 {
					partitions = append(partitions, p.ID)
				}
			}
			if err == nil && len(partitions) == 0 {
				err = fmt.Errorf("Kafka topic %s has no partitions", l.Topic)
			}
		}
		cancel()
		if err == nil {
			slices.Sort(partitions)
			return slices.Compact(partitions), nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", addr, err))
		if ctx.Err() != nil {
			break
		}
	}
	return nil, errors.Join(failures...)
}

// Each bootstrap gets its own timeout so an unavailable address does not prevent
// probing the remaining brokers. DialLeader discovers the partition's current leader.
func (l *Listener) offsets(ctx context.Context, partition int) (int64, int64, error) {
	var failures []error
	for _, addr := range l.Brokers {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := kgo.DialLeader(probeCtx, "tcp", addr, l.Topic, partition)
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
