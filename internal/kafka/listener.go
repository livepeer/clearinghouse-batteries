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
	"strings"
	"sync/atomic"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/sqlite"
	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	kgo "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type Listener struct {
	Metrics *metrics.Metrics
	DB      *store.Store
	Broker  string
	Topic   string
	Dialer  *kgo.Dialer
	Ready   atomic.Bool
}

// OpenBroker initializes persistent storage and binds the listener before returning.
// The caller must run Serve and close the broker, including on startup failure.
func OpenBroker(ctx context.Context, bind, advertisedAddr, topic, dir string, credentials []serviceauth.KafkaCredential) (*minikafka.Broker, error) {
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
	cfg := minikafka.Config{Addr: bind, AdvertisedAddr: advertisedAddr, Store: backend}
	if credentials != nil {
		cfg.SASL, cfg.Authorization = brokerConfig(topic, credentials)
	}
	broker, err := minikafka.Open(cfg)
	if err != nil {
		backend.Close()
		return nil, err
	}
	return broker, nil
}

func (l *Listener) Run(ctx context.Context) (result error) {
	defer func() {
		if result != nil && ctx.Err() == nil {
			reason := "consumer"
			if strings.Contains(result.Error(), "offset gap") {
				reason = "offset_gap"
			}
			if strings.Contains(result.Error(), "outside retained offsets") {
				reason = "retained_offsets"
			}
			l.Metrics.Error("accounting", "run", reason)
		}
	}()
	defer l.Ready.Store(false)
	if l.Broker == "" {
		return errors.New("kafka broker address required")
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
	// Initialize bounded outcome series before processing so the first event is visible to rate queries.
	for _, partition := range partitions {
		for _, outcome := range []string{"applied", "ignored", "duplicate"} {
			l.Metrics.Add("usage_events_total", 0, []string{"topic", "partition", "outcome", "reason"}, l.Topic, fmt.Sprint(partition), outcome, "none")
		}
		for _, reason := range []string{"envelope", "conflicting_id", "ticket_data", "unknown_session", "session_binding", "invalid_ticket", "missing_usd_fee", "other"} {
			l.Metrics.Add("usage_events_total", 0, []string{"topic", "partition", "outcome", "reason"}, l.Topic, fmt.Sprint(partition), "quarantined", reason)
		}
	}
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
		return nil, 0, fmt.Errorf("kafka topic %s partition %d offsets: %w", l.Topic, partition, err)
	}
	if next < first || next > last {
		return nil, 0, fmt.Errorf("kafka topic %s partition %d checkpoint %d outside retained offsets [%d,%d]; broker data was lost or replaced", l.Topic, partition, next, first, last)
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
			return fmt.Errorf("kafka topic %s partition %d: %w", l.Topic, partition, err)
		}
		if msg.Offset != next {
			return fmt.Errorf("kafka offset gap for %s partition %d: expected %d, received %d", l.Topic, partition, next, msg.Offset)
		}
		if err := l.DB.Ingest(ctx, l.Topic, partition, msg.Offset, msg.Value); err != nil {
			return fmt.Errorf("kafka topic %s partition %d: %w", l.Topic, partition, err)
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
		return nil, fmt.Errorf("kafka topic %s has no partitions", l.Topic)
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

// MetricSnapshot probes durable checkpoints, never reader prefetch positions.
func (l *Listener) MetricSnapshot(ctx context.Context, checkpoints *store.Store) (out []prometheus.Metric, err error) {
	parts, err := l.partitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range parts {
		var first, last, next int64
		for attempt := 0; attempt < 2; attempt++ {
			next, _, _, err = checkpoints.Checkpoint(ctx, "kafka", store.KafkaStream(l.Topic, p))
			if err != nil {
				return nil, err
			}
			first, last, err = l.offsets(ctx, p)
			if err != nil {
				return nil, err
			}
			if next >= first && next <= last {
				break
			}
			if attempt == 1 {
				return nil, errors.New("checkpoint outside retained offsets")
			}
		}
		for name, n := range map[string]int64{"kafka_checkpoint_next_offset": next, "kafka_log_start_offset": first, "kafka_log_end_offset": last, "accounting_lag_records": max(0, last-next)} {
			out = append(out, metrics.Gauge(name, float64(n), []string{"topic", "partition"}, l.Topic, fmt.Sprint(p)))
		}
	}
	return out, nil
}
