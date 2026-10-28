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

// partitionSnapshot uses the durable checkpoint, never reader prefetch positions.
type partitionSnapshot struct {
	Partition        int
	First, End, Next int64
	ObservedAt       time.Time
}

func (l *Listener) probe(ctx context.Context, checkpoints *store.Store, now func() time.Time) ([]partitionSnapshot, error) {
	parts, err := l.partitions(ctx)
	if err != nil {
		return nil, err
	}
	var out []partitionSnapshot
	for _, p := range parts {
		var first, last, next int64
		var observed time.Time
		for attempt := 0; attempt < 2; attempt++ {
			observed = now()
			next, _, _, err = checkpoints.Checkpoint(ctx, "kafka", store.KafkaStream(l.Topic, p))
			if err != nil {
				return nil, err
			}
			first, last, err = l.offsets(ctx, p)
			if err != nil {
				return nil, err
			}
			if first >= 0 && last >= first && next >= first && next <= last {
				break
			}
			if attempt == 1 {
				return nil, errors.New("checkpoint outside retained offsets")
			}
		}
		out = append(out, partitionSnapshot{Partition: p, First: first, End: last, Next: next, ObservedAt: observed})
	}
	return out, nil
}

func (l *Listener) MetricSnapshot(ctx context.Context, checkpoints *store.Store) ([]prometheus.Metric, error) {
	parts, err := l.probe(ctx, checkpoints, time.Now)
	if err != nil {
		return nil, err
	}
	var out []prometheus.Metric
	for _, p := range parts {
		for name, n := range map[string]int64{"kafka_checkpoint_next_offset": p.Next, "kafka_log_start_offset": p.First, "kafka_log_end_offset": p.End, "accounting_lag_records": p.End - p.Next} {
			out = append(out, metrics.Gauge(name, float64(n), []string{"topic", "partition"}, l.Topic, fmt.Sprint(p.Partition)))
		}
	}
	return out, nil
}

type offsetObservation struct {
	end int64
	at  int64
}

type partitionCoverage struct {
	next, end, observed, covered int64
	pending                      []offsetObservation
}

type lagCoverage struct {
	partitions map[int]*partitionCoverage
	maxAge     time.Duration
}

// update accepts probe-validated partitions and offsets. Coverage advances only
// when a checkpoint crosses a previously observed broker end offset.
func (c *lagCoverage) update(parts []partitionSnapshot, checked int64) (store.KafkaLag, error) {
	h := store.KafkaLag{CheckedAt: checked}
	seen := make(map[int]bool, len(parts))
	clockRegressed := false
	for _, p := range parts {
		at := p.ObservedAt.UnixMilli()
		if at <= 0 || at > checked {
			return h, errors.New("invalid kafka lag observation")
		}
		seen[p.Partition] = true
		if prev := c.partitions[p.Partition]; prev != nil {
			if p.Next < prev.next || p.End < prev.end {
				return h, errors.New("kafka offsets moved backwards")
			}
			clockRegressed = clockRegressed || at < prev.observed
		}
	}
	for p := range c.partitions {
		if !seen[p] {
			return h, errors.New("kafka partition disappeared")
		}
	}
	if clockRegressed {
		// Keep offset baselines to detect data loss, but require new live time
		// evidence instead of waiting for the old wall clock to catch up.
		for _, prev := range c.partitions {
			prev.observed, prev.covered, prev.pending = 0, 0, nil
		}
		return h, errors.New("kafka lag clock moved backwards")
	}
	if c.partitions == nil {
		c.partitions = make(map[int]*partitionCoverage)
	}
	h.CoveredThrough = checked
	for _, p := range parts {
		prev := c.partitions[p.Partition]
		if prev == nil {
			prev = &partitionCoverage{}
			c.partitions[p.Partition] = prev
		}
		at := p.ObservedAt.UnixMilli()
		prev.next, prev.end, prev.observed = p.Next, p.End, at
		prev.pending = append(prev.pending, offsetObservation{end: p.End, at: at})
		cutoff := checked - c.maxAge.Milliseconds()
		consumed := 0
		for _, fence := range prev.pending {
			if fence.end <= p.Next {
				prev.covered = fence.at
			} else if fence.at > cutoff {
				break
			}
			consumed++
		}
		// Copy the bounded window so a stalled stream does not retain old buffers.
		prev.pending = append([]offsetObservation(nil), prev.pending[consumed:]...)
		h.CoveredThrough = min(h.CoveredThrough, prev.covered)
	}
	h.Available = h.CoveredThrough > 0
	return h, nil
}

func (c *lagCoverage) poll(ctx context.Context, l *Listener, now func() time.Time) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := now().UnixMilli()
	parts, err := l.probe(probeCtx, l.DB, now)
	h := store.KafkaLag{CheckedAt: started}
	if err == nil {
		h, err = c.update(parts, now().UnixMilli())
		// Use the start of the complete probe for its expiry, not completion time.
		h.CheckedAt = started
		h.CoveredThrough = min(h.CoveredThrough, started)
	}
	// Publish failures as well as successes. If the database cannot be written,
	// the previous proof still expires at the authorization threshold.
	writeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	return errors.Join(err, l.DB.SetKafkaLag(writeCtx, l.Topic, h))
}

// MonitorLag runs independently of Prometheus and saves consumer progress
// for webhook processes sharing the database.
func (l *Listener) MonitorLag(ctx context.Context, maxAge time.Duration) error {
	if maxAge < store.MinKafkaLag {
		return errors.New("maximum kafka lag must be at least 1s")
	}
	coverage := &lagCoverage{maxAge: maxAge}
	// Leave room for probing and millisecond timestamp rounding before expiry.
	ticker := time.NewTicker(min(5*time.Second, maxAge/4))
	defer ticker.Stop()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := l.DB.SetKafkaLag(cleanup, l.Topic, store.KafkaLag{}); err != nil {
			slog.Error("invalidate Kafka lag", "error", err)
		}
	}()
	for ctx.Err() == nil {
		if err := coverage.poll(ctx, l, time.Now); err != nil && ctx.Err() == nil {
			l.Metrics.Error("kafka", "lag", "unavailable")
			slog.Warn("Kafka lag unavailable", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	return nil
}
