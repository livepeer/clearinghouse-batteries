package kafka

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/j0sh/minikafka/storage/sqlite"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestRealBrokerRestartAndReplay(t *testing.T) {
	f := testutil.New(t, "100")
	dir := filepath.Join(t.TempDir(), "kafka")
	ctx := context.Background()
	start := func() (*Listener, func()) {
		t.Helper()
		broker, err := OpenBroker(ctx, testutil.Port(t), "events", dir, nil)
		require.NoError(t, err)
		l := &Listener{DB: f.DB, Broker: broker.Addr(), Topic: "events"}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 2)
		go func() { done <- broker.Serve(runCtx) }()
		go func() { done <- l.Run(runCtx) }()
		testutil.Eventually(t, func() bool { return l.Ready.Load() })
		return l, func() {
			cancel()
			broker.Close()
			for range 2 {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("Kafka shutdown timed out")
				}
			}
		}
	}
	l, stop := start()
	require.FileExists(t, filepath.Join(dir, "minikafka_+meta.db"))
	require.FileExists(t, filepath.Join(dir, "minikafka_events_0.db"))
	raw := f.Event(t, "one", "70", testutil.PM)
	// Match the writer configuration used by go-livepeer's monitor producer.
	produce := func(l *Listener, values ...[]byte) {
		t.Helper()
		w := kgo.NewWriter(kgo.WriterConfig{Brokers: []string{l.Broker}, Topic: l.Topic, Balancer: kgo.CRC32Balancer{}, BatchTimeout: time.Millisecond})
		defer w.Close()
		messages := []kgo.Message{}
		for _, v := range values {
			messages = append(messages, kgo.Message{Value: v})
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, w.WriteMessages(ctx, messages...))
	}
	produce(l, raw)
	testutil.Eventually(t, func() bool {
		n, _, _, _ := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("events", 0))
		return n == 1
	})
	stop()
	l, stop = start()
	defer stop()
	produce(l, raw, []byte(`{"bad":true}`), f.Event(t, "two", "50", testutil.PM))
	testutil.Eventually(t, func() bool {
		n, _, _, _ := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("events", 0))
		return n == 4
	})
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "-20" {
		t.Fatal(bal)
	}
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&count))
	if count != 2 {
		t.Fatal(count)
	}
}

func TestAllPartitionsRestartAndReplay(t *testing.T) {
	f := testutil.New(t, "100")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	backend, err := sqlite.Open(dir + string(filepath.Separator))
	require.NoError(t, err)
	require.NoError(t, backend.CreateTopic(ctx, "events", minikafka.TopicOptions{Partitions: 3}))
	require.NoError(t, backend.Close())
	start := func() (*minikafka.Broker, func()) {
		t.Helper()
		broker, err := OpenBroker(ctx, "127.0.0.1:0", "events", dir, nil)
		require.NoError(t, err)
		listener := &Listener{DB: f.DB, Broker: broker.Addr(), Topic: "events"}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 2)
		go func() { done <- broker.Serve(runCtx) }()
		go func() { done <- listener.Run(runCtx) }()
		closed := false
		shutdown := func() {
			t.Helper()
			if closed {
				return
			}
			closed = true
			stop()
			for range 2 {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal("partition readers did not stop")
				}
			}
			require.NoError(t, broker.Close())
			require.False(t, listener.Ready.Load())
		}
		t.Cleanup(shutdown)
		testutil.Eventually(t, func() bool { return listener.Ready.Load() })
		return broker, shutdown
	}
	checkpoint := func(partition int, want int64) {
		t.Helper()
		testutil.Eventually(t, func() bool {
			next, _, _, err := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("events", partition))
			return err == nil && next == want
		})
	}
	broker, stop := start()
	publish := func(partition int32, values ...[]byte) {
		t.Helper()
		for _, raw := range values {
			_, err := broker.PublishToPartition(ctx, "events", partition, nil, raw)
			require.NoError(t, err)
		}
	}
	first := f.Event(t, "first", "10", testutil.PM)
	second := f.Event(t, "second", "20", testutil.PM)
	// Empty partition 0 must not block progress in another partition.
	publish(2, first)
	checkpoint(2, 1)
	publish(1, second, []byte(`{"bad":true}`))
	publish(0, first, f.Event(t, "third", "30", testutil.PM))
	checkpoint(0, 2)
	checkpoint(1, 2)
	stop()
	broker, stop = start()
	publish(2, second, f.Event(t, "fourth", "5", testutil.PM))
	publish(1, f.Event(t, "fifth", "7", testutil.PM))
	checkpoint(2, 3)
	checkpoint(1, 3)
	stop()
	for partition, want := range []int64{2, 3, 3} {
		var count int64
		require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM usage_events WHERE partition=?`, partition).Scan(&count))
		require.Equal(t, want, count)
		checkpoint(partition, want)
	}
	var applied, duplicates, quarantined int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FILTER (WHERE status='applied'),count(*) FILTER (WHERE status='duplicate'),count(*) FILTER (WHERE status='quarantined') FROM usage_events`).Scan(&applied, &duplicates, &quarantined))
	require.Equal(t, 5, applied)
	require.Equal(t, 2, duplicates)
	require.Equal(t, 1, quarantined)
	balance, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	require.Equal(t, "28", balance.String())
}

func TestConsumerFailureStopsAllPartitions(t *testing.T) {
	f := testutil.New(t, "100")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend := memory.Open()
	require.NoError(t, backend.CreateTopic(ctx, "events", minikafka.TopicOptions{Partitions: 3}))
	broker, err := minikafka.Open(minikafka.Config{Addr: "127.0.0.1:0", Store: backend})
	require.NoError(t, err)
	listener := &Listener{DB: f.DB, Broker: broker.Addr(), Topic: "events"}
	brokerDone, listenerDone := make(chan error, 1), make(chan error, 1)
	go func() { brokerDone <- broker.Serve(ctx) }()
	go func() { listenerDone <- listener.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, broker.Close())
		select {
		case err := <-brokerDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("broker did not stop")
		}
	})
	testutil.Eventually(t, func() bool { return listener.Ready.Load() })
	// A fatal write on one partition must stop the other readers, including idle ones.
	_, err = f.DB.DB.Exec(`CREATE TRIGGER fail_usage BEFORE INSERT ON usage_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	require.NoError(t, err)
	_, err = broker.PublishToPartition(ctx, "events", 2, nil, f.Event(t, "fail", "10", testutil.PM))
	require.NoError(t, err)
	select {
	case err := <-listenerDone:
		require.ErrorContains(t, err, "fixture failure")
	case <-ctx.Done():
		t.Fatal("partition readers did not stop after a failed write")
	}
	require.False(t, listener.Ready.Load())
}

func TestBrokerUsesDirectoryPathVerbatim(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data.v1")
	broker, err := OpenBroker(context.Background(), "127.0.0.1:0", "events", dir, nil)
	require.NoError(t, err)
	require.NoError(t, broker.Close())
	require.FileExists(t, filepath.Join(dir, "minikafka_+meta.db"))
	require.FileExists(t, filepath.Join(dir, "minikafka_events_0.db"))
}

func TestBrokerUsesCurrentDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	broker, err := OpenBroker(context.Background(), "127.0.0.1:0", "events", ".", nil)
	require.NoError(t, err)
	require.NoError(t, broker.Close())
	require.FileExists(t, "minikafka_+meta.db")
	require.FileExists(t, "minikafka_events_0.db")
}

func TestBrokerConcurrentPublishAndShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	start := func() (*minikafka.Broker, context.Context, func()) {
		t.Helper()
		broker, err := OpenBroker(ctx, "127.0.0.1:0", "events", dir, nil)
		require.NoError(t, err)
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- broker.Serve(runCtx) }()
		closed := false
		closeBroker := func() {
			t.Helper()
			if closed {
				return
			}
			closed = true
			stop()
			require.NoError(t, broker.Close())
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("broker shutdown timed out")
			}
		}
		t.Cleanup(closeBroker)
		return broker, runCtx, closeBroker
	}
	broker, runCtx, stop := start()
	acknowledged := make(map[int64]string)
	for _, shutdown := range []bool{false, true} {
		type result struct {
			offset int64
			value  string
			err    error
		}
		const producers = 64
		results := make(chan result, producers)
		ready := make(chan struct{})
		for i := range producers {
			go func() {
				<-ready
				value := fmt.Sprintf("shutdown=%t producer=%d", shutdown, i)
				offset, err := broker.Publish(runCtx, "events", nil, []byte(value))
				results <- result{offset, value, err}
			}()
		}
		close(ready)
		for i := range producers {
			var r result
			select {
			case r = <-results:
			case <-ctx.Done():
				t.Fatal("publishers did not finish")
			}
			if !shutdown || i == 0 {
				require.NoError(t, r.err)
			}
			if r.err == nil {
				if _, exists := acknowledged[r.offset]; exists {
					t.Fatalf("duplicate acknowledged offset %d", r.offset)
				}
				acknowledged[r.offset] = r.value
			}
			// Once one concurrent write succeeds, interrupt the remaining work.
			// Best-effort shutdown may fail requests but must preserve successes.
			if shutdown && i == 0 {
				stop()
			}
		}
	}

	reopened, _, _ := start()
	conn, err := kgo.DialLeader(ctx, "tcp", reopened.Addr(), "events", 0)
	require.NoError(t, err)
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	require.NoError(t, conn.SetDeadline(deadline))
	first, last, err := conn.ReadOffsets()
	require.NoError(t, err)
	if first != 0 || last < int64(len(acknowledged)) {
		t.Fatalf("unexpected retained offsets [%d,%d] for %d successes", first, last, len(acknowledged))
	}
	_, err = conn.Seek(0, kgo.SeekAbsolute)
	require.NoError(t, err)
	for offset := int64(0); offset < last; offset++ {
		msg, err := conn.ReadMessage(1 << 20)
		require.NoError(t, err)
		if msg.Offset != offset {
			t.Fatalf("offset gap: got %d, want %d", msg.Offset, offset)
		}
		if value, ok := acknowledged[offset]; ok {
			if string(msg.Value) != value {
				t.Fatalf("offset %d: got %q, want %q", offset, msg.Value, value)
			}
			delete(acknowledged, offset)
		}
	}
	if len(acknowledged) != 0 {
		t.Fatalf("lost acknowledged records: %v", acknowledged)
	}
}

func TestConsumerRejectsLostOffsets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		next      int64
		retain    int64
		partition int
	}{
		{"retention passed checkpoint", 0, 1, 0},
		{"checkpoint beyond broker", 3, 0, 0},
		{"partition 2 retention passed checkpoint", 0, 1, 2},
		{"partition 2 checkpoint beyond broker", 3, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testutil.New(t, "100")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			backend := memory.Open()
			require.NoError(t, backend.Init(ctx))
			require.NoError(t, backend.CreateTopic(ctx, "events", minikafka.TopicOptions{Partitions: 3, Retention: minikafka.RetentionPolicy{MaxMessages: tc.retain}}))
			_, err := backend.Append(ctx, minikafka.AppendRequest{Topic: "events", Partition: int32(tc.partition), Records: []minikafka.Record{{Value: []byte("one")}, {Value: []byte("two")}}})
			require.NoError(t, err)
			require.NoError(t, backend.ApplyRetention(ctx, "events"))
			broker, err := minikafka.Open(minikafka.Config{Addr: testutil.Port(t), Store: backend})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- broker.Serve(ctx) }()
			defer func() { cancel(); broker.Close(); require.NoError(t, <-done) }()
			require.NoError(t, f.DB.Write(ctx, func(tx *sql.Tx) error {
				return store.SetCheckpoint(ctx, tx, "kafka", store.KafkaStream("events", tc.partition), tc.next, "")
			}))

			l := &Listener{DB: f.DB, Broker: broker.Addr(), Topic: "events"}
			readCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			defer stop()
			if err := l.Run(readCtx); err == nil || !strings.Contains(err.Error(), "outside retained offsets") {
				t.Fatalf("want retained-offset failure, got %v", err)
			}
			if l.Ready.Load() {
				t.Fatal("failed consumer is ready")
			}
			next, _, _, err := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("events", tc.partition))
			require.NoError(t, err)
			if next != tc.next {
				t.Fatal("failure changed checkpoint", next)
			}
		})
	}
}
