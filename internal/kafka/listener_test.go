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
		broker, err := OpenBroker(ctx, testutil.Port(t), "events", dir)
		require.NoError(t, err)
		l := &Listener{DB: f.DB, Brokers: []string{testutil.Port(t), broker.Addr()}, Topic: "events"}
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
	raw := f.Event(t, "one", "70", testutil.PM)
	// Match the writer configuration used by go-livepeer's monitor producer.
	produce := func(l *Listener, values ...[]byte) {
		t.Helper()
		w := kgo.NewWriter(kgo.WriterConfig{Brokers: l.Brokers[1:], Topic: l.Topic, Balancer: kgo.CRC32Balancer{}, BatchTimeout: time.Millisecond})
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
	testutil.Eventually(t, func() bool { n, _, _, _ := f.DB.Checkpoint(ctx, "kafka", "events"); return n == 1 })
	stop()
	l, stop = start()
	defer stop()
	produce(l, raw, []byte(`{"bad":true}`), f.Event(t, "two", "50", testutil.PM))
	testutil.Eventually(t, func() bool { n, _, _, _ := f.DB.Checkpoint(ctx, "kafka", "events"); return n == 4 })
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

func TestBrokerConcurrentPublishAndShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	start := func() (*minikafka.Broker, context.Context, func()) {
		t.Helper()
		broker, err := OpenBroker(ctx, "127.0.0.1:0", "events", dir)
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
		name   string
		next   int64
		retain int64
	}{
		{"retention passed checkpoint", 0, 1},
		{"checkpoint beyond broker", 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testutil.New(t, "100")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			backend := memory.Open()
			require.NoError(t, backend.Init(ctx))
			require.NoError(t, backend.CreateTopic(ctx, "events", minikafka.TopicOptions{Retention: minikafka.RetentionPolicy{MaxMessages: tc.retain}}))
			_, err := backend.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{Value: []byte("one")}, {Value: []byte("two")}}})
			require.NoError(t, err)
			require.NoError(t, backend.ApplyRetention(ctx, "events"))
			broker, err := minikafka.Open(minikafka.Config{Addr: testutil.Port(t), Store: backend})
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- broker.Serve(ctx) }()
			defer func() { cancel(); broker.Close(); require.NoError(t, <-done) }()
			require.NoError(t, f.DB.Write(ctx, func(tx *sql.Tx) error {
				return store.SetCheckpoint(ctx, tx, "kafka", "events", tc.next, "")
			}))

			l := &Listener{DB: f.DB, Brokers: []string{broker.Addr()}, Topic: "events"}
			readCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			defer stop()
			if err := l.Run(readCtx); err == nil || !strings.Contains(err.Error(), "outside retained offsets") {
				t.Fatalf("want retained-offset failure, got %v", err)
			}
			if l.Ready.Load() {
				t.Fatal("failed consumer is ready")
			}
			next, _, _, err := f.DB.Checkpoint(ctx, "kafka", "events")
			require.NoError(t, err)
			if next != tc.next {
				t.Fatal("failure changed checkpoint", next)
			}
		})
	}
}
