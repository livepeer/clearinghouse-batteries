package kafka

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
)

func TestRealBrokerRestartAndReplay(t *testing.T) {
	f := testutil.New(t, "100")
	dir := filepath.Join(t.TempDir(), "kafka")
	ctx := context.Background()
	start := func() (*Listener, func()) {
		t.Helper()
		broker, err := OpenBroker(ctx, testutil.Port(t), "events", dir)
		testutil.Must(t, err)
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
					testutil.Must(t, err)
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
		testutil.Must(t, w.WriteMessages(ctx, messages...))
	}
	produce(l, raw)
	testutil.Eventually(t, func() bool { n, _, _, _ := f.DB.Checkpoint(ctx, "kafka", "events"); return n == 1 })
	stop()
	l, stop = start()
	defer stop()
	produce(l, raw, []byte(`{"bad":true}`), f.Event(t, "two", "50", testutil.PM))
	testutil.Eventually(t, func() bool { n, _, _, _ := f.DB.Checkpoint(ctx, "kafka", "events"); return n == 4 })
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	testutil.Must(t, err)
	if bal.String() != "-20" {
		t.Fatal(bal)
	}
	var count int
	testutil.Must(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&count))
	if count != 2 {
		t.Fatal(count)
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
			testutil.Must(t, backend.Init(ctx))
			testutil.Must(t, backend.CreateTopic(ctx, "events", minikafka.TopicOptions{Retention: minikafka.RetentionPolicy{MaxMessages: tc.retain}}))
			_, err := backend.Append(ctx, minikafka.AppendRequest{Topic: "events", Records: []minikafka.Record{{Value: []byte("one")}, {Value: []byte("two")}}})
			testutil.Must(t, err)
			testutil.Must(t, backend.ApplyRetention(ctx, "events"))
			broker, err := minikafka.Open(minikafka.Config{Addr: testutil.Port(t), Store: backend})
			testutil.Must(t, err)
			done := make(chan error, 1)
			go func() { done <- broker.Serve(ctx) }()
			defer func() { cancel(); broker.Close(); testutil.Must(t, <-done) }()
			testutil.Must(t, f.DB.Write(ctx, func(tx *sql.Tx) error {
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
			testutil.Must(t, err)
			if next != tc.next {
				t.Fatal("failure changed checkpoint", next)
			}
		})
	}
}
