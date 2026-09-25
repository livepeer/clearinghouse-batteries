package kafka

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func TestBrokerReadWriteUsers(t *testing.T) {
	f := testutil.New(t, "100")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	access := &BrokerAccess{
		Read:  Credential{Username: "accounting", Password: "read-secret"},
		Write: Credential{Username: "producer", Password: "write-secret"},
	}
	broker, err := OpenBroker(ctx, "127.0.0.1:0", "events", t.TempDir(), access)
	require.NoError(t, err)
	readerDialer, err := NewDialer(&access.Read)
	require.NoError(t, err)
	listener := &Listener{DB: f.DB, Broker: broker.Addr(), Topic: "events", Dialer: readerDialer}
	done := make(chan error, 2)
	go func() { done <- broker.Serve(ctx) }()
	go func() { done <- listener.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, broker.Close())
		for range 2 {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("authenticated Kafka services did not stop")
			}
		}
	})
	testutil.Eventually(t, func() bool { return listener.Ready.Load() })

	plainWriter := &kgo.Dialer{SASLMechanism: plain.Mechanism{Username: access.Write.Username, Password: access.Write.Password}}
	scramWriter, err := NewDialer(&access.Write)
	require.NoError(t, err)
	for i, dialer := range []*kgo.Dialer{plainWriter, scramWriter} {
		conn, err := dialer.DialLeader(ctx, "tcp", broker.Addr(), "events", 0)
		require.NoError(t, err)
		_, err = conn.WriteMessages(kgo.Message{Value: f.Event(t, string(rune('a'+i)), "10", testutil.PM)})
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	testutil.Eventually(t, func() bool {
		next, _, _, err := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("events", 0))
		return err == nil && next == 2
	})

	dialCtx, stopDial := context.WithCancel(ctx)
	defer stopDial()
	readConn, err := readerDialer.DialLeader(dialCtx, "tcp", broker.Addr(), "events", 0)
	require.NoError(t, err)
	stopDial()
	_, err = readConn.WriteMessages(kgo.Message{Value: []byte("denied")})
	require.ErrorContains(t, err, "Topic Authorization Failed")
	require.NoError(t, readConn.Close())
	readConn, err = readerDialer.DialLeader(ctx, "tcp", broker.Addr(), "events", 0)
	require.NoError(t, err)
	_, last, err := readConn.ReadOffsets()
	require.NoError(t, err)
	require.Equal(t, int64(2), last)
	require.NoError(t, readConn.Close())
	writeConn, err := scramWriter.DialLeader(ctx, "tcp", broker.Addr(), "events", 0)
	require.NoError(t, err)
	_, _, err = writeConn.ReadOffsets()
	require.ErrorContains(t, err, "Topic Authorization Failed")
	require.NoError(t, writeConn.Close())

	badDialer, err := NewDialer(&Credential{Username: access.Read.Username, Password: "wrong"})
	require.NoError(t, err)
	_, err = badDialer.DialContext(ctx, "tcp", broker.Addr())
	require.Error(t, err)
	unauthenticated, err := kgo.DialContext(ctx, "tcp", broker.Addr())
	require.NoError(t, err)
	require.NoError(t, unauthenticated.SetDeadline(time.Now().Add(time.Second)))
	_, err = unauthenticated.ReadPartitions("events")
	require.Error(t, err)
	_ = unauthenticated.Close()
}

func TestSASLDialCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "dialer timeout"} {
		t.Run(mode, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, _ := ln.Accept()
				accepted <- conn
			}()
			dialer, err := NewDialer(&Credential{Username: "reader", Password: "secret"})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := context.Canceled
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				want = context.DeadlineExceeded
			} else if mode == "dialer timeout" {
				dialer.Timeout = 100 * time.Millisecond
				want = context.DeadlineExceeded
			}
			done := make(chan error, 1)
			go func() {
				conn, err := dialer.DialContext(ctx, "tcp", ln.Addr().String())
				if conn != nil {
					conn.Close()
				}
				done <- err
			}()
			select {
			case peer := <-accepted:
				require.NotNil(t, peer)
				defer peer.Close()
			case <-time.After(3 * time.Second):
				t.Fatal("TCP connection was not accepted")
			}
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, want)
			case <-time.After(3 * time.Second):
				t.Fatal("SASL did not stop after cancellation")
			}
		})
	}
}
