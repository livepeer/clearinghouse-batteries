package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func TestTOMLCredsFile(t *testing.T) {
	data, err := os.ReadFile("../../creds.example.toml")
	require.NoError(t, err)
	contents := string(data)
	for _, secret := range []string{"operator-secret", "signer-secret", "read-secret", "write-secret"} {
		contents = strings.Replace(contents, `secret = ""`, `secret = "`+secret+`"`, 1)
	}
	path := filepath.Join(t.TempDir(), "creds.toml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	registry, err := serviceauth.Load(path)
	require.NoError(t, err)
	require.Equal(t, 1, registry.Count("management"))
	require.Equal(t, 1, registry.Count("webhook"))
	p := ServeParams{EnableKafka: true, EnableAccounting: true}
	access, dialer, err := p.kafkaSecurity(registry)
	require.NoError(t, err)
	require.Len(t, access, 2)
	require.NotNil(t, dialer)
}

func TestKafkaRegistryConfiguration(t *testing.T) {
	path := testCredsFile(t)
	registry, err := serviceauth.Load(path)
	require.NoError(t, err)
	p := ServeParams{EnableKafka: true, EnableAccounting: true, CredsFile: path, KafkaBind: "127.0.0.1:9092", KafkaTopic: "events"}
	require.NoError(t, p.Validate())
	access, dialer, err := p.kafkaSecurity(registry)
	require.NoError(t, err)
	require.Len(t, access, 2)
	require.NotNil(t, dialer)

	external := ServeParams{EnableAccounting: true, KafkaBroker: "broker:9092", KafkaTopic: "events", CredsFile: path}
	require.NoError(t, external.Validate())
	access, dialer, err = external.kafkaSecurity(registry)
	require.NoError(t, err)
	require.Nil(t, access)
	require.NotNil(t, dialer)

	for _, tc := range []struct{ name, data, want string }{
		{"missing write", `{"id":"r","secret":"secret","kafka":{"username":"reader","allow":["read"],"accounting":true}}`, "read and write"},
		{"shared role", `{"id":"r","secret":"secret","kafka":{"username":"reader","allow":["read","write"],"accounting":true}}`, "must be distinct"},
		{"missing accounting", `{"id":"r","secret":"a","kafka":{"username":"reader","allow":["read"]}},{"id":"w","secret":"b","kafka":{"username":"writer","allow":["write"]}}`, "accounting reader"},
		{"missing secret", `{"id":"broken"}`, "missing required param"},
		{"normalized duplicate", `{"id":"r","secret":"a","kafka":{"username":"reader","allow":["read"],"accounting":true}},{"id":"w","secret":"b","kafka":{"username":"read\u00ader","allow":["write"]}}`, "duplicate Kafka username"},
		{"invalid writer secret", `{"id":"r","secret":"a","kafka":{"username":"reader","allow":["read"],"accounting":true}},{"id":"w","secret":"sensitive\u0007","kafka":{"username":"writer","allow":["write"]}}`, "Kafka secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p.CredsFile = testCredsFile(t, tc.data)
			p.DBPath = filepath.Join(t.TempDir(), "accounting.db")
			require.ErrorContains(t, Serve(t.Context(), p), tc.want)
			files, err := os.ReadDir(filepath.Dir(p.DBPath))
			require.NoError(t, err)
			require.Empty(t, files, "invalid credentials must not create databases or lock files")
		})
	}
}

func TestExternalAccountingUsesRegistryCredential(t *testing.T) {
	dir := t.TempDir()
	access := testRegistry(t).KafkaCredentials()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dbPath := filepath.Join(dir, "accounts.db")
	// Finish SQLite initialization before Serve opens its connection. Keep this
	// connection for polling so checks do not repeatedly configure WAL mode.
	db, err := store.Open(ctx, dbPath, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	broker, err := kafka.OpenBroker(ctx, "127.0.0.1:0", "events", filepath.Join(dir, "broker"), access)
	require.NoError(t, err)
	brokerDone := make(chan error, 1)
	go func() { brokerDone <- broker.Serve(ctx) }()
	p := ServeParams{Common: Common{DBPath: dbPath}, EnableAccounting: true, KafkaBroker: broker.Addr(), KafkaTopic: "events", CredsFile: testCredsFile(t)}
	appDone := make(chan struct{})
	var appErr error
	go func() {
		appErr = Serve(ctx, p)
		close(appDone)
	}()
	t.Cleanup(func() {
		cancel()
		<-appDone
		closeErr := broker.Close()
		brokerErr := <-brokerDone
		require.NoError(t, appErr)
		require.NoError(t, closeErr)
		require.NoError(t, brokerErr)
	})
	writer := kgo.NewWriter(kgo.WriterConfig{Brokers: []string{broker.Addr()}, Topic: "events", BatchTimeout: time.Millisecond, Dialer: &kgo.Dialer{SASLMechanism: plain.Mechanism{Username: "producer", Password: "write-secret"}}})
	require.NoError(t, writer.WriteMessages(ctx, kgo.Message{Value: []byte(`{"id":"external-auth","type":"other","data":{}}`)}))
	require.NoError(t, writer.Close())
	testutil.Eventually(t, func() bool {
		select {
		case <-appDone:
			t.Fatalf("accounting service exited before processing the event: %v", appErr)
		default:
		}
		next, _, _, err := db.Checkpoint(ctx, "kafka", store.KafkaStream("events", 0))
		require.NoError(t, err)
		return next == 1
	})
}
