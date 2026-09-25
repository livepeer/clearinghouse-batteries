package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestKafkaSecurityConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kafka-auth.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"read":{"username":"accounting","password":"read-secret"},
		"write":{"username":"producer","password":"write-secret"}
	}`), 0600))
	p := ServeParams{EnableKafka: true, EnableAccounting: true, KafkaAuthFile: path, KafkaBind: "127.0.0.1:9092", KafkaTopic: "events"}
	require.NoError(t, p.Validate())
	access, dialer, err := p.kafkaSecurity()
	require.NoError(t, err)
	require.Equal(t, "accounting", access.Read.Username)
	require.Equal(t, "producer", access.Write.Username)
	require.NotNil(t, dialer)

	external := ServeParams{EnableAccounting: true, KafkaBroker: "broker:9092", KafkaTopic: "events", KafkaAuthFile: path}
	require.NoError(t, external.Validate())
	_, _, err = external.kafkaSecurity()
	require.ErrorContains(t, err, "unknown field")
	for _, field := range []string{`"write":{}`, `"write":null`, `"Write":{}`, `"read":{"unknown":true}`} {
		require.NoError(t, os.WriteFile(path, []byte(`{"read":{"username":"accounting","password":"read-secret"},`+field+`}`), 0600))
		_, _, err = external.kafkaSecurity()
		require.ErrorContains(t, err, "unknown field")
	}
	require.NoError(t, os.WriteFile(path, []byte(`{"read":{"username":"accounting","password":"read-secret"}}`), 0600))
	access, dialer, err = external.kafkaSecurity()
	require.NoError(t, err)
	require.Equal(t, "accounting", access.Read.Username)
	require.NotNil(t, dialer)

}

func TestKafkaSecurityRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    ServeParams
		want string
	}{
		{"auth without Kafka", ServeParams{EnableOnchainListener: true, KafkaAuthFile: "auth.json"}, "--kafka-auth-file requires"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.ErrorContains(t, tc.p.Validate(), tc.want) })
	}
	path := filepath.Join(t.TempDir(), "kafka-auth.json")
	p := ServeParams{EnableKafka: true, KafkaAuthFile: path}
	for _, tc := range []struct {
		name, data, want string
	}{
		{"same user", `{"read":{"username":"same","password":"a"},"write":{"username":"same","password":"b"}}`, "must be distinct"},
		{"missing write", `{"read":{"username":"reader","password":"a"}}`, "write username and password"},
		{"wildcard user", `{"read":{"username":"*","password":"a"},"write":{"username":"writer","password":"b"}}`, "invalid Kafka read credential"},
		{"unknown field", `{"read":{"username":"reader","password":"a"},"write":{"username":"writer","password":"b"},"admin":true}`, "unknown field"},
		{"trailing data", `{"read":{"username":"reader","password":"a"},"write":{"username":"writer","password":"b"}} true`, "one JSON object"},
		{"oversized whitespace", `{"read":{"username":"reader","password":"a"},"write":{"username":"writer","password":"b"}}` + strings.Repeat(" ", maxKafkaAuthFileBytes), "64 KiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tc.data), 0600))
			_, _, err := p.kafkaSecurity()
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestKafkaSecurityCLIConfig(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "auth.json")
	configPath := filepath.Join(dir, "serve.toml")
	require.NoError(t, os.WriteFile(authPath, []byte(`{
		"read":{"username":"accounting","password":"read-secret"},
		"write":{"username":"producer","password":"write-secret"}
	}`), 0600))
	require.NoError(t, os.WriteFile(configPath, []byte("EnableKafka = true\nKafkaAuthFile = \""+authPath+"\"\n"), 0600))
	read := func(args ...string) ServeParams {
		t.Helper()
		var got ServeParams
		cmd := command[ServeParams]("serve", "test", func(p *ServeParams, _ *cobra.Command) error {
			got = *p
			return nil
		})
		cmd.SetArgs(args)
		require.NoError(t, cmd.Execute())
		return got
	}
	fromConfig := read("--config-file", configPath)
	require.Equal(t, authPath, fromConfig.KafkaAuthFile)
	t.Setenv("CLEARINGHOUSE_KAFKA_AUTH_FILE", authPath)
	require.Equal(t, authPath, read("--config-file", configPath).KafkaAuthFile)
	require.Equal(t, authPath, read("--kafka-auth-file", authPath).KafkaAuthFile)
}

func TestKafkaAuthFileFailsBeforeDatabaseOpen(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "auth.json")
	dbPath := filepath.Join(dir, "accounting.db")
	require.NoError(t, os.WriteFile(authPath, []byte(`{"read":{}}`), 0600))
	err := Serve(context.Background(), ServeParams{
		Common: Common{DBPath: dbPath}, EnableKafka: true, EnableAccounting: true,
		KafkaBind: "127.0.0.1:9092", KafkaTopic: "events", KafkaAuthFile: authPath,
	})
	require.ErrorContains(t, err, "read username and password")
	require.NoFileExists(t, dbPath)
}

func TestExternalAccountingUsesReadCredentials(t *testing.T) {
	dir := t.TempDir()
	authPath := filepath.Join(dir, "auth.json")
	require.NoError(t, os.WriteFile(authPath, []byte(`{"read":{"username":"accounting","password":"read-secret"}}`), 0600))
	access := &kafka.BrokerAccess{
		Read:  kafka.Credential{Username: "accounting", Password: "read-secret"},
		Write: kafka.Credential{Username: "producer", Password: "write-secret"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	broker, err := kafka.OpenBroker(ctx, "127.0.0.1:0", "events", filepath.Join(dir, "broker"), access)
	require.NoError(t, err)
	brokerDone := make(chan error, 1)
	go func() { brokerDone <- broker.Serve(ctx) }()
	p := ServeParams{
		Common:           Common{DBPath: filepath.Join(dir, "accounts.db")},
		EnableAccounting: true, KafkaBroker: broker.Addr(), KafkaTopic: "events", KafkaAuthFile: authPath,
	}
	appDone := make(chan error, 1)
	go func() { appDone <- Serve(ctx, p) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-appDone)
		require.NoError(t, broker.Close())
		require.NoError(t, <-brokerDone)
	})

	writer := kgo.NewWriter(kgo.WriterConfig{
		Brokers: []string{broker.Addr()}, Topic: "events", BatchTimeout: time.Millisecond,
		Dialer: &kgo.Dialer{SASLMechanism: plain.Mechanism{Username: access.Write.Username, Password: access.Write.Password}},
	})
	require.NoError(t, writer.WriteMessages(ctx, kgo.Message{Value: []byte(`{"id":"external-auth","type":"other","data":{}}`)}))
	require.NoError(t, writer.Close())
	testutil.Eventually(t, func() bool {
		db, err := store.Open(ctx, p.DBPath, false)
		if err != nil {
			return false
		}
		defer db.Close()
		next, _, _, err := db.Checkpoint(ctx, "kafka", store.KafkaStream("events", 0))
		return err == nil && next == 1
	})
}
