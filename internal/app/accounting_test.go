package app

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestAccountingValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    ServeParams
		want string
	}{
		{"missing broker", ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableAccounting: true}, "--kafka-broker required"},
		{"ambiguous source", ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableKafka: true, KafkaBroker: "broker:9092"}, "cannot be used"},
		{"advertised without embedded broker", ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableAccounting: true, KafkaBroker: "broker:9092", KafkaAdvertisedAddr: "public:9093"}, "--kafka-advertised-addr requires --enable-kafka"},
		{"invalid address", ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableAccounting: true, KafkaBroker: "broker"}, "invalid Kafka broker"},
		{"invalid port", ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableAccounting: true, KafkaBroker: "broker:99999"}, "invalid Kafka broker"},
		{"empty host", ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableAccounting: true, KafkaBroker: ":9092"}, "invalid Kafka broker"},
		{"invalid topic", ServeParams{MaxKafkaLag: time.Minute, EnableAccounting: true, KafkaBroker: "broker:9092", KafkaTopic: "bad topic"}, "invalid Kafka topic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, tc.p.Validate(), tc.want)
		})
	}
}

func TestKafkaAdvertisedAddrValidation(t *testing.T) {
	p := ServeParams{MaxKafkaLag: time.Minute, EnableKafka: true, CredsFile: "creds.json", KafkaBind: "127.0.0.1:9092", KafkaTopic: "events"}
	for _, addr := range []string{"", "kafka.example.com:9093", "203.0.113.1:443", "[2001:db8::1]:65535"} {
		p.KafkaAdvertisedAddr = addr
		require.NoError(t, p.Validate(), addr)
	}
	for _, addr := range []string{"broker", ":9093", "broker:", "broker:0", "broker:65536", "broker:-1", "broker:tls", "broker:+9093", "::1:9093"} {
		p.KafkaAdvertisedAddr = addr
		require.ErrorContains(t, p.Validate(), "invalid Kafka advertised address", addr)
	}
}

func TestAccountingConfigAndPrecedence(t *testing.T) {
	for _, format := range []string{"json", "toml"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "serve."+format)
			content := `{"EnableAccounting":true,"KafkaBroker":"config:9092"}`
			if format == "toml" {
				content = "EnableAccounting = true\nKafkaBroker = \"config:9092\"\n"
			}
			require.NoError(t, os.WriteFile(path, []byte(content), 0600))
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
			got := read("--config-file", path)
			require.Equal(t, time.Minute, got.MaxKafkaLag)
			require.True(t, got.EnableAccounting)
			require.False(t, got.EnableKafka)
			require.Equal(t, "config:9092", got.KafkaBroker)
			t.Setenv("CLEARINGHOUSE_KAFKA_BROKER", "env:9092")
			t.Setenv("CLEARINGHOUSE_ENABLE_ACCOUNTING", "false")
			t.Setenv("CLEARINGHOUSE_ENABLE_AUTH_WEBHOOK", ":8080")
			t.Setenv("CLEARINGHOUSE_WEBHOOK_TOKEN", "test")
			got = read("--config-file", path)
			require.False(t, got.EnableAccounting)
			require.Equal(t, "env:9092", got.KafkaBroker)
			got = read("--config-file", path, "--enable-accounting", "--kafka-broker", "flag:9092")
			require.True(t, got.EnableAccounting)
			require.Equal(t, "flag:9092", got.KafkaBroker)
			// Configuration files, environment, and flags use normal precedence.
			content = `{"MaxKafkaLag":90000000000}`
			if format == "toml" {
				content = "MaxKafkaLag = \"90s\"\n"
			}
			require.NoError(t, os.WriteFile(path, []byte(content), 0600))
			require.Equal(t, 90*time.Second, read("--config-file", path).MaxKafkaLag)
			t.Setenv("CLEARINGHOUSE_MAX_KAFKA_LAG", "120s")
			require.Equal(t, 120*time.Second, read("--config-file", path).MaxKafkaLag)
			require.Equal(t, 180*time.Second, read("--config-file", path, "--max-kafka-lag", "180s").MaxKafkaLag)
		})
	}
	help := cli(t, "serve", "--help")
	for _, want := range []string{"--max-kafka-lag", "CLEARINGHOUSE_MAX_KAFKA_LAG", "--enable-accounting", "--kafka-broker", "--kafka-advertised-addr", "CLEARINGHOUSE_ENABLE_ACCOUNTING", "CLEARINGHOUSE_KAFKA_BROKER", "CLEARINGHOUSE_KAFKA_ADVERTISED_ADDR", "Run embedded Kafka broker", "Run accounting service"} {
		require.Contains(t, help, want)
	}
}

func TestMaxKafkaLagValidation(t *testing.T) {
	p := ServeParams{KafkaTopic: "events", CredsFile: "creds.json", EnableAuthWebhook: mustHTTPBind(t, ":8080")}
	for _, age := range []time.Duration{0, -time.Second, time.Nanosecond, time.Millisecond, time.Second - time.Nanosecond} {
		p.MaxKafkaLag = age
		require.ErrorContains(t, p.Validate(), "--max-kafka-lag must be at least 1s")
	}
	p.MaxKafkaLag = time.Second
	require.NoError(t, p.Validate())
	p.KafkaTopic = "bad topic"
	require.ErrorContains(t, p.Validate(), "invalid Kafka topic")
}

func TestKafkaLagStartupInvalidation(t *testing.T) {
	f := testutil.New(t, "100")
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	p := ServeParams{
		Common: Common{DBPath: f.Path}, CredsFile: testCredsFile(t),
		EnableAccounting: true, KafkaBroker: "127.0.0.1:1",
		KafkaTopic: f.DB.KafkaTopic, MaxKafkaLag: time.Minute,
		EnableAuthWebhook: mustHTTPBind(t, occupied.Addr().String()),
	}
	require.ErrorContains(t, Serve(t.Context(), p), "address already in use")
	d, err := f.DB.Authorize(t.Context(), f.Request)
	require.NoError(t, err)
	require.Equal(t, 503, d.Status)
	require.Equal(t, "kafka lag unavailable", d.Reason)
}

func TestKafkaLagWithoutMetrics(t *testing.T) {
	for _, age := range []time.Duration{time.Minute, time.Second} {
		t.Run(age.String(), func(t *testing.T) {
			f := testutil.New(t, "100")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			broker, err := kafka.OpenBroker(ctx, testutil.Port(t), "", "events", t.TempDir(), nil)
			require.NoError(t, err)
			brokerDone := make(chan error, 1)
			go func() { brokerDone <- broker.Serve(ctx) }()
			defer func() { cancel(); broker.Close(); require.NoError(t, <-brokerDone) }()
			appCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			p := ServeParams{
				Common: Common{DBPath: f.Path}, EnableAccounting: true,
				KafkaBroker: broker.Addr(), KafkaTopic: "events", MaxKafkaLag: age,
			}
			go func() { done <- Serve(appCtx, p) }()
			stopped := false
			defer func() {
				stop()
				if !stopped {
					require.NoError(t, <-done)
				}
			}()
			f.DB.KafkaTopic = p.KafkaTopic
			f.DB.MaxKafkaLag = age
			testutil.Eventually(t, func() bool {
				d, err := f.DB.Authorize(ctx, f.Request)
				return err == nil && d.Status == 200
			})
			require.False(t, p.EnableMetrics.IsValid())
			if age == time.Second {
				until := time.Now().Add(2500 * time.Millisecond)
				for time.Now().Before(until) {
					d, err := f.DB.Authorize(ctx, f.Request)
					require.NoError(t, err)
					require.Equal(t, 200, d.Status, "an idle topic must stay fresh across multiple threshold intervals")
					time.Sleep(20 * time.Millisecond)
				}
			}
			stop()
			require.NoError(t, <-done)
			stopped = true
			d, err := f.DB.Authorize(ctx, f.Request)
			require.NoError(t, err)
			require.Equal(t, 503, d.Status, "graceful shutdown invalidates the proof")
		})
	}
}
