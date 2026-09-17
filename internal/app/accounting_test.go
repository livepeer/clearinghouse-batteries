package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestAccountingValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    ServeParams
		want string
	}{
		{"missing brokers", ServeParams{EnableAccounting: true}, "--kafka-brokers required"},
		{"ambiguous source", ServeParams{EnableKafka: true, KafkaBrokers: []string{"broker:9092"}}, "cannot be used"},
		{"invalid address", ServeParams{EnableAccounting: true, KafkaBrokers: []string{"broker"}}, "invalid Kafka broker"},
		{"invalid port", ServeParams{EnableAccounting: true, KafkaBrokers: []string{"broker:99999"}}, "invalid Kafka broker"},
		{"empty host", ServeParams{EnableAccounting: true, KafkaBrokers: []string{":9092"}}, "invalid Kafka broker"},
		{"invalid topic", ServeParams{EnableAccounting: true, KafkaBrokers: []string{"broker:9092"}, KafkaTopic: "bad topic"}, "invalid Kafka topic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, tc.p.Validate(), tc.want)
		})
	}
}

func TestAccountingConfigAndPrecedence(t *testing.T) {
	for _, format := range []string{"json", "toml"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "serve."+format)
			content := `{"EnableAccounting":true,"KafkaBrokers":["config1:9092","config2:9092"]}`
			if format == "toml" {
				content = "EnableAccounting = true\nKafkaBrokers = [\"config1:9092\", \"config2:9092\"]\n"
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
			require.True(t, got.EnableAccounting)
			require.False(t, got.EnableKafka)
			require.Equal(t, []string{"config1:9092", "config2:9092"}, got.KafkaBrokers)
			t.Setenv("CLEARINGHOUSE_KAFKA_BROKERS", "env1:9092,env2:9092")
			t.Setenv("CLEARINGHOUSE_ENABLE_ACCOUNTING", "false")
			t.Setenv("CLEARINGHOUSE_ENABLE_AUTH_WEBHOOK", "true")
			t.Setenv("CLEARINGHOUSE_WEBHOOK_TOKEN", "test")
			got = read("--config-file", path)
			require.False(t, got.EnableAccounting)
			require.Equal(t, []string{"env1:9092", "env2:9092"}, got.KafkaBrokers)
			got = read("--config-file", path, "--enable-accounting", "--kafka-brokers", "flag1:9092,flag2:9092")
			require.True(t, got.EnableAccounting)
			require.Equal(t, []string{"flag1:9092", "flag2:9092"}, got.KafkaBrokers)
		})
	}
	help := cli(t, "serve", "--help")
	for _, want := range []string{"--enable-accounting", "--kafka-brokers", "CLEARINGHOUSE_ENABLE_ACCOUNTING", "CLEARINGHOUSE_KAFKA_BROKERS", "Run embedded Kafka broker", "Run accounting service"} {
		require.Contains(t, help, want)
	}
}
