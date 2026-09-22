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
		{"missing broker", ServeParams{EnableAccounting: true}, "--kafka-broker required"},
		{"ambiguous source", ServeParams{EnableKafka: true, KafkaBroker: "broker:9092"}, "cannot be used"},
		{"invalid address", ServeParams{EnableAccounting: true, KafkaBroker: "broker"}, "invalid Kafka broker"},
		{"invalid port", ServeParams{EnableAccounting: true, KafkaBroker: "broker:99999"}, "invalid Kafka broker"},
		{"empty host", ServeParams{EnableAccounting: true, KafkaBroker: ":9092"}, "invalid Kafka broker"},
		{"invalid topic", ServeParams{EnableAccounting: true, KafkaBroker: "broker:9092", KafkaTopic: "bad topic"}, "invalid Kafka topic"},
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
		})
	}
	help := cli(t, "serve", "--help")
	for _, want := range []string{"--enable-accounting", "--kafka-broker", "CLEARINGHOUSE_ENABLE_ACCOUNTING", "CLEARINGHOUSE_KAFKA_BROKER", "Run embedded Kafka broker", "Run accounting service"} {
		require.Contains(t, help, want)
	}
}
