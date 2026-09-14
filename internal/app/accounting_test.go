package app

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/spf13/cobra"
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
			if err := tc.p.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
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
			testutil.Must(t, os.WriteFile(path, []byte(content), 0600))
			read := func(args ...string) ServeParams {
				t.Helper()
				var got ServeParams
				cmd := command[ServeParams]("serve", "test", func(p *ServeParams, _ *cobra.Command) error {
					got = *p
					return nil
				})
				cmd.SetArgs(args)
				testutil.Must(t, cmd.Execute())
				return got
			}
			got := read("--config-file", path)
			if !got.EnableAccounting || got.EnableKafka || !reflect.DeepEqual(got.KafkaBrokers, []string{"config1:9092", "config2:9092"}) {
				t.Fatal(got)
			}
			t.Setenv("CLEARINGHOUSE_KAFKA_BROKERS", "env1:9092,env2:9092")
			t.Setenv("CLEARINGHOUSE_ENABLE_ACCOUNTING", "false")
			t.Setenv("CLEARINGHOUSE_ENABLE_AUTH_WEBHOOK", "true")
			t.Setenv("CLEARINGHOUSE_WEBHOOK_TOKEN", "test")
			got = read("--config-file", path)
			if got.EnableAccounting || !reflect.DeepEqual(got.KafkaBrokers, []string{"env1:9092", "env2:9092"}) {
				t.Fatal(got)
			}
			got = read("--config-file", path, "--enable-accounting", "--kafka-brokers", "flag1:9092,flag2:9092")
			if !got.EnableAccounting || !reflect.DeepEqual(got.KafkaBrokers, []string{"flag1:9092", "flag2:9092"}) {
				t.Fatal(got)
			}
		})
	}
	help := cli(t, "serve", "--help")
	for _, want := range []string{"--enable-accounting", "--kafka-brokers", "CLEARINGHOUSE_ENABLE_ACCOUNTING", "CLEARINGHOUSE_KAFKA_BROKERS", "Run embedded Kafka broker", "Run accounting service"} {
		if !strings.Contains(help, want) {
			t.Fatalf("missing help %q", want)
		}
	}
}
