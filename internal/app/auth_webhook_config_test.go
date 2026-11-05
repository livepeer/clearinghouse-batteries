package app

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestHTTPBindValidation(t *testing.T) {
	for _, listener := range []struct{ field, flag string }{
		{"EnableAuthWebhook", "enable-auth-webhook"},
		{"EnableManagementAPI", "enable-management-api"},
		{"EnableMetrics", "enable-metrics"},
	} {
		for _, source := range []string{"flag", "env", "json", "toml"} {
			for _, tc := range []struct {
				input, want string
				invalid     bool
			}{
				{":8080", "127.0.0.1:8080", false},
				{"[::1]:8080", "[::1]:8080", false},
				{"0.0.0.0:8080", "", true},
				{"unsafe@0.0.0.0:8080", "0.0.0.0:8080", false},
			} {
				t.Run(listener.field+"/"+source+"/"+tc.input, func(t *testing.T) {
					var got ServeParams
					called := false
					cmd := command[ServeParams]("serve", "test", func(p *ServeParams, _ *cobra.Command) error {
						got, called = *p, true
						return nil
					})
					cmd.SetOut(io.Discard)
					cmd.SetErr(io.Discard)
					args := []string{}
					switch source {
					case "flag":
						args = append(args, "--"+listener.flag, tc.input)
					case "env":
						t.Setenv("CLEARINGHOUSE_"+strings.ToUpper(strings.ReplaceAll(listener.flag, "-", "_")), tc.input)
					case "json", "toml":
						config := fmt.Sprintf("{%q: %q}", listener.field, tc.input)
						if source == "toml" {
							config = fmt.Sprintf("%s = %q\n", listener.field, tc.input)
						}
						path := filepath.Join(t.TempDir(), "serve."+source)
						require.NoError(t, os.WriteFile(path, []byte(config), 0600))
						args = append(args, "--config-file", path)
					}
					cmd.SetArgs(args)
					err := cmd.Execute()
					if tc.invalid {
						require.ErrorContains(t, err, "unsafe@"+tc.input)
						require.True(t, boa.IsUserInputError(err))
						require.False(t, called)
						return
					}
					require.NoError(t, err)
					require.True(t, called)
					bind := map[string]boa.HTTPBind{
						"EnableAuthWebhook":   got.EnableAuthWebhook,
						"EnableManagementAPI": got.EnableManagementAPI,
						"EnableMetrics":       got.EnableMetrics,
					}[listener.field]
					require.Equal(t, tc.want, bind.String())
				})
			}
		}
	}
}

func TestAuthWebhookRequiresCredentials(t *testing.T) {
	p := ServeParams{MaxKafkaLag: time.Minute, KafkaTopic: "livepeer-signing", EnableAuthWebhook: mustHTTPBind(t, ":8080")}
	require.ErrorContains(t, p.Validate(), "--creds-file")
	p.CredsFile = "creds.json"
	require.NoError(t, p.Validate())
}

func TestServeConfigPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serve.toml")
	credsPath := testCredsFile(t)
	config := fmt.Sprintf("EnableKafka = true\nEnableAuthWebhook = \":7101\"\nCredsFile = %q\nKafkaAdvertisedAddr = \"config:9093\"\n", credsPath)
	require.NoError(t, os.WriteFile(path, []byte(config), 0600))
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

	require.Empty(t, read("--enable-kafka", "--creds-file", credsPath).KafkaAdvertisedAddr)
	fromConfig := read("--config-file", path)
	require.Equal(t, "127.0.0.1:7101", fromConfig.EnableAuthWebhook.String())
	require.Equal(t, credsPath, fromConfig.CredsFile)
	require.True(t, fromConfig.EnableKafka)
	require.Equal(t, "config:9093", fromConfig.KafkaAdvertisedAddr)

	t.Setenv("CLEARINGHOUSE_ENABLE_AUTH_WEBHOOK", ":7102")
	envCredsPath := testCredsFile(t)
	t.Setenv("CLEARINGHOUSE_CREDS_FILE", envCredsPath)
	t.Setenv("CLEARINGHOUSE_KAFKA_ADVERTISED_ADDR", "env:9093")
	fromEnv := read("--config-file", path)
	require.Equal(t, "127.0.0.1:7102", fromEnv.EnableAuthWebhook.String())
	require.Equal(t, envCredsPath, fromEnv.CredsFile)
	require.Equal(t, "env:9093", fromEnv.KafkaAdvertisedAddr)

	got := read("--config-file", path, "--enable-auth-webhook", "unsafe@0.0.0.0:7103", "--creds-file", credsPath, "--kafka-advertised-addr", "flag:9093")
	require.Equal(t, "0.0.0.0:7103", got.EnableAuthWebhook.String())
	require.Equal(t, credsPath, got.CredsFile)
	require.Equal(t, "flag:9093", got.KafkaAdvertisedAddr)
}
