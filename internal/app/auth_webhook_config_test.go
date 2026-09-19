package app

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestHTTPBindParsing(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{":8080", "127.0.0.1:8080"},
		{"127.0.0.2:8081", "127.0.0.2:8081"},
		{"[::1]:8082", "[::1]:8082"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			var bind HTTPBind
			require.NoError(t, bind.UnmarshalText([]byte(tc.input)))
			require.Equal(t, tc.want, bind.String())
			require.True(t, bind.IsValid())
		})
	}

	var disabled HTTPBind
	require.NoError(t, disabled.UnmarshalText(nil))
	require.False(t, disabled.IsValid())

	for _, input := range []string{
		"true",
		"127.0.0.1",
		"127.0.0.1:",
		":0",
		":65536",
		":http",
		"localhost:8080",
		"::1:8080",
	} {
		t.Run("invalid_"+input, func(t *testing.T) {
			var bind HTTPBind
			require.Error(t, bind.UnmarshalText([]byte(input)))
		})
	}
}

func TestAuthWebhookBindValidation(t *testing.T) {
	for _, input := range []string{":8080", "127.0.0.2:8080", "[::1]:8080"} {
		t.Run("loopback_"+input, func(t *testing.T) {
			p := ServeParams{EnableAuthWebhook: mustHTTPBind(t, input), WebhookToken: "token"}
			require.NoError(t, p.Validate())
		})
	}

	for _, input := range []string{"0.0.0.0:8080", "[::]:8080", "10.0.0.1:8080", "8.8.8.8:8080"} {
		t.Run("non_loopback_"+input, func(t *testing.T) {
			p := ServeParams{EnableAuthWebhook: mustHTTPBind(t, input), WebhookToken: "token"}
			require.ErrorContains(t, p.Validate(), "--unsafe-http-bind")
			p.UnsafeHTTPBind = true
			require.NoError(t, p.Validate())
		})
	}

	p := ServeParams{EnableAuthWebhook: mustHTTPBind(t, ":8080")}
	require.ErrorContains(t, p.Validate(), "CLEARINGHOUSE_WEBHOOK_TOKEN")
	require.NoError(t, (ServeParams{UnsafeHTTPBind: true, EnableKafka: true, KafkaBind: "127.0.0.1:9092", KafkaTopic: "events"}).Validate())
}

func TestAuthWebhookConfigPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "serve.toml")
	tokenPath := filepath.Join(dir, "webhook-token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("token"), 0600))
	config := fmt.Sprintf("EnableAuthWebhook = \":7101\"\nWebhookTokenFile = %q\n", tokenPath)
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

	fromConfig := read("--config-file", path)
	require.Equal(t, "127.0.0.1:7101", fromConfig.EnableAuthWebhook.String())
	require.Equal(t, "token", fromConfig.WebhookToken)

	t.Setenv("CLEARINGHOUSE_ENABLE_AUTH_WEBHOOK", ":7102")
	require.Equal(t, "127.0.0.1:7102", read("--config-file", path).EnableAuthWebhook.String())

	got := read("--config-file", path, "--enable-auth-webhook", ":7103", "--unsafe-http-bind")
	require.Equal(t, "127.0.0.1:7103", got.EnableAuthWebhook.String())
	require.True(t, got.UnsafeHTTPBind)
}
