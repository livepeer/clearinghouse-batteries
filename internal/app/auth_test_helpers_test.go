package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/stretchr/testify/require"
)

const testOperatorToken = "test-operator-secret"
const testWebhookToken = "test-webhook-secret"

const testHTTPCredentials = `
	{"id":"operator","secret":"test-operator-secret","management":{"allow":["grants.*","allocations.*","api_keys.*","sessions.*","settlements.*","usage.*","ledger.*","escrow.*"]}},
	{"id":"signer","secret":"test-webhook-secret","webhook":{"authorize":true}}`

func testCredsFile(t testing.TB, credentials ...string) string {
	t.Helper()
	if len(credentials) == 0 {
		credentials = []string{testHTTPCredentials,
			`{"id":"accounting","secret":"read-secret","kafka":{"username":"accounting","allow":["read"],"accounting":true}}`,
			`{"id":"producer","secret":"write-secret","kafka":{"username":"producer","allow":["write"]}}`}
	}
	path := filepath.Join(t.TempDir(), "creds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"credentials":[`+strings.Join(credentials, ",")+`]}`), 0600))
	return path
}

func testRegistry(t testing.TB, credentials ...string) *serviceauth.Registry {
	t.Helper()
	registry, err := serviceauth.Load(testCredsFile(t, credentials...))
	require.NoError(t, err)
	return registry
}

func testHTTPCredsFile(t testing.TB) string {
	t.Helper()
	return testCredsFile(t, testHTTPCredentials)
}
