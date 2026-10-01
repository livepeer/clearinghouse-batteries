package serviceauth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/stretchr/testify/require"
)

func init() { boa.RegisterConfigFormat(".toml", toml.Unmarshal) }

func loadFixture(t *testing.T, format, data string) (*Registry, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auth."+format)
	require.NoError(t, os.WriteFile(path, fixtureData(t, format, data), 0600))
	return Load(path)
}

func fixtureData(t *testing.T, format, data string) []byte {
	t.Helper()
	if format == "toml" {
		var fields map[string]any
		require.NoError(t, json.Unmarshal([]byte(data), &fields))
		encoded, err := toml.Marshal(fields)
		require.NoError(t, err)
		return encoded
	}
	return []byte(data)
}

func request(token string) *http.Request {
	r := httptest.NewRequest("GET", "/", nil)
	if token != "" {
		r.Header.Set(Header, token)
	}
	return r
}

func TestRegistryCredentialsAndResourceWildcards(t *testing.T) {
	for _, format := range []string{"json", "toml"} {
		t.Run(format, func(t *testing.T) {
			r, err := loadFixture(t, format, `{"credentials":[
		{"id":"operator","secret":"operator-secret.with.dot","management":{"allow":["grants.*","api_keys.create"]}},
		{"id":"signer","secret":"signer secret\t café","webhook":{"authorize":true}},
		{"id":"reader","secret":"reader-secret","kafka":{"username":"accounting","allow":["read"],"accounting":true}},
		{"id":"writer","secret":"writer-secret","kafka":{"username":"producer","allow":["write"]}}
	]}`)
			require.NoError(t, err)
			require.Equal(t, 1, r.Count("management"))
			require.Equal(t, 2, r.Count("kafka"))
			require.Equal(t, []KafkaCredential{
				{Username: "accounting", Password: "reader-secret", Read: true, Accounting: true},
				{Username: "producer", Password: "writer-secret", Write: true},
			}, r.KafkaCredentials())
			require.Equal(t, http.StatusOK, r.Check(request("operator-secret.with.dot"), "management", "grants.publish"))
			require.Equal(t, http.StatusForbidden, r.Check(request("operator-secret.with.dot"), "management", "allocations.fund"))
			require.Equal(t, http.StatusForbidden, r.Check(request("operator-secret.with.dot"), "management", "api_keys.create", "allocations.create"))
			require.Equal(t, http.StatusUnauthorized, r.Check(request("signer secret\t café"), "management", "grants.read"))
			require.Equal(t, http.StatusUnauthorized, r.Check(request("operator.operator-secret.with.dot"), "management", "grants.read"))
			require.Equal(t, http.StatusUnauthorized, r.Check(request("wrong"), "management", "grants.read"))
			require.Equal(t, http.StatusUnauthorized, r.Check(request(""), "management", "grants.read"))
			standardHeader := request("")
			standardHeader.Header.Set("Authorization", "Bearer operator-secret.with.dot")
			require.Equal(t, http.StatusUnauthorized, r.Check(standardHeader, "management", "grants.read"))
			require.Equal(t, http.StatusOK, r.Check(request("signer secret\t café"), "webhook", "authorize"))
			duplicate := request("operator-secret.with.dot")
			duplicate.Header.Add(Header, "operator-secret.with.dot")
			require.Equal(t, http.StatusUnauthorized, r.Check(duplicate, "management", "grants.read"))
		})
	}
}

func TestRegistryRejectsUnsafeConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, data, want string }{
		{"no credentials", `{}`, "missing required param"},
		{"empty credentials", `{"credentials":[]}`, "min 1"},
		{"missing id", `{"credentials":[{"secret":"secret","webhook":{"authorize":true}}]}`, "missing required param"},
		{"missing secret", `{"credentials":[{"id":"a","webhook":{"authorize":true}}]}`, "missing required param"},
		{"invalid id", `{"credentials":[{"id":"invalid.id","secret":"secret","webhook":{"authorize":true}}]}`, "pattern"},
		{"invalid secret", `{"credentials":[{"id":"a","secret":"sensitive\nsecret","webhook":{"authorize":true}}]}`, "invalid auth credential secret"},
		{"leading space", `{"credentials":[{"id":"a","secret":" sensitive","management":{"allow":["grants.read"]}}]}`, "invalid auth credential secret"},
		{"trailing tab", `{"credentials":[{"id":"a","secret":"sensitive\t","webhook":{"authorize":true}}]}`, "invalid auth credential secret"},
		{"control byte", `{"credentials":[{"id":"a","secret":"sensitive\u0007secret","webhook":{"authorize":true}}]}`, "invalid auth credential secret"},
		{"delete byte", `{"credentials":[{"id":"a","secret":"sensitive\u007fsecret","webhook":{"authorize":true}}]}`, "invalid auth credential secret"},
		{"duplicate id", `{"credentials":[{"id":"a","secret":"a","webhook":{"authorize":true}},{"id":"a","secret":"b","webhook":{"authorize":true}}]}`, "duplicate auth credential id"},
		{"duplicate secret", `{"credentials":[{"id":"a","secret":"same","webhook":{"authorize":true}},{"id":"b","secret":"same","webhook":{"authorize":true}}]}`, "duplicate auth credential secret"},
		{"multiple integrations", `{"credentials":[{"id":"a","secret":"a","webhook":{"authorize":true},"management":{"allow":["grants.read"]}}]}`, "exactly one integration"},
		{"invalid permission", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["grants.delete"]}}]}`, "allowed values"},
		{"multiple actions", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["grants.read create"]}}]}`, "allowed values"},
		{"action suffix", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["allocations.status revoke"]}}]}`, "allowed values"},
		{"whitespace", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["grants.read "]}}]}`, "allowed values"},
		{"empty action", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["grants."]}}]}`, "allowed values"},
		{"invalid wildcard", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["*.read"]}}]}`, "allowed values"},
		{"unknown resource", `{"credentials":[{"id":"a","secret":"a","management":{"allow":["unknown.*"]}}]}`, "allowed values"},
		{"no integration", `{"credentials":[{"id":"a","secret":"a"}]}`, "exactly one integration"},
		{"empty management", `{"credentials":[{"id":"a","secret":"a","management":{"allow":[]}}]}`, "min 1"},
		{"disabled webhook", `{"credentials":[{"id":"a","secret":"a","webhook":{"authorize":false}}]}`, "allowed values"},
		{"missing Kafka username", `{"credentials":[{"id":"a","secret":"a","kafka":{"allow":["read"]}}]}`, "missing required param"},
		{"empty Kafka", `{"credentials":[{"id":"a","secret":"a","kafka":{"username":"a","allow":[]}}]}`, "min 1"},
		{"invalid Kafka action", `{"credentials":[{"id":"a","secret":"a","kafka":{"username":"a","allow":["read write"]}}]}`, "allowed values"},
		{"duplicate Kafka username", `{"credentials":[{"id":"a","secret":"a","kafka":{"username":"same","allow":["read"]}},{"id":"b","secret":"b","kafka":{"username":"same","allow":["write"]}}]}`, "duplicate Kafka username"},
		{"normalized duplicate username", `{"credentials":[{"id":"a","secret":"a","kafka":{"username":"reader","allow":["read"]}},{"id":"b","secret":"b","kafka":{"username":"read\u00ader","allow":["write"]}}]}`, "duplicate Kafka username"},
		{"empty normalized username", `{"credentials":[{"id":"a","secret":"sensitive","kafka":{"username":"\u00ad","allow":["read"]}}]}`, "Kafka username"},
		{"invalid SCRAM username", `{"credentials":[{"id":"a","secret":"sensitive","kafka":{"username":"user\u0007","allow":["read"]}}]}`, "Kafka username"},
		{"invalid SCRAM secret", `{"credentials":[{"id":"a","secret":"sensitive\u0007","kafka":{"username":"user","allow":["read"]}}]}`, "Kafka secret"},
		{"accounting cannot write", `{"credentials":[{"id":"a","secret":"a","kafka":{"username":"a","allow":["write"],"accounting":true}}]}`, "requires read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []string{"json", "toml"} {
				t.Run(format, func(t *testing.T) {
					_, err := loadFixture(t, format, tc.data)
					require.ErrorContains(t, err, tc.want)
					require.True(t, boa.IsUserInputError(err))
					require.NotContains(t, err.Error(), "same")
					require.NotContains(t, err.Error(), "sensitive")
				})
			}
		})
	}
}

func TestRegistryRejectsUnknownFields(t *testing.T) {
	for _, tc := range []struct{ name, data, field string }{
		{"root", `{"unused":"sensitive-value","credentials":[{"id":"a","secret":"sensitive-value","webhook":{"authorize":true}}]}`, "unused"},
		{"excluded source", `{"Source":"sensitive-value","credentials":[{"id":"a","secret":"sensitive-value","webhook":{"authorize":true}}]}`, "Source"},
		{"credential", `{"credentials":[{"id":"a","secret":"sensitive-value","webhook":{"authorize":true},"unused":"sensitive-value"}]}`, "credentials[0].unused"},
		{"later credential", `{"credentials":[{"id":"a","secret":"first","webhook":{"authorize":true}},{"id":"b","secret":"sensitive-value","webhook":{"authorize":true},"unused":"sensitive-value"}]}`, "credentials[1].unused"},
		{"management", `{"credentials":[{"id":"a","secret":"sensitive-value","management":{"allow":["grants.read"],"unused":"sensitive-value"}}]}`, "credentials[0].management.unused"},
		{"webhook", `{"credentials":[{"id":"a","secret":"first","webhook":{"authorize":true}},{"id":"b","secret":"sensitive-value","webhook":{"authorize":true,"unused":"sensitive-value"}}]}`, "credentials[1].webhook.unused"},
		{"Kafka", `{"credentials":[{"id":"a","secret":"first","webhook":{"authorize":true}},{"id":"b","secret":"sensitive-value","kafka":{"username":"reader","allow":["read"],"unused":"sensitive-value"}}]}`, "credentials[1].kafka.unused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []string{"json", "toml"} {
				t.Run(format, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "auth."+format)
					require.NoError(t, os.WriteFile(path, fixtureData(t, format, tc.data), 0600))
					registry, err := Load(path)
					require.Nil(t, registry)
					require.ErrorContains(t, err, "unknown config field")
					require.ErrorContains(t, err, tc.field)
					require.ErrorContains(t, err, path)
					require.True(t, boa.IsUserInputError(err))
					require.NotContains(t, err.Error(), "sensitive-value")
				})
			}
		})
	}
}

func TestRegistryLargeValidFiles(t *testing.T) {
	data := `{"credentials":[{"id":"a","secret":"secret","webhook":{"authorize":true}}]}`
	for _, format := range []string{"json", "toml"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth."+format)
			require.NoError(t, os.WriteFile(path, append(fixtureData(t, format, data), []byte(strings.Repeat(" ", 128<<10))...), 0600))
			_, err := Load(path)
			require.NoError(t, err)
		})
	}
}

func TestRegistryMalformedFiles(t *testing.T) {
	for format, data := range map[string]string{"json": `{ "credentials": [] } true`, "toml": `[[credentials]`} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth."+format)
			require.NoError(t, os.WriteFile(path, []byte(data), 0600))
			_, err := Load(path)
			require.ErrorContains(t, err, "failed to unmarshal config file")
		})
	}
}

func TestKafkaCredentialsAreCopied(t *testing.T) {
	r, err := loadFixture(t, "json", `{"credentials":[{"id":"reader","secret":"secret","kafka":{"username":"reader","allow":["read"],"accounting":true}}]}`)
	require.NoError(t, err)
	copy := r.KafkaCredentials()
	copy[0].Write = true
	copy[0].Password = "changed"
	require.Equal(t, []KafkaCredential{{Username: "reader", Password: "secret", Read: true, Accounting: true}}, r.KafkaCredentials())
}

func TestRegistryChangesOnReload(t *testing.T) {
	for _, format := range []string{"json", "toml"} {
		t.Run(format, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth."+format)
			write := func(secret string) {
				t.Helper()
				require.NoError(t, os.WriteFile(path, fixtureData(t, format, `{"credentials":[{"id":"op","secret":"`+secret+`","management":{"allow":["grants.read"]}}]}`), 0600))
			}
			write("first")
			old, err := Load(path)
			require.NoError(t, err)
			write("second")
			fresh, err := Load(path)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, old.Check(request("first"), "management", "grants.read"))
			require.Equal(t, http.StatusUnauthorized, old.Check(request("second"), "management", "grants.read"))
			require.Equal(t, http.StatusUnauthorized, fresh.Check(request("first"), "management", "grants.read"))
			require.Equal(t, http.StatusOK, fresh.Check(request("second"), "management", "grants.read"))
		})
	}
}
