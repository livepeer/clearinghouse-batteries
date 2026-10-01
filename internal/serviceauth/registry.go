// Package serviceauth loads service credentials and checks HTTP permissions.
package serviceauth

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/xdg-go/stringprep"
)

const Header = "Livepeer-Clearinghouse-Token"

var managementPermissions = []string{
	"grants.*", "grants.read", "grants.create", "grants.fund", "grants.status",
	"allocations.*", "allocations.read", "allocations.create", "allocations.fund", "allocations.status", "allocations.revoke",
	"api_keys.*", "api_keys.read", "api_keys.create", "api_keys.revoke",
	"sessions.*", "sessions.read", "sessions.revoke",
	"settlements.*", "settlements.read",
	"usage.*", "usage.read",
	"ledger.*", "ledger.read",
	"escrow.*", "escrow.read",
}

type entry struct {
	ID         string `boa:"configonly" pattern:"^[A-Za-z0-9_-]{1,64}$"`
	Secret     string `boa:"configonly"`
	Management *struct {
		Allow []string `boa:"configonly" min:"1"`
	}
	Webhook *struct {
		Authorize bool `boa:"configonly" alts:"true"`
	}
	Kafka *struct {
		Username   string   `boa:"configonly"`
		Allow      []string `boa:"configonly" min:"1" alts:"read,write"`
		Accounting bool     `boa:"configonly" optional:"true"`
	}
}

func (e *entry) InitCtx(ctx *boa.HookContext) error {
	if e.Management != nil {
		boa.Param(ctx, &e.Management.Allow).SetAlternatives(managementPermissions)
	}
	return nil
}

type config struct {
	Source      string  `configfile:"true" boa:"configonly" json:"-" toml:"-"`
	Credentials []entry `boa:"configonly" min:"1"`
	registry    *Registry
}

func (c *config) InitCtx(ctx *boa.HookContext) error {
	c.registry = &Registry{principals: make(map[[32]byte]principal), counts: make(map[string]int)}
	boa.Param(ctx, &c.Credentials).SetCustomValidator(c.registry.add)
	return nil
}

type principal struct {
	integration string
	allow       []string
}

type KafkaCredential struct {
	Username   string
	Password   string
	Read       bool
	Write      bool
	Accounting bool
}

type Registry struct {
	principals map[[32]byte]principal
	kafka      []KafkaCredential
	counts     map[string]int
}

// Load reads one immutable registry snapshot. A restart is required to rotate it.
func Load(path string) (*Registry, error) {
	data := config{Source: path}
	if err := (boa.Cmd[config]{Params: &data, RawArgs: []string{}, RejectUnknown: true}).Validate(); err != nil {
		return nil, err
	}
	return data.registry, nil
}

func (r *Registry) add(entries []entry) error {
	ids := make(map[string]bool)
	usernames := make(map[string]bool)
	for _, item := range entries {
		if err := (boa.Cmd[entry]{Params: &item, RawArgs: []string{}}).Validate(); err != nil {
			return err
		}
		if item.Kafka == nil && (strings.Trim(item.Secret, " \t") != item.Secret ||
			strings.ContainsFunc(item.Secret, func(c rune) bool { return c < ' ' && c != '\t' || c == 127 })) {
			return errors.New("invalid auth credential secret")
		}
		if ids[item.ID] {
			return fmt.Errorf("duplicate auth credential id %q", item.ID)
		}
		ids[item.ID] = true
		hash := sha256.Sum256([]byte(item.Secret))
		if _, exists := r.principals[hash]; exists {
			return errors.New("duplicate auth credential secret")
		}
		var p principal
		switch {
		case item.Management != nil && item.Webhook == nil && item.Kafka == nil:
			p = principal{"management", item.Management.Allow}
		case item.Webhook != nil && item.Management == nil && item.Kafka == nil:
			p = principal{"webhook", []string{"authorize"}}
		case item.Kafka != nil && item.Management == nil && item.Webhook == nil:
			p = principal{"kafka", item.Kafka.Allow}
			// SCRAM normalizes identities; SASL/PLAIN still uses the original spelling.
			username, err := stringprep.SASLprep.Prepare(item.Kafka.Username)
			if err != nil || username == "" || username == "*" || usernames[username] {
				return fmt.Errorf("invalid or duplicate Kafka username for %q", item.ID)
			}
			if _, err := stringprep.SASLprep.Prepare(item.Secret); err != nil {
				return fmt.Errorf("invalid Kafka secret for %q", item.ID)
			}
			usernames[username] = true
			credential := KafkaCredential{Username: item.Kafka.Username, Password: item.Secret,
				Read: slices.Contains(p.allow, "read"), Write: slices.Contains(p.allow, "write"), Accounting: item.Kafka.Accounting}
			if credential.Accounting && !credential.Read {
				return fmt.Errorf("Kafka accounting credential %q requires read", item.ID)
			}
			r.kafka = append(r.kafka, credential)
		default:
			return fmt.Errorf("auth credential %q must configure exactly one integration", item.ID)
		}
		r.principals[hash] = p
		r.counts[p.integration]++
	}
	return nil
}

func (r *Registry) Count(integration string) int {
	if r == nil {
		return 0
	}
	return r.counts[integration]
}

func (r *Registry) KafkaCredentials() []KafkaCredential {
	if r == nil {
		return nil
	}
	return slices.Clone(r.kafka)
}

// Check authenticates one HTTP credential and authorizes all required actions.
func (r *Registry) Check(request *http.Request, integration string, permissions ...string) int {
	if r == nil {
		return http.StatusUnauthorized
	}
	values := request.Header.Values(Header)
	if len(values) != 1 {
		return http.StatusUnauthorized
	}
	// Not constant-time, but hashing prevents prefix matches via lookup timing.
	hash := sha256.Sum256([]byte(values[0]))
	p, exists := r.principals[hash]
	if !exists || p.integration != integration {
		return http.StatusUnauthorized
	}
	for _, permission := range permissions {
		resource, _, hasResource := strings.Cut(permission, ".")
		if !slices.Contains(p.allow, permission) && !(hasResource && slices.Contains(p.allow, resource+".*")) {
			return http.StatusForbidden
		}
	}
	return http.StatusOK
}

func (r *Registry) Require(w http.ResponseWriter, request *http.Request, integration string, permissions ...string) bool {
	status := r.Check(request, integration, permissions...)
	if status == http.StatusOK {
		return true
	}
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, http.StatusText(status), status)
	return false
}
