package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/units"
	"github.com/livepeer/clearinghouse/migrations"
	"github.com/spf13/cobra"
)

func init() {
	boa.RegisterConfigFormat(".toml", toml.Unmarshal)
}

type IDParams struct {
	Common
	ID string `descr:"Resource ID"`
}
type ListParams struct{ Common }
type StatusParams struct {
	IDParams
	Status string `descr:"New status"`
}
type FundParams struct {
	IDParams
	AmountETH string `descr:"Additional ETH (exact decimal; allocations also accept 'all')"`
}
type CreateParams struct {
	Common
	Name        string
	AmountETH   string `default:"0" descr:"Initial ETH funding (allocations also accept 'all')"`
	Sponsor     string `optional:"true"`
	Beneficiary string `optional:"true"`
	GrantID     string `optional:"true"`
	Status      string `optional:"true" descr:"Initial status"`
	Metadata    string `default:"{}"`
	StartsAt    string `optional:"true" descr:"RFC3339 start time"`
	EndsAt      string `optional:"true" descr:"RFC3339 end time"`
}
type KeyParams struct {
	Common
	AllocationID string `optional:"true"`
	GrantID      string `optional:"true"`
	Name         string
	AmountETH    string `optional:"true" descr:"Required ETH funding with --grant-id; accepts 'all'"`
}

func command[T any](use, short string, fn func(*T, *cobra.Command) error, enrich ...boa.ParamEnricher) *cobra.Command {
	return boa.Cmd[T]{Use: use, Short: short, ParamEnrich: boa.ParamEnricherCombine(
		boa.ParamEnricherDefault,
		boa.ParamEnricherEnv,
		boa.ParamEnricherEnvPrefix("CLEARINGHOUSE"),
		boa.ParamEnricherCombine(enrich...),
	), PreValidateFunc: func(p *T, c *cobra.Command, args []string) error {
		// Config decoding can reuse a slice's backing array, which would mutate
		// boa's saved CLI/environment value before it reapplies precedence.
		if serve, ok := any(p).(*ServeParams); ok {
			serve.KafkaBrokers = append([]string(nil), serve.KafkaBrokers...)
		}
		// Load the whole leaf command, not just its embedded Common struct.
		return boa.LoadConfigFile(any(p).(interface{ configPath() string }).configPath(), p, nil)
	}, RunFuncE: func(p *T, c *cobra.Command, args []string) error {
		if len(args) > 0 {
			return errors.New("unexpected positional arguments")
		}
		return fn(p, c)
	}}.ToCobra()
}

func statusValues(values string) boa.ParamEnricher {
	return func(_ []boa.Parameter, p boa.Parameter, field string) error {
		if field == "Status" {
			p.SetAlternatives(strings.Split(values, ","))
			p.SetDescription(p.GetDescription() + ": " + strings.ReplaceAll(values, ",", ", "))
		}
		return nil
	}
}

func Root(out, errOut io.Writer) *cobra.Command {
	root := &cobra.Command{Use: "clearinghouse", Short: "Livepeer grant accounting", SilenceErrors: true, SilenceUsage: true}
	root.SetOut(out)
	root.SetErr(errOut)
	root.AddCommand(command[ServeParams]("serve", "Run selected online integrations", func(p *ServeParams, c *cobra.Command) error { return Serve(c.Context(), *p) }))
	for _, kind := range []string{"grant", "allocation"} {
		initial, statuses := "draft,active,paused", "draft,active,paused,closed"
		if kind == "allocation" {
			initial, statuses = "active,paused", "active,paused,exhausted,revoked"
		}
		group := &cobra.Command{Use: kind, Short: "Manage " + kind + "s"}
		group.AddCommand(command[CreateParams]("create", "Create and fund a "+kind, func(p *CreateParams, c *cobra.Command) error {
			starts, err := parseTime(p.StartsAt)
			if err != nil {
				return err
			}
			ends, err := parseTime(p.EndsAt)
			if err != nil {
				return err
			}
			if kind == "allocation" && p.GrantID == "" {
				return errors.New("--grant-id required")
			}
			amount, err := inputAmount(p.AmountETH, kind == "allocation")
			if err != nil {
				return err
			}
			return withDB(c, p.Common, false, func(db *store.Store) (any, error) {
				id, err := db.Create(c.Context(), kind, store.Create{Name: p.Name, Sponsor: p.Sponsor, Beneficiary: p.Beneficiary, GrantID: p.GrantID, Amount: amount, Status: p.Status, Metadata: p.Metadata, Starts: starts, Ends: ends})
				return map[string]string{"id": id}, err
			})
		}, statusValues(initial)))
		addRead(group, kind)
		group.AddCommand(command[StatusParams]("set-status", "Change lifecycle status", func(p *StatusParams, c *cobra.Command) error {
			return withDB(c, p.Common, false, func(db *store.Store) (any, error) {
				return map[string]string{"id": p.ID, "status": p.Status}, db.SetStatus(c.Context(), kind, p.ID, p.Status)
			})
		}, statusValues(statuses)))
		group.AddCommand(command[FundParams]("fund", "Add funding in ETH", func(p *FundParams, c *cobra.Command) error {
			amount, err := inputAmount(p.AmountETH, kind == "allocation")
			if err != nil {
				return err
			}
			return withDB(c, p.Common, false, func(db *store.Store) (any, error) {
				return map[string]string{"id": p.ID}, db.Fund(c.Context(), kind, p.ID, amount)
			})
		}))
		if kind == "allocation" {
			addRevoke(group, kind)
		}
		root.AddCommand(group)
	}
	keys := &cobra.Command{Use: "api-key", Short: "Manage grant API keys"}
	keys.AddCommand(command[KeyParams]("create", "Create a key; the secret is shown once", func(p *KeyParams, c *cobra.Command) error {
		if (p.AllocationID == "") == (p.GrantID == "") {
			return errors.New("specify exactly one of --allocation-id or --grant-id")
		}
		if p.AllocationID != "" && p.AmountETH != "" {
			return errors.New("--amount-eth is only valid with --grant-id")
		}
		if p.GrantID != "" && p.AmountETH == "" {
			return errors.New("--amount-eth required with --grant-id")
		}
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) {
			if p.AllocationID != "" {
				id, key, err := db.CreateKey(c.Context(), p.AllocationID, p.Name)
				return map[string]string{"allocation_id": p.AllocationID, "id": id, "api_key": key}, err
			}
			amount, err := inputAmount(p.AmountETH, true)
			if err != nil {
				return nil, err
			}
			allocation, id, key, err := db.CreateKeyForGrant(c.Context(), p.GrantID, p.Name, amount)
			return map[string]string{"allocation_id": allocation, "id": id, "api_key": key}, err
		})
	}))
	keys.AddCommand(listCommand("api-key"))
	addRevoke(keys, "api-key")
	root.AddCommand(keys)
	sessions := &cobra.Command{Use: "session", Short: "Inspect or revoke payment sessions"}
	addRead(sessions, "session")
	addRevoke(sessions, "session")
	root.AddCommand(sessions)
	settlements := &cobra.Command{Use: "settlement", Short: "Inspect treasury redemptions and attribution"}
	settlements.AddCommand(listCommand("settlement"))
	root.AddCommand(settlements)
	escrow := &cobra.Command{Use: "escrow", Short: "Inspect on-chain payment accounting"}
	escrow.AddCommand(command[ListParams]("report", "Report confirmed deposit and reserve balances", func(p *ListParams, c *cobra.Command) error {
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) { return db.EscrowReport(c.Context()) })
	}))
	escrow.AddCommand(command[ListParams]("activity", "List on-chain payment activity", func(p *ListParams, c *cobra.Command) error {
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) { return db.EscrowActivity(c.Context()) })
	}))
	root.AddCommand(escrow)
	usage := &cobra.Command{Use: "usage", Short: "Inspect applied and quarantined Kafka events"}
	usage.AddCommand(listCommand("usage"))
	root.AddCommand(usage)
	ledger := &cobra.Command{Use: "ledger", Short: "Inspect exact balances"}
	ledger.AddCommand(command[ListParams]("report", "Report credit-minus-debit ETH balances", func(p *ListParams, c *cobra.Command) error {
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) { return db.Report(c.Context()) })
	}))
	root.AddCommand(ledger)
	migrate := &cobra.Command{Use: "migrate", Short: "Manage embedded SQL migrations (down destroys accounting data)"}
	for _, action := range []string{"up", "down", "status"} {
		migrate.AddCommand(command[ListParams](action, "Migration "+action, func(p *ListParams, c *cobra.Command) error {
			// Up may create a database. Down/status must not auto-apply missing migrations.
			return withDB(c, p.Common, action == "up", func(db *store.Store) (any, error) {
				if action == "down" {
					if err := migrations.Down(c.Context(), db.DB); err != nil {
						return nil, err
					}
				}
				return migrations.List(c.Context(), db.DB)
			})
		}))
	}
	root.AddCommand(migrate)
	return root
}

func listCommand(kind string) *cobra.Command {
	return command[ListParams]("list", "List "+kind+" records as JSON", func(p *ListParams, c *cobra.Command) error {
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) { return db.List(c.Context(), kind, "") })
	})
}
func addRead(group *cobra.Command, kind string) {
	group.AddCommand(listCommand(kind))
	group.AddCommand(command[IDParams]("show", "Show one "+kind, func(p *IDParams, c *cobra.Command) error {
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) { return db.List(c.Context(), kind, p.ID) })
	}))
}
func addRevoke(group *cobra.Command, kind string) {
	group.AddCommand(command[IDParams]("revoke", "Revoke a "+kind, func(p *IDParams, c *cobra.Command) error {
		return withDB(c, p.Common, false, func(db *store.Store) (any, error) {
			return map[string]string{"id": p.ID, "status": "revoked"}, db.SetStatus(c.Context(), kind, p.ID, "revoked")
		})
	}))
}
func withDB(c *cobra.Command, p Common, mutate bool, fn func(*store.Store) (any, error)) error {
	db, err := store.Open(c.Context(), p.DBPath, mutate)
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := fn(db)
	if err != nil {
		return err
	}
	result, err = displayETH(result)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(c.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func inputAmount(value string, allowAll bool) (string, error) {
	if value == "all" {
		if allowAll {
			return value, nil
		}
		return "", errors.New("'all' is only valid for allocation funding")
	}
	return units.ETHToWei(value)
}
func parseTime(s string) (*int64, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, err
	}
	ms := t.UnixMilli()
	return &ms, nil
}

func Execute(ctx context.Context, args []string, out, errOut io.Writer) error {
	root := Root(out, errOut)
	root.SetArgs(args)
	return root.ExecuteContext(ctx)
}
