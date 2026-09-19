package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func cli(t *testing.T, args ...string) string {
	t.Helper()
	var out, stderr bytes.Buffer
	require.NoError(t, Execute(context.Background(), args, &out, &stderr))
	return out.String()
}

func mustHTTPBind(t testing.TB, value string) HTTPBind {
	t.Helper()
	var bind HTTPBind
	require.NoError(t, bind.UnmarshalText([]byte(value)))
	return bind
}

func object(t *testing.T, s string) map[string]string {
	t.Helper()
	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(s), &m))
	return m
}

func TestCLIManagementAndSecretOnce(t *testing.T) {
	t.Setenv("CLEARINGHOUSE_DB_PATH", filepath.Join(t.TempDir(), "accounts.db"))
	cli(t, "migrate", "up")
	g := object(t, cli(t, "grant", "create", "--name", "grant", "--amount-eth", "1", "--status", "active"))["id"]
	a := object(t, cli(t, "allocation", "create", "--name", "allocation", "--grant-id", g, "--amount-eth", "0.5"))["id"]
	k := object(t, cli(t, "api-key", "create", "--allocation-id", a, "--name", "gateway"))
	if k["allocation_id"] != a {
		t.Fatal(k)
	}
	if !strings.HasPrefix(k["api_key"], "lpg_"+k["id"]+"_") {
		t.Fatal(k)
	}
	keys := cli(t, "api-key", "list")
	if strings.Contains(keys, k["api_key"]) || strings.Contains(keys, "secret_hash") {
		t.Fatal("secret leaked")
	}
	for _, args := range [][]string{
		{"grant", "list"}, {"grant", "show", "--id", g},
		{"grant", "fund", "--id", g, "--amount-eth", "0.5"},
		{"allocation", "fund", "--id", a, "--amount-eth", "0.25"},
		{"allocation", "set-status", "--id", a, "--status", "paused"},
		{"allocation", "set-status", "--id", a, "--status", "active"},
		{"grant", "set-status", "--id", g, "--status", "paused"},
		{"grant", "set-status", "--id", g, "--status", "active"},
		{"allocation", "list"}, {"allocation", "show", "--id", a},
		{"api-key", "revoke", "--id", k["id"]},
		{"allocation", "revoke", "--id", a}, {"session", "list"},
		{"settlement", "list"}, {"escrow", "report"}, {"escrow", "activity"}, {"usage", "list"}, {"migrate", "status"},
	} {
		cli(t, args...)
	}
	if report := cli(t, "ledger", "report"); !strings.Contains(report, `"balance_eth": "1.5"`) || strings.Contains(report, "_wei") {
		t.Fatal(report)
	}
	var out bytes.Buffer
	if err := Execute(context.Background(), []string{"allocation", "set-status", "--id", a, "--status", "active"}, &out, &out); err == nil {
		t.Fatal("reopened revoked allocation")
	}
	cli(t, "migrate", "down")
	cli(t, "migrate", "up")
	if got := strings.TrimSpace(cli(t, "grant", "list")); got != "[]" {
		t.Fatal(got)
	}
}

func TestCLIAllocationAllAndKeyAutoAllocation(t *testing.T) {
	t.Setenv("CLEARINGHOUSE_DB_PATH", filepath.Join(t.TempDir(), "accounts.db"))
	cli(t, "migrate", "up")
	g := object(t, cli(t, "grant", "create", "--name", "grant", "--amount-eth", "2", "--status", "active"))["id"]
	k := object(t, cli(t, "api-key", "create", "--grant-id", g, "--name", "gateway", "--amount-eth", "all"))
	if k["allocation_id"] == "" || !strings.HasPrefix(k["api_key"], "lpg_"+k["id"]+"_") {
		t.Fatal(k)
	}
	allocation := cli(t, "allocation", "show", "--id", k["allocation_id"])
	if !strings.Contains(allocation, `"name": "gateway"`) || !strings.Contains(allocation, `"allocated_eth": "2"`) {
		t.Fatal(allocation)
	}
	empty := object(t, cli(t, "api-key", "create", "--grant-id", g, "--name", "empty-gateway", "--amount-eth", "all"))
	if got := cli(t, "allocation", "show", "--id", empty["allocation_id"]); !strings.Contains(got, `"allocated_eth": "0"`) || !strings.Contains(got, `"status": "exhausted"`) {
		t.Fatal(got)
	}

	for _, args := range [][]string{
		{"api-key", "create", "--grant-id", g, "--name", "missing-amount"},
		{"api-key", "create", "--allocation-id", k["allocation_id"], "--grant-id", g, "--name", "conflict", "--amount-eth", "1"},
		{"api-key", "create", "--allocation-id", k["allocation_id"], "--name", "irrelevant", "--amount-eth", "1"},
		{"grant", "fund", "--id", g, "--amount-eth", "all"},
	} {
		var out bytes.Buffer
		if err := Execute(context.Background(), args, &out, &out); err == nil {
			t.Fatalf("validation allowed %v", args)
		}
	}

	before := cli(t, "api-key", "list")
	var out bytes.Buffer
	err := Execute(context.Background(), []string{"api-key", "create", "--grant-id", "missing", "--name", "atomic", "--amount-eth", "1"}, &out, &out)
	if err == nil || cli(t, "api-key", "list") != before {
		t.Fatal("failed auto-allocation was not atomic", err)
	}
}

func TestBoaConfigEnvironmentValidationAndHelp(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "grant.json")
	contents, err := json.Marshal(map[string]any{"Name": "from-config", "DBPath": filepath.Join(dir, "configured.db"), "AmountETH": "0.21", "Status": "active"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config, contents, 0600))
	t.Setenv("CLEARINGHOUSE_DB_PATH", filepath.Join(dir, "environment.db"))
	cli(t, "migrate", "up")
	cli(t, "grant", "create", "--config-file", config, "--name", "from-flag")
	if got := cli(t, "grant", "list"); !strings.Contains(got, "from-flag") || !strings.Contains(got, `"total_eth": "0.21"`) || strings.Contains(got, "_wei") {
		t.Fatal(got)
	}
	if _, err := os.Stat(filepath.Join(dir, "configured.db")); !os.IsNotExist(err) {
		t.Fatal("environment did not override config")
	}
	for _, args := range [][]string{{"serve"}, {"grant", "create"}, {"serve", "--enable-auth-webhook"}, {"serve", "--http-bind", "127.0.0.1:8080"}, {"serve", "--enable-auth-webhook", "0.0.0.0:8080", "--webhook-token", "test"}, {"serve", "--enable-kafka", "--kafka-bind", "0.0.0.0:9092"}, {"serve", "--enable-onchain-listener"}} {
		var out bytes.Buffer
		if err := Execute(context.Background(), args, &out, &out); err == nil {
			t.Fatalf("validation allowed %v", args)
		}
	}
	help := cli(t, "serve", "--help")
	for _, want := range []string{"--enable-auth-webhook string", "--unsafe-http-bind", "CLEARINGHOUSE_UNSAFE_HTTP_BIND", "--enable-kafka", "--enable-onchain-listener", "Run on-chain RPC listener", "--ticket-broker", "--start-block", "--config-file", "Configuration file", "CLEARINGHOUSE_DB_PATH", "CLEARINGHOUSE_TICKET_BROKER"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %s", want)
		}
	}
	if strings.Contains(help, "--enable-chain") || strings.Contains(help, "CLEARINGHOUSE_ENABLE_CHAIN") {
		t.Fatal("unsupported listener option exposed")
	}
	if strings.Contains(help, "--http-bind string") {
		t.Fatal("removed HTTP bind option exposed")
	}
	for _, line := range strings.Split(help, "\n") {
		if strings.Contains(line, "--enable-auth-webhook") && strings.Contains(line, "default") {
			t.Fatal("auth webhook bind has a default")
		}
	}
	if help := cli(t, "grant", "create", "--help"); !strings.Contains(help, "--amount-eth") || strings.Contains(help, "--amount-wei") {
		t.Fatal(help)
	}
	escrowHelp := cli(t, "escrow", "--help")
	for _, want := range []string{"Inspect on-chain payment accounting", "List on-chain payment activity", "Report confirmed deposit and reserve balances"} {
		if !strings.Contains(escrowHelp, want) {
			t.Fatalf("escrow help missing %q", want)
		}
	}
	t.Setenv("CLEARINGHOUSE_ENABLE_CHAIN", "true")
	var out bytes.Buffer
	err = Execute(context.Background(), []string{"serve"}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "enable at least one") {
		t.Fatal("unsupported listener environment variable was accepted", err)
	}
	t.Setenv("CLEARINGHOUSE_ENABLE_ONCHAIN_LISTENER", "true")
	out.Reset()
	err = Execute(context.Background(), []string{"serve"}, &out, &out)
	if err == nil || !strings.Contains(err.Error(), "--rpc-url required") {
		t.Fatal("on-chain listener environment variable was not loaded", err)
	}
}

func TestTOMLConfigAndPrecedence(t *testing.T) {
	dir := t.TempDir()
	database := filepath.Join(dir, "environment.db")
	t.Setenv("CLEARINGHOUSE_DB_PATH", database)
	cli(t, "migrate", "up")
	config := filepath.Join(dir, "grant.toml")
	contents := []byte("Name = \"from-toml\"\nDBPath = \"" + filepath.Join(dir, "configured.db") + "\"\nAmountETH = \"0.125\"\nStatus = \"active\"\n")
	require.NoError(t, os.WriteFile(config, contents, 0600))
	cli(t, "grant", "create", "--config-file", config, "--name", "from-flag")
	got := cli(t, "grant", "list")
	if !strings.Contains(got, `"name": "from-flag"`) || !strings.Contains(got, `"total_eth": "0.125"`) {
		t.Fatal(got)
	}
	if _, err := os.Stat(filepath.Join(dir, "configured.db")); !os.IsNotExist(err) {
		t.Fatal("environment did not override TOML config")
	}
}

func TestManagementCommandsDoNotAutoMigrate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.db")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	t.Setenv("CLEARINGHOUSE_DB_PATH", path)
	var out bytes.Buffer
	if err := Execute(context.Background(), []string{"grant", "list"}, &out, &out); err == nil {
		t.Fatal("management command migrated an empty database")
	}
	db, err := store.Open(context.Background(), path, false)
	require.NoError(t, err)
	defer db.Close()
	var count int
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='migrations'`).Scan(&count))
	if count != 0 {
		t.Fatal("management command created migration metadata")
	}
}

func TestOrdinaryCommandsSkipMigrationPreflightButServeRejectsDrift(t *testing.T) {
	f := testutil.New(t, "100")
	_, err := f.DB.DB.Exec(`INSERT INTO migrations(version,filename,sha256,applied_at_ms) VALUES (999,'999_unknown.sql',?,0)`, strings.Repeat("0", 64))
	require.NoError(t, err)
	t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
	if got := cli(t, "grant", "list"); !strings.Contains(got, f.Grant) {
		t.Fatal(got)
	}
	var out bytes.Buffer
	if err := Execute(context.Background(), []string{"migrate", "status"}, &out, &out); err == nil || !strings.Contains(err.Error(), "unknown migration") {
		t.Fatal(err)
	}
	p := ServeParams{Common: Common{DBPath: f.Path}, EnableAuthWebhook: mustHTTPBind(t, "127.0.0.1:8080"), WebhookToken: "token"}
	if err := Serve(context.Background(), p); err == nil || !strings.Contains(err.Error(), "unknown migration") {
		t.Fatal(err)
	}
}

// A real HTTP JSON-RPC server exercises component startup without external chain access.
type startupRPC struct{}

func (startupRPC) ChainId() hexutil.Big { return hexutil.Big(*big.NewInt(42161)) }
func (startupRPC) GetBlockByNumber(context.Context, rpc.BlockNumber, bool) (*types.Header, error) {
	return &types.Header{Number: big.NewInt(0), Difficulty: big.NewInt(0), GasLimit: 30000000}, nil
}
func (startupRPC) GetLogs(context.Context, json.RawMessage) ([]types.Log, error) {
	return []types.Log{}, nil
}

func TestAllComponentCombinations(t *testing.T) {
	r := rpc.NewServer()
	require.NoError(t, r.RegisterName("eth", startupRPC{}))
	srv := httptest.NewServer(r)
	defer srv.Close()
	for mask := 1; mask < 16; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			dir := t.TempDir()
			start := int64(0)
			p := ServeParams{Common: Common{DBPath: filepath.Join(dir, "accounts.db")}, EnableKafka: mask&2 != 0, EnableOnchainListener: mask&4 != 0, WebhookToken: "test-token", KafkaBind: testutil.Port(t), KafkaTopic: "events", RPCURL: srv.URL, ChainID: "42161", TicketBroker: testutil.Contract, SignerAddresses: []string{testutil.Sender}, StartBlock: &start, Confirmations: 0, BlockBatchSize: 10, ReorgLookback: 4, PollInterval: 10 * time.Millisecond}
			if mask&1 != 0 {
				p.EnableAuthWebhook = mustHTTPBind(t, testutil.Port(t))
			}
			p.EnableAccounting = mask&8 != 0
			producerAddr := p.KafkaBind
			if p.EnableAccounting && !p.EnableKafka {
				broker, err := kafka.OpenBroker(context.Background(), testutil.Port(t), p.KafkaTopic, filepath.Join(dir, "external"))
				require.NoError(t, err)
				brokerCtx, stop := context.WithCancel(context.Background())
				brokerDone := make(chan error, 1)
				go func() { brokerDone <- broker.Serve(brokerCtx) }()
				t.Cleanup(func() {
					stop()
					broker.Close()
					require.NoError(t, <-brokerDone)
				})
				producerAddr = broker.Addr()
				p.KafkaBrokers = []string{testutil.Port(t), producerAddr}
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, p) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Error("shutdown timed out")
				}
			})
			testutil.Eventually(t, func() bool {
				if p.EnableAuthWebhook.IsValid() {
					client := http.Client{Timeout: time.Second}
					res, err := client.Get("http://" + p.EnableAuthWebhook.String() + "/readyz")
					if err != nil {
						return false
					}
					res.Body.Close()
					if res.StatusCode != 200 {
						return false
					}
				}
				if p.EnableOnchainListener {
					db, err := store.Open(ctx, p.DBPath, false)
					if err != nil {
						return false
					}
					defer db.Close()
					next, _, found, err := db.Checkpoint(ctx, "chain", p.chainConfig().Stream())
					if err != nil || !found || next != 1 {
						return false
					}
				}
				if p.EnableKafka {
					conn, err := kgo.DialContext(ctx, "tcp", p.KafkaBind)
					if err != nil {
						return false
					}
					_ = conn.SetDeadline(time.Now().Add(time.Second))
					_, err = conn.ReadPartitions(p.KafkaTopic)
					conn.Close()
					if err != nil {
						return false
					}
				}
				return true
			})
			if p.EnableAccounting {
				w := kgo.NewWriter(kgo.WriterConfig{Brokers: []string{producerAddr}, Topic: p.KafkaTopic, BatchTimeout: time.Millisecond})
				writeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				require.NoError(t, w.WriteMessages(writeCtx, kgo.Message{Value: []byte(`{"id":"startup","type":"other","data":{}}`)}))
				stop()
				require.NoError(t, w.Close())
				testutil.Eventually(t, func() bool {
					db, err := store.Open(ctx, p.DBPath, false)
					if err != nil {
						return false
					}
					defer db.Close()
					next, _, _, err := db.Checkpoint(ctx, "kafka", p.KafkaTopic)
					return err == nil && next == 1
				})
				if _, err := os.Stat(p.DBPath + ".accounting.lock"); err != nil {
					t.Fatalf("consumer did not create accounting lock: %v", err)
				}
				if !p.EnableKafka {
					matches, err := filepath.Glob(filepath.Join(dir, "minikafka_*.db"))
					if err != nil || len(matches) != 0 {
						t.Fatalf("external consumer created embedded broker storage: %v, %v", matches, err)
					}
				}
			}
			if p.EnableKafka {
				for _, name := range []string{"minikafka_+meta.db", "minikafka_events.db"} {
					if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
						t.Fatalf("embedded broker database is not beside accounting database: %s: %v", name, err)
					}
				}
			}
			if !p.EnableAuthWebhook.IsValid() && !p.EnableAccounting && !p.EnableOnchainListener {
				if _, err := os.Stat(p.DBPath); !os.IsNotExist(err) {
					t.Fatalf("broker-only mode touched accounting database: %v", err)
				}
				if _, err := os.Stat(p.DBPath + ".accounting.lock"); !os.IsNotExist(err) {
					t.Fatalf("broker-only mode acquired accounting lock: %v", err)
				}
			}
		})
	}
}

func TestServeFromJSONConfig(t *testing.T) {
	dir := t.TempDir()
	bind := testutil.Port(t)
	path := filepath.Join(dir, "serve.json")
	_, port, err := net.SplitHostPort(bind)
	require.NoError(t, err)
	data, err := json.Marshal(map[string]any{"DBPath": filepath.Join(dir, "config-only.db"), "EnableAuthWebhook": ":" + port, "WebhookToken": "fixture-token"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var out bytes.Buffer
		done <- Execute(ctx, []string{"serve", "--config-file", path}, &out, &out)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Error("shutdown timed out")
		}
	})
	testutil.Eventually(t, func() bool {
		client := http.Client{Timeout: time.Second}
		res, err := client.Get("http://" + bind + "/readyz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == 200
	})
	if _, err := os.Stat(filepath.Join(dir, "config-only.db")); err != nil {
		t.Fatal(err)
	}
}

func TestCLISessionCommands(t *testing.T) {
	f := testutil.New(t, "100")
	t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
	cli(t, "session", "show", "--id", f.Session)
	cli(t, "session", "revoke", "--id", f.Session)
	if s := cli(t, "session", "show", "--id", f.Session); !strings.Contains(s, `"status": "revoked"`) {
		t.Fatal(s)
	}
}

func TestCLIEscrowCommands(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	stream := "42161:" + testutil.Contract + ":" + testutil.Sender
	require.NoError(t, f.DB.BootstrapChain(ctx, stream, store.Block{Number: -1}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "12", Reserve: "3"}}))
	t.Setenv("CLEARINGHOUSE_DB_PATH", f.Path)
	if report := cli(t, "escrow", "report"); !strings.Contains(report, `"deposit_balance_eth": "0.000000000000000012"`) || !strings.Contains(report, `"total_balance_eth": "0.000000000000000015"`) || strings.Contains(report, "_wei") {
		t.Fatal(report)
	}
	if activity := cli(t, "escrow", "activity"); !strings.Contains(activity, `"event_type": "opening_snapshot"`) || !strings.Contains(activity, `"reserve_amount_eth": "0.000000000000000003"`) || strings.Contains(activity, "_wei") {
		t.Fatal(activity)
	}

	blockHash := "0x" + strings.Repeat("a", 64)
	txHash := "0x" + strings.Repeat("b", 64)
	settlement := store.Settlement{ChainID: "42161", Contract: testutil.Contract, TxHash: txHash, BlockHash: blockHash, Sender: testutil.Sender, Recipient: testutil.Orch, FaceValue: "20", PaidAmount: "14", DepositPaid: "12", ReservePaid: "2", WinProb: "1", Nonce: "1", Rand: "1", PMSessionID: "0x" + strings.Repeat("c", 64), AuxData: "0x", BlockNumber: 0, LogIndex: 0, Timestamp: 1}
	event := store.EscrowEvent{Type: store.WinningTicketRedeemed, ChainID: "42161", Contract: testutil.Contract, TxHash: txHash, BlockHash: blockHash, Sender: testutil.Sender, Recipient: testutil.Orch, Amount: "20", DepositAmount: "0", ReserveAmount: "0", BlockNumber: 0, LogIndex: 0, Timestamp: 1, Settlement: &settlement}
	require.NoError(t, f.DB.ApplyChain(ctx, stream, []store.Block{{Number: 0, Hash: blockHash}}, []store.EscrowEvent{event}, 4))
	if got := cli(t, "settlement", "list"); !strings.Contains(got, `"face_value_eth": "0.00000000000000002"`) || !strings.Contains(got, `"reserve_paid_eth": "0.000000000000000002"`) || strings.Contains(got, "_wei") {
		t.Fatal(got)
	}

	require.NoError(t, f.DB.Ingest(ctx, "test", 0, f.Event(t, "usage-output", "7", testutil.PM)))
	if got := cli(t, "usage", "list"); !strings.Contains(got, `"computed_fee_eth": "0.000000000000000007"`) || strings.Contains(got, "_wei") {
		t.Fatal(got)
	}
}
