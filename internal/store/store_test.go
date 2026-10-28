package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/livepeer/clearinghouse/internal/units"
	"github.com/livepeer/clearinghouse/migrations"
	"github.com/stretchr/testify/require"
)

func TestIDIsBase64URLUUIDv7(t *testing.T) {
	id := store.ID()
	if len(id) != 22 {
		t.Fatalf("ID length = %d, want 22", len(id))
	}
	raw, err := base64.RawURLEncoding.DecodeString(id)
	require.NoError(t, err)
	if len(raw) != 16 {
		t.Fatalf("decoded ID length = %d, want 16", len(raw))
	}
	if version := raw[6] >> 4; version != 7 {
		t.Fatalf("UUID version = %d, want 7", version)
	}
	if variant := raw[8] >> 6; variant != 2 {
		t.Fatalf("UUID variant = %d, want 2", variant)
	}
}

func TestAuthorizeAcceptsUnderscoreInBase64URLKeyID(t *testing.T) {
	f := testutil.New(t, "100")
	raw := bytes.Repeat([]byte{0xff}, 16)
	raw[6] = 0x7f
	raw[8] = 0xbf
	id := base64.RawURLEncoding.EncodeToString(raw)
	key := "lpg_" + id + "_secret"
	hash := sha256.Sum256([]byte(key))
	_, err := f.DB.DB.Exec(`INSERT INTO api_keys(id,allocation_id,name,prefix,secret_hash,created_at_ms) VALUES (?,?,?,?,?,?)`, id, f.Allocation, "underscore", "lpg_"+id, hash[:], 0)
	require.NoError(t, err)
	req := store.AuthRequest{
		Headers: http.Header{"Authorization": []string{"Bearer " + key}},
		State:   &store.RemoteState{StateID: "state-underscore", OrchestratorAddress: testutil.Orch, App: "test-app", Type: "live", FeeWei: "1"},
	}
	decision, err := f.DB.Authorize(context.Background(), req)
	require.NoError(t, err)
	if decision.Status != 200 {
		t.Fatalf("auth: %+v", decision)
	}
}

func TestMigrationsConstraintsAndRoundTrip(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	for _, q := range []string{`UPDATE ledger_entries SET amount_units='1'`, `DELETE FROM ledger_entries`, `DELETE FROM ledger_transactions`, `UPDATE ledger_transactions SET reason='changed'`, `INSERT INTO api_keys(id,allocation_id,name,prefix,secret_hash,created_at_ms) VALUES ('bad','missing','bad','bad',zeroblob(32),0)`} {
		if _, err := f.DB.DB.ExecContext(ctx, q); err == nil {
			t.Fatalf("constraint allowed %s", q)
		}
	}
	for _, q := range []string{`UPDATE grants SET total_units='01'`, `UPDATE grants SET currency='usd'`, `UPDATE grant_allocations SET status='unknown'`, `UPDATE grant_allocations SET currency='usd'`, `UPDATE payment_sessions SET orchestrator='0xgggggggggggggggggggggggggggggggggggggggg'`, `UPDATE api_keys SET secret_hash=zeroblob(31)`} {
		if _, err := f.DB.DB.ExecContext(ctx, q); err == nil {
			t.Fatalf("check allowed %s", q)
		}
	}
	for _, q := range []string{
		`INSERT INTO grants(id,name,total_units,status,created_at_ms) VALUES ('defaults-grant','defaults','0','draft',0) RETURNING metadata`,
		`INSERT INTO grant_allocations(id,grant_id,name,allocated_units,currency,status,created_at_ms) VALUES ('defaults-allocation','defaults-grant','defaults','0','usd','exhausted',0) RETURNING metadata`,
	} {
		var metadata string
		require.NoError(t, f.DB.DB.QueryRowContext(ctx, q).Scan(&metadata), q)
		require.Equal(t, "", metadata, q)
	}
	// Constraint indexes have NULL SQL; compare the complete set of explicit indexes.
	indexes, err := f.DB.Rows(ctx, `SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name`)
	require.NoError(t, err)
	actual := make([]string, len(indexes))
	for i, index := range indexes {
		actual[i] = index["name"].(string)
	}
	require.ElementsMatch(t, []string{"allocations_grant", "keys_allocation", "sessions_allocation", "usage_status", "usage_session", "usage_manifest", "signing_match", "balances_owner", "ledger_account", "ledger_transaction", "settlements_match", "settlements_block", "settlements_session", "ticket_broker_events_block", "ticket_broker_events_sender", "metric_balances_owner", "metric_balances_negative", "metric_balances_nonnegative", "settlements_oldest_unmatched", "escrow_snapshots_block"}, actual)
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	list, err := migrations.List(ctx, f.DB.DB)
	require.NoError(t, err)
	require.NotEmpty(t, list)
	for _, item := range list {
		require.True(t, item.Applied)
	}
	for range list {
		require.NoError(t, migrations.Down(ctx, f.DB.DB))
	}
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grants`).Scan(&count))
	if count != 0 {
		t.Fatal(count)
	}
}

// Legacy data can be inserted without write helpers, then upgraded atomically.
func insertMetricSettlement(t testing.TB, db *store.Store, id, face, paid, status string) {
	t.Helper()
	hash := sha256.Sum256([]byte(id))
	_, err := db.DB.Exec(`INSERT INTO settlements (id,chain_id,contract_address,tx_hash,log_index,block_number,block_hash,sender,recipient,face_value_wei,paid_amount_wei,deposit_paid_wei,reserve_paid_wei,win_probability,sender_nonce,recipient_rand,pm_session_id,aux_data,match_status,status,created_at_ms,settled_at_ms)
 VALUES (?,'42161',?,?,0,1,?,?,?, ?,?,?,'0','1','0','0',?,'0x','unmatched',?,1234,1234)`, id, testutil.Contract, fmt.Sprintf("0x%x", hash), testutil.PM, testutil.Sender, testutil.Orch, face, paid, paid, testutil.PM, status)
	require.NoError(t, err)
}

func rollbackMetricSummaries(t testing.TB, db *store.Store) {
	t.Helper()
	list, err := migrations.List(t.Context(), db.DB)
	require.NoError(t, err)
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Applied && list[i].Filename >= "003_metric_summaries.sql" {
			require.NoError(t, migrations.Down(t.Context(), db.DB))
		}
	}
	var tables int
	require.NoError(t, db.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='metric_amounts'`).Scan(&tables))
	require.Zero(t, tables, "summary rebuild requires rolling back migration 003")
}

func TestMetricBackfillExactAmountsAndQuarantineReasons(t *testing.T) {
	amount := strings.Repeat("9", 256)
	f := testutil.New(t, amount)
	ctx := t.Context()
	_, err := f.DB.Create(ctx, "grant", store.Create{Name: "second", Amount: amount, Currency: "eth"})
	require.NoError(t, err)
	checkAmount := func(name, want string) {
		t.Helper()
		var got string
		require.NoError(t, f.DB.DB.QueryRow(`SELECT amount_units FROM metric_amounts WHERE name=? AND currency='eth'`, name).Scan(&got))
		require.Equal(t, want, got)
	}
	// Force aggregate growth beyond the input limit without rounding a unit away.
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, amount))
	n, _ := new(big.Int).SetString(amount, 10)
	twice := new(big.Int).Mul(n, big.NewInt(2)).String()
	checkAmount("grant_unallocated", twice)
	require.NoError(t, f.DB.BootstrapChain(ctx, "chain", store.Block{Number: -1}, []store.EscrowSnapshot{
		{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: amount, Reserve: "0"},
		{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Orch, Deposit: amount, Reserve: "0"},
	}))
	checkAmount("escrow_deposit", twice)
	rollbackMetricSummaries(t, f.DB)
	insertMetricSettlement(t, f.DB, "one", amount, "1", "settled")
	insertMetricSettlement(t, f.DB, "second", amount, "0", "settled")
	insertMetricSettlement(t, f.DB, "orphaned", amount, "1", "orphaned")
	reasons := []string{"", "malformed envelope or missing event id/type/data", "event id reused with different payload", "malformed signed-ticket data", "missing auth_id or unknown payment session", "event does not match session binding", "invalid fee, ticket count, request, timestamp, or PM session", "USD fee unavailable", "unknown one", "unknown two"}
	for i, reason := range reasons {
		_, err := f.DB.DB.Exec(`INSERT INTO usage_events(id,topic,partition,offset,raw_payload,status,error,created_at_ms) VALUES (?,'legacy',0,?,X'','quarantined',?,1)`, fmt.Sprint(i), i, reason)
		require.NoError(t, err)
	}
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	checkAmount("grant_unallocated", twice)
	checkAmount("escrow_deposit", twice)
	checkAmount("settlement_face_value", twice)
	checkAmount("settlement_shortfall", new(big.Int).Sub(new(big.Int).Mul(n, big.NewInt(2)), big.NewInt(1)).String())
	for _, reason := range reasons {
		var got int
		require.NoError(t, f.DB.DB.QueryRow(`SELECT value FROM metric_counts WHERE kind='quarantines' AND label1=?`, store.QuarantineReason(reason)).Scan(&got))
		want := 1
		if store.QuarantineReason(reason) == "other" {
			want = 2
		}
		require.Equal(t, want, got)
	}
	assertMetricSummaryRebuild(t, f.DB)
}

func TestMetricMigrationBackfillFailure(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := t.Context()
	rollbackMetricSummaries(t, f.DB)
	_, err := f.DB.DB.Exec(`CREATE TRIGGER fail_metadata BEFORE INSERT ON migrations WHEN NEW.filename='003_metric_summaries.sql' BEGIN SELECT RAISE(ABORT,'backfill rollback'); END`)
	require.NoError(t, err)
	require.ErrorContains(t, migrations.Up(ctx, f.DB.DB), "backfill rollback")
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name LIKE 'metric_%' OR name='settlements_oldest_unmatched' OR name='escrow_snapshots_block'`).Scan(&count))
	require.Zero(t, count)
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_metadata`)
	require.NoError(t, err)
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	testutil.AssertMetricSnapshot(t, f.DB)
	// Exercise an error in the monetary pass itself, after SQL initialization.
	rollbackMetricSummaries(t, f.DB)
	_, err = f.DB.DB.Exec(`PRAGMA ignore_check_constraints=ON; UPDATE account_balances SET balance_units='bad' WHERE account_type='allocation_available'`)
	require.NoError(t, err)
	require.ErrorContains(t, migrations.Up(ctx, f.DB.DB), "invalid metric amount")
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='metric_amounts'`).Scan(&count))
	require.Zero(t, count)
	_, err = f.DB.DB.Exec(`UPDATE account_balances SET balance_units='100' WHERE account_type='allocation_available'; PRAGMA ignore_check_constraints=OFF`)
	require.NoError(t, err)
	require.NoError(t, migrations.Up(ctx, f.DB.DB))
	testutil.AssertMetricSnapshot(t, f.DB)
}

func TestEscrowReportUsesArbitraryWidthAmounts(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	amount := strings.Repeat("9", 200)
	stream := "42161:" + testutil.Contract + ":" + testutil.Sender
	require.NoError(t, f.DB.BootstrapChain(ctx, stream, store.Block{Number: -1}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: amount, Reserve: amount}}))
	report, err := f.DB.EscrowReport(ctx)
	require.NoError(t, err)
	want, ok := new(big.Int).SetString(amount, 10)
	if !ok {
		t.Fatal("invalid fixture")
	}
	want.Mul(want, big.NewInt(2))
	if len(report) != 1 || report[0]["deposit_balance_wei"] != amount || report[0]["reserve_balance_wei"] != amount || report[0]["total_balance_wei"] != want.String() {
		t.Fatal(report)
	}
	assertBalanced(t, f.DB)
}

func TestEscrowSnapshotIsAHardReorgBoundary(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	stream := "42161:" + testutil.Contract + ":" + testutil.Sender
	hash := "0x" + strings.Repeat("a", 64)
	require.NoError(t, f.DB.BootstrapChain(ctx, stream, store.Block{Number: 5, Hash: hash}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "1", Reserve: "2"}}))
	err := f.DB.Rewind(ctx, stream, "42161", testutil.Contract, store.Block{Number: 4, Hash: "0x" + strings.Repeat("b", 64)})
	if !errors.Is(err, store.ErrEscrowSnapshotReorg) {
		t.Fatal(err)
	}
	next, gotHash, found, err := f.DB.Checkpoint(ctx, "chain", stream)
	require.NoError(t, err)
	if !found || next != 6 || gotHash != hash {
		t.Fatal(next, gotHash, found)
	}
}

func TestEscrowMetricsSharedOwner(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := t.Context()
	opening := store.EscrowSnapshot{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "7", Reserve: "3"}
	require.NoError(t, f.DB.BootstrapChain(ctx, "first", store.Block{Number: -1}, []store.EscrowSnapshot{opening}))
	// The second stream registers the already funded account without adding funds.
	opening.Deposit, opening.Reserve = "0", "0"
	require.NoError(t, f.DB.BootstrapChain(ctx, "second", store.Block{Number: -1}, []store.EscrowSnapshot{opening}))
	check := func(deposit, reserve string, unfunded int64) {
		t.Helper()
		for _, item := range []struct{ bucket, amount string }{{"deposit", deposit}, {"reserve", reserve}} {
			var amount string
			var empty int64
			require.NoError(t, f.DB.DB.QueryRow(`SELECT amount_units FROM metric_amounts WHERE name=? AND currency='eth'`, "escrow_"+item.bucket).Scan(&amount))
			require.Equal(t, item.amount, amount)
			require.NoError(t, f.DB.DB.QueryRow(`SELECT value FROM metric_counts WHERE kind='escrow_unfunded_signers' AND label1=? AND label2=''`, item.bucket).Scan(&empty))
			require.Equal(t, unfunded, empty)
		}
		assertMetricSummaryRebuild(t, f.DB)
	}
	check("14", "6", 0)
	withdraw := store.EscrowEvent{Type: store.Withdrawal, ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, TxHash: testutil.PM, BlockHash: testutil.PM, BlockNumber: 1, Amount: "0", DepositAmount: "7", ReserveAmount: "3"}
	require.NoError(t, f.DB.ApplyChain(ctx, "first", []store.Block{{Number: 1, Hash: withdraw.BlockHash}}, []store.EscrowEvent{withdraw}, 10))
	check("0", "0", 2)
	deposit := withdraw
	deposit.Type, deposit.Amount, deposit.DepositAmount, deposit.ReserveAmount = store.DepositFunded, "5", "0", "0"
	deposit.BlockNumber, deposit.LogIndex, deposit.BlockHash = 2, 1, "0x"+strings.Repeat("2", 64)
	reserve := deposit
	reserve.Type, reserve.Amount, reserve.LogIndex = store.ReserveFunded, "11", 2
	require.NoError(t, f.DB.ApplyChain(ctx, "first", []store.Block{{Number: 2, Hash: deposit.BlockHash}}, []store.EscrowEvent{deposit, reserve}, 10))
	check("10", "22", 0)
}

func TestMetricSummaryReversalFailure(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := t.Context()
	require.NoError(t, f.DB.BootstrapChain(ctx, "chain", store.Block{Number: -1}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "10", Reserve: "0"}}))
	settlement := store.Settlement{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Recipient: testutil.Orch, TxHash: testutil.PM, BlockHash: testutil.PM, BlockNumber: 1, FaceValue: "10", PaidAmount: "10", DepositPaid: "10", ReservePaid: "0", WinProb: "1", Nonce: "0", Rand: "0", PMSessionID: testutil.PM, AuxData: "0x", Timestamp: 1}
	event := store.EscrowEvent{Type: store.WinningTicketRedeemed, ChainID: settlement.ChainID, Contract: settlement.Contract, Sender: settlement.Sender, Recipient: settlement.Recipient, TxHash: settlement.TxHash, BlockHash: settlement.BlockHash, BlockNumber: 1, Amount: "10", DepositAmount: "10", ReserveAmount: "0", Timestamp: 1, Settlement: &settlement}
	_, err := f.DB.DB.Exec(`CREATE TRIGGER fail_settlement BEFORE UPDATE ON metric_amounts WHEN NEW.name='settlement_face_value' BEGIN SELECT RAISE(ABORT,'settlement summary'); END`)
	require.NoError(t, err)
	require.ErrorContains(t, f.DB.ApplyChain(ctx, "chain", []store.Block{{Number: 1, Hash: testutil.PM}}, []store.EscrowEvent{event}, 10), "settlement summary")
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_settlement`)
	require.NoError(t, err)
	require.NoError(t, f.DB.ApplyChain(ctx, "chain", []store.Block{{Number: 1, Hash: testutil.PM}}, []store.EscrowEvent{event}, 10))
	before := metricSummaryState(t, f.DB)
	_, err = f.DB.DB.Exec(`CREATE TRIGGER fail_settlement BEFORE UPDATE ON metric_amounts WHEN NEW.name='settlement_face_value' BEGIN SELECT RAISE(ABORT,'settlement summary'); END`)
	require.NoError(t, err)
	require.ErrorContains(t, f.DB.Rewind(ctx, "chain", "42161", testutil.Contract, store.Block{Number: 0, Hash: testutil.PM}), "settlement summary")
	require.Equal(t, before, metricSummaryState(t, f.DB))
	testutil.AssertMetricSnapshot(t, f.DB)
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_settlement`)
	require.NoError(t, err)
	require.NoError(t, f.DB.Rewind(ctx, "chain", "42161", testutil.Contract, store.Block{Number: 0, Hash: testutil.PM}))
	assertMetricSummaryRebuild(t, f.DB)
}

func TestIngestExactMoneyReplayQuarantineOverdraw(t *testing.T) {
	const budget = "100000000000000000000000000000000000000000000000000000000000000000000"
	f := testutil.New(t, budget)
	ctx := context.Background()
	raw := f.Event(t, "event-1", budget, testutil.PM)
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 0, raw))
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 0, raw))
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 1, raw))
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 2, []byte(`{bad`)))
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 3, f.Event(t, "event-2", "17", testutil.PM)))
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "-17" {
		t.Fatal(bal)
	}
	d, err := f.DB.Authorize(ctx, f.Request)
	require.NoError(t, err)
	if d.Status != 402 {
		t.Fatal(d)
	}
	var signed int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&signed))
	if signed != 2 {
		t.Fatal(signed)
	}
	rows, err := f.DB.Rows(ctx, `SELECT status FROM usage_events ORDER BY offset`)
	require.NoError(t, err)
	if rows[1]["status"] != "duplicate" || rows[2]["status"] != "quarantined" {
		t.Fatal(rows)
	}
	next, _, _, err := f.DB.Checkpoint(ctx, "kafka", store.KafkaStream("test", 0))
	require.NoError(t, err)
	if next != 4 {
		t.Fatal(next)
	}
	if err := f.DB.Ingest(ctx, "test", 0, 5, raw); err == nil {
		t.Fatal("gap accepted")
	}
	// A new connection resumes from the durable accounting checkpoint.
	db, err := store.Open(ctx, f.Path, false)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ingest(ctx, "test", 0, 3, f.Event(t, "event-2", "17", testutil.PM)))
	assertBalanced(t, f.DB)
}

func TestMetricSummaryFailureRollsBack(t *testing.T) {
	for _, operation := range []string{"funding", "quarantine", "bootstrap"} {
		t.Run(operation, func(t *testing.T) {
			f := testutil.New(t, "100")
			ctx := t.Context()
			before := f.ManagementState(t)
			var err error
			switch operation {
			case "funding":
				_, err = f.DB.DB.Exec(`CREATE TRIGGER fail_summary BEFORE UPDATE ON metric_amounts BEGIN SELECT RAISE(ABORT,'summary failed'); END`)
				require.NoError(t, err)
				err = f.DB.Fund(ctx, "grant", f.Grant, "1")
			case "quarantine":
				_, err = f.DB.DB.Exec(`CREATE TRIGGER fail_summary BEFORE INSERT ON metric_counts BEGIN SELECT RAISE(ABORT,'summary failed'); END`)
				require.NoError(t, err)
				err = f.DB.Ingest(ctx, "events", 0, 0, []byte("{bad"))
				var count int
				require.NoError(t, f.DB.DB.QueryRow(`SELECT (SELECT count(*) FROM usage_events)+(SELECT count(*) FROM ingestion_checkpoints)`).Scan(&count))
				require.Zero(t, count)
			case "bootstrap":
				_, err = f.DB.DB.Exec(`CREATE TRIGGER fail_summary BEFORE INSERT ON metric_balances BEGIN SELECT RAISE(ABORT,'summary failed'); END`)
				require.NoError(t, err)
				err = f.DB.BootstrapChain(ctx, "chain", store.Block{Number: -1}, []store.EscrowSnapshot{{ChainID: "42161", Contract: testutil.Contract, Sender: testutil.Sender, Deposit: "0", Reserve: "0"}})
				var count int
				require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM escrow_snapshots`).Scan(&count))
				require.Zero(t, count)
			}
			require.ErrorContains(t, err, "summary failed")
			require.Equal(t, before, f.ManagementState(t))
			testutil.AssertMetricSnapshot(t, f.DB)
		})
	}
}

func TestIngestManifestID(t *testing.T) {
	f := testutil.New(t, "100")
	for offset, tc := range []struct {
		name, value, manifest, status string
	}{
		{"valid", `"manifest-1"`, "manifest-1", "applied"},
		{"numeric string", `"1"`, "1", "applied"},
		{"missing", "", "", "applied"},
		{"null", `null`, "", "applied"},
		{"empty", `""`, "", "applied"},
		{"number", `1`, "", "quarantined"},
		{"boolean", `true`, "", "quarantined"},
		{"object", `{}`, "", "quarantined"},
		{"array", `[]`, "", "quarantined"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var env store.Envelope
			require.NoError(t, json.Unmarshal(f.Event(t, tc.name, "1", testutil.PM), &env))
			var data map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(env.Data, &data))
			if tc.value == "" {
				delete(data, "manifest_id")
			} else {
				data["manifest_id"] = json.RawMessage(tc.value)
			}
			var err error
			env.Data, err = json.Marshal(data)
			require.NoError(t, err)
			raw, err := json.Marshal(env)
			require.NoError(t, err)
			require.NoError(t, f.DB.Ingest(t.Context(), "manifest", 0, int64(offset), raw))
			var manifest, status string
			require.NoError(t, f.DB.DB.QueryRow(`SELECT manifest_id,status FROM usage_events WHERE event_id=?`, tc.name).Scan(&manifest, &status))
			require.Equal(t, tc.manifest, manifest)
			require.Equal(t, tc.status, status)
		})
	}
}

func TestUsageManifestColumn(t *testing.T) {
	f := testutil.New(t, "100")
	const manifest = "manifest+%/&é"
	for i, event := range []struct {
		id, status string
		manifest   any
		linked     bool
	}{
		{"applied", "applied", manifest, true},
		{"quarantined", "quarantined", manifest, true},
		{"ignored", "ignored", manifest, true},
		{"duplicate", "duplicate", manifest, true},
		{"unassociated", "quarantined", manifest, false},
		{"missing", "applied", "", true},
		{"null", "applied", nil, true},
		{"numeric-string", "applied", "1", true},
	} {
		var session any
		if event.linked {
			session = f.Session
		}
		// The stored column controls filtering even when raw_payload is malformed.
		_, err := f.DB.DB.Exec(`INSERT INTO usage_events(id,topic,partition,offset,raw_payload,payment_session_id,manifest_id,status,created_at_ms) VALUES (?,'manifest',0,?,?,?,?,?,0)`, event.id, i, []byte(`{bad`), session, event.manifest, event.status)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		manifest string
		want     []string
	}{
		{manifest, []string{"applied", "quarantined", "ignored", "duplicate"}},
		{"1", []string{"numeric-string"}},
		{"missing", []string{}},
	} {
		t.Run(tc.manifest, func(t *testing.T) {
			rows, err := f.DB.List(t.Context(), "usage", store.ListOptions{ManifestID: tc.manifest})
			require.NoError(t, err)
			ids := make([]string, len(rows))
			for i, row := range rows {
				ids[i] = row["id"].(string)
			}
			require.Equal(t, tc.want, ids)
		})
	}
	all, err := f.DB.List(t.Context(), "usage", store.ListOptions{})
	require.NoError(t, err)
	require.Len(t, all, 8)
}

func TestQuarantineBindingsAndConflictingID(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	valid := f.Event(t, "event", "10", testutil.PM)
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 0, valid))
	variants := [][]byte{
		[]byte(strings.ReplaceAll(string(valid), `"10"`, `"20"`)),
		[]byte(strings.ReplaceAll(string(f.Event(t, "unknown", "10", testutil.PM)), f.Session, "unknown")),
		[]byte(strings.ReplaceAll(string(f.Event(t, "binding", "10", testutil.PM)), "state-1", "state-other")),
		[]byte(strings.ReplaceAll(string(f.Event(t, "missing", "10", testutil.PM)), f.Session, "")),
		[]byte(strings.ReplaceAll(string(f.Event(t, "wrong-pipeline", "10", testutil.PM)), `"pipeline":"live"`, `"pipeline":"wrong"`)),
		[]byte(strings.ReplaceAll(string(f.Event(t, "fixed-pipeline", "10", testutil.PM)), `"pipeline":"live"`, `"pipeline":"fixed"`)),
		[]byte(strings.ReplaceAll(string(f.Event(t, "empty-pipeline", "10", testutil.PM)), `"pipeline":"live"`, `"pipeline":""`)),
	}
	for i, raw := range variants {
		require.NoError(t, f.DB.Ingest(ctx, "test", 0, int64(i+1), raw))
	}
	var quarantined int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM usage_events WHERE status='quarantined'`).Scan(&quarantined))
	if quarantined != len(variants) {
		t.Fatal(quarantined)
	}
	var signed int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM signing_authorizations`).Scan(&signed))
	require.Equal(t, 1, signed)
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "90" {
		t.Fatal(bal)
	}
}

func TestStableRematchDoesNotWriteSettlementMetrics(t *testing.T) {
	f := testutil.New(t, "100")
	insertMetricSettlement(t, f.DB, "redemption", "10", "7", "settled")
	insertMetricSettlement(t, f.DB, "orphaned", "10", "7", "orphaned")
	rollbackMetricSummaries(t, f.DB)
	require.NoError(t, migrations.Up(t.Context(), f.DB.DB))
	now := time.Now().UnixMilli()
	require.NoError(t, f.DB.SetKafkaLag(t.Context(), f.DB.KafkaTopic, store.KafkaLag{CheckedAt: now, CoveredThrough: now, Available: true}))
	require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, 0, f.Event(t, "first", "1", testutil.PM)))
	testutil.AssertMetricSnapshot(t, f.DB)
	_, err := f.DB.DB.Exec(`CREATE TRIGGER fail_rematch BEFORE UPDATE ON settlements BEGIN SELECT RAISE(ABORT,'stable rematch wrote settlement'); END;
 CREATE TRIGGER fail_count BEFORE UPDATE ON metric_counts WHEN NEW.kind='settlements' BEGIN SELECT RAISE(ABORT,'stable rematch wrote count'); END;`)
	require.NoError(t, err)
	require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, 1, f.Event(t, "second", "1", testutil.PM)))
	testutil.AssertMetricSnapshot(t, f.DB)
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_rematch; DROP TRIGGER fail_count`)
	require.NoError(t, err)
	state := *f.Request.State
	state.StateID = "another-state"
	request := f.Request
	request.State = &state
	decision, err := f.DB.Authorize(t.Context(), request)
	require.NoError(t, err)
	var event map[string]any
	require.NoError(t, json.Unmarshal(f.Event(t, "ambiguous", "1", testutil.PM), &event))
	event["data"].(map[string]any)["auth_id"] = decision.AuthID
	event["data"].(map[string]any)["session_id"] = state.StateID
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	require.NoError(t, f.DB.Ingest(t.Context(), "events", 0, 2, raw))
	testutil.AssertMetricSnapshot(t, f.DB)
	for _, status := range []string{"settled", "orphaned"} {
		var n int
		require.NoError(t, f.DB.DB.QueryRow(`SELECT value FROM metric_counts WHERE kind='settlements' AND label1=? AND label2='ambiguous'`, status).Scan(&n))
		require.Equal(t, 1, n)
	}
	assertMetricSummaryRebuild(t, f.DB)
}

func TestConcurrentFundingCannotOverallocate(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "100"))
	var wg sync.WaitGroup
	var success atomic.Int32
	errs := make(chan error, 12)
	for i := range 12 {
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err != nil {
				errs <- err
				return
			}
			defer db.Close()
			_, err = db.Create(ctx, "allocation", store.Create{Name: fmt.Sprint(i), GrantID: f.Grant, Amount: "30"})
			if err == nil {
				success.Add(1)
			} else if !strings.Contains(err.Error(), "insufficient") {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if success.Load() != 3 {
		t.Fatal(success.Load())
	}
	bal, err := store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if bal.String() != "10" {
		t.Fatal(bal)
	}
	assertBalanced(t, f.DB)
}

func TestAllAllocationCreationAndFunding(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()

	zeroGrant, err := f.DB.Create(ctx, "grant", store.Create{Name: "empty", Amount: "0"})
	require.NoError(t, err)
	zeroAllocation, err := f.DB.Create(ctx, "allocation", store.Create{Name: "empty allocation", GrantID: zeroGrant, Amount: "all"})
	require.NoError(t, err)
	var allocated, status string
	require.NoError(t, f.DB.DB.QueryRow(`SELECT allocated_units,status FROM grant_allocations WHERE id=?`, zeroAllocation).Scan(&allocated, &status))
	if allocated != "0" || status != "exhausted" {
		t.Fatal(allocated, status)
	}

	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "75"))
	allocation, err := f.DB.Create(ctx, "allocation", store.Create{Name: "everything", GrantID: f.Grant, Amount: "all"})
	require.NoError(t, err)
	require.NoError(t, f.DB.DB.QueryRow(`SELECT allocated_units,status FROM grant_allocations WHERE id=?`, allocation).Scan(&allocated, &status))
	if allocated != "75" || status != "active" {
		t.Fatal(allocated, status)
	}
	if err := f.DB.Fund(ctx, "allocation", allocation, "all"); err == nil {
		t.Fatal("funded allocation with an empty grant balance")
	}
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "25"))
	require.NoError(t, f.DB.Fund(ctx, "allocation", allocation, "all"))
	require.NoError(t, f.DB.DB.QueryRow(`SELECT allocated_units FROM grant_allocations WHERE id=?`, allocation).Scan(&allocated))
	if allocated != "100" {
		t.Fatal(allocated)
	}
	assertBalanced(t, f.DB)
}

func TestConcurrentAllUsesOneTransactionalBalance(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "100"))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err == nil {
				defer db.Close()
				_, err = db.Create(ctx, "allocation", store.Create{Name: "all-" + fmt.Sprint(i), GrantID: f.Grant, Amount: "all"})
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	rows, err := f.DB.Rows(ctx, `SELECT allocated_units,status FROM grant_allocations WHERE name LIKE 'all-%'`)
	require.NoError(t, err)
	var funded int
	for _, row := range rows {
		switch row["allocated_units"] {
		case "100":
			funded++
			if row["status"] != "active" {
				t.Fatal(row)
			}
		case "0":
			if row["status"] != "exhausted" {
				t.Fatal(row)
			}
		default:
			t.Fatal(row)
		}
	}
	if len(rows) != 8 || funded != 1 {
		t.Fatal(rows)
	}

	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "60"))
	var fundingSuccess atomic.Int32
	errs = make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err == nil {
				defer db.Close()
				err = db.Fund(ctx, "allocation", f.Allocation, "all")
				if err == nil {
					fundingSuccess.Add(1)
				} else if strings.Contains(err.Error(), "funding must be positive") {
					err = nil
				}
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	if fundingSuccess.Load() != 1 {
		t.Fatal(fundingSuccess.Load())
	}
	assertBalanced(t, f.DB)
}

func TestCreateKeyForGrantIsAtomicAndUsesAllocationDefaults(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Fund(ctx, "grant", f.Grant, "50"))
	var before int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grant_allocations`).Scan(&before))
	_, err := f.DB.DB.Exec(`CREATE TRIGGER fail_auto_key BEFORE INSERT ON api_keys BEGIN SELECT RAISE(ABORT,'fixture failure'); END`)
	require.NoError(t, err)
	if _, _, _, err := f.DB.CreateKeyForGrant(ctx, f.Grant, "rolled back", "10"); err == nil {
		t.Fatal("expected key insertion failure")
	}
	var after int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM grant_allocations`).Scan(&after))
	if after != before {
		t.Fatal("allocation was not rolled back", before, after)
	}
	remaining, err := store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if remaining.String() != "50" {
		t.Fatal(remaining)
	}
	_, err = f.DB.DB.Exec(`DROP TRIGGER fail_auto_key`)
	require.NoError(t, err)

	allocation, keyID, key, err := f.DB.CreateKeyForGrant(ctx, f.Grant, "gateway", "all")
	require.NoError(t, err)
	if allocation == "" || keyID == "" || !strings.HasPrefix(key, "lpg_"+keyID+"_") {
		t.Fatal(allocation, keyID, key)
	}
	var name, beneficiary, amount, metadata, allocationStatus string
	var starts, ends any
	require.NoError(t, f.DB.DB.QueryRow(`SELECT name,beneficiary,allocated_units,metadata,starts_at_ms,ends_at_ms,status FROM grant_allocations WHERE id=?`, allocation).Scan(&name, &beneficiary, &amount, &metadata, &starts, &ends, &allocationStatus))
	if name != "gateway" || beneficiary != "" || amount != "50" || metadata != "" || starts != nil || ends != nil || allocationStatus != "active" {
		t.Fatal(name, beneficiary, amount, metadata, starts, ends, allocationStatus)
	}
	assertBalanced(t, f.DB)
}

func TestRevocationReturnsOnlyUnspentAndLateChargeIsRecorded(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 0, f.Event(t, "one", "40", testutil.PM)))
	require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "revoked"))
	require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "revoked"))
	rows, err := f.DB.List(ctx, "allocation", store.ListOptions{ID: f.Allocation})
	require.NoError(t, err)
	require.Equal(t, "revoked", rows[0]["status"])
	require.Equal(t, "40", rows[0]["allocated_units"])
	require.NoError(t, f.DB.Ingest(ctx, "test", 0, 1, f.Event(t, "late", "20", testutil.PM)))
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if bal.String() != "-20" {
		t.Fatal(bal)
	}
	bal, err = store.Balance(ctx, f.DB.DB, "grant_unallocated", f.Grant)
	require.NoError(t, err)
	if bal.String() != "60" {
		t.Fatal(bal)
	}
	if err := f.DB.SetStatus(ctx, "allocation", f.Allocation, "active"); err == nil {
		t.Fatal("revoked allocation reopened")
	}
}

func assertBalanced(t *testing.T, db *store.Store) {
	t.Helper()
	rows, err := db.DB.Query(`SELECT transaction_id,count(*),count(DISTINCT amount_units),count(DISTINCT direction) FROM ledger_entries GROUP BY transaction_id`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id string
		var n, a, d int
		require.NoError(t, rows.Scan(&id, &n, &a, &d))
		if n != 2 || a != 1 || d != 2 {
			t.Fatalf("unbalanced %s", id)
		}
	}
	require.NoError(t, rows.Err())
}

func TestConcurrentSpendsNeverLoseCharges(t *testing.T) {
	f := testutil.New(t, "100")
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		raw := f.Event(t, fmt.Sprint(i), "10", testutil.PM)
		wg.Go(func() {
			db, err := store.Open(ctx, f.Path, false)
			if err == nil {
				defer db.Close()
				err = db.Ingest(ctx, fmt.Sprint(i), 0, 0, raw)
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	b, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	require.NoError(t, err)
	if b.String() != "-60" {
		t.Fatal(b)
	}
	assertBalanced(t, f.DB)
}

func TestAllocationBalanceAndAuthorization(t *testing.T) {
	for _, currency := range []string{"eth", "usd"} {
		for _, pause := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pause=%t", currency, pause), func(t *testing.T) {
				f := testutil.NewCurrency(t, "10000000000000000", currency) // 0.01
				ctx := t.Context()
				status := func(want string, authStatus int) {
					t.Helper()
					decision, err := f.DB.Authorize(ctx, f.Request)
					require.NoError(t, err)
					require.Equal(t, authStatus, decision.Status)
					var got string
					require.NoError(t, f.DB.DB.QueryRow(`SELECT status FROM grant_allocations WHERE id=?`, f.Allocation).Scan(&got))
					require.Equal(t, want, got)
				}
				require.ErrorIs(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "exhausted"), store.ErrInvalidManagementInput)
				status("active", http.StatusOK)
				for _, tc := range []struct {
					fee    string
					status int
				}{{"0.01", 200}, {"0.010000000000000001", 402}, {"", 400}} {
					f.Request.State.FeeWei, _ = units.DecimalToUnits(tc.fee, currency)
					f.Request.State.FeeUSD = tc.fee
					if currency == "usd" {
						f.Request.State.FeeWei = "1"
					}
					status("active", tc.status)
				}
				f.Request.State.FeeWei = "1"
				f.Request.State.FeeUSD = "0.000000000000000001"
				balance, err := store.BalanceCurrency(ctx, f.DB.DB, "allocation_available", f.Allocation, currency)
				require.NoError(t, err)
				require.Equal(t, "10000000000000000", balance.String())

				var event map[string]any
				require.NoError(t, json.Unmarshal(f.Event(t, "spent", "10000000000000000", testutil.PM), &event))
				event["data"].(map[string]any)["computed_fee_usd"] = "0.01"
				raw, err := json.Marshal(event)
				require.NoError(t, err)
				require.NoError(t, f.DB.Ingest(ctx, "exhaustion", 0, 0, raw))
				status("exhausted", http.StatusPaymentRequired)
				require.ErrorIs(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "exhausted"), store.ErrInvalidManagementInput)
				require.ErrorIs(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "active"), store.ErrManagementConflict)
				if pause {
					require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "paused"))
					status("paused", http.StatusForbidden)
				}
				require.NoError(t, f.DB.FundCurrency(ctx, "grant", f.Grant, "25", currency))
				require.NoError(t, f.DB.FundCurrency(ctx, "allocation", f.Allocation, "25", currency))
				if pause {
					status("paused", http.StatusForbidden)
					require.NoError(t, f.DB.SetStatus(ctx, "allocation", f.Allocation, "active"))
				}
				status("active", http.StatusOK)
			})
		}
	}
}

func TestKafkaLagAuthorization(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	ms := now.UnixMilli()
	for _, tc := range []struct {
		name    string
		h       store.KafkaLag
		missing bool
		want    string
	}{
		{name: "missing", missing: true, want: "kafka lag unavailable"},
		{name: "unavailable", h: store.KafkaLag{CheckedAt: ms, CoveredThrough: ms}, want: "kafka lag unavailable"},
		{name: "unknown coverage", h: store.KafkaLag{CheckedAt: ms, Available: true}, want: "kafka lag unavailable"},
		{name: "unknown probe", h: store.KafkaLag{CoveredThrough: ms, Available: true}, want: "kafka lag unavailable"},
		{name: "healthy", h: store.KafkaLag{CheckedAt: ms, CoveredThrough: ms, Available: true}},
		{name: "below backlog limit", h: store.KafkaLag{CheckedAt: ms, CoveredThrough: ms - 59_999, Available: true}},
		{name: "at backlog limit", h: store.KafkaLag{CheckedAt: ms, CoveredThrough: ms - 60_000, Available: true}, want: "kafka lag exceeded"},
		{name: "above backlog limit", h: store.KafkaLag{CheckedAt: ms, CoveredThrough: ms - 60_001, Available: true}, want: "kafka lag exceeded"},
		{name: "below probe limit", h: store.KafkaLag{CheckedAt: ms - 59_999, CoveredThrough: ms - 59_999, Available: true}},
		{name: "at probe limit", h: store.KafkaLag{CheckedAt: ms - 60_000, CoveredThrough: ms - 60_000, Available: true}, want: "kafka lag unavailable"},
		{name: "above probe limit", h: store.KafkaLag{CheckedAt: ms - 60_001, CoveredThrough: ms - 60_001, Available: true}, want: "kafka lag unavailable"},
		{name: "future probe", h: store.KafkaLag{CheckedAt: ms + 1, CoveredThrough: ms, Available: true}, want: "kafka lag unavailable"},
		{name: "future coverage", h: store.KafkaLag{CheckedAt: ms, CoveredThrough: ms + 1, Available: true}, want: "kafka lag unavailable"},
		{name: "coverage after probe", h: store.KafkaLag{CheckedAt: ms - 1, CoveredThrough: ms, Available: true}, want: "kafka lag unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testutil.New(t, "100")
			f.DB.Now = func() time.Time { return now }
			_, err := f.DB.DB.Exec(`DELETE FROM kafka_lag; UPDATE payment_sessions SET last_seen_at_ms=1; UPDATE api_keys SET last_used_at_ms=1`)
			require.NoError(t, err)
			// Evidence for another topic must not authorize this topic.
			require.NoError(t, f.DB.SetKafkaLag(t.Context(), "other", store.KafkaLag{CheckedAt: ms, CoveredThrough: ms, Available: true}))
			if !tc.missing {
				require.NoError(t, f.DB.SetKafkaLag(t.Context(), f.DB.KafkaTopic, tc.h))
			}
			for _, stateID := range []string{f.Request.State.StateID, "new-state"} {
				state := *f.Request.State
				state.StateID = stateID
				req := f.Request
				req.State = &state
				d, err := f.DB.Authorize(t.Context(), req)
				require.NoError(t, err)
				require.Zero(t, d.Expiry)
				if tc.want == "" {
					require.Equal(t, 200, d.Status)
					continue
				}
				require.Equal(t, 503, d.Status)
				require.Equal(t, tc.want, d.Reason)
				require.Empty(t, d.AuthID)
				seen, used := activity(t, f.DB, f.Session, f.KeyID)
				require.EqualValues(t, 1, seen)
				require.EqualValues(t, 1, used)
				var count int
				require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM payment_sessions`).Scan(&count))
				require.Equal(t, 1, count)
			}
		})
	}
}

func TestNewAuthorizationRechecksKafkaLag(t *testing.T) {
	f := testutil.New(t, "100")
	other, err := store.Open(t.Context(), f.Path, false)
	require.NoError(t, err)
	defer other.Close()
	tx, err := other.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	read, resume := make(chan struct{}), make(chan struct{})
	first := true
	now := time.Now().Add(time.Second)
	f.DB.Now = func() time.Time {
		if first {
			first = false
			close(read)
			<-resume
		}
		return now
	}
	state := *f.Request.State
	state.StateID = "racing-new-state"
	req := f.Request
	req.State = &state
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan store.Decision, 1)
	errCh := make(chan error, 1)
	go func() {
		d, err := f.DB.Authorize(ctx, req)
		done <- d
		errCh <- err
	}()
	select {
	case <-read:
	case <-ctx.Done():
		t.Fatal("initial authorization read blocked on writer")
	}
	_, err = tx.ExecContext(ctx, `UPDATE kafka_lag SET available=0`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	close(resume)
	require.NoError(t, <-errCh)
	d := <-done
	require.Equal(t, 503, d.Status)
	require.Equal(t, "kafka lag unavailable", d.Reason)
	var count int
	require.NoError(t, f.DB.DB.QueryRow(`SELECT count(*) FROM payment_sessions`).Scan(&count))
	require.Equal(t, 1, count)
}
