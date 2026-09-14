package chain

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
)

type rpcFixture struct {
	mu              sync.Mutex
	headers         []*types.Header
	logs            []types.Log
	queries         []ethereum.FilterQuery
	deposit         *big.Int
	reserve         *big.Int
	chainID         *big.Int
	broker          common.Address
	emptyCall       bool
	controllerCalls int
	controllerData  []byte
}

func (f *rpcFixture) ChainID(context.Context) (*big.Int, error) {
	if f.chainID != nil {
		return new(big.Int).Set(f.chainID), nil
	}
	return big.NewInt(42161), nil
}
func (f *rpcFixture) HeaderByNumber(ctx context.Context, n *big.Int) (*types.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n == nil {
		return f.headers[len(f.headers)-1], nil
	}
	if !n.IsInt64() || n.Sign() < 0 || n.Int64() >= int64(len(f.headers)) {
		return nil, nil
	}
	return f.headers[n.Int64()], nil
}
func (f *rpcFixture) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q)
	result := []types.Log{}
	for _, l := range f.logs {
		if l.BlockNumber >= q.FromBlock.Uint64() && l.BlockNumber <= q.ToBlock.Uint64() {
			result = append(result, l)
		}
	}
	return result, nil
}
func (f *rpcFixture) CallContract(_ context.Context, call ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	if call.To != nil && *call.To == common.HexToAddress(arbitrumOneController) {
		f.controllerCalls++
		f.controllerData = append([]byte(nil), call.Data...)
		broker := f.broker
		if broker == (common.Address{}) {
			broker = common.HexToAddress(testutil.Contract)
		}
		return controllerABI.Methods["getContract"].Outputs.Pack(broker)
	}
	if f.emptyCall {
		return nil, nil
	}
	sender := struct {
		Deposit       *big.Int
		WithdrawRound *big.Int
	}{new(big.Int).Set(f.deposit), new(big.Int)}
	reserve := struct {
		FundsRemaining        *big.Int
		ClaimedInCurrentRound *big.Int
	}{new(big.Int).Set(f.reserve), new(big.Int)}
	return ticketABI.Methods["getSenderInfo"].Outputs.Pack(sender, reserve)
}
func makeHeaders(from int, parent common.Hash, tag string, to int) []*types.Header {
	var out []*types.Header
	for i := from; i <= to; i++ {
		h := &types.Header{Number: big.NewInt(int64(i)), ParentHash: parent, Time: uint64(1800000000 + i), Difficulty: big.NewInt(0), GasLimit: 30000000, Extra: []byte(tag)}
		out = append(out, h)
		parent = h.Hash()
	}
	return out
}
func makeLog(t *testing.T, h *types.Header, nonce int64) types.Log {
	t.Helper()
	data, err := ticketABI.Events["WinningTicketRedeemed"].Inputs.NonIndexed().Pack(big.NewInt(500), big.NewInt(1), big.NewInt(nonce), big.NewInt(1), []byte{1, 2, 3})
	testutil.Must(t, err)
	return types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{EventID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes()), common.BytesToHash(common.HexToAddress(testutil.Orch).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(nonce)), Index: uint(nonce*10 + 1)}
}
func makeTransfer(t *testing.T, h *types.Header, nonce, amount int64) types.Log {
	t.Helper()
	data, err := ticketABI.Events["WinningTicketTransfer"].Inputs.NonIndexed().Pack(big.NewInt(amount))
	testutil.Must(t, err)
	return types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{WinningTicketTransferID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes()), common.BytesToHash(common.HexToAddress(testutil.Orch).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(nonce)), Index: uint(nonce * 10)}
}
func makeRedemptionLogs(t *testing.T, h *types.Header, nonce int64) []types.Log {
	return []types.Log{makeTransfer(t, h, nonce, 500), makeLog(t, h, nonce)}
}
func makeFundingLog(t *testing.T, h *types.Header, tx, index, amount int64, name string) types.Log {
	t.Helper()
	event := ticketABI.Events[name]
	data, err := event.Inputs.NonIndexed().Pack(big.NewInt(amount))
	testutil.Must(t, err)
	return types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{event.ID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(tx)), Index: uint(index)}
}
func makeWithdrawalLog(t *testing.T, h *types.Header, tx, index, deposit, reserve int64) types.Log {
	t.Helper()
	event := ticketABI.Events["Withdrawal"]
	data, err := event.Inputs.NonIndexed().Pack(big.NewInt(deposit), big.NewInt(reserve))
	testutil.Must(t, err)
	return types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{event.ID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(tx)), Index: uint(index)}
}
func makePaymentLogs(t *testing.T, h *types.Header, tx, index, faceValue, paid, reservePaid int64) []types.Log {
	t.Helper()
	var logs []types.Log
	if reservePaid > 0 {
		event := ticketABI.Events["ReserveClaimed"]
		data, err := event.Inputs.NonIndexed().Pack(common.HexToAddress(testutil.Orch), big.NewInt(reservePaid))
		testutil.Must(t, err)
		logs = append(logs, types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{event.ID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(tx)), Index: uint(index)})
		index++
	}
	if paid > 0 {
		event := ticketABI.Events["WinningTicketTransfer"]
		data, err := event.Inputs.NonIndexed().Pack(big.NewInt(paid))
		testutil.Must(t, err)
		logs = append(logs, types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{event.ID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes()), common.BytesToHash(common.HexToAddress(testutil.Orch).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(tx)), Index: uint(index)})
		index++
	}
	event := ticketABI.Events["WinningTicketRedeemed"]
	data, err := event.Inputs.NonIndexed().Pack(big.NewInt(faceValue), big.NewInt(1), big.NewInt(tx), big.NewInt(1), []byte{1, 2, 3})
	testutil.Must(t, err)
	logs = append(logs, types.Log{Address: common.HexToAddress(testutil.Contract), Topics: []common.Hash{event.ID, common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes()), common.BytesToHash(common.HexToAddress(testutil.Orch).Bytes())}, Data: data, BlockNumber: h.Number.Uint64(), BlockHash: h.Hash(), TxHash: common.BigToHash(big.NewInt(tx)), Index: uint(index)})
	return logs
}
func newListener(t *testing.T, f *testutil.Fixture) (*Listener, *rpcFixture) {
	t.Helper()
	api := &rpcFixture{headers: makeHeaders(0, common.Hash{}, "main", 6), deposit: big.NewInt(5000), reserve: new(big.Int)}
	start := int64(1)
	return &Listener{DB: f.DB, RPC: api, Config: Config{URL: "fixture", ChainID: "42161", Contract: testutil.Contract, Senders: []string{testutil.Sender}, Start: &start, Confirmations: 2, BatchSize: 10, Lookback: 4, Poll: time.Millisecond}}, api
}

func TestRPCConfirmationDuplicateDelayedAttributionAndReorg(t *testing.T) {
	f := testutil.New(t, "100")
	l, api := newListener(t, f)
	ctx := context.Background()
	originalHeaders := append([]*types.Header{}, api.headers...)
	log := makeLog(t, api.headers[3], 1)
	api.logs = append([]types.Log{makeTransfer(t, api.headers[3], 1, 500), log, log}, makeRedemptionLogs(t, api.headers[6], 2)...)
	progress, err := l.Step(ctx)
	testutil.Must(t, err)
	if !progress {
		t.Fatal("no progress")
	}
	next, _, _, err := f.DB.Checkpoint(ctx, "chain", l.Config.Stream())
	testutil.Must(t, err)
	if next != 5 {
		t.Fatal(next)
	}
	rows, err := f.DB.List(ctx, "settlement", "")
	testutil.Must(t, err)
	if len(rows) != 1 || rows[0]["match_status"] != "unmatched" {
		t.Fatal(rows)
	}
	query := api.queries[0]
	if len(query.Addresses) != 1 || query.Addresses[0] != common.HexToAddress(testutil.Contract) || len(query.Topics) != 2 || len(query.Topics[0]) != len(EventIDs) || len(query.Topics[1]) != 1 || query.Topics[1][0] != common.BytesToHash(common.HexToAddress(testutil.Sender).Bytes()) {
		t.Fatal(query)
	}
	for _, id := range EventIDs {
		if !slices.Contains(query.Topics[0], id) {
			t.Fatalf("filter missing event %s: %v", id, query)
		}
	}
	pm := crypto.Keccak256Hash(common.LeftPadBytes(big.NewInt(1).Bytes(), 32)).Hex()
	testutil.Must(t, f.DB.Ingest(ctx, "test", 0, f.Event(t, "usage", "10", pm)))
	rows, err = f.DB.List(ctx, "settlement", "")
	testutil.Must(t, err)
	if rows[0]["match_status"] != "matched" || rows[0]["payment_session_id"] != f.Session || rows[0]["authorization_id"] != nil {
		t.Fatal(rows)
	}
	bal, err := store.Balance(ctx, f.DB.DB, "allocation_available", f.Allocation)
	testutil.Must(t, err)
	if bal.String() != "90" {
		t.Fatal(bal)
	}
	bal, err = store.Balance(ctx, f.DB.DB, "treasury_settled_spend", "42161:"+testutil.Sender)
	testutil.Must(t, err)
	if bal.String() != "500" {
		t.Fatal(bal)
	}
	// Fork block 3; retain a common ancestor at block 2, then ingest the replacement payout.
	api.headers = append(api.headers[:3], makeHeaders(3, api.headers[2].Hash(), "fork", 6)...)
	api.logs = makeRedemptionLogs(t, api.headers[3], 3)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	rows, err = f.DB.List(ctx, "settlement", "")
	testutil.Must(t, err)
	if len(rows) != 2 {
		t.Fatal(rows)
	}
	bal, err = store.Balance(ctx, f.DB.DB, "treasury_settled_spend", "42161:"+testutil.Sender)
	testutil.Must(t, err)
	if bal.String() != "500" {
		t.Fatal(bal)
	}
	// The original branch becomes canonical again: reverse the fork and repost the original once.
	api.headers = originalHeaders
	api.logs = []types.Log{makeTransfer(t, api.headers[3], 1, 500), log}
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	bal, err = store.Balance(ctx, f.DB.DB, "treasury_settled_spend", "42161:"+testutil.Sender)
	testutil.Must(t, err)
	if bal.String() != "500" {
		t.Fatal(bal)
	}
	var generation int
	testutil.Must(t, f.DB.DB.QueryRow(`SELECT generation FROM settlements WHERE tx_hash=?`, strings.ToLower(log.TxHash.Hex())).Scan(&generation))
	if generation != 1 {
		t.Fatal(generation)
	}
}

func TestAmbiguousSessionsAndDeepReorg(t *testing.T) {
	f := testutil.New(t, "100")
	l, api := newListener(t, f)
	ctx := context.Background()
	pm := crypto.Keccak256Hash(common.LeftPadBytes(big.NewInt(1).Bytes(), 32)).Hex()
	testutil.Must(t, f.DB.Ingest(ctx, "test", 0, f.Event(t, "first", "10", pm)))
	req := f.Request
	state := *req.State
	state.StateID = "state-2"
	req.State = &state
	d, err := f.DB.Authorize(ctx, req)
	testutil.Must(t, err)
	second := f.Event(t, "second", "10", pm)
	second = bytes.ReplaceAll(second, []byte(f.Session), []byte(d.AuthID))
	second = bytes.ReplaceAll(second, []byte("state-1"), []byte("state-2"))
	testutil.Must(t, f.DB.Ingest(ctx, "test", 1, second))
	api.logs = makeRedemptionLogs(t, api.headers[3], 1)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	rows, err := f.DB.List(ctx, "settlement", "")
	testutil.Must(t, err)
	if rows[0]["match_status"] != "ambiguous" || rows[0]["payment_session_id"] != nil {
		t.Fatal(rows)
	}
	api.headers = makeHeaders(0, common.Hash{}, "no-common-ancestor", 6)
	api.logs = nil
	_, err = l.Step(ctx)
	if err == nil || !strings.Contains(err.Error(), "common-ancestor") {
		t.Fatal(err)
	}
	bal, err := store.Balance(ctx, f.DB.DB, "treasury_settled_spend", "42161:"+testutil.Sender)
	testutil.Must(t, err)
	if bal.String() != "500" {
		t.Fatal("changed ledger on deep reorg", bal)
	}
}

func TestRPCFailureDoesNotAdvanceAndDefaultStart(t *testing.T) {
	f := testutil.New(t, "100")
	l, api := newListener(t, f)
	ctx := context.Background()
	l.Config.Start = nil
	progress, err := l.Step(ctx)
	testutil.Must(t, err)
	if progress {
		t.Fatal("unconfirmed current head reported progress")
	}
	next, hash, found, err := f.DB.Checkpoint(ctx, "chain", l.Config.Stream())
	testutil.Must(t, err)
	if !found || next != 6 || hash != strings.ToLower(api.headers[5].Hash().Hex()) {
		t.Fatal(next, hash, found)
	}

	f = testutil.New(t, "100")
	l, api = newListener(t, f)
	start := int64(1)
	l.Config.Start = &start
	bad := makeLog(t, api.headers[3], 1)
	bad.BlockHash = common.Hash{}
	api.logs = []types.Log{makeTransfer(t, api.headers[3], 1, 500), bad}
	if _, err := l.Step(ctx); err == nil {
		t.Fatal("noncanonical log accepted")
	}
	next, _, _, err = f.DB.Checkpoint(ctx, "chain", l.Config.Stream())
	testutil.Must(t, err)
	if next != 1 {
		t.Fatal(next)
	}
}

func TestCheckpointTakesPrecedenceOverChangedStart(t *testing.T) {
	f := testutil.New(t, "100")
	l, _ := newListener(t, f)
	ctx := context.Background()
	_, err := l.Step(ctx)
	testutil.Must(t, err)

	changed := int64(0)
	l.Config.Start = &changed
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	next, _, found, err := f.DB.Checkpoint(ctx, "chain", l.Config.Stream())
	testutil.Must(t, err)
	if !found || next != 5 {
		t.Fatal(next, found)
	}
}

func TestResolveChainConfiguration(t *testing.T) {
	f := testutil.New(t, "100")
	l, api := newListener(t, f)
	l.Config.ChainID = ""
	l.Config.Contract = ""
	testutil.Must(t, l.resolveConfig(context.Background()))
	if l.Config.ChainID != "42161" || l.Config.Contract != testutil.Contract {
		t.Fatal(l.Config)
	}
	wantData, err := controllerABI.Pack("getContract", crypto.Keccak256Hash([]byte("TicketBroker")))
	testutil.Must(t, err)
	if api.controllerCalls != 1 || !bytes.Equal(api.controllerData, wantData) {
		t.Fatal(api.controllerCalls, api.controllerData)
	}

	l, _ = newListener(t, f)
	l.Config.ChainID = "1"
	if err := l.resolveConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatal(err)
	}

	l, api = newListener(t, f)
	api.chainID = big.NewInt(1)
	l.Config.ChainID = ""
	l.Config.Contract = ""
	if err := l.resolveConfig(context.Background()); err == nil || !strings.Contains(err.Error(), "--ticket-broker required") {
		t.Fatal(err)
	}

	l.Config.Contract = testutil.Contract
	testutil.Must(t, l.resolveConfig(context.Background()))
	if api.controllerCalls != 0 {
		t.Fatal("explicit TicketBroker override queried the Controller")
	}
}

func TestEscrowFundingPartialRedemptionWithdrawalAndReporting(t *testing.T) {
	f := testutil.New(t, "100")
	l, api := newListener(t, f)
	originalHeaders := append([]*types.Header{}, api.headers...)
	api.deposit, api.reserve = big.NewInt(100), big.NewInt(50)
	api.logs = append(api.logs, makeFundingLog(t, api.headers[2], 20, 20, 10, "DepositFunded"))
	api.logs = append(api.logs, makeFundingLog(t, api.headers[2], 20, 21, 20, "ReserveFunded"))
	api.logs = append(api.logs, makePaymentLogs(t, api.headers[3], 30, 30, 200, 150, 40)...)
	api.logs = append(api.logs, makeWithdrawalLog(t, api.headers[4], 40, 40, 0, 30))
	originalLogs := append([]types.Log{}, api.logs...)
	ctx := context.Background()
	_, err := l.Step(ctx)
	testutil.Must(t, err)

	report, err := f.DB.EscrowReport(ctx)
	testutil.Must(t, err)
	if len(report) != 1 || report[0]["deposit_balance_wei"] != "0" || report[0]["reserve_balance_wei"] != "0" || report[0]["total_balance_wei"] != "0" || report[0]["confirmed_through_block"] != "4" {
		t.Fatal(report)
	}
	settlements, err := f.DB.List(ctx, "settlement", "")
	testutil.Must(t, err)
	if len(settlements) != 1 || settlements[0]["face_value_wei"] != "200" || settlements[0]["paid_amount_wei"] != "150" || settlements[0]["deposit_paid_wei"] != "110" || settlements[0]["reserve_paid_wei"] != "40" {
		t.Fatal(settlements)
	}
	activity, err := f.DB.EscrowActivity(ctx)
	testutil.Must(t, err)
	if len(activity) != 7 || activity[0]["event_type"] != "opening_snapshot" {
		t.Fatal(activity)
	}
	for account, want := range map[string]string{"treasury_cash": "-150", "treasury_settled_spend": "150"} {
		balance, err := store.Balance(ctx, f.DB.DB, account, "42161:"+testutil.Sender)
		testutil.Must(t, err)
		if balance.String() != want {
			t.Fatalf("%s=%s want %s", account, balance, want)
		}
	}
	decision, err := f.DB.Authorize(ctx, f.Request)
	testutil.Must(t, err)
	if decision.Status != 200 {
		t.Fatal("empty escrow affected authorization", decision)
	}

	// Replace the payout and withdrawal blocks. Both escrow portions and the
	// withdrawal return to their state at the common ancestor.
	api.headers = append(api.headers[:3], makeHeaders(3, api.headers[2].Hash(), "fork-escrow", 6)...)
	api.logs = nil
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	report, err = f.DB.EscrowReport(ctx)
	testutil.Must(t, err)
	if report[0]["deposit_balance_wei"] != "110" || report[0]["reserve_balance_wei"] != "70" {
		t.Fatal(report)
	}
	spend, err := store.Balance(ctx, f.DB.DB, "treasury_settled_spend", "42161:"+testutil.Sender)
	testutil.Must(t, err)
	if spend.Sign() != 0 {
		t.Fatal(spend)
	}

	// Re-canonicalizing the original blocks reposts each movement once.
	api.headers, api.logs = originalHeaders, originalLogs
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	report, err = f.DB.EscrowReport(ctx)
	testutil.Must(t, err)
	if report[0]["deposit_balance_wei"] != "0" || report[0]["reserve_balance_wei"] != "0" {
		t.Fatal(report)
	}
	spend, err = store.Balance(ctx, f.DB.DB, "treasury_settled_spend", "42161:"+testutil.Sender)
	testutil.Must(t, err)
	if spend.String() != "150" {
		t.Fatal(spend)
	}
}

func TestGenesisAndPredeploymentOpeningSnapshots(t *testing.T) {
	for _, start := range []int64{0, 1} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			f := testutil.New(t, "100")
			l, api := newListener(t, f)
			l.Config.Start = &start
			l.Config.Senders = []string{testutil.Sender, "0x4444444444444444444444444444444444444444"}
			if start == 1 {
				api.emptyCall = true
			}
			_, err := l.Step(context.Background())
			testutil.Must(t, err)
			report, err := f.DB.EscrowReport(context.Background())
			testutil.Must(t, err)
			if len(report) != 2 || report[0]["total_balance_wei"] != "0" || report[1]["total_balance_wei"] != "0" {
				t.Fatal(report)
			}
		})
	}
}

func TestEscrowFundingReorgAndReplay(t *testing.T) {
	f := testutil.New(t, "100")
	l, api := newListener(t, f)
	api.deposit = big.NewInt(100)
	ctx := context.Background()
	originalHeaders := append([]*types.Header{}, api.headers...)
	funding := makeFundingLog(t, api.headers[3], 30, 30, 50, "DepositFunded")
	api.logs = []types.Log{funding}
	_, err := l.Step(ctx)
	testutil.Must(t, err)
	assertDeposit := func(want string) {
		report, err := f.DB.EscrowReport(ctx)
		testutil.Must(t, err)
		if report[0]["deposit_balance_wei"] != want {
			t.Fatalf("deposit=%s want %s", report[0]["deposit_balance_wei"], want)
		}
	}
	assertDeposit("150")
	api.headers = append(api.headers[:3], makeHeaders(3, api.headers[2].Hash(), "fork-funding", 6)...)
	api.logs = nil
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	assertDeposit("100")
	api.headers = originalHeaders
	api.logs = []types.Log{funding}
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	_, err = l.Step(ctx)
	testutil.Must(t, err)
	assertDeposit("150")
	var generation int
	testutil.Must(t, f.DB.DB.QueryRow(`SELECT generation FROM ticket_broker_events WHERE tx_hash=?`, strings.ToLower(funding.TxHash.Hex())).Scan(&generation))
	if generation != 1 {
		t.Fatal(generation)
	}
}

func TestInvalidPaymentSequencesDoNotAdvance(t *testing.T) {
	tests := map[string]func(*testing.T, *rpcFixture) []types.Log{
		"incomplete": func(t *testing.T, api *rpcFixture) []types.Log {
			return []types.Log{makeTransfer(t, api.headers[3], 1, 10)}
		},
		"reserve exceeds transfer": func(t *testing.T, api *rpcFixture) []types.Log {
			return makePaymentLogs(t, api.headers[3], 1, 10, 10, 5, 6)
		},
		"deposit split exceeds balance": func(t *testing.T, api *rpcFixture) []types.Log {
			api.deposit = big.NewInt(10)
			return makePaymentLogs(t, api.headers[3], 1, 10, 20, 20, 0)
		},
		"withdrawal does not empty escrow": func(t *testing.T, api *rpcFixture) []types.Log {
			return []types.Log{makeWithdrawalLog(t, api.headers[3], 1, 10, 1, 0)}
		},
	}
	for name, logs := range tests {
		t.Run(name, func(t *testing.T) {
			f := testutil.New(t, "100")
			l, api := newListener(t, f)
			api.logs = logs(t, api)
			if _, err := l.Step(context.Background()); err == nil {
				t.Fatal("invalid payment accepted")
			}
			next, _, found, err := f.DB.Checkpoint(context.Background(), "chain", l.Config.Stream())
			testutil.Must(t, err)
			if !found || next != 1 {
				t.Fatal(next, found)
			}
		})
	}
}
