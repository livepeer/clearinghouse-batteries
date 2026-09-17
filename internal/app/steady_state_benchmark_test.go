package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/livepeer/clearinghouse/internal/chain"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/testutil"
	kgo "github.com/segmentio/kafka-go"
)

const (
	benchmarkTicketsPerBatch = 100
	benchmarkAllocationWei   = "1000000000000000000000000000000"
	benchmarkTopic           = "benchmark-signing"
	benchmarkWebhookToken    = "benchmark-webhook-token"
)

var benchmarkContractABI = func() abi.ABI {
	parsed, err := abi.JSON(bytes.NewBufferString(`[
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"DepositFunded","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"WinningTicketTransfer","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"faceValue","type":"uint256"},{"indexed":false,"name":"winProb","type":"uint256"},{"indexed":false,"name":"senderNonce","type":"uint256"},{"indexed":false,"name":"recipientRand","type":"uint256"},{"indexed":false,"name":"auxData","type":"bytes"}],"name":"WinningTicketRedeemed","type":"event"}
]`))
	if err != nil {
		panic(err)
	}
	return parsed
}()

type benchmarkLayout string

const (
	benchmarkSeparate benchmarkLayout = "separate"
	benchmarkShared   benchmarkLayout = "shared"
)

type benchmarkCredential struct {
	allocation string
	authBody   []byte
	session    string
	state      string
}

type benchmarkRPC struct {
	mu          sync.RWMutex
	headers     []*types.Header
	logsByBlock [][]types.Log
	visible     int64
}

func (r *benchmarkRPC) ChainId() hexutil.Big {
	return hexutil.Big(*big.NewInt(42161))
}

func (r *benchmarkRPC) GetBlockByNumber(_ context.Context, number rpc.BlockNumber, _ bool) (*types.Header, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := int64(number)
	if number == rpc.LatestBlockNumber {
		n = r.visible
	}
	if n < 0 || n > r.visible || n >= int64(len(r.headers)) {
		return nil, nil
	}
	return r.headers[n], nil
}

func (r *benchmarkRPC) GetLogs(_ context.Context, raw json.RawMessage) ([]types.Log, error) {
	var query struct {
		From string `json:"fromBlock"`
		To   string `json:"toBlock"`
	}
	if err := json.Unmarshal(raw, &query); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	from, err := benchmarkBlockNumber(query.From, r.visible)
	if err != nil {
		return nil, err
	}
	to, err := benchmarkBlockNumber(query.To, r.visible)
	if err != nil {
		return nil, err
	}
	if from < 0 || to < from || to > r.visible {
		return nil, errors.New("invalid benchmark log range")
	}
	logs := []types.Log{}
	for block := from; block <= to; block++ {
		logs = append(logs, r.logsByBlock[block]...)
	}
	return logs, nil
}

func (r *benchmarkRPC) advance() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.visible+1 >= int64(len(r.headers)) {
		return errors.New("benchmark chain exhausted")
	}
	r.visible++
	return nil
}

func benchmarkBlockNumber(value string, latest int64) (int64, error) {
	switch value {
	case "", "latest":
		return latest, nil
	case "earliest":
		return 0, nil
	}
	n, err := hexutil.DecodeUint64(value)
	if err != nil || n > uint64(^uint64(0)>>1) {
		return 0, errors.New("invalid benchmark block number")
	}
	return int64(n), nil
}

func BenchmarkSteadyState(b *testing.B) {
	for _, layout := range []benchmarkLayout{benchmarkSeparate, benchmarkShared} {
		b.Run("layout="+string(layout), func(b *testing.B) {
			for _, workers := range []int{1, 8, 32} {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					benchmarkSteadyState(b, layout, workers)
				})
			}
		})
	}
}

func benchmarkSteadyState(b *testing.B, layout benchmarkLayout, workers int) {
	b.StopTimer()
	if b.N > int(^uint(0)>>1)/benchmarkTicketsPerBatch {
		b.Fatal("benchmark size overflows ticket count")
	}
	totalTickets := b.N * benchmarkTicketsPerBatch
	ctx := context.Background()
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "clearinghouse.db")
	credentials, err := setupBenchmarkCredentials(ctx, dbPath, layout, workers)
	if err != nil {
		b.Fatal(err)
	}
	tickets, err := prepareBenchmarkTickets(totalTickets, credentials)
	if err != nil {
		b.Fatal(err)
	}
	chainRPC, err := prepareBenchmarkChain(b.N)
	if err != nil {
		b.Fatal(err)
	}
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("eth", chainRPC); err != nil {
		b.Fatal(err)
	}
	rpcHTTP := httptest.NewServer(rpcServer)
	defer rpcHTTP.Close()

	httpAddr := benchmarkPort(b)
	kafkaAddr := benchmarkPort(b)
	startBlock := int64(0)
	params := ServeParams{
		Common:                Common{DBPath: dbPath},
		EnableAuthWebhook:     true,
		EnableKafka:           true,
		EnableAccounting:      true,
		EnableOnchainListener: true,
		HTTPBind:              httpAddr,
		WebhookToken:          benchmarkWebhookToken,
		KafkaBind:             kafkaAddr,
		KafkaTopic:            benchmarkTopic,
		KafkaDataDir:          filepath.Join(dir, "kafka"),
		RPCURL:                rpcHTTP.URL,
		ChainID:               "42161",
		TicketBroker:          testutil.Contract,
		SignerAddresses:       []string{testutil.Sender},
		StartBlock:            &startBlock,
		Confirmations:         0,
		PollInterval:          time.Millisecond,
		BlockBatchSize:        2000,
		ReorgLookback:         256,
	}
	serviceCtx, stopService := context.WithCancel(ctx)
	serviceDone := make(chan error, 1)
	go func() { serviceDone <- Serve(serviceCtx, params) }()
	serviceStopped := false
	defer func() {
		if serviceStopped {
			return
		}
		stopService()
		select {
		case <-serviceDone:
		case <-time.After(15 * time.Second):
			b.Error("clearinghouse benchmark service did not stop")
		}
	}()

	observer, err := store.Open(ctx, dbPath, false)
	if err != nil {
		b.Fatal(err)
	}
	defer observer.Close()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = workers + 2
	transport.MaxIdleConnsPerHost = workers + 2
	transport.MaxConnsPerHost = workers + 2
	httpClient := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	defer transport.CloseIdleConnections()
	stream := params.chainConfig().Stream()
	readyCtx, cancelReady := context.WithTimeout(ctx, 15*time.Second)
	err = waitBenchmark(readyCtx, func() (bool, error) {
		select {
		case serviceErr := <-serviceDone:
			serviceStopped = true
			return false, fmt.Errorf("clearinghouse stopped during startup: %w", serviceErr)
		default:
		}
		response, requestErr := httpClient.Get("http://" + httpAddr + "/readyz")
		if requestErr != nil {
			return false, nil
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false, nil
		}
		next, _, found, checkpointErr := observer.Checkpoint(readyCtx, "chain", stream)
		return found && next == 1, checkpointErr
	})
	cancelReady()
	if err != nil {
		b.Fatal(err)
	}

	writer := kgo.NewWriter(kgo.WriterConfig{
		Brokers:      []string{kafkaAddr},
		Topic:        benchmarkTopic,
		Balancer:     kgo.CRC32Balancer{},
		BatchTimeout: time.Millisecond,
	})
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writer.Close()
		}
	}()

	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	var completed atomic.Int64
	var firstErr error
	var errOnce sync.Once
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancelWork()
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	var workersDone sync.WaitGroup
	workersDone.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer workersDone.Done()
			credential := credentials[worker]
			for ticket := worker; ticket < totalTickets; ticket += workers {
				if err := authorizeBenchmarkTicket(workCtx, httpClient, httpAddr, credential.authBody, credential.session); err != nil {
					fail(err)
					return
				}
				if err := writer.WriteMessages(workCtx, kgo.Message{Value: tickets[ticket]}); err != nil {
					fail(err)
					return
				}
				if completed.Add(1)%benchmarkTicketsPerBatch == 0 {
					if err := chainRPC.advance(); err != nil {
						fail(err)
						return
					}
				}
			}
		}()
	}
	workersDone.Wait()
	if firstErr != nil {
		b.StopTimer()
		b.Fatal(firstErr)
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, 30*time.Second)
	err = waitBenchmark(drainCtx, func() (bool, error) {
		kafkaNext, _, kafkaFound, checkpointErr := observer.Checkpoint(drainCtx, "kafka", benchmarkTopic)
		if checkpointErr != nil {
			return false, checkpointErr
		}
		chainNext, _, chainFound, checkpointErr := observer.Checkpoint(drainCtx, "chain", stream)
		if checkpointErr != nil {
			return false, checkpointErr
		}
		return kafkaFound && kafkaNext == int64(totalTickets) && chainFound && chainNext == int64(b.N+1), nil
	})
	cancelDrain()
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	seconds := b.Elapsed().Seconds()
	if seconds > 0 {
		b.ReportMetric(float64(totalTickets)/seconds, "tickets/s")
		b.ReportMetric(float64(b.N)/seconds, "redemptions/s")
	}
	b.ReportMetric(benchmarkTicketsPerBatch, "tickets/op")
	if err := verifySteadyStateBenchmark(ctx, observer, layout, credentials, totalTickets, b.N, stream); err != nil {
		b.Fatal(err)
	}

	err = writer.Close()
	writerClosed = true
	if err != nil {
		b.Fatal(err)
	}
	stopService()
	select {
	case err := <-serviceDone:
		serviceStopped = true
		if err != nil {
			b.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		b.Fatal("clearinghouse benchmark service did not stop")
	}
}

func setupBenchmarkCredentials(ctx context.Context, path string, layout benchmarkLayout, workers int) ([]benchmarkCredential, error) {
	db, err := store.Open(ctx, path, true)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	allocationAmount, _ := new(big.Int).SetString(benchmarkAllocationWei, 10)
	allocationCount := int64(workers)
	if layout == benchmarkShared {
		allocationCount = 1
	}
	grantAmount := new(big.Int).Mul(new(big.Int).Set(allocationAmount), big.NewInt(allocationCount))
	grant, err := db.Create(ctx, "grant", store.Create{Name: "benchmark", Amount: grantAmount.String(), Status: "active"})
	if err != nil {
		return nil, err
	}
	allocations := make([]string, workers)
	if layout == benchmarkShared {
		allocation, err := db.Create(ctx, "allocation", store.Create{Name: "shared", GrantID: grant, Amount: benchmarkAllocationWei})
		if err != nil {
			return nil, err
		}
		for i := range allocations {
			allocations[i] = allocation
		}
	} else {
		for i := range allocations {
			allocation, err := db.Create(ctx, "allocation", store.Create{Name: fmt.Sprintf("worker-%d", i), GrantID: grant, Amount: benchmarkAllocationWei})
			if err != nil {
				return nil, err
			}
			allocations[i] = allocation
		}
	}
	credentials := make([]benchmarkCredential, workers)
	for i := range credentials {
		_, key, err := db.CreateKey(ctx, allocations[i], fmt.Sprintf("worker-%d", i))
		if err != nil {
			return nil, err
		}
		state := fmt.Sprintf("benchmark-state-%d", i)
		request := store.AuthRequest{
			Headers: http.Header{"Authorization": []string{"Bearer " + key}},
			State:   &store.RemoteState{StateID: state, App: "benchmark", Type: "live", OrchestratorAddress: testutil.Orch},
		}
		decision, err := db.Authorize(ctx, request)
		if err != nil {
			return nil, err
		}
		if decision.Status != http.StatusOK || decision.AuthID == "" {
			return nil, fmt.Errorf("create benchmark session: %+v", decision)
		}
		request.State.AuthID = decision.AuthID
		body, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		credentials[i] = benchmarkCredential{allocation: allocations[i], authBody: body, session: decision.AuthID, state: state}
	}
	return credentials, nil
}

func prepareBenchmarkTickets(total int, credentials []benchmarkCredential) ([][]byte, error) {
	tickets := make([][]byte, total)
	for ticket := range tickets {
		credential := credentials[ticket%len(credentials)]
		payload := struct {
			ID   string `json:"id"`
			Type string `json:"type"`
			Data any    `json:"data"`
		}{
			ID:   fmt.Sprintf("benchmark-ticket-%d", ticket+1),
			Type: "create_signed_ticket",
			Data: map[string]any{
				"session_id":         credential.state,
				"auth_id":            credential.session,
				"app":                "benchmark",
				"pipeline":           "live",
				"request_id":         fmt.Sprintf("benchmark-request-%d", ticket+1),
				"orch_address":       testutil.Orch,
				"pm_session_id":      benchmarkPMSession(int64(ticket + 1)),
				"computed_fee":       "1",
				"sequence_number":    ticket,
				"num_tickets":        1,
				"previous_time_unix": 1800000000000 + ticket,
				"current_time_unix":  1800000000001 + ticket,
				"billable_secs":      1,
				"pixels":             0,
			},
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		tickets[ticket] = raw
	}
	return tickets, nil
}

func prepareBenchmarkChain(redemptions int) (*benchmarkRPC, error) {
	r := &benchmarkRPC{
		headers:     make([]*types.Header, redemptions+1),
		logsByBlock: make([][]types.Log, redemptions+1),
	}
	contract := common.HexToAddress(testutil.Contract)
	sender := common.HexToAddress(testutil.Sender)
	recipient := common.HexToAddress(testutil.Orch)
	parent := common.Hash{}
	for block := 0; block <= redemptions; block++ {
		header := &types.Header{
			Number:     big.NewInt(int64(block)),
			ParentHash: parent,
			Time:       uint64(1800000000 + block),
			Difficulty: new(big.Int),
			GasLimit:   30000000,
			Extra:      []byte("clearinghouse-benchmark"),
		}
		r.headers[block] = header
		parent = header.Hash()
		if block == 0 {
			data, err := benchmarkContractABI.Events["DepositFunded"].Inputs.NonIndexed().Pack(mustBenchmarkAmount())
			if err != nil {
				return nil, err
			}
			r.logsByBlock[block] = []types.Log{{
				Address: contract, Topics: []common.Hash{chain.DepositFundedID, common.BytesToHash(sender.Bytes())}, Data: data,
				BlockNumber: 0, BlockHash: header.Hash(), TxHash: common.BigToHash(big.NewInt(1)), Index: 0,
			}}
			continue
		}
		amount := big.NewInt(1)
		transfer, err := benchmarkContractABI.Events["WinningTicketTransfer"].Inputs.NonIndexed().Pack(amount)
		if err != nil {
			return nil, err
		}
		rand := big.NewInt(int64(block * benchmarkTicketsPerBatch))
		redeemed, err := benchmarkContractABI.Events["WinningTicketRedeemed"].Inputs.NonIndexed().Pack(amount, big.NewInt(1), big.NewInt(int64(block)), rand, []byte{1})
		if err != nil {
			return nil, err
		}
		txHash := common.BigToHash(big.NewInt(int64(block + 1)))
		topics := []common.Hash{common.BytesToHash(sender.Bytes()), common.BytesToHash(recipient.Bytes())}
		r.logsByBlock[block] = []types.Log{
			{Address: contract, Topics: append([]common.Hash{chain.WinningTicketTransferID}, topics...), Data: transfer, BlockNumber: uint64(block), BlockHash: header.Hash(), TxHash: txHash, Index: 0},
			{Address: contract, Topics: append([]common.Hash{chain.EventID}, topics...), Data: redeemed, BlockNumber: uint64(block), BlockHash: header.Hash(), TxHash: txHash, Index: 1},
		}
	}
	return r, nil
}

func authorizeBenchmarkTicket(ctx context.Context, client *http.Client, address string, body []byte, session string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/v1/signer/authorize", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Livepeer-Clearinghouse-Token", benchmarkWebhookToken)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("authorization HTTP status %d", response.StatusCode)
	}
	var decision store.Decision
	if err := json.NewDecoder(response.Body).Decode(&decision); err != nil {
		return err
	}
	if decision.Status != http.StatusOK || decision.AuthID != session {
		return fmt.Errorf("authorization denied: %+v", decision)
	}
	return nil
}

func waitBenchmark(ctx context.Context, ready func() (bool, error)) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := ready()
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func verifySteadyStateBenchmark(ctx context.Context, db *store.Store, layout benchmarkLayout, credentials []benchmarkCredential, tickets, redemptions int, stream string) error {
	var usage, applied, authorizations int
	if err := db.DB.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE status='applied') FROM usage_events`).Scan(&usage, &applied); err != nil {
		return err
	}
	if err := db.DB.QueryRowContext(ctx, `SELECT count(*) FROM signing_authorizations`).Scan(&authorizations); err != nil {
		return err
	}
	if usage != tickets || applied != tickets || authorizations != tickets {
		return fmt.Errorf("ticket persistence mismatch: usage=%d applied=%d authorizations=%d want=%d", usage, applied, authorizations, tickets)
	}
	var settlements, matched int
	if err := db.DB.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE status='settled' AND match_status='matched') FROM settlements`).Scan(&settlements, &matched); err != nil {
		return err
	}
	if settlements != redemptions || matched != redemptions {
		return fmt.Errorf("settlement mismatch: settlements=%d matched=%d want=%d", settlements, matched, redemptions)
	}
	kafkaNext, _, kafkaFound, err := db.Checkpoint(ctx, "kafka", benchmarkTopic)
	if err != nil {
		return err
	}
	chainNext, _, chainFound, err := db.Checkpoint(ctx, "chain", stream)
	if err != nil {
		return err
	}
	if !kafkaFound || kafkaNext != int64(tickets) || !chainFound || chainNext != int64(redemptions+1) {
		return fmt.Errorf("checkpoint mismatch: kafka=%d chain=%d", kafkaNext, chainNext)
	}
	initial := mustBenchmarkAmount()
	if layout == benchmarkShared {
		if err := verifyBenchmarkAllocation(ctx, db, credentials[0].allocation, initial, tickets); err != nil {
			return fmt.Errorf("shared allocation: %w", err)
		}
	} else {
		for worker, credential := range credentials {
			count := 0
			if worker < tickets {
				count = 1 + (tickets-1-worker)/len(credentials)
			}
			if err := verifyBenchmarkAllocation(ctx, db, credential.allocation, initial, count); err != nil {
				return fmt.Errorf("worker %d allocation: %w", worker, err)
			}
		}
	}
	escrowID := "42161:" + strings.ToLower(testutil.Contract) + ":" + strings.ToLower(testutil.Sender)
	escrow, err := store.Balance(ctx, db.DB, "escrow_deposit", escrowID)
	if err != nil {
		return err
	}
	wantEscrow := new(big.Int).Sub(new(big.Int).Set(initial), big.NewInt(int64(redemptions)))
	if escrow.Cmp(wantEscrow) != 0 {
		return fmt.Errorf("escrow deposit balance=%s want=%s", escrow, wantEscrow)
	}
	settled, err := store.Balance(ctx, db.DB, "treasury_settled_spend", "42161:"+strings.ToLower(testutil.Sender))
	if err != nil {
		return err
	}
	if settled.Cmp(big.NewInt(int64(redemptions))) != 0 {
		return fmt.Errorf("treasury settled balance=%s want=%d", settled, redemptions)
	}
	return nil
}

func verifyBenchmarkAllocation(ctx context.Context, db *store.Store, allocation string, initial *big.Int, tickets int) error {
	available, err := store.Balance(ctx, db.DB, "allocation_available", allocation)
	if err != nil {
		return err
	}
	wantAvailable := new(big.Int).Sub(new(big.Int).Set(initial), big.NewInt(int64(tickets)))
	if available.Cmp(wantAvailable) != 0 {
		return fmt.Errorf("available balance=%s want=%s", available, wantAvailable)
	}
	spent, err := store.Balance(ctx, db.DB, "allocation_spent", allocation)
	if err != nil {
		return err
	}
	if spent.Cmp(big.NewInt(int64(tickets))) != 0 {
		return fmt.Errorf("spent balance=%s want=%d", spent, tickets)
	}
	return nil
}

func benchmarkPMSession(ticket int64) string {
	return crypto.Keccak256Hash(common.LeftPadBytes(big.NewInt(ticket).Bytes(), 32)).Hex()
}

func mustBenchmarkAmount() *big.Int {
	amount, ok := new(big.Int).SetString(benchmarkAllocationWei, 10)
	if !ok {
		panic("invalid benchmark allocation")
	}
	return amount
}

func benchmarkPort(b *testing.B) string {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		b.Fatal(err)
	}
	return address
}
