// Package chain ingests confirmed TicketBroker logs with replayable checkpoints.
package chain

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/livepeer/clearinghouse/internal/store"
	"golang.org/x/sync/errgroup"
)

const contractABI = `[
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"DepositFunded","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"reserveHolder","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"ReserveFunded","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"reserveHolder","type":"address"},{"indexed":false,"name":"claimant","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"ReserveClaimed","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":false,"name":"deposit","type":"uint256"},{"indexed":false,"name":"reserve","type":"uint256"}],"name":"Withdrawal","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount","type":"uint256"}],"name":"WinningTicketTransfer","type":"event"},
{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"faceValue","type":"uint256"},{"indexed":false,"name":"winProb","type":"uint256"},{"indexed":false,"name":"senderNonce","type":"uint256"},{"indexed":false,"name":"recipientRand","type":"uint256"},{"indexed":false,"name":"auxData","type":"bytes"}],"name":"WinningTicketRedeemed","type":"event"},
{"inputs":[{"name":"_sender","type":"address"}],"name":"getSenderInfo","outputs":[{"components":[{"name":"deposit","type":"uint256"},{"name":"withdrawRound","type":"uint256"}],"name":"sender","type":"tuple"},{"components":[{"name":"fundsRemaining","type":"uint256"},{"name":"claimedInCurrentRound","type":"uint256"}],"name":"reserve","type":"tuple"}],"stateMutability":"view","type":"function"}
]`

const controllerContractABI = `[
{"inputs":[{"name":"_id","type":"bytes32"}],"name":"getContract","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}
]`

const (
	arbitrumOneChainID    = "42161"
	arbitrumOneController = "0xD8E8328501E9645d16Cf49539efC04f734606ee4"
)

var ticketABI = func() abi.ABI {
	a, err := abi.JSON(strings.NewReader(contractABI))
	if err != nil {
		panic(err)
	}
	return a
}()
var controllerABI = func() abi.ABI {
	a, err := abi.JSON(strings.NewReader(controllerContractABI))
	if err != nil {
		panic(err)
	}
	return a
}()
var (
	DepositFundedID         = ticketABI.Events["DepositFunded"].ID
	ReserveFundedID         = ticketABI.Events["ReserveFunded"].ID
	ReserveClaimedID        = ticketABI.Events["ReserveClaimed"].ID
	WithdrawalID            = ticketABI.Events["Withdrawal"].ID
	WinningTicketTransferID = ticketABI.Events["WinningTicketTransfer"].ID
	EventID                 = ticketABI.Events["WinningTicketRedeemed"].ID
	EventIDs                = []common.Hash{DepositFundedID, ReserveFundedID, ReserveClaimedID, WithdrawalID, WinningTicketTransferID, EventID}
)

type RPC interface {
	ChainID(context.Context) (*big.Int, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
	CallContract(context.Context, ethereum.CallMsg, *big.Int) ([]byte, error)
}
type Config struct {
	URL, ChainID, Contract             string
	Senders                            []string
	Start                              *int64
	Confirmations, BatchSize, Lookback int64
	Poll                               time.Duration
}
type Listener struct {
	DB     *store.Store
	RPC    RPC
	Config Config
	Ready  atomic.Bool
}
type fatalError struct{ error }

func (c Config) Validate() error {
	if c.ChainID != "" {
		n, err := store.Amount(c.ChainID)
		if err != nil || n.Sign() <= 0 {
			return errors.New("chain ID must be a positive integer")
		}
	}
	if c.Contract != "" {
		if _, err := store.Address(c.Contract); err != nil {
			return fmt.Errorf("contract address: %w", err)
		}
	}
	if len(c.Senders) == 0 {
		return errors.New("at least one signer address required")
	}
	for _, s := range c.Senders {
		if _, err := store.Address(s); err != nil {
			return err
		}
	}
	if c.Start != nil && (*c.Start < 0 || *c.Start == math.MaxInt64) {
		return errors.New("invalid start block")
	}
	if c.Confirmations < 0 || c.BatchSize <= 0 || c.Lookback < 1 || c.Poll <= 0 {
		return errors.New("invalid chain polling bounds")
	}
	return nil
}

func (c Config) Stream() string {
	senders := append([]string{}, c.Senders...)
	for i := range senders {
		senders[i] = strings.ToLower(senders[i])
	}
	slices.Sort(senders)
	senders = slices.Compact(senders)
	return c.ChainID + ":" + strings.ToLower(c.Contract) + ":" + strings.Join(senders, ",")
}

func (l *Listener) Run(ctx context.Context) error {
	defer l.Ready.Store(false)
	if err := l.Config.Validate(); err != nil {
		return err
	}
	var client *ethclient.Client
	if l.RPC == nil {
		var err error
		client, err = ethclient.DialContext(ctx, l.Config.URL)
		if err != nil {
			return err
		}
		defer client.Close()
		l.RPC = client
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err := l.resolveConfig(checkCtx)
	cancel()
	if err != nil {
		return err
	}
	// A database has one configured redemption domain; current signer events contain no chain or sender identity.
	rows, err := l.DB.Rows(ctx, `SELECT stream FROM ingestion_checkpoints WHERE source='chain'`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if r["stream"] != l.Config.Stream() {
			return errors.New("chain domain changed; use a separate accounting database")
		}
	}
	for {
		stepCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		progress, err := l.Step(stepCtx)
		cancel()
		wasReady := l.Ready.Swap(err == nil)
		if err == nil && !wasReady {
			slog.Info("chain ready", "stream", l.Config.Stream())
		}
		if err != nil {
			var fatal fatalError
			if errors.As(err, &fatal) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("chain poll failed; checkpoint retained", "error", err)
		}
		if progress && err == nil {
			continue
		}
		timer := time.NewTimer(l.Config.Poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (l *Listener) resolveConfig(ctx context.Context) error {
	id, err := l.RPC.ChainID(ctx)
	if err != nil {
		return err
	}
	if id == nil || id.Sign() <= 0 {
		return errors.New("RPC returned an invalid chain ID")
	}
	resolvedChainID := id.String()
	if l.Config.ChainID != "" && l.Config.ChainID != resolvedChainID {
		return fmt.Errorf("RPC chain ID mismatch: configured %s, RPC returned %s", l.Config.ChainID, resolvedChainID)
	}
	l.Config.ChainID = resolvedChainID
	if l.Config.Contract == "" {
		if resolvedChainID != arbitrumOneChainID {
			return fmt.Errorf("--ticket-broker required for chain ID %s", resolvedChainID)
		}
		contract, err := l.resolveTicketBroker(ctx, common.HexToAddress(arbitrumOneController))
		if err != nil {
			return err
		}
		l.Config.Contract = strings.ToLower(contract.Hex())
	}
	if l.Config.ChainID == "" || l.Config.Contract == "" {
		return errors.New("chain configuration did not resolve")
	}
	return l.Config.Validate()
}

func (l *Listener) resolveTicketBroker(ctx context.Context, controller common.Address) (common.Address, error) {
	data, err := controllerABI.Pack("getContract", crypto.Keccak256Hash([]byte("TicketBroker")))
	if err != nil {
		return common.Address{}, err
	}
	result, err := l.RPC.CallContract(ctx, ethereum.CallMsg{To: &controller, Data: data}, nil)
	if err != nil {
		return common.Address{}, fmt.Errorf("resolve TicketBroker from Livepeer Controller: %w", err)
	}
	values, err := controllerABI.Unpack("getContract", result)
	if err != nil {
		return common.Address{}, fmt.Errorf("decode TicketBroker from Livepeer Controller: %w", err)
	}
	if len(values) != 1 {
		return common.Address{}, errors.New("Livepeer Controller returned an invalid TicketBroker result")
	}
	contract, ok := values[0].(common.Address)
	if !ok || contract == (common.Address{}) {
		return common.Address{}, errors.New("Livepeer Controller returned an empty TicketBroker address")
	}
	return contract, nil
}

func (l *Listener) header(ctx context.Context, n int64) (*types.Header, error) {
	h, err := l.RPC.HeaderByNumber(ctx, big.NewInt(n))
	if err != nil {
		return nil, err
	}
	if h == nil || h.Number == nil || !h.Number.IsInt64() || h.Number.Int64() != n {
		return nil, errors.New("RPC returned wrong block header")
	}
	return h, nil
}

func (l *Listener) openingSnapshots(ctx context.Context, next int64) (store.Block, []store.EscrowSnapshot, error) {
	contract := common.HexToAddress(l.Config.Contract)
	anchor := store.Block{Number: next - 1}
	if anchor.Number >= 0 {
		header, err := l.header(ctx, anchor.Number)
		if err != nil {
			return store.Block{}, nil, err
		}
		anchor.Hash = strings.ToLower(header.Hash().Hex())
	}
	snapshots := make([]store.EscrowSnapshot, 0, len(l.Config.Senders))
	seen := map[common.Address]bool{}
	for _, senderValue := range l.Config.Senders {
		sender := common.HexToAddress(senderValue)
		if seen[sender] {
			continue
		}
		seen[sender] = true
		deposit, reserve := new(big.Int), new(big.Int)
		if anchor.Number >= 0 {
			data, err := ticketABI.Pack("getSenderInfo", sender)
			if err != nil {
				return store.Block{}, nil, err
			}
			result, err := l.RPC.CallContract(ctx, ethereum.CallMsg{To: &contract, Data: data}, big.NewInt(anchor.Number))
			if err != nil {
				return store.Block{}, nil, fmt.Errorf("historical getSenderInfo at block %d: %w", anchor.Number, err)
			}
			// eth_call to an address before contract deployment returns empty data.
			if len(result) != 0 {
				var decoded struct {
					Sender struct {
						Deposit       *big.Int
						WithdrawRound *big.Int
					}
					Reserve struct {
						FundsRemaining        *big.Int
						ClaimedInCurrentRound *big.Int
					}
				}
				if err := ticketABI.UnpackIntoInterface(&decoded, "getSenderInfo", result); err != nil {
					return store.Block{}, nil, fmt.Errorf("decode historical getSenderInfo: %w", err)
				}
				if decoded.Sender.Deposit == nil || decoded.Reserve.FundsRemaining == nil {
					return store.Block{}, nil, errors.New("historical getSenderInfo returned nil balances")
				}
				deposit.Set(decoded.Sender.Deposit)
				reserve.Set(decoded.Reserve.FundsRemaining)
			}
		}
		snapshots = append(snapshots, store.EscrowSnapshot{
			ChainID: l.Config.ChainID, Contract: strings.ToLower(contract.Hex()), Sender: strings.ToLower(sender.Hex()),
			Deposit: deposit.String(), Reserve: reserve.String(),
		})
	}
	if anchor.Number >= 0 {
		header, err := l.header(ctx, anchor.Number)
		if err != nil {
			return store.Block{}, nil, err
		}
		if strings.ToLower(header.Hash().Hex()) != anchor.Hash {
			return store.Block{}, nil, errors.New("chain changed during opening snapshot")
		}
	}
	return anchor, snapshots, nil
}

func (l *Listener) Step(ctx context.Context) (bool, error) {
	c := l.Config
	stream := c.Stream()
	next, hash, found, err := l.DB.Checkpoint(ctx, "chain", stream)
	if err != nil {
		return false, err
	}
	if !found {
		if c.Start != nil {
			next = *c.Start
		} else {
			head, err := l.latestHeader(ctx)
			if err != nil {
				return false, err
			}
			next = head.Number.Int64()
		}
		anchor, snapshots, err := l.openingSnapshots(ctx, next)
		if err != nil {
			return false, err
		}
		if err := l.DB.BootstrapChain(ctx, stream, anchor, snapshots); err != nil {
			return false, err
		}
		hash = anchor.Hash
		found = true
	}
	if found && next > 0 {
		h, err := l.header(ctx, next-1)
		if err != nil {
			return false, err
		}
		if strings.ToLower(h.Hash().Hex()) != hash {
			blocks, err := l.DB.Blocks(ctx, stream)
			if err != nil {
				return false, err
			}
			for _, b := range blocks {
				if next-1-b.Number > c.Lookback {
					break
				}
				canonical, err := l.header(ctx, b.Number)
				if err != nil {
					return false, err
				}
				if strings.ToLower(canonical.Hash().Hex()) == b.Hash {
					err = l.DB.Rewind(ctx, stream, c.ChainID, strings.ToLower(c.Contract), b)
					if errors.Is(err, store.ErrEscrowSnapshotReorg) {
						return false, fatalError{err}
					}
					return true, err
				}
			}
			return false, fatalError{errors.New("reorg exceeds retained common-ancestor history")}
		}
	}
	head, err := l.latestHeader(ctx)
	if err != nil {
		return false, err
	}
	confirmed := head.Number.Int64() - c.Confirmations
	if confirmed < next {
		return false, nil
	}
	end := next + min(c.BatchSize-1, confirmed-next)
	if end == math.MaxInt64 {
		return false, fatalError{errors.New("block number overflow")}
	}
	blocks := make([]store.Block, 0, end-next+1)
	headers := map[int64]*types.Header{}
	// Bound parallel RPC requests so the default 2,000-block batch does not require
	// 2,000 sequential network round trips. Validate parent links after all reads.
	fetched := make([]*types.Header, end-next+1)
	group, fetchCtx := errgroup.WithContext(ctx)
	group.SetLimit(16)
	for i := range fetched {
		group.Go(func() error {
			h, err := l.header(fetchCtx, next+int64(i))
			fetched[i] = h
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return false, err
	}
	prevHash := hash
	for n := next; n <= end; n++ {
		h := fetched[n-next]
		if prevHash != "" && strings.ToLower(h.ParentHash.Hex()) != prevHash {
			return false, errors.New("chain changed during header fetch")
		}
		prevHash = strings.ToLower(h.Hash().Hex())
		blocks = append(blocks, store.Block{Number: n, Hash: prevHash})
		headers[n] = h
	}
	senderTopics := make([]common.Hash, 0, len(c.Senders))
	allowed := map[common.Address]bool{}
	for _, s := range c.Senders {
		a := common.HexToAddress(s)
		if allowed[a] {
			continue
		}
		allowed[a] = true
		senderTopics = append(senderTopics, common.BytesToHash(a.Bytes()))
	}
	logs, err := l.RPC.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(next), ToBlock: big.NewInt(end), Addresses: []common.Address{common.HexToAddress(c.Contract)}, Topics: [][]common.Hash{EventIDs, senderTopics}})
	if err != nil {
		return false, err
	}
	logs, err = orderedUniqueLogs(logs)
	if err != nil {
		return false, err
	}
	knownTopics := map[common.Hash]bool{}
	for _, id := range EventIDs {
		knownTopics[id] = true
	}
	var events []store.EscrowEvent
	for _, log := range logs {
		if log.Removed || log.BlockNumber > math.MaxInt64 || uint64(log.Index) > math.MaxInt64 {
			return false, errors.New("invalid or removed RPC log")
		}
		h := headers[int64(log.BlockNumber)]
		if h == nil || h.Hash() != log.BlockHash || log.Address != common.HexToAddress(c.Contract) || len(log.Topics) < 2 || !knownTopics[log.Topics[0]] || !allowed[common.BytesToAddress(log.Topics[1].Bytes())] {
			return false, errors.New("RPC log does not match requested domain or canonical block")
		}
		if h.Time > math.MaxInt64/1000 {
			return false, errors.New("block timestamp overflows milliseconds")
		}
		event, err := Decode(log, c.ChainID, int64(h.Time)*1000)
		if err != nil {
			return false, err
		}
		events = append(events, event)
	}
	events, err = correlateRedemptions(events)
	if err != nil {
		return false, err
	}
	last, err := l.header(ctx, end)
	if err != nil {
		return false, err
	}
	if last.Hash() != headers[end].Hash() {
		return false, errors.New("chain changed during log fetch")
	}
	return true, l.DB.ApplyChain(ctx, stream, blocks, events, c.Lookback)
}

func (l *Listener) latestHeader(ctx context.Context) (*types.Header, error) {
	head, err := l.RPC.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}
	if head == nil || head.Number == nil || !head.Number.IsInt64() || head.Number.Sign() < 0 || head.Number.Int64() == math.MaxInt64 {
		return nil, errors.New("invalid head")
	}
	return head, nil
}

func orderedUniqueLogs(logs []types.Log) ([]types.Log, error) {
	sort.SliceStable(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		return logs[i].Index < logs[j].Index
	})
	out := logs[:0]
	for _, log := range logs {
		if len(out) != 0 {
			previous := out[len(out)-1]
			if previous.BlockHash == log.BlockHash && previous.TxHash == log.TxHash && previous.Index == log.Index {
				if previous.Address != log.Address || previous.BlockNumber != log.BlockNumber || previous.Removed != log.Removed || !slices.Equal(previous.Topics, log.Topics) || !bytes.Equal(previous.Data, log.Data) {
					return nil, errors.New("RPC returned conflicting logs with the same identity")
				}
				continue
			}
		}
		out = append(out, log)
	}
	return out, nil
}

func eventBase(log types.Log, chainID, kind string, timestamp int64) store.EscrowEvent {
	return store.EscrowEvent{
		Type: kind, ChainID: chainID, Contract: strings.ToLower(log.Address.Hex()), TxHash: strings.ToLower(log.TxHash.Hex()),
		BlockHash: strings.ToLower(log.BlockHash.Hex()), Sender: strings.ToLower(common.BytesToAddress(log.Topics[1].Bytes()).Hex()),
		Amount: "0", DepositAmount: "0", ReserveAmount: "0", LogIndex: int64(log.Index), BlockNumber: int64(log.BlockNumber), Timestamp: timestamp,
	}
}

func Decode(log types.Log, chainID string, timestamp int64) (store.EscrowEvent, error) {
	if len(log.Topics) < 2 {
		return store.EscrowEvent{}, errors.New("invalid event topics")
	}
	switch log.Topics[0] {
	case DepositFundedID, ReserveFundedID:
		if len(log.Topics) != 2 {
			return store.EscrowEvent{}, errors.New("invalid funding event topics")
		}
		var data struct{ Amount *big.Int }
		name, kind := "DepositFunded", store.DepositFunded
		if log.Topics[0] == ReserveFundedID {
			name, kind = "ReserveFunded", store.ReserveFunded
		}
		if err := ticketABI.UnpackIntoInterface(&data, name, log.Data); err != nil || data.Amount == nil {
			if err == nil {
				err = errors.New("nil funding amount")
			}
			return store.EscrowEvent{}, err
		}
		event := eventBase(log, chainID, kind, timestamp)
		event.Amount = data.Amount.String()
		return event, nil
	case ReserveClaimedID:
		if len(log.Topics) != 2 {
			return store.EscrowEvent{}, errors.New("invalid ReserveClaimed topics")
		}
		var data struct {
			Claimant common.Address
			Amount   *big.Int
		}
		if err := ticketABI.UnpackIntoInterface(&data, "ReserveClaimed", log.Data); err != nil || data.Amount == nil {
			if err == nil {
				err = errors.New("nil reserve claim amount")
			}
			return store.EscrowEvent{}, err
		}
		event := eventBase(log, chainID, store.ReserveClaimed, timestamp)
		event.Recipient, event.Amount = strings.ToLower(data.Claimant.Hex()), data.Amount.String()
		return event, nil
	case WithdrawalID:
		if len(log.Topics) != 2 {
			return store.EscrowEvent{}, errors.New("invalid Withdrawal topics")
		}
		var data struct{ Deposit, Reserve *big.Int }
		if err := ticketABI.UnpackIntoInterface(&data, "Withdrawal", log.Data); err != nil || data.Deposit == nil || data.Reserve == nil {
			if err == nil {
				err = errors.New("nil withdrawal amount")
			}
			return store.EscrowEvent{}, err
		}
		event := eventBase(log, chainID, store.Withdrawal, timestamp)
		event.DepositAmount, event.ReserveAmount = data.Deposit.String(), data.Reserve.String()
		return event, nil
	case WinningTicketTransferID:
		if len(log.Topics) != 3 {
			return store.EscrowEvent{}, errors.New("invalid WinningTicketTransfer topics")
		}
		var data struct{ Amount *big.Int }
		if err := ticketABI.UnpackIntoInterface(&data, "WinningTicketTransfer", log.Data); err != nil || data.Amount == nil {
			if err == nil {
				err = errors.New("nil ticket transfer amount")
			}
			return store.EscrowEvent{}, err
		}
		event := eventBase(log, chainID, store.WinningTicketTransfer, timestamp)
		event.Recipient = strings.ToLower(common.BytesToAddress(log.Topics[2].Bytes()).Hex())
		event.Amount = data.Amount.String()
		return event, nil
	case EventID:
		if len(log.Topics) != 3 {
			return store.EscrowEvent{}, errors.New("invalid WinningTicketRedeemed topics")
		}
		var data struct {
			FaceValue, WinProb, SenderNonce, RecipientRand *big.Int
			AuxData                                        []byte
		}
		if err := ticketABI.UnpackIntoInterface(&data, "WinningTicketRedeemed", log.Data); err != nil || data.FaceValue == nil || data.WinProb == nil || data.SenderNonce == nil || data.RecipientRand == nil {
			if err == nil {
				err = errors.New("nil redemption value")
			}
			return store.EscrowEvent{}, err
		}
		event := eventBase(log, chainID, store.WinningTicketRedeemed, timestamp)
		event.Recipient = strings.ToLower(common.BytesToAddress(log.Topics[2].Bytes()).Hex())
		event.Amount = data.FaceValue.String()
		pm := crypto.Keccak256Hash(common.LeftPadBytes(data.RecipientRand.Bytes(), 32))
		event.Settlement = &store.Settlement{
			ChainID: chainID, Contract: event.Contract, TxHash: event.TxHash, BlockHash: event.BlockHash, Sender: event.Sender, Recipient: event.Recipient,
			FaceValue: data.FaceValue.String(), WinProb: data.WinProb.String(), Nonce: data.SenderNonce.String(), Rand: data.RecipientRand.String(),
			PMSessionID: pm.Hex(), AuxData: "0x" + hex.EncodeToString(data.AuxData), LogIndex: event.LogIndex, BlockNumber: event.BlockNumber, Timestamp: timestamp,
		}
		return event, nil
	default:
		return store.EscrowEvent{}, errors.New("unknown event signature")
	}
}

func samePayment(left, right store.EscrowEvent) bool {
	return left.TxHash == right.TxHash && left.Sender == right.Sender && left.Recipient == right.Recipient
}

func correlateRedemptions(events []store.EscrowEvent) ([]store.EscrowEvent, error) {
	var claim, transfer *store.EscrowEvent
	for i := range events {
		event := &events[i]
		switch event.Type {
		case store.ReserveClaimed:
			if claim != nil || transfer != nil {
				return nil, errors.New("out-of-order ReserveClaimed event")
			}
			claim = event
		case store.WinningTicketTransfer:
			if transfer != nil || claim != nil && !samePayment(*claim, *event) {
				return nil, errors.New("WinningTicketTransfer does not match reserve claim")
			}
			transfer = event
		case store.WinningTicketRedeemed:
			if event.Settlement == nil {
				return nil, errors.New("redemption is missing decoded settlement")
			}
			if claim != nil && transfer == nil {
				return nil, errors.New("ReserveClaimed is missing WinningTicketTransfer")
			}
			paid, reservePaid := new(big.Int), new(big.Int)
			if transfer != nil {
				if !samePayment(*transfer, *event) || claim != nil && !samePayment(*claim, *event) {
					return nil, errors.New("redemption does not match preceding payment events")
				}
				var ok bool
				paid, ok = paid.SetString(transfer.Amount, 10)
				if !ok {
					return nil, errors.New("invalid ticket transfer amount")
				}
			}
			if claim != nil {
				var ok bool
				reservePaid, ok = reservePaid.SetString(claim.Amount, 10)
				if !ok {
					return nil, errors.New("invalid reserve claim amount")
				}
			}
			faceValue, ok := new(big.Int).SetString(event.Settlement.FaceValue, 10)
			if !ok || reservePaid.Cmp(paid) > 0 || paid.Cmp(faceValue) > 0 {
				return nil, errors.New("invalid winning ticket payment amounts")
			}
			depositPaid := new(big.Int).Sub(new(big.Int).Set(paid), reservePaid)
			event.DepositAmount, event.ReserveAmount = depositPaid.String(), reservePaid.String()
			event.Settlement.PaidAmount = paid.String()
			event.Settlement.DepositPaid = depositPaid.String()
			event.Settlement.ReservePaid = reservePaid.String()
			claim, transfer = nil, nil
		default:
			if claim != nil || transfer != nil {
				return nil, errors.New("payment events are not contiguous")
			}
		}
	}
	if claim != nil || transfer != nil {
		return nil, errors.New("incomplete redemption event sequence")
	}
	return events, nil
}
