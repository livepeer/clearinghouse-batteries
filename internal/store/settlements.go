package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/livepeer/clearinghouse/internal/units"
)

const (
	DepositFunded         = "deposit_funded"
	ReserveFunded         = "reserve_funded"
	ReserveClaimed        = "reserve_claimed"
	Withdrawal            = "withdrawal"
	WinningTicketTransfer = "winning_ticket_transfer"
	WinningTicketRedeemed = "winning_ticket_redeemed"
)

var ErrEscrowSnapshotReorg = errors.New("reorg crosses opening snapshot anchor")

type Block struct {
	Number int64
	Hash   string
}

type EscrowSnapshot struct {
	ChainID, Contract, Sender, Deposit, Reserve string
}

type Settlement struct {
	ChainID, Contract, TxHash, BlockHash, Sender, Recipient                                     string
	FaceValue, PaidAmount, DepositPaid, ReservePaid, WinProb, Nonce, Rand, PMSessionID, AuxData string
	LogIndex, BlockNumber, Timestamp                                                            int64
}

type EscrowEvent struct {
	Type, ChainID, Contract, TxHash, BlockHash, Sender, Recipient string
	Amount, DepositAmount, ReserveAmount                          string
	LogIndex, BlockNumber, Timestamp                              int64
	Settlement                                                    *Settlement
}

func treasuryOwner(chain, sender string) string { return chain + ":" + sender }
func escrowOwner(chain, contract, sender string) string {
	return chain + ":" + contract + ":" + sender
}

// BootstrapChain establishes the event ledger's opening state immediately before
// the first scanned block. The snapshot and checkpoint commit atomically.
func (s *Store) BootstrapChain(ctx context.Context, stream string, anchor Block, snapshots []EscrowSnapshot) error {
	if len(snapshots) == 0 {
		return errors.New("empty opening snapshot")
	}
	return s.Write(ctx, func(tx *sql.Tx) error {
		if anchor.Number >= 0 {
			if anchor.Hash == "" {
				return errors.New("snapshot anchor hash required")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO chain_blocks VALUES (?,?,?)`, stream, anchor.Number, anchor.Hash); err != nil {
				return err
			}
		} else if anchor.Number != -1 || anchor.Hash != "" {
			return errors.New("invalid pre-genesis snapshot anchor")
		}
		for _, snapshot := range snapshots {
			deposit, err := Amount(snapshot.Deposit)
			if err != nil {
				return err
			}
			reserve, err := Amount(snapshot.Reserve)
			if err != nil {
				return err
			}
			id := ID()
			if _, err := tx.ExecContext(ctx, `INSERT INTO escrow_snapshots VALUES (?,?,?,?,?,?,?,?,?,?)`, id, stream, snapshot.ChainID, snapshot.Contract, snapshot.Sender, anchor.Number, anchor.Hash, deposit.String(), reserve.String(), time.Now().UnixMilli()); err != nil {
				return err
			}
			treasury, escrow := treasuryOwner(snapshot.ChainID, snapshot.Sender), escrowOwner(snapshot.ChainID, snapshot.Contract, snapshot.Sender)
			if err := Transfer(ctx, tx, "snapshot:"+id+":deposit", "opening deposit", "escrow_snapshot", id, "treasury_cash", treasury, "escrow_deposit", escrow, deposit); err != nil {
				return err
			}
			if err := Transfer(ctx, tx, "snapshot:"+id+":reserve", "opening reserve", "escrow_snapshot", id, "treasury_cash", treasury, "escrow_reserve", escrow, reserve); err != nil {
				return err
			}
		}
		return SetCheckpoint(ctx, tx, "chain", stream, anchor.Number+1, anchor.Hash)
	})
}

func (s *Store) ApplyChain(ctx context.Context, stream string, blocks []Block, events []EscrowEvent, lookback int64) error {
	if len(blocks) == 0 {
		return errors.New("empty block batch")
	}
	return s.Write(ctx, func(tx *sql.Tx) error {
		for _, block := range blocks {
			if _, err := tx.ExecContext(ctx, `INSERT INTO chain_blocks VALUES (?,?,?) ON CONFLICT(stream,number) DO UPDATE SET hash=excluded.hash`, stream, block.Number, block.Hash); err != nil {
				return err
			}
		}
		for _, event := range events {
			if err := applyEscrowEvent(ctx, tx, event); err != nil {
				return err
			}
		}
		last := blocks[len(blocks)-1]
		if err := SetCheckpoint(ctx, tx, "chain", stream, last.Number+1, last.Hash); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM chain_blocks WHERE stream=? AND number<?`, stream, last.Number-lookback)
		return err
	})
}

func applyEscrowEvent(ctx context.Context, tx *sql.Tx, event EscrowEvent) error {
	var id, status, kind, sender, amount, depositAmount, reserveAmount string
	var recipient sql.NullString
	var generation int64
	var blockNumber int64
	err := tx.QueryRowContext(ctx, `SELECT id,status,generation,event_type,sender,recipient,amount_wei,deposit_amount_wei,reserve_amount_wei,block_number FROM ticket_broker_events WHERE chain_id=? AND contract_address=? AND block_hash=? AND tx_hash=? AND log_index=?`, event.ChainID, event.Contract, event.BlockHash, event.TxHash, event.LogIndex).Scan(&id, &status, &generation, &kind, &sender, &recipient, &amount, &depositAmount, &reserveAmount, &blockNumber)
	if err == nil {
		storedRecipient := ""
		if recipient.Valid {
			storedRecipient = recipient.String
		}
		if kind != event.Type || sender != event.Sender || storedRecipient != event.Recipient || amount != event.Amount || depositAmount != event.DepositAmount || reserveAmount != event.ReserveAmount || blockNumber != event.BlockNumber {
			return errors.New("chain log identity reused with different event data")
		}
		if status == "canonical" {
			if event.Type == WinningTicketRedeemed {
				if event.Settlement == nil {
					return errors.New("redemption is missing settlement data")
				}
				return applySettlement(ctx, tx, *event.Settlement)
			}
			return nil
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		id = ID()
		var recipient any
		if event.Recipient != "" {
			recipient = event.Recipient
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO ticket_broker_events (id,chain_id,contract_address,tx_hash,log_index,block_number,block_hash,event_type,sender,recipient,amount_wei,deposit_amount_wei,reserve_amount_wei,status,created_at_ms,occurred_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'canonical',?,?)`, id, event.ChainID, event.Contract, event.TxHash, event.LogIndex, event.BlockNumber, event.BlockHash, event.Type, event.Sender, recipient, event.Amount, event.DepositAmount, event.ReserveAmount, time.Now().UnixMilli(), event.Timestamp)
	} else if err == nil {
		generation++
		_, err = tx.ExecContext(ctx, `UPDATE ticket_broker_events SET status='canonical',generation=?,occurred_at_ms=? WHERE id=?`, generation, event.Timestamp, id)
	}
	if err != nil {
		return err
	}
	treasury, escrow := treasuryOwner(event.ChainID, event.Sender), escrowOwner(event.ChainID, event.Contract, event.Sender)
	key := fmt.Sprintf("event:%s:%d", id, generation)
	switch event.Type {
	case DepositFunded:
		n, err := Amount(event.Amount)
		if err != nil {
			return err
		}
		return Transfer(ctx, tx, key, "deposit funded", "escrow_event", id, "treasury_cash", treasury, "escrow_deposit", escrow, n)
	case ReserveFunded:
		n, err := Amount(event.Amount)
		if err != nil {
			return err
		}
		return Transfer(ctx, tx, key, "reserve funded", "escrow_event", id, "treasury_cash", treasury, "escrow_reserve", escrow, n)
	case Withdrawal:
		deposit, err := Amount(event.DepositAmount)
		if err != nil {
			return err
		}
		reserve, err := Amount(event.ReserveAmount)
		if err != nil {
			return err
		}
		if err := requireExactBalance(ctx, tx, "escrow_deposit", escrow, deposit); err != nil {
			return fmt.Errorf("withdrawal deposit: %w", err)
		}
		if err := requireExactBalance(ctx, tx, "escrow_reserve", escrow, reserve); err != nil {
			return fmt.Errorf("withdrawal reserve: %w", err)
		}
		if err := Transfer(ctx, tx, key+":deposit", "deposit withdrawn", "escrow_event", id, "escrow_deposit", escrow, "treasury_cash", treasury, deposit); err != nil {
			return err
		}
		return Transfer(ctx, tx, key+":reserve", "reserve withdrawn", "escrow_event", id, "escrow_reserve", escrow, "treasury_cash", treasury, reserve)
	case WinningTicketRedeemed:
		if event.Settlement == nil {
			return errors.New("redemption is missing settlement data")
		}
		return applySettlement(ctx, tx, *event.Settlement)
	case ReserveClaimed, WinningTicketTransfer:
		return nil
	default:
		return fmt.Errorf("unknown chain event %q", event.Type)
	}
}

func requireBalance(ctx context.Context, tx *sql.Tx, account, owner string, amount *big.Int) error {
	balance, err := Balance(ctx, tx, account, owner)
	if err != nil {
		return err
	}
	if balance.Cmp(amount) < 0 {
		return fmt.Errorf("insufficient %s balance: have %s, need %s", account, displayETH(balance), displayETH(amount))
	}
	return nil
}

func requireExactBalance(ctx context.Context, tx *sql.Tx, account, owner string, amount *big.Int) error {
	balance, err := Balance(ctx, tx, account, owner)
	if err != nil {
		return err
	}
	if balance.Cmp(amount) != 0 {
		return fmt.Errorf("%s balance mismatch: have %s, event reports %s", account, displayETH(balance), displayETH(amount))
	}
	return nil
}

func displayETH(amount *big.Int) string {
	value, err := units.WeiToETH(amount.String())
	if err != nil {
		return "invalid ETH amount"
	}
	return value + " ETH"
}

func applySettlement(ctx context.Context, tx *sql.Tx, event Settlement) error {
	faceValue, err := Amount(event.FaceValue)
	if err != nil {
		return err
	}
	paid, err := Amount(event.PaidAmount)
	if err != nil {
		return err
	}
	depositPaid, err := Amount(event.DepositPaid)
	if err != nil {
		return err
	}
	reservePaid, err := Amount(event.ReservePaid)
	if err != nil {
		return err
	}
	if new(big.Int).Add(new(big.Int).Set(depositPaid), reservePaid).Cmp(paid) != 0 || paid.Cmp(faceValue) > 0 {
		return errors.New("invalid redemption payment split")
	}
	var id, status string
	var generation int64
	var stored Settlement
	err = tx.QueryRowContext(ctx, `SELECT id,status,generation,block_number,sender,recipient,face_value_wei,paid_amount_wei,deposit_paid_wei,reserve_paid_wei,win_probability,sender_nonce,recipient_rand,pm_session_id,aux_data FROM settlements WHERE chain_id=? AND contract_address=? AND block_hash=? AND tx_hash=? AND log_index=?`, event.ChainID, event.Contract, event.BlockHash, event.TxHash, event.LogIndex).Scan(&id, &status, &generation, &stored.BlockNumber, &stored.Sender, &stored.Recipient, &stored.FaceValue, &stored.PaidAmount, &stored.DepositPaid, &stored.ReservePaid, &stored.WinProb, &stored.Nonce, &stored.Rand, &stored.PMSessionID, &stored.AuxData)
	if err == nil {
		if stored.BlockNumber != event.BlockNumber || stored.Sender != event.Sender || stored.Recipient != event.Recipient || stored.FaceValue != event.FaceValue || stored.PaidAmount != event.PaidAmount || stored.DepositPaid != event.DepositPaid || stored.ReservePaid != event.ReservePaid || stored.WinProb != event.WinProb || stored.Nonce != event.Nonce || stored.Rand != event.Rand || stored.PMSessionID != event.PMSessionID || stored.AuxData != event.AuxData {
			return errors.New("settlement log identity reused with different data")
		}
		if status == "settled" {
			return nil
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		id = ID()
		_, err = tx.ExecContext(ctx, `INSERT INTO settlements (id,chain_id,contract_address,tx_hash,log_index,block_number,block_hash,sender,recipient,face_value_wei,paid_amount_wei,deposit_paid_wei,reserve_paid_wei,win_probability,sender_nonce,recipient_rand,pm_session_id,aux_data,match_status,status,created_at_ms,settled_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'unmatched','settled',?,?)`, id, event.ChainID, event.Contract, event.TxHash, event.LogIndex, event.BlockNumber, event.BlockHash, event.Sender, event.Recipient, event.FaceValue, event.PaidAmount, event.DepositPaid, event.ReservePaid, event.WinProb, event.Nonce, event.Rand, event.PMSessionID, event.AuxData, time.Now().UnixMilli(), event.Timestamp)
	} else if err == nil {
		generation++
		_, err = tx.ExecContext(ctx, `UPDATE settlements SET status='settled',generation=?,settled_at_ms=? WHERE id=?`, generation, event.Timestamp, id)
	}
	if err != nil {
		return err
	}
	escrow := escrowOwner(event.ChainID, event.Contract, event.Sender)
	spend := treasuryOwner(event.ChainID, event.Sender)
	depositBalance, err := Balance(ctx, tx, "escrow_deposit", escrow)
	if err != nil {
		return err
	}
	expectedDeposit := new(big.Int).Set(faceValue)
	if expectedDeposit.Cmp(depositBalance) > 0 {
		expectedDeposit.Set(depositBalance)
	}
	if depositPaid.Cmp(expectedDeposit) != 0 {
		return fmt.Errorf("redemption deposit payment mismatch: have %s, expected %s", displayETH(depositPaid), displayETH(expectedDeposit))
	}
	if err := requireBalance(ctx, tx, "escrow_reserve", escrow, reservePaid); err != nil {
		return fmt.Errorf("redemption reserve: %w", err)
	}
	key := fmt.Sprintf("settle:%s:%d", id, generation)
	if err := Transfer(ctx, tx, key+":deposit", "winning ticket deposit payment", "settlement", id, "escrow_deposit", escrow, "treasury_settled_spend", spend, depositPaid); err != nil {
		return err
	}
	if err := Transfer(ctx, tx, key+":reserve", "winning ticket reserve payment", "settlement", id, "escrow_reserve", escrow, "treasury_settled_spend", spend, reservePaid); err != nil {
		return err
	}
	return rematch(ctx, tx, event.PMSessionID, event.Recipient)
}

// Rewind is called only after RPC headers prove a common ancestor. No usage postings are reversed.
func (s *Store) Rewind(ctx context.Context, stream, chain, contract string, ancestor Block) error {
	return s.Write(ctx, func(tx *sql.Tx) error {
		var anchor int64
		if err := tx.QueryRowContext(ctx, `SELECT min(block_number) FROM escrow_snapshots WHERE stream=?`, stream).Scan(&anchor); err != nil {
			return err
		}
		if ancestor.Number < anchor {
			return ErrEscrowSnapshotReorg
		}
		type settlementReversal struct {
			id, sender, deposit, reserve string
			generation                   int64
		}
		rows, err := tx.QueryContext(ctx, `SELECT id,sender,deposit_paid_wei,reserve_paid_wei,generation FROM settlements WHERE chain_id=? AND contract_address=? AND status='settled' AND block_number>? ORDER BY block_number DESC,log_index DESC`, chain, contract, ancestor.Number)
		if err != nil {
			return err
		}
		var settlements []settlementReversal
		for rows.Next() {
			var reversal settlementReversal
			if err := rows.Scan(&reversal.id, &reversal.sender, &reversal.deposit, &reversal.reserve, &reversal.generation); err != nil {
				rows.Close()
				return err
			}
			settlements = append(settlements, reversal)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, reversal := range settlements {
			deposit, err := Amount(reversal.deposit)
			if err != nil {
				return err
			}
			reserve, err := Amount(reversal.reserve)
			if err != nil {
				return err
			}
			escrow, spend := escrowOwner(chain, contract, reversal.sender), treasuryOwner(chain, reversal.sender)
			key := fmt.Sprintf("orphan:settle:%s:%d", reversal.id, reversal.generation)
			if err := Transfer(ctx, tx, key+":deposit", "orphaned deposit payment reversal", "settlement", reversal.id, "treasury_settled_spend", spend, "escrow_deposit", escrow, deposit); err != nil {
				return err
			}
			if err := Transfer(ctx, tx, key+":reserve", "orphaned reserve payment reversal", "settlement", reversal.id, "treasury_settled_spend", spend, "escrow_reserve", escrow, reserve); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE settlements SET status='orphaned' WHERE id=?`, reversal.id); err != nil {
				return err
			}
		}

		type eventReversal struct {
			id, kind, sender, amount, deposit, reserve string
			generation                                 int64
		}
		rows, err = tx.QueryContext(ctx, `SELECT id,event_type,sender,amount_wei,deposit_amount_wei,reserve_amount_wei,generation FROM ticket_broker_events WHERE chain_id=? AND contract_address=? AND status='canonical' AND block_number>? ORDER BY block_number DESC,log_index DESC`, chain, contract, ancestor.Number)
		if err != nil {
			return err
		}
		var events []eventReversal
		for rows.Next() {
			var reversal eventReversal
			if err := rows.Scan(&reversal.id, &reversal.kind, &reversal.sender, &reversal.amount, &reversal.deposit, &reversal.reserve, &reversal.generation); err != nil {
				rows.Close()
				return err
			}
			events = append(events, reversal)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, reversal := range events {
			treasury, escrow := treasuryOwner(chain, reversal.sender), escrowOwner(chain, contract, reversal.sender)
			key := fmt.Sprintf("orphan:event:%s:%d", reversal.id, reversal.generation)
			switch reversal.kind {
			case DepositFunded, ReserveFunded:
				amount, err := Amount(reversal.amount)
				if err != nil {
					return err
				}
				account := "escrow_deposit"
				if reversal.kind == ReserveFunded {
					account = "escrow_reserve"
				}
				if err := Transfer(ctx, tx, key, "orphaned funding reversal", "escrow_event", reversal.id, account, escrow, "treasury_cash", treasury, amount); err != nil {
					return err
				}
			case Withdrawal:
				deposit, err := Amount(reversal.deposit)
				if err != nil {
					return err
				}
				reserve, err := Amount(reversal.reserve)
				if err != nil {
					return err
				}
				if err := Transfer(ctx, tx, key+":deposit", "orphaned deposit withdrawal reversal", "escrow_event", reversal.id, "treasury_cash", treasury, "escrow_deposit", escrow, deposit); err != nil {
					return err
				}
				if err := Transfer(ctx, tx, key+":reserve", "orphaned reserve withdrawal reversal", "escrow_event", reversal.id, "treasury_cash", treasury, "escrow_reserve", escrow, reserve); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, `UPDATE ticket_broker_events SET status='orphaned' WHERE id=?`, reversal.id); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM chain_blocks WHERE stream=? AND number>?`, stream, ancestor.Number); err != nil {
			return err
		}
		return SetCheckpoint(ctx, tx, "chain", stream, ancestor.Number+1, ancestor.Hash)
	})
}

func (s *Store) Blocks(ctx context.Context, stream string) ([]Block, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT number,hash FROM chain_blocks WHERE stream=? ORDER BY number DESC`, stream)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Block{}
	for rows.Next() {
		var block Block
		if err := rows.Scan(&block.Number, &block.Hash); err != nil {
			return nil, err
		}
		out = append(out, block)
	}
	return out, rows.Err()
}

func (s *Store) EscrowReport(ctx context.Context) ([]map[string]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT stream,chain_id,contract_address,sender,block_number FROM escrow_snapshots ORDER BY chain_id,contract_address,sender`)
	if err != nil {
		return nil, err
	}
	type snapshot struct {
		stream, chain, contract, sender string
		block                           int64
	}
	var snapshots []snapshot
	for rows.Next() {
		var item snapshot
		if err := rows.Scan(&item.stream, &item.chain, &item.contract, &item.sender, &item.block); err != nil {
			rows.Close()
			return nil, err
		}
		snapshots = append(snapshots, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]map[string]string, 0, len(snapshots))
	for _, item := range snapshots {
		owner := escrowOwner(item.chain, item.contract, item.sender)
		deposit, err := Balance(ctx, s.DB, "escrow_deposit", owner)
		if err != nil {
			return nil, err
		}
		reserve, err := Balance(ctx, s.DB, "escrow_reserve", owner)
		if err != nil {
			return nil, err
		}
		next, _, found, err := s.Checkpoint(ctx, "chain", item.stream)
		if err != nil {
			return nil, err
		}
		confirmed := item.block
		if found {
			confirmed = next - 1
		}
		total := new(big.Int).Add(new(big.Int).Set(deposit), reserve)
		out = append(out, map[string]string{
			"chain_id": item.chain, "contract_address": item.contract, "sender": item.sender,
			"confirmed_through_block": fmt.Sprint(confirmed), "deposit_balance_wei": deposit.String(),
			"reserve_balance_wei": reserve.String(), "total_balance_wei": total.String(),
		})
	}
	return out, nil
}

func (s *Store) EscrowActivity(ctx context.Context) ([]map[string]any, error) {
	return s.Rows(ctx, `
SELECT id,event_type,chain_id,contract_address,sender,recipient,tx_hash,log_index,block_number,block_hash,
       amount_wei,deposit_amount_wei,reserve_amount_wei,status,generation,created_at_ms,occurred_at_ms FROM (
 SELECT id,'opening_snapshot' AS event_type,chain_id,contract_address,sender,NULL AS recipient,NULL AS tx_hash,NULL AS log_index,
        block_number,block_hash,'0' AS amount_wei,deposit_wei AS deposit_amount_wei,reserve_wei AS reserve_amount_wei,
        'canonical' AS status,0 AS generation,created_at_ms,created_at_ms AS occurred_at_ms,-1 AS event_order
 FROM escrow_snapshots
 UNION ALL
 SELECT id,event_type,chain_id,contract_address,sender,recipient,tx_hash,log_index,block_number,block_hash,amount_wei,
        deposit_amount_wei,reserve_amount_wei,status,generation,created_at_ms,occurred_at_ms,log_index AS event_order
 FROM ticket_broker_events
) ORDER BY block_number,event_order,id`)
}
