package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/livepeer/clearinghouse/internal/units"
)

type Envelope struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}
type SignedTicketEvent struct {
	SessionID       string      `json:"session_id"`
	AuthID          string      `json:"auth_id"`
	App             string      `json:"app"`
	Pipeline        string      `json:"pipeline"`
	RequestID       string      `json:"request_id"`
	Orchestrator    string      `json:"orch_address"`
	PMSessionID     string      `json:"pm_session_id"`
	ComputedFee     string      `json:"computed_fee"`
	Sequence        uint64      `json:"sequence_number"`
	NumTickets      int         `json:"num_tickets"`
	Started         int64       `json:"previous_time_unix"`
	Ended           int64       `json:"current_time_unix"`
	BillableSeconds json.Number `json:"billable_secs"`
	Pixels          json.Number `json:"pixels"`
}

// Ingest commits an audit record, any financial postings, and the next Kafka offset together.
func (s *Store) Ingest(ctx context.Context, topic string, offset int64, raw []byte) error {
	if offset < 0 || offset == math.MaxInt64 {
		return errors.New("invalid Kafka offset")
	}
	var outcome, detail, overdraw string
	err := s.Write(ctx, func(tx *sql.Tx) error {
		var next int64
		err := tx.QueryRowContext(ctx, `SELECT next_position FROM ingestion_checkpoints WHERE source='kafka' AND stream=?`, topic).Scan(&next)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if offset < next {
			return nil
		}
		if offset > next {
			return fmt.Errorf("Kafka offset gap: expected %d, received %d", next, offset)
		}
		uid, now := ID(), time.Now().UnixMilli()
		status, reason := "applied", ""
		var env Envelope
		var ev SignedTicketEvent
		var eventID any
		var sessionID any
		if err := json.Unmarshal(raw, &env); err != nil || env.ID == "" || env.Type == "" || len(env.Data) == 0 {
			status, reason = "quarantined", "malformed envelope or missing event id/type/data"
		} else {
			eventID = env.ID
			var previous []byte
			err := tx.QueryRowContext(ctx, `SELECT raw_payload FROM usage_events WHERE event_id=?`, env.ID).Scan(&previous)
			if err == nil {
				eventID = nil
				if bytes.Equal(previous, raw) {
					status = "duplicate"
				} else {
					status, reason = "quarantined", "event id reused with different payload"
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if status == "applied" {
				if env.Type != "create_signed_ticket" {
					status = "ignored"
				} else if err := json.Unmarshal(env.Data, &ev); err != nil {
					status, reason = "quarantined", "malformed signed-ticket data"
				}
			}
		}
		var allocation string
		if status == "applied" {
			var state, app, orch string
			err := tx.QueryRowContext(ctx, `SELECT allocation_id,state_id,app,orchestrator FROM payment_sessions WHERE id=?`, ev.AuthID).Scan(&allocation, &state, &app, &orch)
			if errors.Is(err, sql.ErrNoRows) {
				status, reason = "quarantined", "missing auth_id or unknown payment session"
			} else if err != nil {
				return err
			} else {
				_, feeErr := Amount(ev.ComputedFee)
				eventOrch, addrErr := Address(ev.Orchestrator)
				if ev.SessionID != state || ev.App != app || eventOrch != orch || addrErr != nil {
					status, reason = "quarantined", "event does not match session binding"
				} else if feeErr != nil || ev.NumTickets < 1 || ev.NumTickets > 100 || ev.RequestID == "" || !validHash(ev.PMSessionID) || ev.Ended <= 0 || ev.Started < 0 {
					status, reason = "quarantined", "invalid fee, ticket count, request, timestamp, or PM session"
				} else {
					sessionID = ev.AuthID
					ev.Orchestrator = eventOrch
					ev.PMSessionID = strings.ToLower(ev.PMSessionID)
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO usage_events VALUES (?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?)`, uid, eventID, topic, offset, raw, sessionID, ev.Pipeline, ev.RequestID, ev.Started, ev.Ended, string(ev.BillableSeconds), string(ev.Pixels), ev.ComputedFee, status, reason, now); err != nil {
			return err
		}
		outcome, detail = status, reason
		if status == "applied" {
			aid := ID()
			if _, err := tx.ExecContext(ctx, `INSERT INTO signing_authorizations VALUES (?,?,?,?,?,?,?,?,?,'signed',?)`, aid, uid, ev.AuthID, ev.RequestID, fmt.Sprint(ev.Sequence), ev.PMSessionID, ev.Orchestrator, ev.ComputedFee, ev.NumTickets, ev.Ended); err != nil {
				return err
			}
			n, _ := Amount(ev.ComputedFee)
			if err := Transfer(ctx, tx, "usage:"+env.ID, "signed usage", "signing_authorization", aid, "allocation_available", allocation, "allocation_spent", allocation, n); err != nil {
				return err
			}
			balance, err := Balance(ctx, tx, "allocation_available", allocation)
			if err != nil {
				return err
			}
			if balance.Sign() <= 0 {
				if balance.Sign() < 0 {
					overdraw = balance.String()
				}
				if _, err := tx.ExecContext(ctx, `UPDATE grant_allocations SET status='exhausted' WHERE id=? AND status='active'`, allocation); err != nil {
					return err
				}
			}
			if err := rematch(ctx, tx, ev.PMSessionID, ev.Orchestrator); err != nil {
				return err
			}
		}
		return SetCheckpoint(ctx, tx, "kafka", topic, offset+1, "")
	})
	if err == nil {
		if outcome == "quarantined" {
			slog.Warn("usage quarantined", "topic", topic, "offset", offset, "reason", detail)
		}
		if overdraw != "" {
			available, conversionErr := units.WeiToETH(overdraw)
			if conversionErr != nil {
				slog.Error("allocation overdraw amount invalid", "topic", topic, "offset", offset, "error", conversionErr)
			} else {
				slog.Warn("allocation overdraw recorded", "topic", topic, "offset", offset, "available_eth", available)
			}
		}
	}
	return err
}

func validHash(s string) bool {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") {
		return false
	}
	for _, c := range strings.ToLower(s[2:]) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func rematch(ctx context.Context, tx *sql.Tx, pm, orch string) error {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT payment_session_id FROM signing_authorizations WHERE pm_session_id=? AND orchestrator=?`, pm, orch)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	status := "unmatched"
	var id any
	if len(ids) == 1 {
		status, id = "matched", ids[0]
	} else if len(ids) > 1 {
		status = "ambiguous"
	}
	_, err = tx.ExecContext(ctx, `UPDATE settlements SET payment_session_id=?,match_status=? WHERE pm_session_id=? AND recipient=?`, id, status, pm, orch)
	return err
}
