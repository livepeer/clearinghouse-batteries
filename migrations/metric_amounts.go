package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
)

// backfillMetricAmounts completes 003 inside its schema/metadata transaction.
// Counts and extrema are populated by SQL; only monetary arithmetic needs Go.
func backfillMetricAmounts(ctx context.Context, tx *sql.Tx) error {
	type key struct{ name, currency string }
	totals := map[key]*big.Int{}
	add := func(name, currency string, n *big.Int) {
		k := key{name, currency}
		if totals[k] == nil {
			totals[k] = new(big.Int)
		}
		totals[k].Add(totals[k], n)
	}
	rows, err := tx.QueryContext(ctx, `
 SELECT b.account_type,b.currency,b.balance_units,'' FROM account_balances b LEFT JOIN grant_allocations a ON a.id=b.account_id WHERE b.account_type='grant_unallocated' OR (b.account_type IN ('allocation_available','allocation_spent') AND a.currency=b.currency)
 UNION ALL
 SELECT account_type,currency,balance_units,'' FROM metric_balances WHERE account_type IN ('escrow_deposit','escrow_reserve')
 UNION ALL
 SELECT 'settlement','eth',face_value_wei,paid_amount_wei FROM settlements WHERE status='settled'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, currency, value, paid string
		if err := rows.Scan(&name, &currency, &value, &paid); err != nil {
			return err
		}
		n, ok := new(big.Int).SetString(value, 10)
		if !ok || n.String() != value {
			return fmt.Errorf("invalid metric amount %q", value)
		}
		if name == "settlement" {
			p, ok := new(big.Int).SetString(paid, 10)
			if !ok || p.String() != paid || p.Sign() < 0 || n.Sign() < 0 {
				return fmt.Errorf("invalid settlement metric amounts %q/%q", value, paid)
			}
			add("settlement_face_value", currency, n)
			add("settlement_paid", currency, p)
			if n.Cmp(p) > 0 {
				add("settlement_shortfall", currency, new(big.Int).Sub(n, p))
			}
		} else {
			add(name, currency, n)
			if name == "allocation_available" && n.Sign() < 0 {
				add("allocation_overdraw", currency, new(big.Int).Neg(n))
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for k, n := range totals {
		if _, err := tx.ExecContext(ctx, `UPDATE metric_amounts SET amount_units=? WHERE name=? AND currency=?`, n.String(), k.name, k.currency); err != nil {
			return err
		}
	}
	return nil
}
