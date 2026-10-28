package migrations

import (
	"context"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestMetricBackfillCancellationDuringAmountScan(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()
	require.NoError(t, Up(ctx, db))
	require.NoError(t, Down(ctx, db))
	_, err := db.Exec(`INSERT INTO account_balances VALUES ('grant_unallocated','grant','eth','100');
 ALTER TABLE account_balances RENAME TO old_balances;
 CREATE VIEW account_balances AS SELECT account_type,account_id,currency,cancel_metric_amount(balance_units) AS balance_units FROM old_balances;`)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NoError(t, conn.Raw(func(driverConn any) error {
		return driverConn.(*sqlite3.SQLiteConn).RegisterFunc("cancel_metric_amount", func(value string) string {
			cancel()
			return value
		}, false)
	}))
	require.NoError(t, conn.Close())
	require.Error(t, Up(canceled, db))
	require.ErrorIs(t, canceled.Err(), context.Canceled)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='metric_amounts'`).Scan(&count))
	require.Zero(t, count)
	_, err = db.Exec(`DROP VIEW account_balances; ALTER TABLE old_balances RENAME TO account_balances`)
	require.NoError(t, err)
	require.NoError(t, Up(ctx, db))
	var amount string
	require.NoError(t, db.QueryRow(`SELECT amount_units FROM metric_amounts WHERE name='grant_unallocated' AND currency='eth'`).Scan(&amount))
	require.Equal(t, "100", amount)
}
