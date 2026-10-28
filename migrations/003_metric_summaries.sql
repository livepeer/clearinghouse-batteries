-- UP
-- Amounts remain arbitrary-width canonical decimal strings, including signed balances.
CREATE TABLE metric_amounts (
 name TEXT NOT NULL CHECK(name IN ('grant_unallocated','allocation_available','allocation_spent','allocation_overdraw','settlement_face_value','settlement_paid','settlement_shortfall','escrow_deposit','escrow_reserve')),
 currency TEXT NOT NULL CHECK(currency IN ('eth','usd')),
 amount_units TEXT NOT NULL CHECK(amount_units='0' OR
  (substr(amount_units,1,1) BETWEEN '1' AND '9' AND amount_units NOT GLOB '*[^0-9]*') OR
  (substr(amount_units,1,1)='-' AND substr(amount_units,2,1) BETWEEN '1' AND '9' AND substr(amount_units,2) NOT GLOB '*[^0-9]*')),
 PRIMARY KEY(name,currency)
) STRICT;
INSERT INTO metric_amounts SELECT name,currency,'0' FROM
 (SELECT 'grant_unallocated' AS name UNION ALL SELECT 'allocation_available' UNION ALL SELECT 'allocation_spent' UNION ALL SELECT 'allocation_overdraw'),
 (SELECT 'eth' AS currency UNION ALL SELECT 'usd');
INSERT INTO metric_amounts VALUES ('settlement_face_value','eth','0'),('settlement_paid','eth','0'),('settlement_shortfall','eth','0'),('escrow_deposit','eth','0'),('escrow_reserve','eth','0');

CREATE TABLE metric_counts (
 kind TEXT NOT NULL CHECK(kind IN ('grants','allocations','quarantines','settlements','allocations_overdrawn','escrow_unfunded_signers','settlements_underpaid')),
 label1 TEXT NOT NULL, label2 TEXT NOT NULL,
 value INTEGER NOT NULL CHECK(value>=0),
 PRIMARY KEY(kind,label1,label2)
) STRICT;
INSERT INTO metric_counts SELECT 'grants',currency,status,count(*) FROM grants GROUP BY currency,status;
INSERT INTO metric_counts SELECT 'allocations',currency,status,count(*) FROM grant_allocations GROUP BY currency,status;
INSERT INTO metric_counts SELECT 'settlements',status,match_status,count(*) FROM settlements GROUP BY status,match_status;
INSERT INTO metric_counts
 SELECT 'quarantines',CASE error
  WHEN '' THEN 'none'
  WHEN 'malformed envelope or missing event id/type/data' THEN 'envelope'
  WHEN 'event id reused with different payload' THEN 'conflicting_id'
  WHEN 'malformed signed-ticket data' THEN 'ticket_data'
  WHEN 'missing auth_id or unknown payment session' THEN 'unknown_session'
  WHEN 'event does not match session binding' THEN 'session_binding'
  WHEN 'invalid fee, ticket count, request, timestamp, or PM session' THEN 'invalid_ticket'
  WHEN 'USD fee unavailable' THEN 'missing_usd_fee'
  ELSE 'other' END AS reason,'',count(*) FROM usage_events WHERE status='quarantined' GROUP BY reason;
INSERT INTO metric_counts SELECT 'settlements_underpaid','','',count(*) FROM settlements WHERE status='settled' AND
 (length(face_value_wei)>length(paid_amount_wei) OR (length(face_value_wei)=length(paid_amount_wei) AND face_value_wei>paid_amount_wei));

-- Only eligible overdrawn allocations and tracked escrow balances participate in extrema.
-- Escrow source IDs preserve snapshot membership, including missing/zero balance rows.
CREATE TABLE metric_balances (
 account_type TEXT NOT NULL CHECK(account_type IN ('allocation_available','escrow_deposit','escrow_reserve')),
 account_id TEXT NOT NULL, currency TEXT NOT NULL CHECK(currency IN ('eth','usd')), source_id TEXT NOT NULL,
 balance_units TEXT NOT NULL CHECK(balance_units='0' OR
  (substr(balance_units,1,1) BETWEEN '1' AND '9' AND balance_units NOT GLOB '*[^0-9]*') OR
  (substr(balance_units,1,1)='-' AND substr(balance_units,2,1) BETWEEN '1' AND '9' AND substr(balance_units,2) NOT GLOB '*[^0-9]*')),
 PRIMARY KEY(account_type,source_id,currency)
) STRICT;
INSERT INTO metric_balances SELECT b.account_type,b.account_id,b.currency,a.id,b.balance_units
 FROM account_balances b JOIN grant_allocations a ON a.id=b.account_id AND a.currency=b.currency
 WHERE b.account_type='allocation_available' AND substr(b.balance_units,1,1)='-';
INSERT INTO metric_balances
 SELECT kinds.account_type,s.chain_id||':'||s.contract_address||':'||s.sender,'eth',s.id,coalesce(b.balance_units,'0')
 FROM escrow_snapshots s CROSS JOIN (SELECT 'escrow_deposit' AS account_type UNION ALL SELECT 'escrow_reserve') kinds
 LEFT JOIN account_balances b ON b.account_type=kinds.account_type AND b.currency='eth' AND b.account_id=s.chain_id||':'||s.contract_address||':'||s.sender;
INSERT INTO metric_counts SELECT 'allocations_overdrawn',currencies.currency,'',count(b.source_id)
 FROM (SELECT 'eth' AS currency UNION ALL SELECT 'usd') currencies LEFT JOIN metric_balances b ON b.account_type='allocation_available' AND b.currency=currencies.currency GROUP BY currencies.currency;
INSERT INTO metric_counts SELECT 'escrow_unfunded_signers',bucket,'',count(b.source_id)
 FROM (SELECT 'deposit' AS bucket UNION ALL SELECT 'reserve') buckets LEFT JOIN metric_balances b ON b.account_type='escrow_'||bucket AND b.balance_units='0' GROUP BY bucket;

CREATE INDEX metric_balances_owner ON metric_balances(account_type,account_id,currency);
CREATE INDEX metric_balances_negative ON metric_balances(account_type,currency,length(balance_units) DESC,balance_units DESC) WHERE substr(balance_units,1,1)='-';
CREATE INDEX metric_balances_nonnegative ON metric_balances(account_type,currency,length(balance_units),balance_units) WHERE substr(balance_units,1,1)<>'-';
CREATE INDEX settlements_oldest_unmatched ON settlements(created_at_ms) WHERE status='settled' AND match_status='unmatched';
CREATE INDEX escrow_snapshots_block ON escrow_snapshots(block_number);
-- DOWN
DROP INDEX escrow_snapshots_block;
DROP INDEX settlements_oldest_unmatched;
DROP TABLE metric_balances;
DROP TABLE metric_counts;
DROP TABLE metric_amounts;
