-- UP
CREATE TABLE grants (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, sponsor TEXT NOT NULL DEFAULT '',
 total_wei TEXT NOT NULL CHECK(total_wei <> '' AND total_wei NOT GLOB '*[^0-9]*' AND (total_wei='0' OR substr(total_wei,1,1)<>'0')),
 starts_at_ms INTEGER, ends_at_ms INTEGER,
 status TEXT NOT NULL CHECK(status IN ('draft','active','paused','closed')),
 metadata TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata)), created_at_ms INTEGER NOT NULL,
 CHECK(starts_at_ms IS NULL OR ends_at_ms IS NULL OR ends_at_ms > starts_at_ms)
) STRICT;
CREATE TABLE grant_allocations (
 id TEXT PRIMARY KEY, grant_id TEXT NOT NULL REFERENCES grants(id), name TEXT NOT NULL, beneficiary TEXT NOT NULL DEFAULT '',
 allocated_wei TEXT NOT NULL CHECK(allocated_wei <> '' AND allocated_wei NOT GLOB '*[^0-9]*' AND (allocated_wei='0' OR substr(allocated_wei,1,1)<>'0')),
 starts_at_ms INTEGER, ends_at_ms INTEGER,
 status TEXT NOT NULL CHECK(status IN ('active','paused','exhausted','revoked')),
 metadata TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata)), created_at_ms INTEGER NOT NULL,
 CHECK(starts_at_ms IS NULL OR ends_at_ms IS NULL OR ends_at_ms > starts_at_ms)
) STRICT;
CREATE INDEX allocations_grant ON grant_allocations(grant_id);
CREATE TABLE api_keys (
 id TEXT PRIMARY KEY, allocation_id TEXT NOT NULL REFERENCES grant_allocations(id), name TEXT NOT NULL,
 prefix TEXT NOT NULL, secret_hash BLOB NOT NULL UNIQUE CHECK(length(secret_hash)=32),
 created_at_ms INTEGER NOT NULL, last_used_at_ms INTEGER, revoked_at_ms INTEGER,
 UNIQUE(id, allocation_id)
) STRICT;
CREATE INDEX keys_allocation ON api_keys(allocation_id);
CREATE TABLE payment_sessions (
 id TEXT PRIMARY KEY, allocation_id TEXT NOT NULL REFERENCES grant_allocations(id), api_key_id TEXT NOT NULL,
 state_id TEXT NOT NULL UNIQUE, app TEXT NOT NULL, payment_type TEXT NOT NULL,
 orchestrator TEXT NOT NULL CHECK(length(orchestrator)=42 AND substr(orchestrator,1,2)='0x' AND substr(orchestrator,3) NOT GLOB '*[^0-9a-f]*'),
 status TEXT NOT NULL CHECK(status IN ('active','revoked')), created_at_ms INTEGER NOT NULL, last_seen_at_ms INTEGER NOT NULL,
 FOREIGN KEY(api_key_id,allocation_id) REFERENCES api_keys(id,allocation_id)
) STRICT;
CREATE INDEX sessions_allocation ON payment_sessions(allocation_id);
CREATE TABLE usage_events (
 id TEXT PRIMARY KEY, event_id TEXT UNIQUE, topic TEXT NOT NULL, partition INTEGER NOT NULL CHECK(partition=0), offset INTEGER NOT NULL CHECK(offset>=0),
 raw_payload BLOB NOT NULL, payment_session_id TEXT REFERENCES payment_sessions(id),
 pipeline TEXT, request_id TEXT, started_at_ms INTEGER, ended_at_ms INTEGER, billable_seconds TEXT, pixels TEXT, computed_fee_wei TEXT,
 status TEXT NOT NULL CHECK(status IN ('applied','quarantined','ignored','duplicate')), error TEXT NOT NULL DEFAULT '', created_at_ms INTEGER NOT NULL,
 UNIQUE(topic, partition, offset)
) STRICT;
CREATE INDEX usage_status ON usage_events(status,created_at_ms);
CREATE TABLE signing_authorizations (
 id TEXT PRIMARY KEY, usage_event_id TEXT NOT NULL UNIQUE REFERENCES usage_events(id), payment_session_id TEXT NOT NULL REFERENCES payment_sessions(id),
 request_id TEXT NOT NULL, sequence_number TEXT NOT NULL CHECK(sequence_number<>'' AND sequence_number NOT GLOB '*[^0-9]*' AND (sequence_number='0' OR substr(sequence_number,1,1)<>'0')),
 pm_session_id TEXT NOT NULL CHECK(length(pm_session_id)=66 AND substr(pm_session_id,1,2)='0x' AND substr(pm_session_id,3) NOT GLOB '*[^0-9a-f]*'),
 orchestrator TEXT NOT NULL CHECK(length(orchestrator)=42 AND substr(orchestrator,1,2)='0x' AND substr(orchestrator,3) NOT GLOB '*[^0-9a-f]*'),
 computed_fee_wei TEXT NOT NULL CHECK(computed_fee_wei<>'' AND computed_fee_wei NOT GLOB '*[^0-9]*' AND (computed_fee_wei='0' OR substr(computed_fee_wei,1,1)<>'0')),
 num_tickets INTEGER NOT NULL CHECK(num_tickets>0 AND num_tickets<=100),
 status TEXT NOT NULL DEFAULT 'signed' CHECK(status='signed'), signed_at_ms INTEGER NOT NULL
) STRICT;
CREATE INDEX signing_match ON signing_authorizations(pm_session_id,orchestrator,payment_session_id);
CREATE TABLE ledger_transactions (
 id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, reason TEXT NOT NULL,
 reference_type TEXT NOT NULL, reference_id TEXT NOT NULL, created_at_ms INTEGER NOT NULL
) STRICT;
CREATE TABLE ledger_entries (
 id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL REFERENCES ledger_transactions(id),
 account_type TEXT NOT NULL CHECK(account_type IN ('grant_funding_source','grant_unallocated','allocation_available','allocation_spent','treasury_cash','treasury_settled_spend','escrow_deposit','escrow_reserve')),
 account_id TEXT NOT NULL, direction TEXT NOT NULL CHECK(direction IN ('debit','credit')),
 amount_wei TEXT NOT NULL CHECK(amount_wei<>'' AND amount_wei NOT GLOB '*[^0-9]*' AND substr(amount_wei,1,1) BETWEEN '1' AND '9'),
 currency TEXT NOT NULL DEFAULT 'wei' CHECK(currency='wei'), created_at_ms INTEGER NOT NULL
) STRICT;
CREATE INDEX ledger_account ON ledger_entries(account_type,account_id);
CREATE INDEX ledger_transaction ON ledger_entries(transaction_id);
CREATE TABLE account_balances (
 account_type TEXT NOT NULL CHECK(account_type IN ('grant_funding_source','grant_unallocated','allocation_available','allocation_spent','treasury_cash','treasury_settled_spend','escrow_deposit','escrow_reserve')),
 account_id TEXT NOT NULL,
 balance_wei TEXT NOT NULL CHECK(
  balance_wei='0' OR
  (substr(balance_wei,1,1) BETWEEN '1' AND '9' AND balance_wei NOT GLOB '*[^0-9]*') OR
  (substr(balance_wei,1,1)='-' AND substr(balance_wei,2,1) BETWEEN '1' AND '9' AND substr(balance_wei,2) NOT GLOB '*[^0-9]*')
 ),
 PRIMARY KEY(account_type,account_id)
) STRICT;
CREATE TRIGGER ledger_entries_no_update BEFORE UPDATE ON ledger_entries BEGIN SELECT RAISE(ABORT,'ledger is append-only'); END;
CREATE TRIGGER ledger_entries_no_delete BEFORE DELETE ON ledger_entries BEGIN SELECT RAISE(ABORT,'ledger is append-only'); END;
CREATE TRIGGER ledger_transactions_no_update BEFORE UPDATE ON ledger_transactions BEGIN SELECT RAISE(ABORT,'ledger is append-only'); END;
CREATE TRIGGER ledger_transactions_no_delete BEFORE DELETE ON ledger_transactions BEGIN SELECT RAISE(ABORT,'ledger is append-only'); END;
CREATE TABLE settlements (
 id TEXT PRIMARY KEY, chain_id TEXT NOT NULL CHECK(chain_id<>'' AND chain_id NOT GLOB '*[^0-9]*' AND substr(chain_id,1,1) BETWEEN '1' AND '9'),
 contract_address TEXT NOT NULL CHECK(length(contract_address)=42 AND substr(contract_address,1,2)='0x' AND substr(contract_address,3) NOT GLOB '*[^0-9a-f]*'),
 tx_hash TEXT NOT NULL CHECK(length(tx_hash)=66 AND substr(tx_hash,1,2)='0x' AND substr(tx_hash,3) NOT GLOB '*[^0-9a-f]*'),
 log_index INTEGER NOT NULL CHECK(log_index>=0), block_number INTEGER NOT NULL CHECK(block_number>=0),
 block_hash TEXT NOT NULL CHECK(length(block_hash)=66 AND substr(block_hash,1,2)='0x' AND substr(block_hash,3) NOT GLOB '*[^0-9a-f]*'),
 sender TEXT NOT NULL CHECK(length(sender)=42 AND substr(sender,1,2)='0x' AND substr(sender,3) NOT GLOB '*[^0-9a-f]*'),
 recipient TEXT NOT NULL CHECK(length(recipient)=42 AND substr(recipient,1,2)='0x' AND substr(recipient,3) NOT GLOB '*[^0-9a-f]*'),
 face_value_wei TEXT NOT NULL CHECK(face_value_wei<>'' AND face_value_wei NOT GLOB '*[^0-9]*' AND (face_value_wei='0' OR substr(face_value_wei,1,1)<>'0')),
 paid_amount_wei TEXT NOT NULL CHECK(paid_amount_wei<>'' AND paid_amount_wei NOT GLOB '*[^0-9]*' AND (paid_amount_wei='0' OR substr(paid_amount_wei,1,1)<>'0')),
 deposit_paid_wei TEXT NOT NULL CHECK(deposit_paid_wei<>'' AND deposit_paid_wei NOT GLOB '*[^0-9]*' AND (deposit_paid_wei='0' OR substr(deposit_paid_wei,1,1)<>'0')),
 reserve_paid_wei TEXT NOT NULL CHECK(reserve_paid_wei<>'' AND reserve_paid_wei NOT GLOB '*[^0-9]*' AND (reserve_paid_wei='0' OR substr(reserve_paid_wei,1,1)<>'0')),
 win_probability TEXT NOT NULL CHECK(win_probability<>'' AND win_probability NOT GLOB '*[^0-9]*' AND (win_probability='0' OR substr(win_probability,1,1)<>'0')),
 sender_nonce TEXT NOT NULL CHECK(sender_nonce<>'' AND sender_nonce NOT GLOB '*[^0-9]*' AND (sender_nonce='0' OR substr(sender_nonce,1,1)<>'0')),
 recipient_rand TEXT NOT NULL CHECK(recipient_rand<>'' AND recipient_rand NOT GLOB '*[^0-9]*' AND (recipient_rand='0' OR substr(recipient_rand,1,1)<>'0')),
 pm_session_id TEXT NOT NULL CHECK(length(pm_session_id)=66 AND substr(pm_session_id,1,2)='0x' AND substr(pm_session_id,3) NOT GLOB '*[^0-9a-f]*'),
 aux_data TEXT NOT NULL CHECK(substr(aux_data,1,2)='0x' AND length(aux_data)%2=0 AND substr(aux_data,3) NOT GLOB '*[^0-9a-f]*'),
 payment_session_id TEXT REFERENCES payment_sessions(id), authorization_id TEXT REFERENCES signing_authorizations(id),
 match_status TEXT NOT NULL CHECK(match_status IN ('matched','unmatched','ambiguous')),
 status TEXT NOT NULL CHECK(status IN ('settled','orphaned')), generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 created_at_ms INTEGER NOT NULL, settled_at_ms INTEGER NOT NULL,
 UNIQUE(chain_id,contract_address,block_hash,tx_hash,log_index),
 CHECK((match_status='matched' AND payment_session_id IS NOT NULL) OR (match_status<>'matched' AND payment_session_id IS NULL)),
 CHECK(authorization_id IS NULL)
) STRICT;
CREATE INDEX settlements_match ON settlements(pm_session_id,recipient);
CREATE INDEX settlements_block ON settlements(chain_id,contract_address,block_number);
CREATE TABLE escrow_snapshots (
 id TEXT PRIMARY KEY, stream TEXT NOT NULL, chain_id TEXT NOT NULL CHECK(chain_id<>'' AND chain_id NOT GLOB '*[^0-9]*' AND substr(chain_id,1,1) BETWEEN '1' AND '9'),
 contract_address TEXT NOT NULL CHECK(length(contract_address)=42 AND substr(contract_address,1,2)='0x' AND substr(contract_address,3) NOT GLOB '*[^0-9a-f]*'),
 sender TEXT NOT NULL CHECK(length(sender)=42 AND substr(sender,1,2)='0x' AND substr(sender,3) NOT GLOB '*[^0-9a-f]*'),
 block_number INTEGER NOT NULL CHECK(block_number>=-1),
 block_hash TEXT NOT NULL CHECK((block_number=-1 AND block_hash='') OR (block_number>=0 AND length(block_hash)=66 AND substr(block_hash,1,2)='0x' AND substr(block_hash,3) NOT GLOB '*[^0-9a-f]*')),
 deposit_wei TEXT NOT NULL CHECK(deposit_wei<>'' AND deposit_wei NOT GLOB '*[^0-9]*' AND (deposit_wei='0' OR substr(deposit_wei,1,1)<>'0')),
 reserve_wei TEXT NOT NULL CHECK(reserve_wei<>'' AND reserve_wei NOT GLOB '*[^0-9]*' AND (reserve_wei='0' OR substr(reserve_wei,1,1)<>'0')),
 created_at_ms INTEGER NOT NULL, UNIQUE(stream,sender)
) STRICT;
CREATE TABLE ticket_broker_events (
 id TEXT PRIMARY KEY, chain_id TEXT NOT NULL CHECK(chain_id<>'' AND chain_id NOT GLOB '*[^0-9]*' AND substr(chain_id,1,1) BETWEEN '1' AND '9'),
 contract_address TEXT NOT NULL CHECK(length(contract_address)=42 AND substr(contract_address,1,2)='0x' AND substr(contract_address,3) NOT GLOB '*[^0-9a-f]*'),
 tx_hash TEXT NOT NULL CHECK(length(tx_hash)=66 AND substr(tx_hash,1,2)='0x' AND substr(tx_hash,3) NOT GLOB '*[^0-9a-f]*'),
 log_index INTEGER NOT NULL CHECK(log_index>=0), block_number INTEGER NOT NULL CHECK(block_number>=0),
 block_hash TEXT NOT NULL CHECK(length(block_hash)=66 AND substr(block_hash,1,2)='0x' AND substr(block_hash,3) NOT GLOB '*[^0-9a-f]*'),
 event_type TEXT NOT NULL CHECK(event_type IN ('deposit_funded','reserve_funded','reserve_claimed','withdrawal','winning_ticket_transfer','winning_ticket_redeemed')),
 sender TEXT NOT NULL CHECK(length(sender)=42 AND substr(sender,1,2)='0x' AND substr(sender,3) NOT GLOB '*[^0-9a-f]*'),
 recipient TEXT CHECK(recipient IS NULL OR (length(recipient)=42 AND substr(recipient,1,2)='0x' AND substr(recipient,3) NOT GLOB '*[^0-9a-f]*')),
 amount_wei TEXT NOT NULL CHECK(amount_wei<>'' AND amount_wei NOT GLOB '*[^0-9]*' AND (amount_wei='0' OR substr(amount_wei,1,1)<>'0')),
 deposit_amount_wei TEXT NOT NULL CHECK(deposit_amount_wei<>'' AND deposit_amount_wei NOT GLOB '*[^0-9]*' AND (deposit_amount_wei='0' OR substr(deposit_amount_wei,1,1)<>'0')),
 reserve_amount_wei TEXT NOT NULL CHECK(reserve_amount_wei<>'' AND reserve_amount_wei NOT GLOB '*[^0-9]*' AND (reserve_amount_wei='0' OR substr(reserve_amount_wei,1,1)<>'0')),
 status TEXT NOT NULL CHECK(status IN ('canonical','orphaned')), generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 created_at_ms INTEGER NOT NULL, occurred_at_ms INTEGER NOT NULL,
 UNIQUE(chain_id,contract_address,block_hash,tx_hash,log_index)
) STRICT;
CREATE INDEX ticket_broker_events_block ON ticket_broker_events(chain_id,contract_address,block_number);
CREATE INDEX ticket_broker_events_sender ON ticket_broker_events(chain_id,contract_address,sender,block_number);
CREATE TABLE ingestion_checkpoints (
 source TEXT NOT NULL CHECK(source IN ('chain','kafka')), stream TEXT NOT NULL, next_position INTEGER NOT NULL CHECK(next_position>=0),
 block_hash TEXT NOT NULL DEFAULT '' CHECK(block_hash='' OR (length(block_hash)=66 AND substr(block_hash,1,2)='0x' AND substr(block_hash,3) NOT GLOB '*[^0-9a-f]*')),
 PRIMARY KEY(source,stream)
) STRICT;
-- Common-ancestor evidence for blocks with and without redemptions.
CREATE TABLE chain_blocks (
 stream TEXT NOT NULL, number INTEGER NOT NULL CHECK(number>=0),
 hash TEXT NOT NULL CHECK(length(hash)=66 AND substr(hash,1,2)='0x' AND substr(hash,3) NOT GLOB '*[^0-9a-f]*'), PRIMARY KEY(stream,number)
) STRICT;
-- DOWN
DROP TABLE chain_blocks;
DROP TABLE ingestion_checkpoints;
DROP TABLE ticket_broker_events;
DROP TABLE escrow_snapshots;
DROP TABLE settlements;
DROP TABLE account_balances;
DROP TRIGGER ledger_entries_no_update;
DROP TRIGGER ledger_entries_no_delete;
DROP TRIGGER ledger_transactions_no_update;
DROP TRIGGER ledger_transactions_no_delete;
DROP TABLE ledger_entries;
DROP TABLE ledger_transactions;
DROP TABLE signing_authorizations;
DROP TABLE usage_events;
DROP TABLE payment_sessions;
DROP TABLE api_keys;
DROP TABLE grant_allocations;
DROP TABLE grants;
