-- UP
CREATE TABLE management_idempotency (
 grant_id TEXT NOT NULL REFERENCES grants(id),
 idempotency_key TEXT NOT NULL CHECK(length(CAST(idempotency_key AS BLOB)) BETWEEN 1 AND 256),
 fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),
 response_status INTEGER NOT NULL CHECK(response_status BETWEEN 200 AND 299),
 response_json TEXT NOT NULL CHECK(json_valid(response_json)),
 created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(grant_id,idempotency_key)
) STRICT;

-- DOWN
DROP TABLE management_idempotency;
