-- UP
CREATE TABLE kafka_lag (
 topic TEXT PRIMARY KEY,
 checked_at_ms INTEGER NOT NULL CHECK(checked_at_ms>=0),
 covered_through_ms INTEGER NOT NULL CHECK(covered_through_ms>=0),
 available INTEGER NOT NULL CHECK(available IN (0,1))
) STRICT;
-- DOWN
DROP TABLE kafka_lag;
