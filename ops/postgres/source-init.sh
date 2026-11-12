#!/usr/bin/env bash
# The demo source: a help-centre articles table, set up the way Driftless's preflight expects.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v pw="$SOURCE_DRIFTLESS_PASSWORD" <<'SQL'
CREATE TABLE articles (
  id         bigint PRIMARY KEY,
  title      text NOT NULL,
  body       text,
  category   text,
  tenant_id  text,
  acl        text[] NOT NULL DEFAULT '{public}',
  updated_at timestamptz NOT NULL DEFAULT now()
);
-- deletes and TOASTed bodies need the full old row in the WAL
ALTER TABLE articles REPLICA IDENTITY FULL;

-- Debezium's signal table: Driftless writes snapshot requests here
CREATE TABLE driftless_signal (id varchar(64) PRIMARY KEY, type varchar(32) NOT NULL, data text);

CREATE PUBLICATION driftless_articles FOR TABLE articles, driftless_signal;

-- the role Driftless and Debezium use: read articles, write signals, stream changes
CREATE ROLE driftless LOGIN REPLICATION PASSWORD :'pw';
GRANT SELECT ON articles TO driftless;
GRANT SELECT, INSERT ON driftless_signal TO driftless;
SQL
