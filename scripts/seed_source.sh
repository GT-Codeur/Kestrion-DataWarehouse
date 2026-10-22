#!/usr/bin/env bash
# Load schema.sql and the four seed CSVs into the source PostgreSQL database, then verify the row counts.
# Required env: PGHOST (database endpoint), PGPASSWORD. Optional: PGUSER (postgres), PGDATABASE (kestriondb).
set -euo pipefail

: "${PGHOST:?set PGHOST to the RDS endpoint}"
: "${PGPASSWORD:?set PGPASSWORD to the master password}"
export PGUSER="${PGUSER:-postgres}" PGDATABASE="${PGDATABASE:-kestriondb}" PGSSLMODE=require

SRC="$(cd "$(dirname "$0")/../source" && pwd)"
cd "$SRC"

echo "Waiting for the database to accept connections..."
for i in $(seq 1 30); do
  if psql -Atc 'select 1' >/dev/null 2>&1; then break; fi
  [ "$i" -eq 30 ] && { echo "Database not reachable (security group, public access or password?)" >&2; exit 1; }
  sleep 10
done

echo "Creating schema..."
psql -v ON_ERROR_STOP=1 -q -f schema.sql

echo "Loading CSVs in foreign-key order..."
psql -v ON_ERROR_STOP=1 -q <<'SQL'
\copy customers   FROM 'customers.csv'   CSV HEADER
\copy inventory   FROM 'inventory.csv'   CSV HEADER
\copy orders      FROM 'orders.csv'      CSV HEADER
\copy order_lines FROM 'order_lines.csv' CSV HEADER
SQL

counts="$(psql -At -F ' ' -c "select (select count(*) from customers), (select count(*) from inventory), (select count(*) from orders), (select count(*) from order_lines)")"
echo "customers inventory orders order_lines = $counts"
if [ "$counts" != "8 120 1500 4480" ]; then
  echo "Unexpected row counts (expected: 8 120 1500 4480)" >&2
  exit 1
fi
echo "Seed OK"
