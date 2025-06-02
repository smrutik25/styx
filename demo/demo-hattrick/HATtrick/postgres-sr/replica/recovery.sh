#!/bin/bash
set -e

echo "Waiting for primary to be ready..."
until pg_isready -h primary -U replicator -d postgres; do
  sleep 2
done

if [ -s "$PGDATA/PG_VERSION" ]; then
  echo "Data directory already initialized, skipping pg_basebackup"
else
  echo "Wiping data directory..."
  rm -rf "$PGDATA"/*

  echo "Cloning from primary..."
  PGPASSWORD='replica_pass' pg_basebackup \
    -D "$PGDATA" \
    -d "host=primary port=5432 user=replicator dbname=postgres" \
    -Fp -Xs -P -R

  echo "Setting permissions..."
  chown -R postgres:postgres "$PGDATA"
fi

echo "Starting Postgres..."
exec docker-entrypoint.sh postgres \
  -c listen_addresses='*' \
  -c hot_standby=on \
  -c shared_buffers=512MB \
  -c work_mem=64MB \
  -c maintenance_work_mem=256MB \
  -c wal_buffers=16MB
