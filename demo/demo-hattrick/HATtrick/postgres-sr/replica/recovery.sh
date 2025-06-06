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
  -c shared_buffers=${SHARED_BUFFERS} \
  -c work_mem=${WORK_MEM} \
  -c maintenance_work_mem=${MAINTENANCE_WORK_MEM} \
  -c wal_buffers="${WAL_BUFFERS}" \
  -c max_standby_streaming_delay="${MAX_STANDBY_STREAMING_DELAY}" \
  -c max_parallel_workers=4 \
  -c max_parallel_workers_per_gather=2 \
  -c effective_cache_size="${EFFECTIVE_CACHE_SIZE}"
