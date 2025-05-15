#!/bin/bash

scale_factor=$1
epoch_size=$2
max_operator_parallelism=$3
query_engine=${4:-false}
threads_per_worker=1
minimum_amount_of_workers=1

threaded_scale_factor=$(( scale_factor / threads_per_worker ))
threaded_scale_factor=$(( "$minimum_amount_of_workers" > "$threaded_scale_factor" ? "$minimum_amount_of_workers" : "$threaded_scale_factor" ))

docker system prune -f --volumes
# START NEW DEPLOYMENT
docker compose -f docker-compose-kafka.yml up -d
sleep 5
docker compose -f docker-compose-minio.yml up -d
sleep 10
docker compose build --build-arg epoch_size="$epoch_size" --build-arg 'max_operator_parallelism'="$max_operator_parallelism"
QUERY_ENGINE="$query_engine" docker compose up --scale worker="$threaded_scale_factor" -d
sleep 5
if [ "$query_engine" = true ]; then
  docker compose -f docker-compose-query-engine.yml build
  docker compose -f docker-compose-query-engine.yml up -d
fi
sleep 5