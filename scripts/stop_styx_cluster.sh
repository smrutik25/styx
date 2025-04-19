#!/bin/bash

docker compose logs worker | sort -s -t '|' -k1,1 > worker-logs.log
docker compose logs coordinator > coordinator-logs.log
if docker compose ps --services | grep 'query-engine'; then
  docker compose -f docker-compose-query-engine.yml logs query-engine > query-engine-logs.log
fi

# DELETE PREVIOUS DEPLOYMENT
docker compose down --volumes --remove-orphans
docker compose -f docker-compose-query-engine.yml down --volumes --remove-orphans
docker compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker compose -f docker-compose-minio.yml down --volumes --remove-orphans