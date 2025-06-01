#!/bin/bash

docker compose logs worker | sort -t '|' -k1,1 -k2,2 > worker-logs.log
docker compose logs coordinator > coordinator-logs.log

if docker compose -f docker-compose-query-engine.yml ps --services | grep 'query-engine'; then
  sleep 20
  docker compose -f docker-compose-query-engine.yml logs query-engine > query-engine-logs.log
  docker compose -f docker-compose-query-engine.yml down --volumes --remove-orphans
fi

# DELETE PREVIOUS DEPLOYMENT
docker compose down --volumes --remove-orphans
docker compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker compose -f docker-compose-minio.yml down --volumes --remove-orphans