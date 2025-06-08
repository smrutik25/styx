#!/bin/bash

scale_factor="$1"

cd "$(dirname "$0")/postgres-sr"

if [ "$scale_factor" = "1" ]; then
  docker compose -f docker-compose-postgressr-sf1.yml down -v --remove-orphans
elif [ "$scale_factor" = "10" ]; then
  docker compose -f docker-compose-postgressr-sf10.yml down -v --remove-orphans
else
  exit 1
fi
