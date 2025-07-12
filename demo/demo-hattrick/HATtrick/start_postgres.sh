#!/bin/bash

set -e
scale_factor="$1"

cd "$(dirname "$0")/postgres-sr"

docker system prune -f --volumes

if [ "$scale_factor" = "1" ]; then
  docker compose -f docker-compose-postgressr-sf1.yml up --build -d
elif [ "$scale_factor" = "10" ]; then
  docker compose -f docker-compose-postgressr-sf10.yml up --build -d
else
  echo "Invalid scale factor"
  exit 1
fi

sleep 5
