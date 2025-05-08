#!/bin/bash

./scripts/stop_styx_cluster.sh

docker compose -f docker-compose-demo-ycsb.yml down --volumes --remove-orphans
