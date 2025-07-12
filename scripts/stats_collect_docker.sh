#!/bin/bash

scale_factor=$1

COORDINATOR_CONTAINER="styx-coordinator-1"
WORKER_CONTAINERS=$(docker ps --filter "name=worker" --format "{{.Names}}")
QUERY_ENGINE_CONTAINER="styx-query-engine-1"

if [ "$scale_factor" = "1" ]; then
  INTERVAL=10
  DURATION=600
elif [ "$scale_factor" = "10" ]; then
  INTERVAL=20
  DURATION=1800
else
  echo "Invalid scale factor"
  exit 1
fi


OUTFILE_COORDINATOR="stats_coordinator.csv"
OUTFILE_WORKERS="stats_workers.csv"
OUTFILE_QE="stats_query_engine.csv"

rm -rf $OUTFILE_COORDINATOR
rm -rf $OUTFILE_WORKERS
rm -rf $OUTFILE_QE

echo "timestamp,container_name,cpu %,mem usage / limit,mem %,net I/O,block I/O,pids" > $OUTFILE_COORDINATOR
echo "timestamp,container_name,cpu %,mem usage / limit,mem %,net I/O,block I/O,pids" > $OUTFILE_WORKERS
echo "timestamp,container_name,cpu %,mem usage / limit,mem %,net I/O,block I/O,pids" > $OUTFILE_QE

END=$((SECONDS + DURATION))

echo "Starting stats collection for $DURATION seconds with $INTERVAL seconds interval..."
while true; do
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

    docker stats --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}" "$COORDINATOR_CONTAINER" |
    sed "s/^/$TIMESTAMP,$COORDINATOR_CONTAINER,/" >> $OUTFILE_COORDINATOR

    for WORKER in $WORKER_CONTAINERS; do
        docker stats --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}" "$WORKER" |
        sed "s/^/$TIMESTAMP,$WORKER,/" >> $OUTFILE_WORKERS
    done

    docker stats --no-stream --format "{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}" "$QUERY_ENGINE_CONTAINER" |
    sed "s/^/$TIMESTAMP,$QUERY_ENGINE_CONTAINER,/" >> $OUTFILE_QE

    sleep $INTERVAL
done
