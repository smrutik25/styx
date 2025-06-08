#!/bin/bash

CONTAINER_ID="pg_primary"
CONTAINER2_ID="pg_replica"
INTERVAL=10          # in seconds
DURATION=2400        # total time to collect data (in seconds)
OUTFILE="stats_primary.csv"
OUTFILE2="stats_replica.csv"

echo "timestamp,cpu %,mem usage / limit,mem %,net I/O,block I/O,pids" > $OUTFILE
echo "timestamp,cpu %,mem usage / limit,mem %,net I/O,block I/O,pids" > $OUTFILE2

END=$((SECONDS + DURATION))
while [ $SECONDS -lt $END ]; do
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
    docker stats --no-stream --format " {{.CPUPerc}}, {{.MemUsage}}, {{.MemPerc}}, {{.NetIO}}, {{.BlockIO}}, {{.PIDs}}" "$CONTAINER_ID" |
    sed "s/^/$TIMESTAMP,/" >> $OUTFILE
	docker stats --no-stream --format " {{.CPUPerc}}, {{.MemUsage}}, {{.MemPerc}}, {{.NetIO}}, {{.BlockIO}}, {{.PIDs}}" "$CONTAINER2_ID" |
    sed "s/^/$TIMESTAMP,/" >> $OUTFILE2
    sleep $INTERVAL
done

echo "Done collecting stats."
