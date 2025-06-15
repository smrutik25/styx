#!/bin/bash

scale_factor="$1"

make all

rm -rf datagen
rm -rf stats_primary.csv
rm -rf stats_replica.csv
rm -rf monitor.log

echo "Creating data directory"
mkdir datagen
./HATtrickBench -gen -pa ./datagen

if [ "$scale_factor" = "1" ]; then
  warmup=10
  runtime=60
elif [ "$scale_factor" = "10" ]; then
  warmup=20
  runtime=60
else
  echo "Invalid scale factor"
  exit 1
fi

rm -rf "results"

bash start_postgres.sh "$scale_factor"
sleep 10
#./stats_collect_docker.sh > monitor.log 2>&1 &
#echo "Containers started. Monitoring running in background."

./HATtrickBench -init -dsn PostgresPrimary -usr myuser -pwd mypassword -pa /data/datagen -db postgres -sf "$scale_factor"
./HATtrickBench -frontier -dsn PostgresDocker -dsn2 PostgresReplica -usr myuser -pwd mypassword -wd "$warmup" -td "$runtime" -db postgres -t sp  -sf "$scale_factor"

bash stop_postgres.sh "$scale_factor"
echo "Deleting data directory"
rm -rf datagen