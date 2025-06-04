#!/bin/bash

scale_factor="$1"

make all
echo "Current directory: $(pwd)"

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
./HATtrickBench -init -dsn PostgresPrimary -usr myuser -pwd mypassword -pa /data/datagen -db postgres
./HATtrickBench -frontier -dsn PostgresDocker -dsn2 PostgresReplica -usr myuser -pwd mypassword -wd "$warmup" -td "$runtime" -db postgres -t sp

bash stop_postgres.sh "$scale_factor"
rm -rf datagen