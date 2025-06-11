#!/bin/bash

workload_name=$1
input_rate=$2
n_keys=$3
n_part=$4
zipf_const=$5
client_threads=$6
total_time=$7
saving_dir=$8
warmup_seconds=$9
epoch_size=${10}
query_engine=${11:-false}
query_threads=${12:-0}
query_rate=${13:-0}

bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$n_part" "$query_engine"

sleep 10
bash scripts/stats_collect_docker.sh > monitor.log 2>&1 &

if [[ $workload_name == "ycsbt" ]]; then
    # YCSB-T
    run_with_validation=true
    python demo/demo-ycsb/client.py "$client_threads" "$n_keys" "$n_part" "$zipf_const" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$run_with_validation" "$query_engine"
elif [[ $workload_name == "dhr" ]]; then
    # Deathstar Hotel Reservation
    python demo/demo-deathstar-hotel-reservation/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds"
elif [[ $workload_name == "dmr" ]]; then
    # Deathstar Movie Review
    python demo/demo-deathstar-movie-review/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds"
elif [[ $workload_name == "tpcc" ]]; then
    # TPC-C
    bash scripts/generate_tpcc_dataset.sh "$n_keys"
    python demo/demo-tpc-c/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$n_keys"
elif [[ $workload_name == "ssb" ]]; then
    # SSB for HATtrick
#    bash scripts/generate_ssb_dataset.sh "$n_keys"
    python demo/demo-hattrick/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$n_keys" "$query_threads" "$query_rate"
else
    echo "Benchmark not supported!"
fi


bash scripts/stop_styx_cluster.sh