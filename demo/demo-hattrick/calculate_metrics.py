import json
import math
import sys

import pandas as pd
import numpy as np


def main(
        save_dir,
        input_rate,
        query_rate,
        warmup_seconds,
        client_threads,
        query_threads,
        sf):
    exp_name = f"hattrick_SF{sf}_{input_rate * client_threads}_{query_rate * query_threads}"

    origin_input_msgs = pd.read_csv(f'{save_dir}/client_requests.csv',
                                    dtype={'request_id': bytes,
                                           'timestamp': np.uint64}).sort_values('timestamp')

    origin_input_queries = pd.read_csv(f'{save_dir}/client_queries.csv',
                                    dtype={'request_id': bytes,
                                           'timestamp': np.uint64}).sort_values('timestamp')

    duplicate_requests = not origin_input_msgs['request_id'].is_unique

    duplicate_queries = not origin_input_queries['request_id'].is_unique

    output_msgs = pd.read_csv(f'{save_dir}/output.csv',
                              dtype={'request_id': bytes,
                                     'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

    output_queries = pd.read_csv(f'{save_dir}/query_output.csv',
                              dtype={'request_id': bytes,
                                     'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')

    exactly_once_output = output_msgs['request_id'].is_unique
    exactly_once_output_queries = output_queries['request_id'].is_unique

    # remove warmup from input
    input_msgs = origin_input_msgs.loc[(origin_input_msgs['timestamp'] -
                                        origin_input_msgs['timestamp'][0] >= warmup_seconds * 1000)]

    input_queries = origin_input_queries.loc[(origin_input_queries['timestamp'] -
                                        origin_input_queries['timestamp'][0] >= warmup_seconds * 1000)]

    # Perform a left join to include all rows from client_df
    joined = input_msgs.merge(output_msgs, on='request_id', how='left', suffixes=('_client', '_output'))
    joined_queries = input_queries.merge(output_queries, on='request_id', how='left', suffixes=('_client', '_output'))

    missed = len(joined[joined['timestamp_output'].isna()])
    missed_queries = len(joined_queries[joined_queries['timestamp_output'].isna()])

    joined = joined.dropna()
    runtime = joined['timestamp_output'] - joined['timestamp_client']

    joined_queries = joined_queries.dropna()
    runtime_queries = joined_queries['timestamp_output'] - joined_queries['timestamp_client']

    start_time = -math.inf
    throughput = {}
    bucket_id = -1

    # 1 second (ms) (i.e. bucket size)
    granularity = 1000

    for t in output_msgs['timestamp']:
        if t - start_time > granularity:
            bucket_id += 1
            start_time = t
            throughput[bucket_id] = 1
        else:
            throughput[bucket_id] += 1

    throughput_vals = list(throughput.values())

    req_ids = output_msgs['request_id']
    dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

    start_time = -math.inf
    throughput = {}
    bucket_id = -1

    # 1 second (ms) (i.e. bucket size)
    granularity = 1000

    for t in output_queries['timestamp']:
        if t - start_time > granularity:
            bucket_id += 1
            start_time = t
            throughput[bucket_id] = 1
        else:
            throughput[bucket_id] += 1

    query_throughput_vals = list(throughput.values())

    req_ids = output_queries['request_id']
    dup_queries = output_queries[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

    res_dict = {
        "duplicate_requests": duplicate_requests,
        "duplicate_queries": duplicate_queries,
        "exactly_once_output": exactly_once_output,
        "exactly_once_output_queries": exactly_once_output_queries,
        "transactional latency (ms)": {10: np.percentile(runtime, 10),
                         20: np.percentile(runtime, 20),
                         30: np.percentile(runtime, 30),
                         40: np.percentile(runtime, 40),
                         50: np.percentile(runtime, 50),
                         60: np.percentile(runtime, 60),
                         70: np.percentile(runtime, 70),
                         80: np.percentile(runtime, 80),
                         90: np.percentile(runtime, 90),
                         95: np.percentile(runtime, 95),
                         99: np.percentile(runtime, 99),
                         "max": max(runtime),
                         "min": min(runtime),
                         "mean": np.average(runtime)
                         },
        "analytical latency (ms)": {10: np.percentile(runtime_queries, 10),
                                       20: np.percentile(runtime_queries, 20),
                                       30: np.percentile(runtime_queries, 30),
                                       40: np.percentile(runtime_queries, 40),
                                       50: np.percentile(runtime_queries, 50),
                                       60: np.percentile(runtime_queries, 60),
                                       70: np.percentile(runtime_queries, 70),
                                       80: np.percentile(runtime_queries, 80),
                                       90: np.percentile(runtime_queries, 90),
                                       95: np.percentile(runtime_queries, 95),
                                       99: np.percentile(runtime_queries, 99),
                                       "max": max(runtime_queries),
                                       "min": min(runtime_queries),
                                       "mean": np.average(runtime_queries)
                                       },
        "missed messages": missed,
        "missed queries": missed_queries,
        "transactional_throughput": {
            "max": max(throughput_vals),
            "avg": sum(throughput_vals) / len(throughput_vals),
            "TPS": throughput_vals
        },
        "analytical_throughput": {
            "max": max(query_throughput_vals),
            "avg": sum(query_throughput_vals) / len(query_throughput_vals),
            "TPS": query_throughput_vals
        },
        "duplicate_messages": len(dup),
        "duplicate_queries_resp": len(dup_queries),
    }

    with open(f'{save_dir}/{exp_name}.json', 'w', encoding='utf-8') as f:
        json.dump(res_dict, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    main(
        sys.argv[1],
        int(sys.argv[2]),
        int(sys.argv[3]),
        int(sys.argv[4]),
        int(sys.argv[5])
    )
