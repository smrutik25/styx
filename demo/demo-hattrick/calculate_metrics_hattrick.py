import ast
import json
import math
import sys

import pandas as pd
import numpy as np


def txns_completed_upto_query(timestamp, df):
    return df.loc[df['timestamp'] <= timestamp, 'txn_id'].tolist()


def min_txn_not_in_query_response(row):
    txn_ids = row['txn_completed_till_ts']
    query_response = row['query_response']
    if query_response:
        max_query_response = max(query_response)
        txn_ids = [txn_id for txn_id in txn_ids if txn_id > max_query_response]
        return min(txn_ids) if txn_ids else -1
    if txn_ids:
        return 0
    return -1


last_valid = None
def fill_zero_with_last(row):
    global last_valid
    if row == 0 or pd.isna(row):
        return last_valid
    else:
        last_valid = row
        return row


def main(
        save_dir,
        input_rate,
        query_rate,
        warmup_seconds,
        client_threads,
        query_threads,
        sf,
        freshness=True,
        hat=True):
    exp_name = f"hattrick_SF{sf}_{input_rate * client_threads}_{query_rate * query_threads}"
    res_dict = {}
    if input_rate:
        origin_input_msgs = pd.read_csv(f'{save_dir}/client_requests.csv',
                                        dtype={'request_id': bytes,
                                               'timestamp': np.uint64,
                                               'txn_id': 'Int64'}).sort_values('timestamp')
        duplicate_requests = not origin_input_msgs['request_id'].is_unique
        output_msgs = pd.read_csv(f'{save_dir}/output.csv',
                                  dtype={'request_id': bytes,
                                         'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')
        exactly_once_output = output_msgs['request_id'].is_unique
        input_msgs = origin_input_msgs.loc[(origin_input_msgs['timestamp'] -
                                            origin_input_msgs['timestamp'][0] >= warmup_seconds * 1000)]

        joined = input_msgs.merge(output_msgs, on='request_id', how='left', suffixes=('_client', '_output'))

        missed = len(joined[joined['timestamp_output'].isna()])
        joined = joined.dropna()
        runtime = joined['timestamp_output'] - joined['timestamp_client']
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
        total_time = (max(joined['timestamp_output']) - min(joined['timestamp_client'])) // granularity

        req_ids = output_msgs['request_id']
        dup = output_msgs[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")
        res_dict["duplicate_requests"] = duplicate_requests
        res_dict["exactly_once_output"] = exactly_once_output
        res_dict["transactional latency (ms)"] = {
                         10: np.percentile(runtime, 10),
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
                         }
        res_dict["missed messages"] = missed
        res_dict["transactional_throughput"] = {
            "max": max(throughput_vals),
            "avg": sum(throughput_vals) / len(throughput_vals),
            "TPS": throughput_vals
        }
        res_dict["transactions_processed_per_second"] = len(joined) / total_time
        res_dict["duplicate_messages"] = len(dup)

    if query_rate:
        origin_input_queries = pd.read_csv(f'{save_dir}/client_queries.csv',
                                        dtype={'request_id': bytes,
                                               'timestamp': np.uint64}).sort_values('timestamp')
        duplicate_queries = not origin_input_queries['request_id'].is_unique
        output_queries = pd.read_csv(f'{save_dir}/query_output.csv',
                                  dtype={'request_id': bytes,
                                         'timestamp': np.uint64}, low_memory=False).sort_values('timestamp')
        exactly_once_output_queries = output_queries['request_id'].is_unique
        input_queries = origin_input_queries.loc[(origin_input_queries['timestamp'] -
                                                  origin_input_queries['timestamp'][0] >= warmup_seconds * 1000)]
        joined_queries = input_queries.merge(output_queries, on='request_id', how='left',
                                             suffixes=('_client', '_output'))

        missed_queries = len(joined_queries[joined_queries['timestamp_output'].isna()])
        joined_queries = joined_queries.dropna()
        runtime_queries = joined_queries['timestamp_output'] - joined_queries['timestamp_client']

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
        total_time = (max(joined_queries['timestamp_output']) - min(joined_queries['timestamp_client'])) // granularity

        req_ids = output_queries['request_id']
        dup_queries = output_queries[req_ids.isin(req_ids[req_ids.duplicated()])].sort_values("request_id")

        res_dict["duplicate_queries"] = duplicate_queries
        res_dict["exactly_once_output_queries"] = exactly_once_output_queries
        res_dict["analytical latency (ms)"] = {
                                       10: np.percentile(runtime_queries, 10),
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
                                       }
        res_dict["missed queries"] = missed_queries
        res_dict["analytical_throughput"] = {
            "max": max(query_throughput_vals),
            "avg": sum(query_throughput_vals)/len(query_throughput_vals),
            "TPS": query_throughput_vals
        }
        res_dict["queries_processed_per_second"] = len(joined_queries) / total_time
        res_dict["duplicate_queries_resp"] = len(dup_queries)

        if freshness and "transactional_throughput" in res_dict:
            freshness_requests = pd.read_csv(f'{save_dir}/freshness_queries.csv',
                                      dtype={'request_id': bytes,
                                             'timestamp': np.uint64}, low_memory=False)
            if len(freshness_requests) > 0:
                freshness_response = pd.read_csv(f'{save_dir}/freshness_output.csv',
                                          dtype={'request_id': bytes}, low_memory=False)
                freshness_response['response'] = freshness_response['response'].apply(ast.literal_eval)

                new_txns = origin_input_msgs[origin_input_msgs["txn_id"].notnull()]
                new_txns = pd.merge(new_txns[["request_id", "txn_id"]], output_msgs[['request_id', 'timestamp']],
                                    on="request_id", how="inner")

                global last_valid
                last_valid = min(new_txns['txn_id'])
                freshness = pd.merge(freshness_requests[['request_id', 'timestamp']],
                                     freshness_response[['request_id', 'response']],
                                     on="request_id", how="inner")
                freshness['query_response'] = freshness['response'].apply(lambda x: [elem for inner in x for elem in inner])
                freshness = freshness[['timestamp', 'query_response']]
                freshness['txn_completed_till_ts'] = freshness['timestamp'].apply(
                    lambda ts: txns_completed_upto_query(ts, new_txns))
                freshness['first_unseen_txn'] = freshness.apply(min_txn_not_in_query_response, axis=1)
                freshness['first_unseen_txn'] = freshness['first_unseen_txn'].astype('Int64')
                freshness['txn_id'] = freshness['first_unseen_txn'].apply(fill_zero_with_last).astype('Int64')
                freshness = pd.merge(freshness, new_txns, on="txn_id", how="left", suffixes=('_query', '_txn'))
                freshness['freshness'] = freshness.apply(lambda row: row['timestamp_query'] - row['timestamp_txn']
                                                         if isinstance(row['txn_id'], int) else 0, axis=1)
                freshness = freshness['freshness'].fillna(0).tolist()

                res_dict["freshness (ms)"] = {
                               95: np.percentile(freshness, 95),
                               99: np.percentile(freshness, 99),
                               "max": max(freshness),
                               "min": min(freshness),
                               "mean": np.average(freshness)
                               }
    if hat:
        with open(f'{save_dir.split("/")[0]}/hattrick/{exp_name}.json', 'w', encoding='utf-8') as f:
            json.dump(res_dict, f, ensure_ascii=False, indent=4)
    else:
        with open(f'{save_dir}/{exp_name}.json', 'w', encoding='utf-8') as f:
            json.dump(res_dict, f, ensure_ascii=False, indent=4)

    tps = res_dict["transactional_throughput"]["avg"] if "transactional_throughput" in res_dict else 0
    qps = res_dict["analytical_throughput"]["avg"] if "analytical_throughput" in res_dict else 0
    return tps, qps


if __name__ == '__main__':
    main(
        sys.argv[1],
        int(sys.argv[2]),
        int(sys.argv[3]),
        int(sys.argv[4]),
        int(sys.argv[5]),
        int(sys.argv[6]),
        int(sys.argv[7]),
        bool(sys.argv[8])
    )
