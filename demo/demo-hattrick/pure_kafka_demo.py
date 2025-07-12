# SF1:  ./scripts/run_experiment.sh ssb 1000 1 4 0.0 1 60 results 10 100 true 1 20
# SF10:  ./scripts/run_experiment.sh ssb 1000 10 4 0.0 1 60 results 10 100 true 1 20

import multiprocessing
import os
import math
import sys
import random
from datetime import datetime, timedelta
from typing import Any

from minio import Minio


from timeit import default_timer as timer
import time
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import kafka_output_consumer
import calculate_metrics_hattrick
import init_data_hattrick

from styx.client import SyncStyxClient
from analytical_queries import analytical_queries
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from functions import (customer_operator, customer_idx_operator, date_operator, date_idx_operator, history_operator,
                       line_order_operator, new_order_txn_operator, part_operator, payment_txn_operator,
                       supplier_operator, supplier_idx_operator)


random.seed(42)

SAVE_DIR: str = sys.argv[1]
txn_threads = int(sys.argv[2])
N_PARTITIONS = int(sys.argv[3])
messages_per_second = int(sys.argv[4])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[5])
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9092'
warmup_seconds = int(sys.argv[6])
SF = int(sys.argv[7])
query_threads = int(sys.argv[8])
queries_per_second = int(sys.argv[9])
num_freshness_queries = 20
num_queries = len(analytical_queries)
q_sleeps_per_second = 10
cust_size = 30000 * SF
supp_size = 2000 * SF
part_size = 200000 * math.floor(1 + math.log2(SF))
lo_size = 1500000 * SF
last_order_key = lo_size + 1

start_date = datetime.strptime("19920101", '%Y%m%d')
end_date = datetime.strptime("19981231", '%Y%m%d')
delta_days = (end_date - start_date).days + 1
date_list = [(start_date + timedelta(days=x)).strftime('%B %-d, %Y') for x in range(delta_days)]

if SF == 10:
    data_file_path = "HATtrick/datagen_sf10"
else:
    data_file_path = "HATtrick/datagen"

script_path = os.path.dirname(os.path.realpath(__file__))
freshness_per_txn = messages_per_second * seconds // num_freshness_queries


g = StateflowGraph('hattrick_benchmark', operator_state_backend=LocalStateBackend.DICT)
customer_operator.set_n_partitions(N_PARTITIONS)
customer_idx_operator.set_n_partitions(N_PARTITIONS)
date_operator.set_n_partitions(N_PARTITIONS)
date_idx_operator.set_n_partitions(N_PARTITIONS)
history_operator.set_n_partitions(N_PARTITIONS)
line_order_operator.set_n_partitions(N_PARTITIONS)
new_order_txn_operator.set_n_partitions(N_PARTITIONS)
part_operator.set_n_partitions(N_PARTITIONS)
payment_txn_operator.set_n_partitions(N_PARTITIONS)
supplier_operator.set_n_partitions(N_PARTITIONS)
supplier_idx_operator.set_n_partitions(N_PARTITIONS)

g.add_operators(customer_operator, customer_idx_operator, date_operator, date_idx_operator,
                history_operator, line_order_operator, new_order_txn_operator, part_operator, payment_txn_operator,
                supplier_operator, supplier_idx_operator)


def update_query(st, min_order_key, max_order_key):
    st.last_query = st.query_template.format(
        min_order_key=min_order_key,
        max_order_key=max_order_key
    )
    st.query_count += 1


def init_styx(styx):
    styx.set_graph(g)
    styx.init_metadata(g)
    init_data_hattrick.main(styx, N_PARTITIONS, script_path, data_file_path)
    time.sleep(5)
    styx.submit_dataflow(g)


def get_new_line_order_transaction(front_end_key):
    """Return parameters for new line_order transaction"""
    line_order_n = random.randint(1, 7)
    supplier_prefix = "Supplier#"
    customer_name = f"Customer#{str(random.randint(1, cust_size)).zfill(9)}"
    line_orders = {}
    for i in range(line_order_n):
        part = random.randint(1, part_size)
        line_orders[i + 1] = {
            "PK": part,
            "SN": f"{supplier_prefix}{str(random.randint(1, supp_size)).zfill(9)}",
            "OP": random.randint(0, 4),
            "SP": random.randint(0, 6),
            "Q": random.randint(0, 50),
            "D": random.randint(0, 10),
            "SC": random.randint(0, 1000),
            "T": random.randint(0, 8),
            "SM": random.randint(0, 6)
        }
    params: dict[str, Any] = {
        "OK": front_end_key,
        "OD": date_list[front_end_key % delta_days],
        "CN": customer_name,
        "LO": line_orders
        }
    return new_order_txn_operator, front_end_key, 'new_order_txn', (params,)


def get_payment_transaction(front_end_key):
    """Return parameters for payment transaction"""
    params: dict[str, Any] = {
        "AMT": random.randint(50, 1000),
        "ORDERKEY": front_end_key,
        "SUPPKEY": random.randint(1, supp_size),
        "CUSTKEY": random.randint(1, cust_size)
    }
    # choice = random.randint(1, 100)
    # if choice <= 60:
    #     params["CUSTNAME"] = f"Customer#{str(random.randint(1, cust_size)).zfill(9)}"
    # else:
    #     params["CUSTKEY"] = random.randint(1, cust_size)
    return payment_txn_operator, front_end_key, 'payment_txn', (params,)


def hattrick_transaction_generator(proc_num, shared_state):
    c = last_order_key + 1
    while True:
        front_end_key = proc_num + c
        coin = random.randint(1, 100)
        if c % freshness_per_txn == 0:
            update_query(shared_state, front_end_key - (freshness_per_txn * 3), front_end_key)
        if coin < 50:
            yield get_new_line_order_transaction(front_end_key)
        else:
            yield get_payment_transaction(front_end_key)
        c += 1


def hattrick_query_generator(shared_state):
    c = random.randint(1, num_queries)
    freshness = 0
    while True:
        if freshness < shared_state.query_count:
            yield num_queries, shared_state.last_query
            freshness += 1
        yield c % num_queries, analytical_queries[c % num_queries]
        c += 1


def transactional_benchmark_runner(args) -> (dict[bytes, dict], dict[bytes, dict]):
    proc_num, shared_state = args
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    hattrick_generator = hattrick_transaction_generator(proc_num, shared_state)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(hattrick_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f'{func_name} {key}->{params}', "txn_id": key
                                                    if func_name == "new_order_txn" else None}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
    styx.close()
    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def analytical_benchmark_runner(args) -> (dict[bytes, dict], dict[bytes, dict]):
    proc_num, shared_state = args
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    hattrick_generator = hattrick_query_generator(shared_state)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(queries_per_second):
            if i % (queries_per_second // q_sleeps_per_second) == 0:
                time.sleep(sleep_time)
            query_id, query = next(hattrick_generator)
            future = styx.send_query(query)
            timestamp_futures[future.request_id] = {"q": query_id}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
    styx.close()
    for key, metadata in styx.query_delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def transactional_thread_pool(shared_state):
    with ProcessPoolExecutor(max_workers=txn_threads) as executor:
        return list(executor.map(transactional_benchmark_runner, [(i, shared_state) for i in range(txn_threads)]))


def analytical_thread_pool(shared_state):
    with ProcessPoolExecutor(max_workers=query_threads) as executor:
        return list(executor.map(analytical_benchmark_runner, [(i, shared_state) for i in range(query_threads)]))


def main():
    minio = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    init_styx(styx_client)
    del styx_client
    # Sleep so that the init is surely done (snapshot buckets and duckdb)
    print(f'Data populated waiting for {(300 * math.ceil(SF / 2)) // 60} min')
    time.sleep(300 * math.ceil(SF / 2))
    print(f"Freshness will be measured after {freshness_per_txn} transactions")
    manager = multiprocessing.Manager()
    shared_state = manager.Namespace()
    shared_state.query_template = "SELECT ORDERKEY FROM line_order WHERE ORDERKEY BETWEEN {min_order_key} AND {max_order_key}"
    shared_state.last_query = ""
    shared_state.query_count = 0

    with ProcessPoolExecutor(max_workers=2) as main_executor:
        txn_future = main_executor.submit(transactional_thread_pool, shared_state)
        anal_future = main_executor.submit(analytical_thread_pool, shared_state)

        transactional_results = txn_future.result()
        analytical_results = anal_future.result()

    transactional_results = {k: v for d in transactional_results for k, v in d.items()}
    analytical_results = {k: v for d in analytical_results for k, v in d.items()}

    df = pd.DataFrame({"request_id": list(transactional_results.keys()),
                       "timestamp": [res["timestamp"] for res in transactional_results.values()],
                       "op": [res["op"] for res in transactional_results.values()],
                       "txn_id": [res["txn_id"] for res in transactional_results.values()]
                       })
    df['txn_id'] = df['txn_id'].astype('Int64')
    df.to_csv(f'{SAVE_DIR}/client_requests.csv',
                            index=False)
    analytical_df = pd.DataFrame({"request_id": list(analytical_results.keys()),
                                  "timestamp": [res["timestamp"] for res in analytical_results.values()],
                                  "q": [res["q"] for res in analytical_results.values()]
                                  })
    freshness_df = analytical_df[analytical_df["q"] == num_queries]
    analytical_df = analytical_df[analytical_df["q"] < num_queries]

    freshness_df.to_csv(f'{SAVE_DIR}/freshness_queries.csv', index=False)
    analytical_df.to_csv(f'{SAVE_DIR}/client_queries.csv', index=False)


if __name__ == '__main__':
    multiprocessing.set_start_method('fork')
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics_hattrick.main(
        SAVE_DIR,
        messages_per_second,
        queries_per_second,
        warmup_seconds,
        txn_threads,
        query_threads,
        SF,
        hat=False
    )

