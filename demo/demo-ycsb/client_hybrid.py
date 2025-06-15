import os
import sys
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from timeit import default_timer as timer

from minio import Minio
from styx.client.sync_client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

import kafka_output_consumer
import calculate_metrics

from tqdm import tqdm

from ycsb import ycsb_operator
from zipfian_generator import ZipfGenerator
from analytical_queries import analytical_queries

# A run with sensible arguments: python client.py 1 10 4 0 100 10 results 0 10

threads = int(sys.argv[1])
N_ENTITIES = int(sys.argv[2])
N_PARTITIONS = int(sys.argv[3])
STARTING_MONEY = 1_000_000
ZIPF_CONST = float(sys.argv[4])
messages_per_second = int(sys.argv[5])
queries_per_second = 10
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[6])
key_list: list[int] = list(range(N_ENTITIES))
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
# STYX_HOST: str = '35.229.80.128'
# STYX_PORT: int = 8888
KAFKA_URL = 'localhost:9092'
# KAFKA_URL = '35.229.114.18:9094'
SAVE_DIR: str = sys.argv[7]
warmup_seconds: int = int(sys.argv[8])
run_with_validation = bool(sys.argv[9])
num_queries = len(analytical_queries
                  )
####################################################################################################################
g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
ycsb_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(ycsb_operator)

def submit_graph(styx: SyncStyxClient):
    print(f'Partitions: {list(g.nodes.values())[0].n_partitions}')
    styx.submit_dataflow(g)
    print("Graph submitted")


def ycsb_init(styx: SyncStyxClient, operator: Operator, keys: list[int]):
    styx.set_graph(g)
    styx.init_metadata(g)
    partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
    for i in tqdm(keys):
        partition: int = styx.get_operator_partition(i, operator)
        partitions[partition][i] = STARTING_MONEY

    for partition, partition_data in partitions.items():
        styx.init_data(operator, partition, partition_data)
    time.sleep(5)
    submit_graph(styx)


def transactional_ycsb_generator(keys,
                                 operator: Operator,
                                 n: int,
                                 zipf_const: float):
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    uniform_gen = ZipfGenerator(items=n, zipf_const=0.0)
    while True:
        key = keys[next(uniform_gen)]
        key2 = keys[next(zipf_gen)]
        while key2 == key:
            key2 = keys[next(zipf_gen)]
        yield operator, key, 'transfer', (key2, )


def read_only_ycsb_generator(keys, operator: Operator, n: int, zipf_const: float):
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    while True:
        key = keys[next(zipf_gen)]
        yield operator, key, 'read', ()


def ycsb_query_generator():
    c = 0
    while True:
        yield c % num_queries, analytical_queries[c % num_queries]
        c += 1


def transactional_benchmark_runner(proc_num) -> (dict[bytes, dict], dict[bytes, dict]):
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    ycsb_generator = transactional_ycsb_generator(key_list, ycsb_operator, N_ENTITIES, zipf_const=ZIPF_CONST)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    start = timer()
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(ycsb_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f'{func_name} {key}->{params[0]}'}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Transaction latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average transaction latency per second: {(end - start) / seconds}')
    styx.close()
    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def analytical_benchmark_runner(proc_num) -> (dict[bytes, dict], dict[bytes, dict]):
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    ycsb_generator = ycsb_query_generator()
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    start = timer()
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(queries_per_second):
            query_id, query = next(ycsb_generator)
            future = styx.send_query(query)
            timestamp_futures[future.request_id] = {"q": query_id}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Analytical latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average analytical latency per second: {(end - start) / seconds}')
    styx.close()
    for key, metadata in styx.query_delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def transactional_thread_pool():
    with ProcessPoolExecutor(max_workers=1) as executor:
        return list(executor.map(transactional_benchmark_runner, range(threads)))


def analytical_thread_pool():
    with ProcessPoolExecutor(max_workers=1) as executor:
        return list(executor.map(analytical_benchmark_runner, range(threads)))


def main():
    print('Generate and push workload to Styx')

    if N_ENTITIES < 3:
        print("Impossible to run this benchmark with one key")
        return

    minio = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    ycsb_init(styx_client, ycsb_operator, key_list)
    del styx_client
    time.sleep(5)

    with ProcessPoolExecutor(max_workers=2) as main_executor:
        txn_future = main_executor.submit(transactional_thread_pool)
        anal_future = main_executor.submit(analytical_thread_pool)

        transactional_results = txn_future.result()
        analytical_results = anal_future.result()

    transactional_results = {k: v for d in transactional_results for k, v in d.items()}
    assert len(transactional_results) == messages_per_second * seconds * threads

    analytical_results = {k: v for d in analytical_results for k, v in d.items()}
    assert len(analytical_results) == queries_per_second * seconds * threads


    if run_with_validation:
        # wait for system to stabilize
        time.sleep(30)

        styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)

        styx_client.open(consume=False)

        print('Starting Consistency measurement')

        validation_results = {}

        for key in key_list:
            future = styx_client.send_event(operator=ycsb_operator,
                                            key=key,
                                            function="read")
            validation_results[future.request_id] = {"op": f'read -> {key}'}

        styx_client.close()

        for request_id, metadata in styx_client.delivery_timestamps.items():
            validation_results[request_id]["timestamp"] = metadata

        assert len(validation_results) == N_ENTITIES

    os.makedirs(SAVE_DIR, exist_ok=True)
    pd.DataFrame({"request_id": list(transactional_results.keys()),
                  "timestamp": [res["timestamp"] for res in transactional_results.values()],
                  "op": [res["op"] for res in transactional_results.values()]
                  }).sort_values("timestamp").to_csv(f'{SAVE_DIR}/client_requests.csv', index=False)

    pd.DataFrame({"request_id": list(analytical_results.keys()),
                  "timestamp": [res["timestamp"] for res in analytical_results.values()],
                  "q": [res["q"] for res in analytical_results.values()]
                  }).sort_values("timestamp").to_csv(f'{SAVE_DIR}/client_queries.csv', index=False)
    print('Workload completed')


if __name__ == "__main__":
    multiprocessing.set_start_method('fork')
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        N_ENTITIES,
        N_PARTITIONS,
        messages_per_second,
        ZIPF_CONST,
        threads,
        warmup_seconds,
        SAVE_DIR,
        run_with_validation,
        queries=True
    )
