# ./scripts/run_experiment.sh hat 300 1 4 0.0 1 60 results 10 100 true 1 20
# ./scripts/run_experiment.sh hat 300 1 4 0.0 1 60 results 10 100 true 1 20

import multiprocessing
import os
import math
import sys
import shutil
import random
import uvloop
from typing import Any
from datetime import datetime, timedelta
import asyncio
from minio import Minio


from timeit import default_timer as timer
import time
from concurrent.futures import ProcessPoolExecutor
from aiokafka import AIOKafkaConsumer

import pandas as pd
import kafka_output_consumer_hattrick
import calculate_metrics_hattrick
import init_data_hattrick

from styx.common.serialization import msgpack_deserialization
from styx.client import SyncStyxClient
from analytical_queries import analytical_queries
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from functions import (customer_operator, customer_idx_operator, date_operator, date_idx_operator, history_operator,
                       line_order_operator, new_order_txn_operator, part_operator, payment_txn_operator,
                       supplier_operator, supplier_idx_operator)

random.seed(42)

N_PARTITIONS = int(sys.argv[1])
seconds = int(sys.argv[2])
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9092'
warmup_seconds = int(sys.argv[3])
SF = int(sys.argv[4])
SAVE_DIR = sys.argv[5]
num_queries = len(analytical_queries)
improvement_threshold = 0.01
cust_size = 30000 * SF
supp_size = 2000 * SF
part_size = 200000 * math.floor(1 + math.log2(SF))
lo_size = 1500000 * SF
last_order_key = lo_size + 1

start_date = datetime.strptime("19920101", '%Y%m%d')
end_date = datetime.strptime("19981231", '%Y%m%d')
delta_days = (end_date - start_date).days + 1
date_list = [(start_date + timedelta(days=x)).strftime('%B %-d, %Y') for x in range(delta_days)]
script_path = os.path.dirname(os.path.realpath(__file__))
freshness_per_txn = 500

# if SF == 10:
#     data_file_path = "HATtrick/datagen_sf10"
# else:
data_file_path = "HATtrick/datagen"


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
    proc_num, messages_per_second, shared_state = args
    time.sleep(10)
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    hattrick_generator = hattrick_transaction_generator(proc_num, shared_state)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
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
    proc_num, queries_per_second, shared_state = args
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    hattrick_generator = hattrick_query_generator(shared_state)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(queries_per_second):
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


def transactional_thread_pool(shared_state, txn_threads, txn_per_second):
    with ProcessPoolExecutor(max_workers=txn_threads) as executor:
        return list(executor.map(transactional_benchmark_runner, [(i, txn_per_second, shared_state) for i in range(txn_threads)]))


def analytical_thread_pool(shared_state, query_threads, queries_per_second):
    with ProcessPoolExecutor(max_workers=query_threads) as executor:
        return list(executor.map(analytical_benchmark_runner, [(i, queries_per_second, shared_state) for i in range(query_threads)]))


def run_hybrid_load(save_dir, txn_per_second, txn_threads, queries_per_second, query_threads):
    manager = multiprocessing.Manager()
    shared_state = manager.Namespace()
    shared_state.query_template = "SELECT ORDERKEY FROM line_order WHERE ORDERKEY BETWEEN {min_order_key} AND {max_order_key}"
    shared_state.last_query = ""
    shared_state.query_count = 0

    with ProcessPoolExecutor(max_workers=2) as main_executor:
        txn_future = main_executor.submit(transactional_thread_pool, shared_state, txn_threads, txn_per_second)
        anal_future = main_executor.submit(analytical_thread_pool, shared_state, query_threads, queries_per_second)

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
    df.to_csv(f'{save_dir}/client_requests.csv',
              index=False)
    analytical_df = pd.DataFrame({"request_id": list(analytical_results.keys()),
                                  "timestamp": [res["timestamp"] for res in analytical_results.values()],
                                  "q": [res["q"] for res in analytical_results.values()]
                                  })
    freshness_df = analytical_df[analytical_df["q"] == num_queries]
    analytical_df = analytical_df[analytical_df["q"] < num_queries]
    freshness_df.to_csv(f'{save_dir}/freshness_queries.csv', index=False)
    analytical_df.to_csv(f'{save_dir}/client_queries.csv', index=False)


async def find_txn_saturation(kafka_consumer):
    tps_interval = 50
    tps = 500
    txn_threads = 1
    prev_throughput = -1
    print("Coarse throughput saturation calculation for transactions")
    while True:
        tps += tps_interval
        save_dir = f"{SAVE_DIR}/results_txn_{tps}"
        shutil.rmtree(save_dir, ignore_errors=True)
        os.makedirs(save_dir)
        run_hybrid_load(save_dir, tps, txn_threads, 0, 1)
        await kafka_output_consumer_hattrick.main(save_dir, txn_consumer=kafka_consumer)
        throughput = calculate_metrics_hattrick.main(save_dir, tps, 0, warmup_seconds,
                                                     txn_threads, 0, SF, False, sat=True)[0]
        print(f"Throughput for tps {tps} is {throughput}")
        if prev_throughput >= 0 and throughput < prev_throughput * (1 + improvement_threshold):
            if throughput <= prev_throughput:
                tps -= tps_interval
            break
        prev_throughput = throughput
        shutil.rmtree(save_dir, ignore_errors=True)
    print(f"Max input tps is around {tps}")
    tps_interval = 10
    max_tps = tps
    max_throughput = prev_throughput
    tps = max_tps
    print(f"Fine throughput saturation calculation for transactions")
    while True:
        tps += tps_interval
        save_dir = f"{SAVE_DIR}/results_txn_{tps}"
        shutil.rmtree(save_dir, ignore_errors=True)
        os.makedirs(save_dir)
        run_hybrid_load(save_dir, tps, txn_threads, 0, 1)
        await kafka_output_consumer_hattrick.main(save_dir, txn_consumer=kafka_consumer)
        throughput = calculate_metrics_hattrick.main(save_dir, tps, 0, warmup_seconds,
                                                     txn_threads, 0, SF, False, sat=True)[0]
        print(f"Throughput for tps {tps} is {throughput}")
        if throughput < max_throughput * (1 + improvement_threshold):
            if throughput <= prev_throughput:
                tps -= tps_interval
            break
        max_tps = tps
        max_throughput = throughput
        shutil.rmtree(save_dir, ignore_errors=True)
    print(f"Max input tps is {max_tps}")
    return max_tps


async def find_analytical_saturation(kafka_consumer):
    qps_interval = 5
    qps = 5
    query_threads = 1
    prev_throughput = -1
    print("Coarse throughput saturation calculation for queries")
    while True:
        qps += qps_interval
        save_dir = f"{SAVE_DIR}/results_ana_{qps}"
        shutil.rmtree(save_dir, ignore_errors=True)
        os.makedirs(save_dir)
        run_hybrid_load(save_dir, 0, 1, qps, query_threads)
        await kafka_output_consumer_hattrick.main(save_dir, ana_consumer=kafka_consumer)
        throughput = calculate_metrics_hattrick.main(save_dir, 0, qps, warmup_seconds,
                                                     0, query_threads, SF, False, sat=True)[1]
        print(f"Throughput for qps {qps} is {throughput}")
        if prev_throughput >= 0 and throughput < prev_throughput * (1 + improvement_threshold):
            qps -= qps_interval
            break
        prev_throughput = throughput
        shutil.rmtree(save_dir, ignore_errors=True)
    print(f"Max input qps is around {qps}")
    qps_interval = 2
    max_qps = qps
    max_throughput = prev_throughput
    qps = max_qps
    print(f"Fine throughput saturation calculation for queries")
    while True:
        qps += qps_interval
        save_dir = f"{SAVE_DIR}/results_ana_{qps}"
        shutil.rmtree(save_dir, ignore_errors=True)
        os.makedirs(save_dir)
        run_hybrid_load(save_dir, 0, 1, qps, query_threads)
        await kafka_output_consumer_hattrick.main(save_dir, ana_consumer=kafka_consumer)
        throughput = calculate_metrics_hattrick.main(save_dir, 0, qps, warmup_seconds,
                                            1, query_threads, SF, False, sat=True)[1]
        print(f"Throughput for qps {qps} is {throughput}")
        if throughput < max_throughput * (1 + improvement_threshold):
            if throughput <= prev_throughput:
                qps -= qps_interval
            break
        max_qps = qps
        max_throughput = throughput
        shutil.rmtree(save_dir, ignore_errors=True)
    print(f"Max input qps is {max_qps}")
    return max_qps


async def hattrick_benchmark(tps, qps, txn_consumer, ana_consumer):
    intervals = [0, 0.1, 0.2, 0.5, 0.8, 1]
    tps_intervals = [math.floor(tps * i) for i in intervals]
    qps_intervals = [math.floor(qps * i) for i in intervals]
    frontier_file = f"{SAVE_DIR}/frontier_{SF}.csv"
    if os.path.exists(frontier_file):
        os.remove(frontier_file)
    for i in tps_intervals:
        for j in qps_intervals:
            save_dir = f"{SAVE_DIR}/results_{i}_{j}"
            shutil.rmtree(save_dir, ignore_errors=True)
            os.makedirs(save_dir)
            run_hybrid_load(save_dir, i, 1, j, 1)
            await kafka_output_consumer_hattrick.main(save_dir, txn_consumer=txn_consumer, ana_consumer=ana_consumer)
            t_throughput, a_throughput = calculate_metrics_hattrick.main(save_dir, i, j, warmup_seconds,
                                                                         1, 1, SF, True)
            print(f"Throughput frontier for ({i}, {j}) = {t_throughput}, {a_throughput}")
            with open(frontier_file, "a") as f:
                f.write(f"{t_throughput},{a_throughput}\n")
            shutil.rmtree(save_dir, ignore_errors=True)
    print(f"Found frontier, wrote to {frontier_file}")


def all_egress_topics_created(topics: set[str], egress_topic_names: list[str]):
    for topic in egress_topic_names:
        if topic not in topics:
            return False
    return True


async def setup_kafka_txns():
    egress_topic_names: list[str] = g.get_egress_topic_names()
    consumer = AIOKafkaConsumer(
        auto_offset_reset='earliest',
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9092')
    await consumer.start()
    topics = []
    while not all_egress_topics_created(topics, egress_topic_names):
        topics = set(await consumer.topics())
        print(f"Awaiting topics {egress_topic_names} to be created by the Styx coordinator, current topics: {topics}")
        await asyncio.sleep(5)
    consumer.subscribe(topics=egress_topic_names)
    print(f"Consumer subscribed to topics {egress_topic_names}.")
    return consumer


async def setup_kafka_query_engine():
    consumer = AIOKafkaConsumer(
        auto_offset_reset='earliest',
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9092')
    await consumer.start()
    consumer.subscribe(topics=['styx-query-engine--OUT'])
    return consumer


async def main():
    res_dir = f"{SAVE_DIR}/hattrick"
    shutil.rmtree(res_dir, ignore_errors=True)
    os.makedirs(res_dir)
    minio = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    init_styx(styx_client)
    del styx_client
    # Sleep so that the init is surely done (snapshot buckets and duckdb)
    print(f'Data populated waiting for {(180 * math.ceil(SF / 2)) // 60} min')
    time.sleep(180 * math.ceil(SF / 2))
    print(f"Freshness will be measured after {freshness_per_txn} transactions")
    txn_kafka_consumer = await setup_kafka_txns()
    ana_kafka_consumer = await setup_kafka_query_engine()
    try:
        max_tps = await find_txn_saturation(txn_kafka_consumer)
        max_qps = await find_analytical_saturation(ana_kafka_consumer)
        await hattrick_benchmark(max_tps, max_qps, txn_kafka_consumer, ana_kafka_consumer)
    finally:
        await txn_kafka_consumer.stop()
        await ana_kafka_consumer.stop()

if __name__ == '__main__':
    multiprocessing.set_start_method('fork')
    uvloop.run(main())
