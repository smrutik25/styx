import csv
import multiprocessing
import os
import math
import sys
import random
from datetime import datetime, timedelta
from typing import Any

from minio import Minio
from tqdm import tqdm

from timeit import default_timer as timer
import time
from multiprocessing import Pool

import pandas as pd
import kafka_output_consumer
import calculate_metrics

from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from styx.client import SyncStyxClient
from functions import (customer_operator, customer_idx_operator, date_operator, date_idx_operator, history_operator,
                       line_order_operator, new_order_txn_operator, part_operator, payment_txn_operator,
                       supplier_operator, supplier_idx_operator)

random.seed(42)

SAVE_DIR: str = sys.argv[1]
threads = int(sys.argv[2])
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
# cust_size = 30000 * SF
# supp_size = 2000 * SF
# part_size = 200000 * math.floor(1 + math.log2(SF))
# lo_size = 1500000 * SF
# last_order_key = lo_size + 1

cust_size = 300 * SF
supp_size = 20 * SF
part_size = 200 * math.floor(1 + math.log2(SF))
lo_size = 1500 * SF
last_order_key = lo_size + 1

start_date = datetime.strptime("19920101", '%Y%m%d')
end_date = datetime.strptime("19981231", '%Y%m%d')
delta_days = (end_date - start_date).days + 1
date_list = [(start_date + timedelta(days=x)).strftime('%B %-d, %Y') for x in range(delta_days)]

data_file_path = "HATtrick/datagen_new"
script_path = os.path.dirname(os.path.realpath(__file__))

# Create Stateflow Graph
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


def populate_customer(styx: SyncStyxClient):
    with open(os.path.join(script_path, f"{data_file_path}/customer.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        cust_partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        cust_idx_partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating Customer Data"):
            customer_key = int(line[0])
            customer_idx_key = line[1]
            cust_partition: int = styx.get_operator_partition(customer_key, customer_operator)
            cust_idx_partition: int = styx.get_operator_partition(customer_idx_key, customer_idx_operator)
            customer_data = {
                'NAME': line[1],
                'ADDRESS': line[2],
                'CITY': line[3],
                'NATION': line[4],
                'REGION': line[5],
                'PHONE': line[6],
                'MKTSEGMENT': line[7],
                'PAYMENTCNT': int(line[8])
            }
            cust_partitions[cust_partition][customer_key] = customer_data
            cust_idx_partitions[cust_idx_partition][customer_idx_key] = customer_key
        for partition, partition_data in cust_partitions.items():
            print(f"Populating {customer_operator.name}:{partition}...")
            styx.init_data(customer_operator, partition, partition_data)
        for partition, partition_data in cust_idx_partitions.items():
            print(f"Populating {customer_idx_operator.name}:{partition}")
            styx.init_data(customer_idx_operator, partition, partition_data)


def populate_supplier(styx: SyncStyxClient):
    with open(os.path.join(script_path, f"{data_file_path}/supplier.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        sup_partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        sup_idx_partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating Supplier Data"):
            supplier_key = int(line[0])
            supplier_idx_key = line[1]
            sup_partition: int = styx.get_operator_partition(supplier_key, supplier_operator)
            sup_idx_partition: int = styx.get_operator_partition(supplier_idx_key, supplier_idx_operator)
            supplier_data = {
                'NAME': line[1],
                'ADDRESS': line[2],
                'CITY': line[3],
                'NATION': line[4],
                'REGION': line[5],
                'PHONE': line[6],
                'YTD': float(line[7])
            }
            sup_partitions[sup_partition][supplier_key] = supplier_data
            sup_idx_partitions[sup_idx_partition][supplier_idx_key] = supplier_key
        for partition, partition_data in sup_partitions.items():
            print(f"Populating {supplier_operator.name}:{partition}...")
            styx.init_data(supplier_operator, partition, partition_data)
        for partition, partition_data in sup_idx_partitions.items():
            print(f"Populating {supplier_idx_operator.name}:{partition}")
            styx.init_data(supplier_idx_operator, partition, partition_data)


def populate_line_order(styx: SyncStyxClient):
    with open(os.path.join(script_path, f"{data_file_path}/lineorder.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating Lineorder Data"):
            line_order_key = f"{line[0]}:{line[1]}"
            partition: int = styx.get_operator_partition(line_order_key, line_order_operator)
            line_order_data = {
                'CUSTKEY': int(line[2]),
                'PARTKEY': int(line[3]),
                'SUPKEY': int(line[4]),
                'ORDERDATE': int(line[5]),
                'ORDPRIORITY': line[6],
                'SHIPPRIORITY': line[7],
                'QUANTITY': int(line[8]),
                'EXTENDEDPRICE': float(line[9]),
                'DISCOUNT': float(line[11]),
                'REVENUE': float(line[12]),
                'SUPPLYCOST': float(line[13]),
                'TAX': float(line[14]),
                'COMMITDATE': line[15],
                'SHIPMODE': line[16],
            }
            partitions[partition][line_order_key] = line_order_data
        for partition, partition_data in partitions.items():
            print(f"Populating {line_order_operator.name}:{partition}...")
            styx.init_data(line_order_operator, partition, partition_data)


def populate_part(styx: SyncStyxClient):
    with open(os.path.join(script_path, f"{data_file_path}/part.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating Part Data"):
            part_key = int(line[0])
            partition: int = styx.get_operator_partition(part_key, part_operator)
            part_data = {
                'NAME': line[1],
                'MFGR': line[2],
                'CATEGORY': line[3],
                'BRAND1': line[4],
                'COLOR': line[5],
                'TYPE': line[6],
                'SIZE': int(line[7]),
                'CONTAINER': line[8],
                'PRICE': float(line[9])
            }
            partitions[partition][part_key] = part_data
        for partition, partition_data in partitions.items():
            print(f"Populating {part_operator.name}:{partition}...")
            styx.init_data(part_operator, partition, partition_data)


def populate_date(styx: SyncStyxClient):
    with open(os.path.join(script_path, f"{data_file_path}/date.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        date_partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        date_idx_partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating Date Data"):
            date_key = int(line[0])
            date_idx_key = line[1]
            date_partition: int = styx.get_operator_partition(date_key, date_operator)
            date_idx_partition: int = styx.get_operator_partition(date_idx_key, date_idx_operator)
            date_data = {
                'DATE': line[1],
                'DATEOFWEEK': line[2],
                'MONTH': line[3],
                'YEAR': int(line[4]),
                'YEARMONTHNUM': int(line[5]),
                'YEARMONTH': line[6],
                'DAYNUMINWEEK': int(line[7]),
                'DAYNUMINMONTH': int(line[8]),
                'DAYNUMINYEAR': int(line[9]),
                'MONTHNUMINYEAR': int(line[10]),
                'WEEKNUMINYEAR': int(line[11]),
                'SELLINGSEASON': line[12],
                'LASTDAYINWEEKFL': bool(line[13]),
                'LASTDAYINMONTHFL': bool(line[14]),
                'HOLIDAYFL': bool(line[15]),
                'WEEKDAYFL': bool(line[16]),
            }
            date_partitions[date_partition][date_key] = date_data
            date_idx_partitions[date_idx_partition][date_idx_key] = date_key
        for partition, partition_data in date_partitions.items():
            print(f"Populating {date_operator.name}:{partition}...")
            styx.init_data(date_operator, partition, partition_data)
        for partition, partition_data in date_idx_partitions.items():
            print(f"Populating {date_idx_operator.name}:{partition}...")
            styx.init_data(date_idx_operator, partition, partition_data)


def submit_graph(styx: SyncStyxClient):
    print(list(g.nodes.values())[0].n_partitions)
    styx.submit_dataflow(g)
    print("Graph submitted")


def ssb_init(styx: SyncStyxClient):
    styx.set_graph(g)
    styx.init_metadata(g)
    populate_customer(styx)
    populate_supplier(styx)
    populate_line_order(styx)
    populate_part(styx)
    populate_date(styx)
    time.sleep(5)
    submit_graph(styx)


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
        "SUPKEY": random.randint(1, supp_size)
    }
    choice = random.randint(1, 100)
    if choice <= 60:
        params["CUSTNAME"] = f"Customer#{str(random.randint(1, cust_size)).zfill(9)}"
    else:
        params["CUSTKEY"] = random.randint(1, cust_size)
    return payment_txn_operator, front_end_key, 'payment_txn', (params,)


def hattrick_workload_generator(proc_num):
    c = last_order_key + 1
    while True:
        front_end_key = int(f'{proc_num}{c}')
        coin = random.randint(1, 100)
        if coin < 50:
            yield get_new_line_order_transaction(front_end_key)
        else:
            yield get_payment_transaction(front_end_key)
        c += 1


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    hattrick_generator = hattrick_workload_generator(proc_num)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    start = timer()
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                if i % 100 == 0:
                    styx.send_query("SELECT COUNT(*) FROM CUSTOMER;")
                    styx.send_query("SELECT COUNT(*) FROM PART;")
                    styx.send_query("SELECT COUNT(*) FROM DATE;")
                    styx.send_query("SELECT COUNT(*) FROM SUPPLIER;")
                    styx.send_query("SELECT COUNT(*) FROM HISTORY;")
                    styx.send_query("SELECT COUNT(*) FROM LINE_ORDER;")
                time.sleep(sleep_time)
            operator, key, func_name, params = next(hattrick_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f'{func_name} {key}->{params}'}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    styx.close()
    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():
    minio = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    ssb_init(styx_client)
    del styx_client
    print('Data populated waiting for 1 minute')
    # 5 min so that the init is surely done (snapshot buckets and duckdb)
    time.sleep(60)
    # time.sleep(300 * (SF % 5))

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).to_csv(f'{SAVE_DIR}/client_requests.csv',
                            index=False)


if __name__ == '__main__':
    multiprocessing.set_start_method('fork')
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        SAVE_DIR,
        messages_per_second,
        warmup_seconds,
        threads,
        SF
    )

