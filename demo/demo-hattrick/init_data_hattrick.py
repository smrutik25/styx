import os
import csv
import time
from tqdm import tqdm
from styx.client import SyncStyxClient

from functions import (customer_operator, customer_idx_operator, date_operator, date_idx_operator,
                       line_order_operator, part_operator, supplier_operator, supplier_idx_operator)


def populate_customer(styx: SyncStyxClient, script_path, data_file_path, partitions):
    with open(os.path.join(script_path, f"{data_file_path}/customer.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        cust_partitions: dict[int, dict] = {p: {} for p in range(partitions)}
        cust_idx_partitions: dict[int, dict] = {p: {} for p in range(partitions)}
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


def populate_supplier(styx: SyncStyxClient, script_path, data_file_path, partitions):
    with open(os.path.join(script_path, f"{data_file_path}/supplier.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        sup_partitions: dict[int, dict] = {p: {} for p in range(partitions)}
        sup_idx_partitions: dict[int, dict] = {p: {} for p in range(partitions)}
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


def populate_line_order(styx: SyncStyxClient, script_path, data_file_path, partitions):
    with open(os.path.join(script_path, f"{data_file_path}/lineorder.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        partitions: dict[int, dict] = {p: {} for p in range(partitions)}
        for _, line in tqdm(enumerate(reader), desc="Populating Lineorder Data"):
            line_order_key = f"{line[0]}:{line[1]}"
            partition: int = styx.get_operator_partition(line_order_key, line_order_operator)
            line_order_data = {
                'CUSTKEY': int(line[2]),
                'PARTKEY': int(line[3]),
                'SUPPKEY': int(line[4]),
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


def populate_part(styx: SyncStyxClient, script_path, data_file_path, partitions):
    with open(os.path.join(script_path, f"{data_file_path}/part.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        partitions: dict[int, dict] = {p: {} for p in range(partitions)}
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


def populate_date(styx: SyncStyxClient, script_path, data_file_path, partitions):
    with open(os.path.join(script_path, f"{data_file_path}/date.bin"), "r") as f:
        reader = csv.reader(f, delimiter='!')
        date_partitions: dict[int, dict] = {p: {} for p in range(partitions)}
        date_idx_partitions: dict[int, dict] = {p: {} for p in range(partitions)}
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


def main(styx: SyncStyxClient, partitions, script_path, data_file_path):
    populate_customer(styx, script_path, data_file_path, partitions)
    populate_supplier(styx, script_path, data_file_path, partitions)
    populate_line_order(styx, script_path, data_file_path, partitions)
    populate_part(styx, script_path, data_file_path, partitions)
    populate_date(styx, script_path, data_file_path, partitions)
