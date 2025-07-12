import asyncio
import os
import time

import duckdb
import logging
import polars as pl
import numpy as np
from minio import Minio
from styx.common.stateflow_graph import StateflowGraph
from util.duckdb_ddl import QueryEngineTables
from util.snapshot_reader import MinioReader
from util.duckdb_dml import QueryEngineReadWrite

DATABASE_FILE_PATH: str = f"{os.getenv('VOLUME_MOUNT_PATH', 'data')}/duckdb_database.db"
MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
CHUNK_SIZE = 1000000
RECOVERY_TABLE = "recovery"
RECOVERY_COLUMN = "last_snapshot"


class QueryEngineHandler:
    def __init__(self):
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.duckdb_conn = duckdb.connect(database=DATABASE_FILE_PATH)
        self.qe_ddl = QueryEngineTables(self.duckdb_conn)
        self.qe_readwrite = QueryEngineReadWrite(self.duckdb_conn)

    def close_connection(self):
        self.duckdb_conn.close()

    async def recovery_mode(self):
        created_tables = await self.qe_ddl.fetch_created_tables()
        if created_tables:
            self.qe_ddl.set_recovery_mode()
            return True
        else:
            return False

    async def stateflow_graph_to_tables(self, stateflow_graph: StateflowGraph) -> None:
        try:
            for operator_name, operator in iter(stateflow_graph):
                if operator.schema:
                    self.qe_ddl.create_table(operator_name, operator.schema)
            self.qe_readwrite.execute_query(f"CREATE TABLE IF NOT EXISTS {RECOVERY_TABLE} ({RECOVERY_COLUMN} INT)")
            created_tables = await self.qe_ddl.fetch_created_tables()
            logging.warning(f"Tables in database: {", ".join(created_tables)}")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")

    @staticmethod
    def _split_index(df: pl.DataFrame, composite_key: list) -> pl.DataFrame:
        df = df.with_columns(
            pl.col("index")
            .str.split_exact(":", len(composite_key) - 1)
            .alias("composite_struct")
            .struct.rename_fields(composite_key)
        )
        df = df.unnest("composite_struct")
        df = df.drop("index")
        return df

    def _create_df(self, data, table_name, table_index) -> pl.DataFrame:
        columns = self.qe_ddl.tables[table_name]["columns"]
        if not len(data):
            df = pl.DataFrame(data=[], schema=columns)
        else:
            first_value = next(iter(data.values()))
            if isinstance(first_value, dict):
                df = pl.DataFrame([{"index": k, **v} for k, v in data.items()])
            else:
                columns = ["index"] + columns[1:]
                df = pl.DataFrame([dict(zip(columns, row)) for row in data.items()])

            if len(table_index) > 1:
                df = self._split_index(df, table_index)
            else:
                df = df.rename({"index": table_index[0]})

        return df

    async def deserialized_data_to_df(self, snapshot_data: dict[str, dict]) -> dict[str, pl.DataFrame] | None:
        indexes = self.qe_ddl.table_indexes
        tables = self.qe_ddl.tables
        df_data = {}
        if "nested_tables" in tables:
            # TODO: Handle nested logic - add nested dfs to df_data
            pass
        for table_name in tables.keys():
            data = snapshot_data[table_name]
            table_index = indexes[table_name]
            logging.warning(f"Table: {table_name}, rows to upsert: {len(data)}")
            df = self._create_df(data, table_name, table_index)
            df_data[table_name] = df
        return df_data

    async def init_data(self, snapshot_id: str):
        minio_client = MinioReader(self.minio_client, snapshot_id)
        operator_list = self.qe_ddl.operators
        await minio_client.identify_minio_delta(operator_list)
        tables = self.qe_ddl.tables
        # TODO: Handle nested case
        for operator in operator_list:
            table_index = self.qe_ddl.table_indexes[operator]
            async for partition_data in minio_client.deserialize_snapshot_partition(operator):
                await asyncio.sleep(0)
                logging.warning(f"Table: {operator}, rows to insert: {len(partition_data)}")
                start_time = time.perf_counter()
                df_data = self._create_df(partition_data, operator, table_index)
                if df_data.height > 0:
                    end_time = time.perf_counter()
                    logging.warning(f"Dataframe created, took {end_time - start_time}s, inserting into table")
                    num_chunks = int(np.ceil(df_data.height / CHUNK_SIZE))
                    for i in range(num_chunks):
                        chunk = df_data.slice(i * CHUNK_SIZE, CHUNK_SIZE)
                        self.qe_readwrite.init_data(chunk, operator, tables)
                        logging.warning(
                            f"Inserted into table, took {time.perf_counter() - end_time}s")
        start_time = time.perf_counter()
        logging.warning("Loaded init data, creating indexes")
        self.qe_ddl.add_constraints()
        logging.warning(f"Added indexes, took {time.perf_counter() - start_time}s")
        self.qe_readwrite.execute_query(f"INSERT INTO {RECOVERY_TABLE} ({RECOVERY_COLUMN}) VALUES (0)")

    async def _check_prev_snapshot(self, snapshot_id: str):
        current_snapshot = int(snapshot_id)
        prev_snapshot = self.qe_readwrite.read_from_table(f"SELECT * FROM {RECOVERY_TABLE}")[0][0]
        while current_snapshot > prev_snapshot + 1:
            await self.load_snapshots(prev_snapshot + 1)
            prev_snapshot += 1
            logging.warning(f"Loaded previous snapshot in recovery: {prev_snapshot}")

    async def load_snapshots(self, snapshot_id: str) -> None:
        await self._check_prev_snapshot(snapshot_id)
        minio_client = MinioReader(self.minio_client, snapshot_id)
        operator_list = self.qe_ddl.operators
        await minio_client.identify_minio_delta(operator_list)
        snapshot_data = {}
        for operator in operator_list:
            snapshot_data[operator] = await minio_client.deserialize_snapshots(operator)
        df_data = await self.deserialized_data_to_df(snapshot_data)
        tables = self.qe_ddl.tables
        if df_data:
            self.qe_readwrite.write_to_table(df_data, tables)
        self.qe_readwrite.execute_query(f"UPDATE {RECOVERY_TABLE} SET {RECOVERY_COLUMN} = {int(snapshot_id)}")

    async def get_query_result(self, query: str):
        # TODO: User can define format of read output
        """
        duckdb.sql("SELECT 42").fetchall()   # Python objects
        duckdb.sql("SELECT 42").df()         # Pandas DataFrame
        duckdb.sql("SELECT 42").pl()         # Polars DataFrame
        duckdb.sql("SELECT 42").arrow()      # Arrow Table
        duckdb.sql("SELECT 42").fetchnumpy() # NumPy Arrays
        """
        return self.qe_readwrite.read_from_table(query)
