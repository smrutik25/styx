import asyncio
import os
import time

import duckdb
import logging
import pandas as pd
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
            created_tables = await self.qe_ddl.fetch_created_tables()
            logging.warning(f"Tables in database: {", ".join(created_tables)}")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")

    @staticmethod
    async def _split_index(df: pd.DataFrame, composite_key: list):
        df[composite_key] = df['index'].str.split(':', expand=True)
        df = df.drop(columns=['index'])
        return df

    async def _create_df(self, data, table_name, table_index):
        if not len(data):
            df = pd.DataFrame(list(data.items()), columns=self.qe_ddl.tables[table_name]["columns"])
        else:
            first_value = next(iter(data.values()))
            if isinstance(first_value, dict):
                df = pd.DataFrame.from_dict(data, orient="index").reset_index()
            else:
                df = pd.DataFrame(list(data.items()), columns=self.qe_ddl.tables[table_name]["columns"])
            if len(table_index) > 1:
                df = await self._split_index(df, table_index)
            else:
                df = df.rename(columns={'index': table_index[0]})
        return df

    async def deserialized_data_to_df(self, snapshot_data: dict[str, dict]) -> dict[str, pd.DataFrame] | None:
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
            df = await self._create_df(data, table_name, table_index)
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
                df_data = await self._create_df(partition_data, operator, table_index)
                if not df_data.empty:
                    end_time = time.perf_counter()
                    logging.warning(f"Dataframe created, took {end_time - start_time}s, inserting into table")
                    num_chunks = int(np.ceil(len(df_data) / CHUNK_SIZE))
                    for i in range(num_chunks):
                        chunk = df_data.iloc[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]
                        self.qe_readwrite.init_data(chunk, operator, tables)
                        logging.warning(
                            f"Inserted into table, took {time.perf_counter() - end_time}s")
        start_time = time.perf_counter()
        logging.warning("Loaded init data, creating indexes")
        self.qe_ddl.add_constraints()
        logging.warning(f"Added indexes, took {time.perf_counter() - start_time}s")

    async def load_snapshots(self, snapshot_id: str) -> None:
        minio_client = MinioReader(self.minio_client, snapshot_id)
        operator_list = self.qe_ddl.operators
        await minio_client.identify_minio_delta(operator_list)
        snapshot_data = {}
        for operator in self.qe_ddl.operators:
            snapshot_data[operator] = await minio_client.deserialize_snapshots(operator)
        df_data = await self.deserialized_data_to_df(snapshot_data)
        tables = self.qe_ddl.tables
        if df_data:
            self.qe_readwrite.write_to_table(df_data, tables)

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
