import os
import duckdb
import logging
import pandas as pd
from minio import Minio
from styx.common.stateflow_graph import StateflowGraph
from util.duckdb_ddl import QueryEngineTables
from util.snapshot_reader import MinioReader
from util.duckdb_dml import QueryEngineReadWrite

DATABASE_FILE_PATH: str = os.getenv('DATABASE_FILE_PATH', 'query-engine/data/duckdb_database.db')
MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']


class QueryEngineHandler:
    def __init__(self):
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        if os.path.exists(DATABASE_FILE_PATH):
            os.remove(DATABASE_FILE_PATH)
        self.duckdb_conn = duckdb.connect(database=DATABASE_FILE_PATH)
        self.qe_ddl = QueryEngineTables(self.duckdb_conn)
        self.qe_readwrite = QueryEngineReadWrite(self.duckdb_conn)

    async def stateflow_graph_to_tables(self, stateflow_graph: StateflowGraph) -> None:
        try:
            for operator_name, operator in iter(stateflow_graph):
                await self.qe_ddl.create_table(operator_name, operator.schema)
            created_tables = await self.qe_ddl.fetch_created_tables()
            logging.warning(f"Created tables: {", ".join(created_tables)}")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")

    async def deserialized_data_to_df(self, snapshot_data: dict[str, dict]) -> dict[str, pd.DataFrame]:
        indexes = self.qe_ddl.table_indexes
        df_data = {}
        if "nested_tables" in self.qe_ddl.tables:
            # TODO: Handle nested logic - add nested dfs to df_data
            pass
        for table_name in self.qe_ddl.tables.keys():
            data = snapshot_data[table_name]
            first_value = next(iter(data.values()))
            if isinstance(first_value, dict):
                df = pd.DataFrame.from_dict(data, orient="index")
                df.index = df.index.set_names(indexes[table_name])
                df = df.reset_index()
            else:
                # TODO: see if this is correct and covers all use cases (need to handle multiple pk case)
                df = pd.DataFrame(list(data.items()), columns=self.qe_ddl.tables[table_name]["columns"])
            df_data[table_name] = df
        return df_data

    async def load_snapshots(self, snapshot_id: str) -> None:
        minio_client = MinioReader(self.minio_client, snapshot_id)
        await minio_client.identify_minio_delta()
        snapshot_data = {}
        for operator in self.qe_ddl.operators:
            snapshot_data[operator] = await minio_client.deserialize_snapshots(operator)
        df_data = await self.deserialized_data_to_df(snapshot_data)
        self.qe_readwrite.write_to_table(df_data)

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
