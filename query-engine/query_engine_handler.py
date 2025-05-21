import os
import duckdb
import logging
from minio import Minio
from styx.common.stateflow_graph import StateflowGraph
from util.duckdb_ddl import QueryEngineTables
from util.snapshot_reader import MinioReader

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
        self.operator_tables: dict = {}
        self.qe_ddl = QueryEngineTables(self.duckdb_conn)

    async def stateflow_graph_to_tables(self, stateflow_graph: StateflowGraph):
        try:
            for operator_name, operator in iter(stateflow_graph):
                await self.qe_ddl.create_table(operator_name, operator.schema)
        except Exception as e:
            logging.error(f"Error creating tables: {e}")
        self.operator_tables = self.qe_ddl.tables
        logging.warning(f"Operators: {", ".join(self.operator_tables)}")
        created_tables = await self.qe_ddl.fetch_created_tables()
        logging.warning(f"Tables in database: {", ".join(created_tables)}")

    async def load_snapshots(self, snapshot_id: str):
        minio_client = MinioReader(self.minio_client, snapshot_id)
        await minio_client.identify_minio_delta()
        for operator in self.operator_tables.keys():
            res = await minio_client.deserialize_snapshots(operator)
            logging.warning(f"Merged snapshot partitions for {operator}: {len(res)}")

