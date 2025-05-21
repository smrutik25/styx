import logging

import duckdb
from styx.common.query_engine_schema import ColumnSchema, NestedTableSchema


class QueryEngineTables:
    def __init__(self, db_con: duckdb.DuckDBPyConnection):
        self.db_con = db_con
        self.__tables = {}

    @property
    def tables(self):
        return self.__tables

    @staticmethod
    async def _create_column_definition(column: ColumnSchema) -> (list[str], list[str]):
        col_def = f"{column.column_name} {column.data_type}"
        if not column.nullable:
            col_def += " NOT NULL"
        if column.reference:
            col_def += f" REFERENCES {column.reference}"
        return col_def

    async def _create_table_in_duckdb(self, table_name: str, column_definitions: list[str],
                               primary_keys: list[str] = None) -> None:
        pk_constraint = ""
        if primary_keys:
            pk_constraint = f", PRIMARY KEY ({', '.join(primary_keys)})"
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS '{table_name}' ({', '.join(column_definitions)}{pk_constraint});"""
        self.db_con.execute(create_table_sql)

    async def _create_nested_table(self, source_table_name: str, column: NestedTableSchema):
        column_names = []
        column_definitions = []
        primary_keys = []
        for nested_column in column.nested_column_mapping:
            if column.primary_key:
                primary_keys.append(nested_column.column_name)
            column_definitions.append(await self._create_column_definition(nested_column))
            column_names.append(nested_column.column_name)
        self.__tables[source_table_name].setdefault("nested_tables", {})[column.unnest_table_name] = column_names
        await self._create_table_in_duckdb(column.unnest_table_name, column_definitions, primary_keys)

    async def create_table(self, table_name: str, columns: ColumnSchema):
        column_names = []
        column_definitions = []
        primary_keys = []
        self.__tables[table_name] = {}
        for column in columns:
            column_names.append(column.column_name)
            if column.primary_key:
                primary_keys.append(column.column_name)
            column_definitions.append(await self._create_column_definition(column))
            if isinstance(column, NestedTableSchema):
                await self._create_nested_table(table_name, column)
        self.__tables[table_name]["columns"] = column_names
        await self._create_table_in_duckdb(table_name, column_definitions, primary_keys)

    async def describe_table(self, table_name):
        return self.db_con.sql(f"""DESC '{table_name}';""")

    async def fetch_created_tables(self):
        return self.db_con.sql("SHOW TABLES").fetchnumpy()['name'].tolist()


