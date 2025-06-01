import logging

import duckdb
from styx.common.query_engine_schema import ColumnSchema, NestedTableSchema


class QueryEngineTables:
    def __init__(self, db_con: duckdb.DuckDBPyConnection):
        self.db_con = db_con
        self.__tables: dict = {}
        self.__table_indexes: dict = {}
        self.__operators: list = []

    @property
    def tables(self):
        return self.__tables

    @property
    def table_indexes(self):
        return self.__table_indexes

    @property
    def operators(self):
        return self.__operators

    @staticmethod
    def _check_datetime(column: ColumnSchema) -> str:
        if column.datetime_format:
            return f"strptime({column.column_name}, '{column.datetime_format}') AS {column.column_name}"
        return column.column_name

    @staticmethod
    def _get_col_def(column: ColumnSchema) -> str:
        col_def = f"{column.column_name} {column.data_type}"
        if not column.nullable:
            col_def += " NOT NULL"
        if column.reference:
            col_def += f" REFERENCES {column.reference}"
        return col_def

    def _create_column_definitions(self, table_name: str, columns: ColumnSchema) -> (list[str], list[str]):
        column_names = []
        column_definitions = []
        primary_keys = []
        df_column_names = []
        for column in columns:
            column_names.append(column.column_name)
            df_column_names.append(self._check_datetime(column))
            if column.primary_key:
                primary_keys.append(column.column_name)
            column_definitions.append(self._get_col_def(column))
            if isinstance(column, NestedTableSchema):
                nested_column_names, nested_df_column_names, nested_col_defs, nested_primary_keys = (
                    self._create_column_definitions(column.unnest_table_name, column.nested_column_mapping))
                self.__tables[table_name].setdefault("nested_tables", {})[
                    column.unnest_table_name] = {"columns": nested_column_names,
                                                 "df_columns": nested_df_column_names,
                                                 "primary_keys": nested_primary_keys}
                self.create_table(column.unnest_table_name, column.nested_column_mapping, table_type="nested")
        return column_names, df_column_names, column_definitions, primary_keys

    def _create_table_in_duckdb(self, table_name: str, column_definitions: list[str],
                                primary_keys: list[str] = None) -> None:
        pk_constraint = ""
        if primary_keys:
            self.__table_indexes[table_name] = primary_keys
            pk_constraint = f", PRIMARY KEY ({', '.join(primary_keys)})"
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS '{table_name}' ({', '.join(column_definitions)}{pk_constraint});"""
        self.db_con.execute(create_table_sql)

    def create_table(self, table_name: str, columns: ColumnSchema, table_type: str = "base"):
        if table_type == "base":
            self.__tables[table_name] = {}
            column_names, df_column_names, column_definitions, primary_keys = (
                self._create_column_definitions(table_name, columns))
            self.__operators.append(table_name)
            self.__tables[table_name]["columns"] = column_names
            self.__tables[table_name]["df_columns"] = df_column_names
            self.__tables[table_name]["primary_keys"] = primary_keys
        else:
            column_names, df_column_names, column_definitions, primary_keys = (
                self._create_column_definitions(table_name, columns))
        logging.warning(f"Creating table: {table_name}")
        self._create_table_in_duckdb(table_name, column_definitions, primary_keys)

    async def describe_table(self, table_name):
        return self.db_con.sql(f"""DESC '{table_name}';""")

    async def fetch_created_tables(self):
        return self.db_con.sql("SHOW TABLES").fetchnumpy()['name'].tolist()


