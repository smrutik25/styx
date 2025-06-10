import duckdb
import logging
import pandas as pd

from .duckdb_ddl import QueryEngineTables


class QueryEngineReadWrite:
    def __init__(self, db_con: duckdb.DuckDBPyConnection):
        self.db_con = db_con

    @staticmethod
    def _generate_query(table_name: str, tables: dict) -> str:
        insert_str = "INSERT OR REPLACE INTO "
        if "primary_keys" not in tables[table_name]:
            insert_str = "INSERT INTO "
        return (f"{insert_str} {table_name} ({",".join(tables[table_name]["columns"])}) "
                f"SELECT {",".join(tables[table_name]["df_columns"])} FROM df")

    def write_to_table(self, data: dict[str, pd.DataFrame], tables: dict) -> None:
        write_cursor = self.db_con.cursor()
        try:
            write_cursor.begin()
            for table_name in data.keys():
                query = self._generate_query(table_name, tables)
                df = data[table_name]
                write_cursor.execute(query)
            write_cursor.commit()
        except Exception as e:
            logging.error(f"Error in duckdb upsert: {e}")
        finally:
            write_cursor.close()

    def read_from_table(self, query):
        read_cursor = self.db_con.cursor()
        try:
            query_result = read_cursor.execute(query).fetchall()
            return query_result
        except Exception as e:
            logging.error(f"Error in duckdb read: {e}")
        finally:
            read_cursor.close()
