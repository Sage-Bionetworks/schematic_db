"""RDB Queryer"""
import pandas as pd
from schematic_db.rdb.rdb import RelationalDatabase
from schematic_db.query_store.synapse_query_store import QueryStore


class DuplicateColumnError(Exception):
    """Occurs when a query results in a table with duplicate columns"""

    def __init__(self, message: str, table_name: str) -> None:
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}: {self.table_name}"


class RDBQueryer:
    """Queries a database and uploads the results to a query store."""

    def __init__(
        self,
        rdb: RelationalDatabase,
        query_store: QueryStore,
    ):
        self.rdb = rdb
        self.query_store = query_store

    def store_query_results(self, csv_path: str) -> None:
        """Stores the results of queries
        Takes a csv file with two columns named "query" and "table_name", and runs each query,
        storing the result in the query_result_store as a table.

        Args:
            csv_path (str): A path to a csv file.
        """
        csv = pd.read_csv(csv_path)
        for _, row in csv.iterrows():
            self.store_query_result(row["query"], row["table_name"])

    def store_query_result(self, query: str, table_name: str) -> None:
        """Stores the result of a query

        Args:
            query (str): A query in SQL form
            table_name (str): The name of the table the result will be stored as

        Raises:
            DuplicateColumnError: Raised when the query result has duplicate columns
        """
        query_result = self.rdb.execute_sql_query(query)
        column_names = list(query_result.columns)
        if len(column_names) != len(set(column_names)):
            raise DuplicateColumnError("Query result has duplicate columns", table_name)
        self.query_store.store_query_result(table_name, query_result)
