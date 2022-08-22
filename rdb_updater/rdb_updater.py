"""RDB
"""
from typing import List
import pandas as pd
from db_object_config import DBObjectConfigList, DBObjectConfig
from rdb import RelationalDatabase
from manifest_store import ManifestStore
from query_store import QueryStore
from .utils import normalize_table


class UpdateDatabaseError(Exception):
    """UpdateDatabaseError"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ManifestError(Exception):
    """ManifestError"""

    def __init__(self, message, name):
        self.message = message
        self.name = name
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}:{self.name}"


class RDBUpdater:
    """Represents a relational database."""

    def __init__(
        self,
        rdb: RelationalDatabase,
        manifest_store: ManifestStore,
        query_store: QueryStore,
    ):
        self.manifest_store = manifest_store
        self.rdb = rdb
        self.query_store = query_store

    def update_all_database_tables(
        self, manifest_table_names: List[List[str]], table_configs: DBObjectConfigList
    ):
        """
        Updates all tables in the list of table_configs

        Args:
            manifest_table_names (List[List[str]]): A list where each item is a list of the
                names of tables in the manifest store
            table_configs (DBObjectConfigList): A list of generic representations of each
                table as a DBObjectConfig object. The list must be in the correct order to
                update in regards to relationships.
        """
        if len(manifest_table_names) != len(table_configs.configs):
            raise UpdateDatabaseError(
                (
                    "Length of param manifest_table_names is not equal "
                    "to length of param table_configs.configs"
                )
            )
        zipped_list = zip(manifest_table_names, table_configs.configs)
        for tup in zipped_list:
            self.update_database_table(*tup)

    def update_database_table(
        self, manifest_table_names: List[str], table_config: DBObjectConfig
    ):
        """
        Updates a table in the database based on one or more manifests.
        If any of the manifests don't exist an exception will be raised.
        If the table doesn't exist in the database it will be built with the table config.

        Args:
            manifest_table_names (List[str]): A list of the names of tables in the manifest store
            table_config (DBObjectConfig): A generic representation of the table as a
                DBObjectConfig object.
        """
        manifest_tables = [
            self.manifest_store.get_manifest_table(name, table_config)
            for name in manifest_table_names
        ]
        manifest_table = pd.concat(manifest_tables)
        manifest_table = normalize_table(manifest_table, table_config)
        self.rdb.update_table(manifest_table, table_config)

    def store_query_results(self, csv_path: str):
        """Stores the results of queries
        Takes a csv file with two columns named "query" and "table_name", and runs each query,
        storing the result in the query_result_store as a table.

        Args:
            csv_path (str): A path to a csv file.
        """
        csv = pd.read_csv(csv_path)
        for _, row in csv.iterrows():
            self.store_query_result(row["query"], row["table_name"])

    def store_query_result(self, query: str, table_name: str):
        """Stores the result of a query

        Args:
            query (str): A query in SQL form
            table_name (str): The name of the table the result will be stored as
        """
        query_result = self.rdb.execute_sql_query(query)
        self.query_store.store_query_result(table_name, query_result)
