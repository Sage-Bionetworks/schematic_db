"""RelationalDatabase"""
from abc import ABC, abstractmethod
import pandas as pd
from schematic_db.db_config.db_config import DBObjectConfig


class RelationalDatabase(ABC):
    """An interface for relational database types"""

    @abstractmethod
    def get_table_names(self) -> list[str]:
        """Gets the names of the tables in the database

        Returns:
            list[str]: A list of table names
        """

    @abstractmethod
    def get_table_config(self, table_name: str) -> DBObjectConfig:
        """Returns a DBObjectConfig created from the current database table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[DBObjectConfig]: The config for the given table
        """

    @abstractmethod
    def execute_sql_query(self, query: str) -> pd.DataFrame:
        """Executes a valid SQL statement
        Should be used when a result is expected.


        Args:
            query (str): A SQL statement

        Returns:
            pd.DataFrame: The table
        """

    @abstractmethod
    def query_table(self, table_name: str) -> pd.DataFrame:
        """Queries a whole table

        Args:
            table_name (str): The name of the table

        Returns:
            pd.DataFrame: The table
        """

    @abstractmethod
    def add_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Adds a table to the schema

        Args:
            table_name (str): The name of the table
            table_config (DBObjectConfig): The config for the table being added
        """

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """Drops a table from the schema
        Args:
            table_name (str): The id(name) of the table to be dropped
        """

    @abstractmethod
    def drop_all_tables(self) -> None:
        """Drops all tables from the database"""

    @abstractmethod
    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Upserts rows into the given table

        Args:
            table_name (str): The name of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame. It must contain the primary keys of the table
        """

    @abstractmethod
    def delete_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Deletes rows from the given table

        Args:
            table_name (str): The name of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame. It must contain the primary keys of the table
        """
