"""QueryStore"""
from abc import ABC, abstractmethod
import pandas as pd


class QueryStore(ABC):  # pylint: disable=too-few-public-methods
    """An interface for Query Store objects"""

    @abstractmethod
    def store_query_result(self, table_name: str, query_result: pd.DataFrame) -> None:
        """Stores The result of a query

        Args:
            table_name (str): The name of the table the result will be stored as
            query_result (pd.DataFrame): The query result in table form
        """

    @abstractmethod
    def get_table_names(self) -> list[str]:
        """Gets the names of the tables in the store

        Returns:
            list[str]: A list of table names
        """

    @abstractmethod
    def delete_table(self, table_name: str) -> None:
        """Deletes the table from the store
        Args:
            table_name (str): The name of the table to delete
        """
