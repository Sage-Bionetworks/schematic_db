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
