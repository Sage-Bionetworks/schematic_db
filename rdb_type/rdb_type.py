"""RDBType
"""
from abc import ABC, abstractmethod
from typing import List
import pandas as pd
from db_object_config import DBObjectConfig

class RDBType(ABC):
    """ An interface for relational database types
    """

    @abstractmethod
    def execute_sql_statement(self, statement: str):
        """Executes a valid SQL statement
        Should be used when a result isn't expected.
        rdbtype.execute_sql_statement("DROP TABLE IF EXISTS table_name;")

        Args:
            statement (str): A SQL statement

        Returns:
            sqlalchemy.engine.cursor.LegacyCursorResult: A SQL result
        """

    @abstractmethod
    def execute_sql_query(self, query: str) -> pd.DataFrame:

        """Executes a valid SQL statement
        Should be used when a result is expected.

        rdbtype.execute_sql_query("SHOW DATABASES;")

        Args:
            query (str): A SQL statement

        Returns:
            pd.DataFrame: The table
        """

    @abstractmethod
    def query_table(self, table_name: str, table_config: DBObjectConfig) -> pd.DataFrame:
        """Queries the whole table

        Args:
            table_name (str): The name of the table
            table_config (DBObjectConfig): A generic representation of the table as a
            DBObjectConfig object.

        Returns:
            pd.DataFrame: The table
        """

    @abstractmethod
    def get_table_id_from_name(self, table_name: str) -> str:
        """Gets the tables id
        In SQL databases  the id and name are the same. In some rdb types such as Synapse the
        id will be different ie. the Synapse id.

        Args:
            table_name (str): The name of the table

        Returns:
            str: The id of the table
        """

    @abstractmethod
    def get_table_name_from_id(self, table_id: str) -> str:
        """Gets the tables name
        The inverse of get_table_id_from_name

        Args:
            table_id (str): The id of the table

        Returns:
            str: The name of the table
        """

    @abstractmethod
    def add_table(self, table_name: str, table_config: DBObjectConfig):
        """Adds a table to the schema

        rdbtype.add_table(
            "table_name",
            DBObjectConfig(
                name = "table_one",
                attributes = [
                    DBAttributeConfig(name="pk_col", datatype=DBDatatype.Text),
                    DBAttributeConfig(name="string_col", datatype=DBDatatype.Text),
                    DBAttributeConfig(name="int_col", datatype=DBDatatype.Int),
                    DBAttributeConfig(name="double_col", datatype=DBDatatype.Float),
                    DBAttributeConfig(name="date_col", datatype=DBDatatype.Date),
                    DBAttributeConfig(name="bool_col", datatype=DBDatatype.Boolean)
                ],
                primary_keys = ["pk_col"],
                foreign_keys = []
            )
        )

        Args:
            table_name (str): The id(name) of the table to be added
            table_config (DBObjectConfig): A generic representation of the table as a
            DBObjectConfig object.
        """

    @abstractmethod
    def drop_table(self, table_name: str):
        """Drops a table from the schema

        rdbtype.drop_table("table_name")

        Args:
            table_name (str): The id(name) of the table to be dropped
        """

    @abstractmethod
    def add_table_column(self, table_name: str, column_name: str, datatype: str):
        """Adds a column to the given table

        rdbtype.add_table_column("table_name", "name", "varchar(100)")

        Args:
            table_name (str): The id(name) of the table the column will be added to
            column_name (str): The name of the column being added
            datatype (str): The SQL datatype of the column being added
        """

    @abstractmethod
    def drop_table_column(self, table_name: str, column_name: str):
        """Removes a column from the given table

        Args:
            table_name (str): The id(name) of the table the column will be removed from
            column_name (str): The name of the column being removed
        """

    @abstractmethod
    def delete_table_rows(self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig):
        """Deletes rows from the given table

        Args:
            table_name (str): The name of the table the rows will be deleted from
            data (pd.DataFrame): A pandas.DataFrame. It must contain the primary keys of the table
            table_config (DBObjectConfig): A generic representation of the table as a
            DBObjectConfig object.
        """

    @abstractmethod
    def upsert_table_rows(self, table_name: str, data: pd.DataFrame):
        """Updates or inserts rows into the given table

        rdbtype.upsert_table_rows(
            "table_name",
            DataFrame({
                "string_col": ["a","b","c"],
                "int_col": [1,2,3]
            })
        )

        Args:
            table_name (str): The id(name) of the table the rows will be updated or added to
            data (pd.DataFrame): A pandas.DataFrame
        """

    @abstractmethod
    def get_table_names(self) -> List[str]:
        """Gets the names of the tables in the database

        Returns:
            List[str]: A list of table names
        """

    @abstractmethod
    def get_column_names_from_table(self, table_name: str) -> List[str]:
        """Gets the names of the columns from the given table

        Args:
            table_name (str): The id(name) of the table the columns will be returned from

        Returns:
            List[str]: A list fo column names
        """
