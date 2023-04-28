"""Represents a Postgres database."""
from typing import Any
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgres
import pandas as pd
from .sql_alchemy_database import SQLAlchemyDatabase, SQLConfig


class PostgresDatabase(SQLAlchemyDatabase):
    """PostgresDatabase
    - Represents a Postgres database.
    - Implements the RelationalDatabase interface.
    - Handles Postgres specific functionality.
    """

    def __init__(
        self,
        config: SQLConfig,
        verbose: bool = False,
    ):
        """Init

        Args:
            config (SQLConfig): A MySQL config
            verbose (bool): Sends much more to logging.info
        """
        super().__init__(config, verbose, "postgresql")

    def _upsert_table_row(
        self, row: dict[str, Any], table: sa.table, table_name: str
    ) -> None:
        """Upserts a row into a Postgres table

        Args:
            row (dict[str, Any]): A row of a dataframe to be upserted
            table (sa.table):  A synapse table entity to be upserted into
            table_name (str): The name of the table to be upserted into
        """
        statement = sa_postgres.insert(table).values(row)
        statement = statement.on_conflict_do_update(
            constraint=f"{table_name}_pkey", set_=row
        )
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(statement)

    def query_table(self, table_name: str) -> pd.DataFrame:
        """Queries a whole table

        Args:
            table_name (str): The name of the table to query

        Returns:
            pd.DataFrame: The table in pandas.dataframe form
        """
        query = f'SELECT * FROM "{table_name}"'
        return self.execute_sql_query(query)
