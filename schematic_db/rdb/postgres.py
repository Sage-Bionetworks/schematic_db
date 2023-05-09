"""Represents a Postgres database."""
from typing import Any
import numpy as np
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgres
import pandas as pd
from sqlalchemy.inspection import inspect
from sqlalchemy import exc
from .sql_alchemy_database import SQLAlchemyDatabase, SQLConfig
from .rdb import UpsertDatabaseError


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

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Inserts and/or updates the rows of the table

        Args:
            table_name (str): The name of the table to be upserted
            data (pd.DataFrame): The rows to be upserted

        Raises:
            UpsertDatabaseError: Raised when a SQLAlchemy error caught
        """
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        table_schema = self.metadata.tables[table_name]
        primary_key = inspect(table_schema).primary_key.columns.values()[0].name
        try:
            self._upsert_table_rows(rows, table, table_name, primary_key)
        except exc.SQLAlchemyError as exception:
            raise UpsertDatabaseError(table_name) from exception

    def _upsert_table_rows(
        self,
        rows: list[dict[str, Any]],
        table: sa.table,
        table_name: str,
        primary_key: str,
    ) -> None:
        """Upserts a pandas dataframe into a Postgres table

        Args:
            rows (list[dict[str, Any]]): A list of rows of a dataframe to be upserted
            table (sa.table):  A synapse table entity to be upserted into
            table_name (str): The name of the table to be upserted into
            primary_key (str): The name fo the primary key of the table being upserted into
        """
        statement = sa_postgres.insert(table).values(rows)
        update_columns = {
            col.name: col for col in statement.excluded if col.name != primary_key
        }
        statement = statement.on_conflict_do_update(
            constraint=f"{table_name}_pkey", set_=update_columns
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
