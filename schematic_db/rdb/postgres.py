"""Represents a Postgres database."""
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgres
import pandas as pd
import numpy as np
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
            config (MySQLConfig): A MySQL config
            verbose (bool): Sends much more to logging.info
        """
        super().__init__(config, verbose, "postgresql")

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Inserts and/or updates the rows of the table

        Args:
            table_name (str): _The name of the table to be upserted
            data (pd.DataFrame): The rows to be upserted
        """
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        for row in rows:
            statement = sa_postgres.insert(table).values(row)
            statement = statement.on_conflict_do_update(
                constraint=f"{table_name}_pkey", set_=row
            )
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)

    def query_table(self, table_name: str) -> pd.DataFrame:
        query = f'SELECT * FROM "{table_name}"'
        return self.execute_sql_query(query)
