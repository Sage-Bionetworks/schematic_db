"""Represents a Postgres database."""
from typing import Any
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgres
import pandas as pd
import numpy as np
from schematic_db.db_config import DBDatatype, DBAttributeConfig
from .mysql import MySQLDatabase, MySQLConfig

POSTGRES_DATATYPES = {
    DBDatatype.TEXT: sa.VARCHAR,
    DBDatatype.DATE: sa.Date,
    DBDatatype.INT: sa.Integer,
    DBDatatype.FLOAT: sa.Float,
    DBDatatype.BOOLEAN: sa.Boolean,
}


class PostgresDatabase(MySQLDatabase):
    """PostgresDatabase
    - Represents a Postgres database.
    - Implements the RelationalDatabase interface.
    - Handles Postgres specific functionality.
    """

    def __init__(
        self,
        config: MySQLConfig,
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

    def _get_datatype(self, attribute: DBAttributeConfig) -> Any:
        POSTGRES_DATATYPES.get(attribute.datatype)
