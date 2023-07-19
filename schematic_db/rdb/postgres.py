"""Represents a Postgres database."""
from typing import Any
import numpy
import pandas
import sqlalchemy
import sqlalchemy.dialects.postgresql
from sqlalchemy.inspection import inspect
from sqlalchemy import exc
from schematic_db.db_schema.db_schema import ColumnDatatype
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
        column_datatypes = self.column_datatypes.copy()
        column_datatypes.update(
            {
                sqlalchemy.dialects.postgresql.base.TEXT: ColumnDatatype.TEXT,
                sqlalchemy.dialects.postgresql.base.VARCHAR: ColumnDatatype.TEXT,
                sqlalchemy.dialects.postgresql.base.INTEGER: ColumnDatatype.INT,
                sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION: ColumnDatatype.FLOAT,
                sqlalchemy.dialects.postgresql.base.FLOAT: ColumnDatatype.FLOAT,
                sqlalchemy.dialects.postgresql.base.DATE: ColumnDatatype.DATE,
            }
        )
        self.column_datatypes = column_datatypes

    def upsert_table_rows(self, table_name: str, data: pandas.DataFrame) -> None:
        """Inserts and/or updates the rows of the table

        Args:
            table_name (str): The name of the table to be upserted
            data (pandas.DataFrame): The rows to be upserted

        Raises:
            UpsertDatabaseError: Raised when a SQLAlchemy error caught
        """
        table = self._get_table_object(table_name)
        data = data.replace({numpy.nan: None})
        rows = data.to_dict("records")
        table_schema = self._get_current_metadata().tables[table_name]
        primary_key = inspect(table_schema).primary_key.columns.values()[0].name
        try:
            self._upsert_table_rows(rows, table, table_name, primary_key)
        except exc.SQLAlchemyError as exception:
            raise UpsertDatabaseError(table_name) from exception

    def _upsert_table_rows(
        self,
        rows: list[dict[str, Any]],
        table: sqlalchemy.Table,
        table_name: str,
        primary_key: str,
    ) -> None:
        """Upserts a pandas dataframe into a Postgres table

        Args:
            rows (list[dict[str, Any]]): A list of rows of a dataframe to be upserted
            table (sqlalchemy.Table):  A sqlalchemy table entity to be upserted into
            table_name (str): The name of the table to be upserted into
            primary_key (str): The name fo the primary key of the table being upserted into
        """
        statement = sqlalchemy.dialects.postgresql.insert(table).values(rows)
        update_columns = {
            col.name: col for col in statement.excluded if col.name != primary_key
        }
        statement = statement.on_conflict_do_update(
            constraint=f"{table_name}_pkey", set_=update_columns
        )
        with self.engine.begin() as conn:
            conn.execute(statement)

    def query_table(self, table_name: str) -> pandas.DataFrame:
        """Queries a whole table

        Args:
            table_name (str): The name of the table to query

        Returns:
            pandas.DataFrame: The table in pandas.dataframe form
        """
        query = f'SELECT * FROM "{table_name}"'
        return self.execute_sql_query(query)
