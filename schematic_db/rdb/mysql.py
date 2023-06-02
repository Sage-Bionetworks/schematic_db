"""MySQLDatabase"""
from typing import Any
import pandas
import numpy
import sqlalchemy
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import exc
from schematic_db.db_schema.db_schema import (
    ColumnDatatype,
    ColumnSchema,
)
from .sql_alchemy_database import SQLAlchemyDatabase, SQLConfig
from .rdb import UpsertDatabaseError


class MySQLDatabase(SQLAlchemyDatabase):
    """MySQLDatabase
    - Represents a mysql database.
    - Implements the RelationalDatabase interface.
    - Handles MYSQL specific functionality.
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
        super().__init__(config, verbose, "mysql")

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
        for row in rows:
            try:
                self._upsert_table_row(row, table, table_name)
            except exc.SQLAlchemyError as exception:
                raise UpsertDatabaseError(table_name) from exception

    def _upsert_table_row(
        self,
        row: dict[str, Any],
        table: sqlalchemy.Table,
        table_name: str,  # pylint: disable=unused-argument
    ) -> None:
        """Upserts a row into a MySQL table

        Args:
            row (dict[str, Any]): A row of a dataframe to be upserted
            table (sqlalchemy.Table):  A sqlalchemy Table to be upserted into
            table_name (str): The name of the table to be upserted into (unused)
        """
        statement = insert(table).values(row).on_duplicate_key_update(**row)
        with self.engine.begin() as conn:
            conn.execute(statement)

    def _get_datatype(
        self, column_schema: ColumnSchema, primary_key: str, foreign_keys: list[str]
    ) -> Any:
        """
        Gets the datatype of the column based on its schema

        Args:
            column_schema (ColumnSchema): The schema of the column
            primary_key (str): The primary key fo the column (unused)
            foreign_keys (list[str]): A list of foreign keys for the the column

        Returns:
            Any: The SQLAlchemy datatype
        """
        datatypes = {
            ColumnDatatype.TEXT: sqlalchemy.VARCHAR(5000),
            ColumnDatatype.DATE: sqlalchemy.Date,
            ColumnDatatype.INT: sqlalchemy.Integer,
            ColumnDatatype.FLOAT: sqlalchemy.Float,
            ColumnDatatype.BOOLEAN: sqlalchemy.Boolean,
        }
        # Keys need to be max 100 chars
        if column_schema.datatype == ColumnDatatype.TEXT and (
            column_schema.name == primary_key or column_schema.name in foreign_keys
        ):
            return sqlalchemy.VARCHAR(100)
        # Strings that need to be indexed need to be max 1000 chars
        if column_schema.index and column_schema.datatype == ColumnDatatype.TEXT:
            return sqlalchemy.VARCHAR(1000)

        # Otherwise use datatypes dict
        return datatypes[column_schema.datatype]
