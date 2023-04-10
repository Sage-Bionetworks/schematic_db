"""MySQLDatabase"""
from typing import Any
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from schematic_db.db_schema.db_schema import (
    ColumnDatatype,
    ColumnSchema,
)
from .sql_alchemy_database import SQLAlchemyDatabase, SQLConfig


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

    def _upsert_table_row(
        self,
        row: dict[str, Any],
        table: sa.table,
        table_name: str,  # pylint: disable=unused-argument
    ) -> None:
        statement = insert(table).values(row).on_duplicate_key_update(**row)
        with self.engine.connect().execution_options(autocommit=True) as conn:
            conn.execute(statement)

    def _get_datatype(
        self, column_schema: ColumnSchema, primary_key: str, foreign_keys: list[str]
    ) -> Any:
        datatypes = {
            ColumnDatatype.TEXT: sa.VARCHAR(5000),
            ColumnDatatype.DATE: sa.Date,
            ColumnDatatype.INT: sa.Integer,
            ColumnDatatype.FLOAT: sa.Float,
            ColumnDatatype.BOOLEAN: sa.Boolean,
        }
        # Keys need to be max 100 chars
        if column_schema.datatype == ColumnDatatype.TEXT and (
            column_schema.name == primary_key or column_schema.name in foreign_keys
        ):
            return sa.VARCHAR(100)
        # Strings that need to be indexed need to be max 1000 chars
        if column_schema.index and column_schema.datatype == ColumnDatatype.TEXT:
            return sa.VARCHAR(1000)

        # Otherwise use datatypes dict
        return datatypes[column_schema.datatype]
