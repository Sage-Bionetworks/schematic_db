"""SQLAlchemy"""
from typing import Any
from dataclasses import dataclass
import pandas as pd
import numpy as np
import sqlalchemy as sa
import sqlalchemy_utils.functions
from sqlalchemy.inspection import inspect
from schematic_db.db_schema.db_schema import (
    TableSchema,
    ColumnDatatype,
    ColumnSchema,
    ForeignKeySchema,
)
from .rdb import RelationalDatabase, UpsertDatabaseError


class DataframeKeyError(Exception):
    """DataframeKeyError"""

    def __init__(self, message: str, key: str) -> None:
        self.message = message
        self.key = key
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}:{self.key}"


def create_foreign_key_column(
    name: str,
    datatype: str,
    foreign_table_name: str,
    foreign_table_column: str,
) -> sa.Column:
    """Creates a sqlalchemy.column that is a foreign key

    Args:
        name (str): The name of the column
        datatype (str): The SQL datatype of the column
        foreign_table_name (str): The name of the table the foreign key is referencing
        foreign_table_column (str): The name of the column the foreign key is referencing

    Returns:
        sa.Column: A sqlalchemy.column
    """
    col = sa.Column(
        name,
        datatype,
        sa.ForeignKey(
            f"{foreign_table_name}.{foreign_table_column}",
            ondelete="CASCADE",
        ),
        nullable=True,
    )
    return col


def create_foreign_key_configs(
    table_schema: sa.sql.schema.Table,
) -> list[ForeignKeySchema]:
    """Creates a list of foreign key configs from a sqlalchemy table schema

    Args:
        table_schema (sa.sql.schema.Table): A sqlalchemy table schema

    Returns:
        list[ForeignKeySchema]: A list of foreign key configs
    """
    foreign_keys = inspect(table_schema).foreign_keys
    return [
        ForeignKeySchema(
            name=key.parent.name,
            foreign_table_name=key.column.table.name,
            foreign_column_name=key.column.name,
        )
        for key in foreign_keys
    ]


def create_column_schemas(
    table_schema: sa.sql.schema.Table, indices: list[str]
) -> list[ColumnSchema]:
    """Creates a list of column schemas from a sqlalchemy table schema

    Args:
        table_schema (sa.sql.schema.Table):A sqlalchemy table schema

    Returns:
        list[ColumnSchema]: A list of column schemas
    """
    datatypes = {
        sa.String: ColumnDatatype.TEXT,
        sa.VARCHAR: ColumnDatatype.TEXT,
        sa.Date: ColumnDatatype.DATE,
        sa.Integer: ColumnDatatype.INT,
        sa.Float: ColumnDatatype.FLOAT,
        sa.Boolean: ColumnDatatype.BOOLEAN,
    }
    columns = table_schema.c
    return [
        ColumnSchema(
            name=column.name,
            datatype=datatypes[type(column.type)],
            required=not column.nullable,
            index=column.name in indices,
        )
        for column in columns
    ]


@dataclass
class SQLConfig:
    """A config for a SQL database."""

    username: str
    password: str
    host: str
    name: str


class SQLAlchemyDatabase(
    RelationalDatabase
):  # pylint: disable=too-many-instance-attributes
    """
    - Represents a sql database via sqlalchemy.
    - Implements the RelationalDatabase interface.
    - Handles generic SQL specific functionality.
    - Not intended to be used, only inherited from
    """

    def __init__(
        self, config: SQLConfig, verbose: bool = False, db_type_string: str = "sql"
    ):
        """Init

        Args:
            config (MySQLConfig): A MySQL config
            verbose (bool): Sends much more to logging.info
            db_type_string (str): They type of database in string form
        """
        self.username = config.username
        self.password = config.password
        self.host = config.host
        self.name = config.name
        self.verbose = verbose
        self.db_type_string = db_type_string

        self.create_database()
        self.metadata = sa.MetaData()

    def drop_database(self) -> None:
        """Drops the database from the server"""
        sqlalchemy_utils.functions.drop_database(self.engine.url)

    def create_database(self) -> None:
        """Creates the database"""
        url = f"{self.db_type_string}://{self.username}:{self.password}@{self.host}/{self.name}"
        db_exists = sqlalchemy_utils.functions.database_exists(url)
        if not db_exists:
            sqlalchemy_utils.functions.create_database(url)
        engine = sa.create_engine(url, encoding="utf-8", echo=self.verbose)
        self.engine = engine

    def drop_all_tables(self) -> None:
        for tbl in reversed(self.metadata.sorted_tables):
            self.drop_table(tbl)

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        result = self._execute_sql_statement(query).fetchall()
        table = pd.DataFrame(result)
        return table

    def get_table_schema(self, table_name: str) -> TableSchema:
        """Creates a table schema from a sqlalchemy table schema

        Args:
            table_name (str): The name of the table

        Returns:
            TableSchema: A schema for the table
        """
        table_schema = self.metadata.tables[table_name]
        primary_key = inspect(table_schema).primary_key.columns.values()[0].name
        indices = self._get_column_indices(table_name)
        return TableSchema(
            name=table_name,
            primary_key=primary_key,
            foreign_keys=create_foreign_key_configs(table_schema),
            columns=create_column_schemas(table_schema, indices),
        )

    def drop_table(self, table_name: str) -> None:
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        table.drop(self.engine)
        self.metadata.clear()

    def delete_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        i = sa.inspect(table)
        pkey_column = list(column for column in i.columns if column.primary_key)[0]
        values = data[pkey_column.name].values.tolist()
        statement = sa.delete(table).where(pkey_column.in_(values))
        self._execute_sql_statement(statement)

    def get_table_names(self) -> list[str]:
        inspector = sa.inspect(self.engine)
        return sorted(inspector.get_table_names())

    def add_table(self, table_name: str, table_schema: TableSchema) -> None:
        """Adds a table to the schema

        Args:
            table_name (str): The name of the table
            table_schema (TableSchema): The schema for the table to be added
        """
        columns = self._create_columns(table_schema)
        sa.Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)

    def query_table(self, table_name: str) -> pd.DataFrame:
        query = f"SELECT * FROM `{table_name}`"
        return self.execute_sql_query(query)

    def _execute_sql_statement(self, statement: str) -> Any:
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def _create_columns(self, table_schema: TableSchema) -> list[sa.Column]:
        columns = [
            self._create_column(att, table_schema) for att in table_schema.columns
        ]
        columns.append(sa.PrimaryKeyConstraint(table_schema.primary_key))
        return columns

    def _create_column(
        self, column_schema: ColumnSchema, table_config: TableSchema
    ) -> sa.Column:
        sql_datatype = self._get_datatype(
            column_schema,
            table_config.primary_key,
            table_config.get_foreign_key_names(),
        )

        # Add foreign key constraints if needed
        if column_schema.name in table_config.get_foreign_key_names():
            key = table_config.get_foreign_key_by_name(column_schema.name)
            return create_foreign_key_column(
                column_schema.name,
                sql_datatype,
                key.foreign_table_name,
                key.foreign_column_name,
            )

        return sa.Column(
            column_schema.name,
            sql_datatype,
            # column is nullable if not required
            nullable=not column_schema.required,
            index=column_schema.index,
            # column is unique if it is a primary key
            unique=column_schema.name == table_config.primary_key,
        )

    def _get_column_indices(self, table_name: str) -> list[str]:
        indices = inspect(self.engine).get_indexes(table_name)
        return [idx["column_names"][0] for idx in indices]

    def _get_datatype(
        self,
        column_schema: ColumnSchema,
        primary_key: str,  # pylint: disable=unused-argument
        foreign_keys: list[str],  # pylint: disable=unused-argument
    ) -> Any:
        """Some _get_datatype methods depend on primary and foreign"""
        datatypes = {
            ColumnDatatype.TEXT: sa.VARCHAR,
            ColumnDatatype.DATE: sa.Date,
            ColumnDatatype.INT: sa.Integer,
            ColumnDatatype.FLOAT: sa.Float,
            ColumnDatatype.BOOLEAN: sa.Boolean,
        }
        return datatypes[column_schema.datatype]

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Inserts and/or updates the rows of the table

        Args:
            table_name (str): _The name of the table to be upserted
            data (pd.DataFrame): The rows to be upserted
        """
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        for row in rows:
            try:
                self._upsert_table_row(row, table, table_name)
            except (sa.exc.OperationalError, sa.exc.IntegrityError) as exc:
                raise UpsertDatabaseError(table_name) from exc

    def _upsert_table_row(
        self, row: dict[str, Any], table: sa.table, table_name
    ) -> None:
        pass
