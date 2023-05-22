"""SQLAlchemy"""
from typing import Any
from dataclasses import dataclass
import pandas as pd
import numpy as np
import sqlalchemy as sa
import sqlalchemy_utils
from sqlalchemy import exc
from sqlalchemy.inspection import inspect
from schematic_db.db_schema.db_schema import (
    TableSchema,
    ColumnDatatype,
    ColumnSchema,
    ForeignKeySchema,
)
from .rdb import RelationalDatabase, InsertDatabaseError


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
        table_schema (sa.sql.schema.Table): A sqlalchemy table schema
        indices (list[str]): A list of columns in the schema to be indexed

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
        """Drops all tables in the schema"""
        metadata = sa.schema.MetaData(self.engine)
        metadata.reflect()
        metadata.drop_all()
        self.metadata.clear()

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        """Executes a sql query returning a table

        Args:
            query (str): A query written in SQL that returns a table

        Returns:
            pd.DataFrame: The query result in pandas.Dataframe form
        """
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

    def insert_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Inserts the rows of the table into a target table in the database

        Args:
            table_name (str): The name of the table to be inserted into
            data (pd.DataFrame): The rows to be inserted

        Raises:
            InsertDatabaseError: Raised when a SQLAlchemy error caught
        """
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        statement = sa.insert(table).values(rows)
        try:
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)
        except exc.SQLAlchemyError as exception:
            raise InsertDatabaseError(table_name) from exception

    def drop_table(self, table_name: str) -> None:
        """Drops a table from the schema

        Args:
            table_name (str): The name of the table to be dropped
        """
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        table.drop(self.engine)
        self.metadata.clear()

    def delete_table_rows(self, table_name: str, data: pd.DataFrame) -> None:
        """Deletes rows from a table

        Args:
            table_name (str): The name fo the table to delete rows from
            data (pd.DataFrame): A pandas dataframe, rows will eb deleted from the table
             in the database where the primary keys match this dataframe.
        """
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        i = sa.inspect(table)
        pkey_column = list(column for column in i.columns if column.primary_key)[0]
        values = data[pkey_column.name].values.tolist()
        statement = sa.delete(table).where(pkey_column.in_(values))
        self._execute_sql_statement(statement)

    def get_table_names(self) -> list[str]:
        """Gets the names of all tables in the database

        Returns:
            list[str]: A list of table names
        """
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
        """Queries a whole table

        Args:
            table_name (str): The name of the table to query

        Returns:
            pd.DataFrame: The table in pandas.Dataframe form
        """
        query = f"SELECT * FROM `{table_name}`"
        return self.execute_sql_query(query)

    def _execute_sql_statement(self, statement: str) -> Any:
        """Executes a sql statement

        Args:
            statement (str): A sql statement in string form

        Returns:
            Any: The result, idf any, from the sql statement
        """
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def _create_columns(self, table_schema: TableSchema) -> list[sa.Column]:
        """Creates a list SQLAlchemy columns for a table

        Args:
            table_schema (TableSchema): The schema of the table to create columns for

        Returns:
            list[sa.Column]: A list SQLAlchemy columns
        """
        columns = [
            self._create_column(att, table_schema) for att in table_schema.columns
        ]
        columns.append(sa.PrimaryKeyConstraint(table_schema.primary_key))
        return columns

    def _create_column(
        self, column_schema: ColumnSchema, table_schema: TableSchema
    ) -> sa.Column:
        """Creates a SQLAlchemy column

        Args:
            column_schema (ColumnSchema): The schema for the column
            table_schema (TableSchema): The schema for the table

        Returns:
            sa.Column: a SQLAlchemy column
        """
        sql_datatype = self._get_datatype(
            column_schema,
            table_schema.primary_key,
            table_schema.get_foreign_key_names(),
        )

        # Add foreign key constraints if needed
        if column_schema.name in table_schema.get_foreign_key_names():
            key = table_schema.get_foreign_key_by_name(column_schema.name)
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
            unique=column_schema.name == table_schema.primary_key,
        )

    def _get_column_indices(self, table_name: str) -> list[str]:
        """Gets the tables current column indices

        Args:
            table_name (str): The name of the table

        Returns:
            list[str]: A list of column names that are currently indexed
        """
        indices = inspect(self.engine).get_indexes(table_name)
        return [idx["column_names"][0] for idx in indices]

    def _get_datatype(
        self,
        column_schema: ColumnSchema,
        primary_key: str,  # pylint: disable=unused-argument
        foreign_keys: list[str],  # pylint: disable=unused-argument
    ) -> Any:
        """
        Gets the datatype of the column based on its schema
        Other _get_datatype methods depend on primary and foreign keys

        Args:
            column_schema (ColumnSchema): The schema of the column
            primary_key (str): The primary key fo the column (unused)
            foreign_keys (list[str]): A list of foreign keys for the the column (unused)

        Returns:
            Any: The SQLAlchemy datatype
        """
        datatypes = {
            ColumnDatatype.TEXT: sa.VARCHAR,
            ColumnDatatype.DATE: sa.Date,
            ColumnDatatype.INT: sa.Integer,
            ColumnDatatype.FLOAT: sa.Float,
            ColumnDatatype.BOOLEAN: sa.Boolean,
        }
        return datatypes[column_schema.datatype]
