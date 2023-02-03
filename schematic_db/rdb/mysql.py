"""MySQLDatabase"""
from typing import Any
from dataclasses import dataclass
import pandas as pd
import numpy as np
import sqlalchemy as sa
import sqlalchemy_utils.functions
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import exc
from schematic_db.db_config import (
    DBConfig,
    DBObjectConfig,
    DBDatatype,
    DBAttributeConfig,
    DBForeignKey,
)
from .rdb import RelationalDatabase, UpdateDBTableError

MYSQL_DATATYPES = {
    DBDatatype.TEXT: sa.VARCHAR(5000),
    DBDatatype.DATE: sa.Date,
    DBDatatype.INT: sa.Integer,
    DBDatatype.FLOAT: sa.Float,
    DBDatatype.BOOLEAN: sa.Boolean,
}

CONFIG_DATATYPES = {
    sa.String: DBDatatype.TEXT,
    sa.VARCHAR: DBDatatype.TEXT,
    sa.Date: DBDatatype.DATE,
    sa.Integer: DBDatatype.INT,
    sa.Float: DBDatatype.FLOAT,
    sa.Boolean: DBDatatype.BOOLEAN,
}


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


def create_foreign_key_configs(table_schema: sa.sql.schema.Table) -> list[DBForeignKey]:
    """Creates a list of foreign key configs from a sqlalchemy table schema

    Args:
        table_schema (sa.sql.schema.Table): A sqlalchemy table schema

    Returns:
        list[DBForeignKey]: A list of foreign key configs
    """
    foreign_keys = inspect(table_schema).foreign_keys
    return [
        DBForeignKey(
            name=key.parent.name,
            foreign_object_name=key.column.table.name,
            foreign_attribute_name=key.column.name,
        )
        for key in foreign_keys
    ]


def create_attribute_configs(
    table_schema: sa.sql.schema.Table,
) -> list[DBAttributeConfig]:
    """Creates a list of attribute configs from a sqlalchemy table schema

    Args:
        table_schema (sa.sql.schema.Table):A sqlalchemy table schema

    Returns:
        list[DBAttributeConfig]: A list of foreign key configs
    """
    columns = table_schema.c
    return [
        DBAttributeConfig(
            name=col.name,
            datatype=CONFIG_DATATYPES[type(col.type)],
            required=not col.nullable,
        )
        for col in columns
    ]


@dataclass
class MySQLConfig:
    """A config for a MySQL database."""

    username: str
    password: str
    host: str
    name: str


class MySQLDatabase(RelationalDatabase):  # pylint: disable=too-many-instance-attributes
    """MySQLDatabase
    - Represents a mysql database.
    - Implements the RelationalDatabase interface.
    - Handles MYSQL specific functionality.
    """

    def __init__(
        self, config: MySQLConfig, verbose: bool = False, db_type_string: str = "mysql"
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

    def delete_all_tables(self) -> None:
        self.drop_all_tables()

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        result = self._execute_sql_statement(query).fetchall()
        table = pd.DataFrame(result)
        return table

    def get_db_config(self) -> DBConfig:
        table_names = self.get_table_names()
        config_list = [self.get_table_config(name) for name in table_names]
        return DBConfig(config_list)

    def get_table_config(self, table_name: str) -> DBObjectConfig:
        """Creates a table config from a sqlalchemy table schema

        Args:
            table_name (str): The name of the table

        Returns:
            DBObjectConfig: A config for the table
        """
        table_schema = self.metadata.tables[table_name]
        primary_key = inspect(table_schema).primary_key.columns.values()[0].name

        return DBObjectConfig(
            name=table_name,
            primary_key=primary_key,
            foreign_keys=create_foreign_key_configs(table_schema),
            attributes=create_attribute_configs(table_schema),
        )

    def update_table(self, data: pd.DataFrame, table_config: DBObjectConfig) -> None:
        table_names = self.get_table_names()
        table_name = table_config.name
        if table_name not in table_names:
            self.add_table(table_name, table_config)
        try:
            self.upsert_table_rows(table_name, data)
        except exc.SQLAlchemyError as error:
            error_msg = str(error.__dict__["orig"])
            raise UpdateDBTableError(table_name, error_msg) from error

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

    def add_table(self, table_name: str, table_config: DBObjectConfig) -> None:
        """Adds a table to the schema

        Args:
            table_name (str): The name of the table
            table_config (DBObjectConfig): The config for the table to be added
        """
        columns = self._create_columns(table_config)
        sa.Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)

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
            statement = insert(table).values(row).on_duplicate_key_update(**row)
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)

    def query_table(self, table_name: str) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name};"
        table = self.execute_sql_query(query)
        return table

    def _execute_sql_statement(self, statement: str) -> Any:
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def _create_columns(self, table_config: DBObjectConfig) -> list[sa.Column]:
        columns = [
            self._create_column(att, table_config) for att in table_config.attributes
        ]
        columns.append(sa.PrimaryKeyConstraint(table_config.primary_key))
        return columns

    def _create_column(
        self, attribute: DBAttributeConfig, table_config: DBObjectConfig
    ) -> sa.Column:
        att_name = attribute.name
        primary_key = table_config.primary_key
        foreign_keys = table_config.get_foreign_key_names()
        nullable = not attribute.required

        # If column is a key, set datatype to sa.String(100)
        if att_name == primary_key or att_name in foreign_keys:
            sql_datatype = sa.String(100)
        else:
            sql_datatype = MYSQL_DATATYPES.get(attribute.datatype)

        if att_name in foreign_keys:
            key = table_config.get_foreign_key_by_name(att_name)
            return create_foreign_key_column(
                att_name,
                sql_datatype,
                key.foreign_object_name,
                key.foreign_attribute_name,
            )
        return sa.Column(att_name, sql_datatype, nullable=nullable)

    def _get_datatype(self, attribute: DBAttributeConfig) -> Any:
        MYSQL_DATATYPES.get(attribute.datatype)
