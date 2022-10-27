"""MySQLDatabase"""
from typing import Optional, Any
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from sqlalchemy import exc
from schematic_db.db_config import DBObjectConfig, DBDatatype
from .rdb import RelationalDatabase, UpdateDBTableError

MYSQL_DATATYPES = {
    DBDatatype.TEXT: sa.Text(5000),
    DBDatatype.DATE: sa.Date,
    DBDatatype.INT: sa.Integer,
    DBDatatype.FLOAT: sa.Float,
    DBDatatype.BOOLEAN: sa.Boolean,
}

PANDAS_DATATYPES = {DBDatatype.INT: "Int64", DBDatatype.BOOLEAN: "boolean"}


class DataframeKeyError(Exception):
    """DataframeKeyError"""

    def __init__(self, message: str, key: str) -> None:
        self.message = message
        self.key = key
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}:{self.key}"


class MySQLDatabase(RelationalDatabase):
    """MySQLDatabase
    - Represents a mysql database.
    - Implements the RelationalDatabase interface.
    - Handles MYSQL specific functionality.
    """

    def __init__(self, config_dict: dict, verbose: Optional[bool] = False):
        """Init
        An initial connection is created to the database without the schema.
        The schema will be created if it doesn't exist.
        A second connection is created with the schema.
        The second connection is used to create the sqlalchemy connection and metadata.

        Args:
            config_dict (dict): A dict with fields ["username", "password", "host", "schema"]
            verbose (bool): Sends much more to logging.info
        """
        username = config_dict.get("username")
        password = config_dict.get("password")
        host = config_dict.get("host")
        schema = config_dict.get("schema")

        url = f"mysql://{username}:{password}@{host}/"
        engine = sa.create_engine(url, encoding="utf-8", echo=verbose)
        create_statement = f"CREATE DATABASE IF NOT EXISTS {schema};"
        engine.execute(create_statement)

        url2 = f"mysql://{username}:{password}@{host}/{schema}"
        engine2 = sa.create_engine(url2, encoding="utf-8", echo=verbose)
        self.engine = engine2
        self.metadata = sa.MetaData()

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        result = self._execute_sql_statement(query).fetchall()
        table = pd.DataFrame(result)
        return table

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
        self._execute_sql_statement(f"DROP TABLE IF EXISTS `{table_name}`;")
        self.metadata.clear()

    def delete_table_rows(
        self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig
    ) -> None:
        primary_keys = table_config.primary_keys
        for key in primary_keys:
            if key not in list(data.columns):
                raise DataframeKeyError("Primary key missing from data:", key)
        data = data[primary_keys]
        tuples = list(data.itertuples(index=False, name=None))
        tuples = [(f"'{i}'" for i in tup) for tup in tuples]
        tuple_strings = ["(" + ",".join(tup) + ")" for tup in tuples]
        tuple_string = ",".join(tuple_strings)
        statement = f"DELETE FROM {table_name} WHERE ({','.join(primary_keys)}) IN ({tuple_string})"
        self._execute_sql_statement(statement)

    def get_table_names(self) -> list[str]:
        inspector = sa.inspect(self.engine)
        return inspector.get_table_names()

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

    def query_table(
        self, table_name: str, table_config: DBObjectConfig
    ) -> pd.DataFrame:
        """Queries an entire table

        Args:
            table_name (str): The table to be queried
            table_config (DBObjectConfig): The config for the table to be queried

        Returns:
            pd.DataFrame: The query result
        """
        query = f"SELECT * FROM {table_name};"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            pandas_value = PANDAS_DATATYPES.get(att.datatype, None)
            if pandas_value is not None:
                table = table.astype({att.name: pandas_value})
        return table

    def _execute_sql_statement(self, statement: str) -> Any:
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def _create_columns(self, table_config: DBObjectConfig) -> list[sa.Column]:
        columns = []
        for att in table_config.attributes:
            att_name = att.name
            primary_keys = table_config.primary_keys
            foreign_keys = table_config.get_foreign_key_names()

            # If column is a key, set datatype to sa.String(100)
            if att_name in primary_keys or att_name in foreign_keys:
                sql_datatype = sa.String(100)
            else:
                sql_datatype = MYSQL_DATATYPES.get(att.datatype)

            if att_name in foreign_keys:
                key = table_config.get_foreign_key_by_name(att_name)
                col = sa.Column(
                    att_name,
                    sql_datatype,
                    sa.ForeignKey(
                        f"{key.foreign_object_name}.{key.foreign_attribute_name}"
                    ),
                    nullable=True,
                )
            else:
                col = sa.Column(att_name, sql_datatype)
            columns.append(col)

        if primary_keys != []:
            columns.append(sa.PrimaryKeyConstraint(*primary_keys))
        return columns
