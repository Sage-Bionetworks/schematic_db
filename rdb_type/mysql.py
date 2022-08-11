"""MYSQL
"""
from typing import List, Dict
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import insert
from db_object_config import DBObjectConfig, DBDatatype
from .rdb_type import RDBType

class MySQL(RDBType):
    """MYSQL
    - Represents a mysql database.
    - Implements the RDBType interface.
    - Handles MYSQL specific functionality.
    """
    def __init__(self, config_dict: dict):
        """Init
        obj = MySQL({
            username: "root"
            password: "root"
            host: "localhost"
            schema: "test_schema"
        })

        An initial connection is created to the database without the schema.
        The schema will be created if it doesn't exist.
        A second connection is created with the schema.
        The second connection is used to create the sqlalchemy connection and metadata.

        Args:
            config_dict (dict): A dict with mysql specific fields
        """
        username = config_dict.get("username")
        password = config_dict.get("password")
        host = config_dict.get("host")
        schema = config_dict.get("schema")

        url = f"mysql://{username}:{password}@{host}/"
        engine = sa.create_engine(url, encoding = 'utf-8', echo = True)
        create_statement = f"CREATE DATABASE IF NOT EXISTS {schema};"
        engine.execute(create_statement)

        url2 = f"mysql://{username}:{password}@{host}/{schema}"
        engine2 = sa.create_engine(url2, encoding = 'utf-8', echo = True)
        self.engine = engine2
        self.metadata = sa.MetaData()

    def get_table_id_from_name(self, table_name: str) -> str:
        return table_name

    def get_table_name_from_id(self, table_id: str) -> str:
        return table_id

    def execute_sql_statement(self, statement: str):
        with self.engine.connect().execution_options(autocommit=True) as conn:
            result = conn.execute(statement)
        return result

    def execute_sql_query(self, query: str) -> pd.DataFrame:
        result = self.execute_sql_statement(query).fetchall()
        table = pd.DataFrame(result)
        return table

    def query_table(self, table_name: str, table_config: DBObjectConfig) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name};"
        table = self.execute_sql_query(query)
        for att in table_config.attributes:
            if att.datatype == DBDatatype.Int:
                table = table.astype({att.name: 'Int64'})
            elif att.datatype == DBDatatype.Boolean:
                table = table.astype({att.name: 'boolean'})
        return table

    def add_table(self, table_name: str, table_config: DBObjectConfig):
        columns = []
        for att in table_config.attributes:
            att_name = att.name
            att_datatype = att.datatype
            if att_datatype == DBDatatype.Text:
                sql_datatype = sa.String(100)
            elif att_datatype == DBDatatype.Date:
                sql_datatype = sa.Date
            elif att_datatype == DBDatatype.Int:
                sql_datatype = sa.Integer
            elif att_datatype == DBDatatype.Float:
                sql_datatype = sa.Float
            elif att_datatype == DBDatatype.Boolean:
                sql_datatype = sa.Boolean
            else:
                raise ValueError ()
            if att_name in table_config.foreign_keys.keys():
                col = sa.Column(
                    att_name,
                    sql_datatype,
                    sa.ForeignKey(table_config.foreign_keys.get(att_name)),
                    nullable=False
                )
            else:
                col = sa.Column(att_name, sql_datatype)
            columns.append(col)

        if table_config.primary_keys != []:
            columns.append(sa.PrimaryKeyConstraint(*table_config.primary_keys))

        sa.Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)

    def drop_table(self, table_name: str):
        self.execute_sql_statement(f"DROP TABLE IF EXISTS {table_name};")
        self.metadata.clear()

    def add_table_column(self, table_name: str, column_name: str, datatype: str):
        self.execute_sql_statement(f"ALTER TABLE {table_name} ADD {column_name} {datatype};")

    def drop_table_column(self, table_name: str, column_name: str):
        self.execute_sql_statement(f"ALTER TABLE {table_name} DROP {column_name};")

    def delete_table_rows(self, table_name: str, data: pd.DataFrame, table_config: DBObjectConfig):
        primary_keys = table_config.primary_keys
        for col in primary_keys:
            if col not in list(data.columns):
                raise ValueError(f"primary key: {col} missing from data")
        data = data[primary_keys]
        tuples = list(data.itertuples(index=False, name=None))
        tuples = [(f"'{i}'" for i in tup) for tup in tuples]
        tuple_strings = ["(" + ",".join(tup) + ")" for tup in tuples]
        tuple_string = ",".join(tuple_strings)
        statement = f"DELETE FROM {table_name} WHERE ({','.join(primary_keys)}) IN ({tuple_string})"
        self.execute_sql_statement(statement)

    def upsert_table_rows(self, table_name: str, data: pd.DataFrame):
        data = data.replace({np.nan: None})
        rows = data.to_dict("records")
        table = sa.Table(table_name, self.metadata, autoload_with=self.engine)
        for row in rows:
            statement = insert(table).values(row).on_duplicate_key_update(**row)
            with self.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(statement)

    def get_table_names(self) -> List[str]:
        inspector = sa.inspect(self.engine)
        return inspector.get_table_names()

    def get_column_names_from_table(self, table_name: str) -> List[str]:
        columns = self.get_columns_from_table(table_name)
        names = [col.get("name") for col in columns]
        return names

    def get_columns_from_table(self, table_name: str) -> List[Dict]:
        """Gets the columns form the given table

        Args:
            table_name (str): The id(name) of the table the columns will be returned from

        Returns:
            List[Dict]: A list of columns in dict form
        """
        inspector = sa.inspect(self.engine)
        return inspector.get_columns(table_name)

    def get_schemas(self) -> List[str]:
        """Gets the database schemas

        Returns:
            List[str]: A list of names of the the schemas
        """
        inspector = sa.inspect(self.engine)
        return inspector.get_schema_names()

    def get_current_schema(self) -> str:
        """Gets the current database schema

        Returns:
            str: The name of the current schema
        """
        return self.execute_sql_statement("SELECT DATABASE();").fetchall()[0][0]

    def _change_current_schema(self, schema_name: str):
        """Changes the current schema

        Args:
            schema_name (str): The name of the schema to change to
        """
        self.execute_sql_statement(f"USE {schema_name};")

    def _create_schema(self, schema_name: str):
        """Creates a schema

        Args:
            schema_name (str): The name of the schema to create
        """
        self.execute_sql_statement(f"CREATE DATABASE IF NOT EXISTS {schema_name};")

    def _drop_schema(self, schema_name: str):
        """Drops a schema

        Args:
            schema_name (str): The name of the schema to drop
        """
        self.execute_sql_statement(f"DROP DATABASE {schema_name};")
