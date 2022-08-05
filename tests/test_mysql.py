"""
Testing for  MYSQL.

For testing locally there should be a mysql server running.
A config yaml should exist at 'tests/data/local_mysql_config.yml'.
This config should look like:
username: "root"
password: "root"
host: "localhost"
schema: "test_schema"
The schema should not exist on the database at the beginning of the test.
This file is ignored by git.

If the the config doesn't exist, the file at 'tests/data/mysql_config.yml'
will be used.
"""
# pylint: disable=redefined-outer-name
# pylint: disable=too-many-arguments
# pylint: disable=W0212
# pylint: disable=E0401
from datetime import date
import os
import pytest # type: ignore
import sqlalchemy as sa # type: ignore
import pandas as pd # type: ignore
from yaml import safe_load
from rdb_type import MySQL

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")

@pytest.fixture
def config_dict():
    """
    Yields a MYSQL config dict
    """
    with open(CONFIG_PATH, mode="rt", encoding="utf-8") as file:
        config_dict = safe_load(file)
    yield config_dict

@pytest.fixture
def mysql(config_dict):
    """
    Yields a MYSQL object
    """
    obj = MySQL(config_dict["database"])
    yield obj

class TestMYSQL:
    """
    Testing for MYSQL
    """
    def test_execute_sql_statement(self, mysql):
        """
        Testing for MYSQL.execute_sql_statement()
        """
        result = mysql.execute_sql_statement("SHOW DATABASES;")
        assert isinstance(result, sa.engine.cursor.LegacyCursorResult)

    def test_get_table_names(self, mysql):
        """
        Testing for MYSQL.get_table_names()
        """
        assert isinstance(mysql.get_table_names(), list)

    def test_get_columns_from_table(self, mysql, table_one_config):
        """
        Testing for MYSQL.get_columns_from_table()
        """
        mysql.add_table("test_table2", table_one_config)
        columns = mysql._get_columns_from_table(mysql.get_table_names()[0])
        assert isinstance(columns, list)
        assert isinstance(columns[0], dict)
        mysql.drop_table('test_table2')

    def test_get_column_names_from_table(self, mysql, table_one_config):
        """
        Testing for MYSQL.get_column_names_from_table()
        """
        mysql.add_table("test_table2", table_one_config)
        names = mysql.get_column_names_from_table(mysql.get_table_names()[0])
        assert isinstance(names, list)
        assert isinstance(names[0], str)
        mysql.drop_table('test_table2')

    def test_get_add_drop_table(self, mysql, table_one_config):
        """
        Testing for MYSQL.add_table(), and MYSQL.drop_table()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table1", table_one_config)
        assert mysql.get_table_names() == ['test_table1']
        mysql.add_table("test_table2", table_one_config)
        assert mysql.get_table_names() == ['test_table1', 'test_table2']
        mysql.drop_table('test_table1')
        mysql.drop_table('test_table2')
        assert mysql.get_table_names() == []

    def test_add_drop_table_column(self, mysql, table_one_config, table_one):
        """
        Testing for MYSQL.add_table_column(), and MYSQL.drop_table_column()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("test_table1", table_one_config)
        assert mysql.get_column_names_from_table("test_table1") == list(table_one.columns)
        mysql.add_table_column("test_table1", "name", "varchar(100)")
        assert mysql.get_column_names_from_table("test_table1") == list(table_one.columns) + ["name"]
        mysql.drop_table_column("test_table1", "name")
        assert mysql.get_column_names_from_table("test_table1") == list(table_one.columns)
        mysql.drop_table('test_table1')
        assert mysql.get_table_names() == []

    def test_get_schemas(self, mysql):
        """
        Testing for MYSQL.get_schemas()
        """
        assert isinstance(mysql._get_schemas(), list)

    def test_get_current_schema(self, mysql):
        """
        Testing for MYSQL.get_current_schema()
        """
        assert mysql._get_current_schema() == "test_schema"

class TestMYSQLUpdateRows:
    """Testing for MYSQL methods that update rows
    """
    def test_upsert_table_rows1(self, mysql, table_one, table_one_config):
        """  Testing for MYSQL.upsert_table_rows()
        Whole table at once
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]
        mysql.upsert_table_rows("table_one", table_one)
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)
        mysql.drop_table('table_one')
        assert mysql.get_table_names() == []

    def test_upsert_table_rows2(self, mysql, table_one, table_one_config):
        """  Testing for MYSQL.upsert_table_rows()
        Whole table at once
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]

        mysql.upsert_table_rows("table_one", table_one.iloc[0:1, :])
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one.iloc[0:1, :], query_result)

        mysql.upsert_table_rows("table_one", table_one.iloc[1:2, :])
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one.iloc[0:2, :], query_result)

        mysql.upsert_table_rows("table_one", table_one.iloc[2:3, :])
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)

        mysql.drop_table('table_one')
        assert mysql.get_table_names() == []

    def test_upsert_table_rows3(self, mysql, table_one, table_one_config):
        """  Testing for MYSQL.upsert_table_rows()
        Updating a row
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]

        mysql.upsert_table_rows("table_one", table_one)
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)

        mysql.upsert_table_rows(
            "table_one",
            pd.DataFrame({"pk_col": ["key3"], "string_col": ["x"]})
        )
        mysql.upsert_table_rows(
            "table_one",
            pd.DataFrame({"pk_col": ["key3"], "double_col": [3.3], "bool_col": [True]})
        )
        query_result = mysql.query_table("table_one", table_one_config)
        new_table = pd.concat(
            [
                table_one.iloc[0:2, :],
                pd.DataFrame({
                    "pk_col": ["key3"],
                    "string_col": ["x"],
                    "int_col": [3],
                    "double_col": [3.3],
                    "date_col": [date.today()],
                    "bool_col": [True]
                })
            ],
            ignore_index=True
            )
        new_table = new_table.astype({"int_col": "Int64", "bool_col": "boolean"})
        pd.testing.assert_frame_equal(query_result, new_table)

        mysql.drop_table('table_one')
        assert mysql.get_table_names() == []

    def test_delete_table_rows(self, mysql, table_one_config, table_one):
        """
        Testing for MYSQL.delete_table_rows()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]
        mysql.upsert_table_rows("table_one", table_one)
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)
        mysql.delete_table_rows("table_one", "pk_col", ["key1"])
        mysql.delete_table_rows("table_one", "pk_col", ["key2"])
        result = mysql.query_table("table_one", table_one_config)
        assert len(result.index) == 1
        mysql.drop_table('table_one')
        assert mysql.get_table_names() == []
