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
import os
from datetime import datetime
import pytest
import sqlalchemy as sa
import pandas as pd
from yaml import safe_load
from rdb_type import MySQL

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")

@pytest.fixture(scope = "module", name = "config_dict")
def fixture_config_dict():
    """
    Yields a MYSQL config dict
    """
    with open(CONFIG_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config

@pytest.fixture(scope = "module", name = "mysql")
def fixture_mysql(config_dict, table_one_config):
    """
    Yields a MYSQL object
    """
    obj = MySQL(config_dict["database"])
    obj.add_table("table_one", table_one_config)
    yield obj
    obj.drop_table("table_one")

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
        assert mysql.get_table_names() == ["table_one"]

    def test_get_columns_from_table(self, mysql):
        """
        Testing for MYSQL.get_columns_from_table()
        """
        assert mysql.get_table_names() == ["table_one"]
        columns = mysql.get_columns_from_table("table_one")
        assert isinstance(columns, list)
        assert isinstance(columns[0], dict)

    def test_get_column_names_from_table(self, mysql):
        """
        Testing for MYSQL.get_column_names_from_table()
        """
        assert mysql.get_table_names() == ["table_one"]
        names = mysql.get_column_names_from_table("table_one")
        assert isinstance(names, list)
        assert isinstance(names[0], str)

    def test_get_schemas(self, mysql):
        """
        Testing for MYSQL.get_schemas()
        """
        assert isinstance(mysql.get_schemas(), list)

    def test_get_current_schema(self, mysql):
        """
        Testing for MYSQL.get_current_schema()
        """
        assert mysql.get_current_schema() == "test_schema"

class TestMYSQLUpdateTables:
    """Testing for MYSQL methods that update tables
    """
    def test_add_drop_table(self, mysql, table_two_config, table_three_config):
        """
        Testing for MYSQL.add_table(), and MYSQL.drop_table()
        """
        assert mysql.get_table_names() == ["table_one"]
        mysql.add_table("table_two", table_two_config)
        assert mysql.get_table_names() == ["table_one", "table_two"]
        mysql.add_table("table_three", table_three_config)
        assert mysql.get_table_names() == ["table_one", "table_three", "table_two"]

    def test_drop_table(self, mysql):
        """
        Testing for MYSQL.add_table(), and MYSQL.drop_table()
        """
        assert mysql.get_table_names() == ["table_one", "table_three", "table_two"]
        mysql.drop_table("table_three")
        assert mysql.get_table_names() == ["table_one", "table_two"]
        mysql.drop_table("table_two")
        assert mysql.get_table_names() == ["table_one"]

class TestMYSQLUpdateTableColumns:
    """Testing for MYSQL methods that update table columns
    """
    def test_add_drop_table_column(self, mysql, table_one):
        """
        Testing for MYSQL.add_table_column(), and MYSQL.drop_table_column()
        """
        assert mysql.get_table_names() == ["table_one"]
        assert mysql.get_column_names_from_table("table_one") == list(table_one.columns)
        mysql.add_table_column("table_one", "name", "varchar(100)")
        assert mysql.get_column_names_from_table("table_one") == \
            list(table_one.columns) + ["name"]
        mysql.drop_table_column("table_one", "name")
        assert mysql.get_column_names_from_table("table_one") == list(table_one.columns)

class TestMYSQLUpdateRows:
    """Testing for MYSQL methods that update rows
    """
    def test_upsert_table_rows1(self, mysql, table_one, table_one_config):
        """  Testing for MYSQL.upsert_table_rows()
        Whole table at once
        """
        assert mysql.get_table_names() == ["table_one"]
        mysql.upsert_table_rows("table_one", table_one)
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(query_result, table_one)
        mysql.drop_table('table_one')
        mysql.add_table("table_one", table_one_config)

    def test_upsert_table_rows2(self, mysql, table_one, table_one_config):
        """  Testing for MYSQL.upsert_table_rows()
        Whole table at once
        """
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
        mysql.add_table("table_one", table_one_config)

    def test_upsert_table_rows3(self, mysql, table_one, table_one_config):
        """  Testing for MYSQL.upsert_table_rows()
        Updating a row
        """
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
                    "date_col": [datetime(2022, 8, 2)],
                    "bool_col": [True]
                })
            ],
            ignore_index=True
            )
        new_table = new_table.astype({"int_col": "Int64", "bool_col": "boolean"})
        new_table['date_col'] = pd.to_datetime(new_table['date_col']).dt.date
        pd.testing.assert_frame_equal(query_result, new_table)

        mysql.drop_table('table_one')
        mysql.add_table("table_one", table_one_config)

    def test_delete_table_rows1(self, mysql, table_one_config, table_one):
        """Testing for MYSQL.delete_table_rows()
        """
        assert mysql.get_table_names() == ["table_one"]
        mysql.upsert_table_rows("table_one", table_one)
        result1 = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, result1)
        mysql.delete_table_rows("table_one", table_one.iloc[0:2, :], table_one_config)
        result_keys = mysql.query_table("table_one", table_one_config)['pk_col'].to_list()
        correct_keys = table_one.iloc[2:, :]['pk_col'].to_list()
        assert result_keys == correct_keys

        mysql.drop_table('table_one')
        mysql.add_table("table_one", table_one_config)
