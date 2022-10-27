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
from datetime import datetime
import pytest
import pandas as pd
from schematic_db.db_config.db_config import DBObjectConfig
from schematic_db.rdb.mysql import MySQLDatabase


@pytest.mark.fast
class TestMYSQLUpdateTables:  # pylint: disable=too-few-public-methods
    """Testing for MYSQL methods that update tables"""

    def test_add_drop_table(
        self,
        mysql: MySQLDatabase,
        table_one_config: DBObjectConfig,
        table_two_config: DBObjectConfig,
        table_three_config: DBObjectConfig,
    ) -> None:
        """
        Testing for MYSQL.add_table() and and MYSQL.drop_table()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]
        mysql.add_table("table_two", table_two_config)
        assert mysql.get_table_names() == ["table_one", "table_two"]
        mysql.add_table("table_three", table_three_config)
        assert mysql.get_table_names() == ["table_one", "table_three", "table_two"]
        mysql.drop_table("table_three")
        assert mysql.get_table_names() == ["table_one", "table_two"]
        mysql.drop_table("table_two")
        assert mysql.get_table_names() == ["table_one"]
        mysql.drop_table("table_one")
        assert mysql.get_table_names() == []


@pytest.mark.fast
class TestMYSQLGetters:  # pylint: disable=too-few-public-methods
    """
    Testing for MYSQL
    """

    def test_get_table_names(
        self, mysql: MySQLDatabase, table_one_config: DBObjectConfig
    ) -> None:
        """
        Testing for MYSQL.get_table_names()
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]
        mysql.drop_table("table_one")
        assert mysql.get_table_names() == []


@pytest.mark.fast
class TestMYSQLUpdateRows:
    """Testing for MYSQL methods that update rows"""

    def test_upsert_table_rows1(
        self,
        mysql: MySQLDatabase,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for MYSQL.upsert_table_rows()
        Whole table at once
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]
        mysql.upsert_table_rows("table_one", table_one)
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(query_result, table_one)
        mysql.drop_table("table_one")
        assert mysql.get_table_names() == []

    def test_upsert_table_rows2(
        self,
        mysql: MySQLDatabase,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for MYSQL.upsert_table_rows()
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

        mysql.drop_table("table_one")
        assert mysql.get_table_names() == []

    def test_upsert_table_rows3(
        self,
        mysql: MySQLDatabase,
        table_one: pd.DataFrame,
        table_one_config: DBObjectConfig,
    ) -> None:
        """Testing for MYSQL.upsert_table_rows()
        Updating a row
        """
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]

        mysql.upsert_table_rows("table_one", table_one)
        query_result = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, query_result)

        mysql.upsert_table_rows(
            "table_one", pd.DataFrame({"pk_one_col": ["key3"], "string_one_col": ["x"]})
        )
        mysql.upsert_table_rows(
            "table_one",
            pd.DataFrame(
                {
                    "pk_one_col": ["key3"],
                    "double_one_col": [3.3],
                    "bool_one_col": [True],
                }
            ),
        )
        query_result = mysql.query_table("table_one", table_one_config)
        new_table = pd.concat(
            [
                table_one.iloc[0:2, :],
                pd.DataFrame(
                    {
                        "pk_one_col": ["key3"],
                        "string_one_col": ["x"],
                        "int_one_col": [3],
                        "double_one_col": [3.3],
                        "date_one_col": [datetime(2022, 8, 2)],
                        "bool_one_col": [True],
                    }
                ),
            ],
            ignore_index=True,
        )
        new_table = new_table.astype(
            {"int_one_col": "Int64", "bool_one_col": "boolean"}
        )
        new_table["date_one_col"] = pd.to_datetime(new_table["date_one_col"]).dt.date
        pd.testing.assert_frame_equal(query_result, new_table)

        mysql.drop_table("table_one")
        assert mysql.get_table_names() == []

    def test_delete_table_rows1(
        self,
        mysql: MySQLDatabase,
        table_one_config: DBObjectConfig,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for MYSQL.delete_table_rows()"""
        assert mysql.get_table_names() == []
        mysql.add_table("table_one", table_one_config)
        assert mysql.get_table_names() == ["table_one"]
        mysql.upsert_table_rows("table_one", table_one)
        result1 = mysql.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(table_one, result1)
        mysql.delete_table_rows("table_one", table_one.iloc[0:2, :], table_one_config)
        result_keys = mysql.query_table("table_one", table_one_config)[
            "pk_one_col"
        ].to_list()
        correct_keys = table_one.iloc[2:, :]["pk_one_col"].to_list()
        assert result_keys == correct_keys

        mysql.drop_table("table_one")
        assert mysql.get_table_names() == []
