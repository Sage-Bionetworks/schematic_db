"""Testing for RDB.
"""
import os
import pytest
import pandas as pd
from rdb import RDB

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_mysql_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "mysql_config.yml")

@pytest.fixture(scope = 'module', name = "rdb_mysql")
def fixture_rdb_mysql():
    """Yields a RDB object
    """
    rdb = RDB(config_yaml_path = CONFIG_PATH)
    assert rdb.rdb_type.get_table_names() == []
    yield rdb
    test_table_names = ["table_three", "table_one", "table_two"]
    for table_name in test_table_names:
        if table_name in rdb.rdb_type.get_table_names():
            rdb.rdb_type.drop_table(table_name)
    for table_name in rdb.query_result_store.get_table_names():
        rdb.query_result_store.drop_table(table_name)

class TestRDBMySQL:
    """Testing for RDB with MySQL database
    """
    def test_init(self, rdb_mysql):
        """Testing for RDB.init()
        """
        assert isinstance(rdb_mysql, RDB)

    def test_update_database_table1(
        self, rdb_mysql,
        table_one, table_one_config
    ):
        """Testing for RDB.update_database_table()
        """
        assert rdb_mysql.rdb_type.get_table_names() == []

        rdb_mysql.update_database_table(["table_one"], table_one_config)
        result = rdb_mysql.rdb_type.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result, table_one)

        assert rdb_mysql.rdb_type.get_table_names() == ["table_one"]

    def test_update_database_table2(self, rdb_mysql, table_two, table_two_b, table_two_config):
        """Testing for RDB.update_database_table()
        """
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one"]

        rdb_mysql.update_database_table(["table_two", "table_two_b"], table_two_config)
        result = rdb_mysql.rdb_type.query_table("table_two", table_two_config)
        test_table = pd.concat([table_two, table_two_b]).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, test_table)

        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_two"]

    def update_all_database_tables(
        self, rdb_mysql,
        table_one, table_one_config,
        table_two, table_two_config,
        table_three, table_three_config
    ):
        """Testing for update_all_database_tables()
        """
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_two"]
        rdb_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]],
            [table_one_config, table_two_config, table_three_config]
        )
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_three", "table_two"]
        result1 = rdb_mysql.rdb_type.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)
        result2 = rdb_mysql.rdb_type.query_table("table_two", table_two_config)
        pd.testing.assert_frame_equal(result2, table_two)
        result3 = rdb_mysql.rdb_type.query_table("table_three", table_three_config)
        pd.testing.assert_frame_equal(result3, table_three)
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_three", "table_two"]

    def test_store_query_result(
        self, rdb_mysql, table_one_config, table_two_config, table_three_config
    ):
        """Testing for RDB.store_query_result()
        """
        rdb_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]],
            [table_one_config, table_two_config, table_three_config]
        )
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_three", "table_two"]
        assert rdb_mysql.query_result_store.get_table_names() == []

        query = (
            "SELECT * FROM " +\
            "(SELECT pk_col, int_col FROM table_one) AS one " +\
            "INNER JOIN " +\
            "(SELECT fk1_col, string_col FROM table_three) AS three "+\
            "ON one.pk_col = three.fk1_col;"
        )

        rdb_mysql.store_query_result(query, "result_zero")
        assert rdb_mysql.query_result_store.get_table_names() == ["result_zero"]

    def test_store_query_results(
        self, rdb_mysql, table_one_config, table_two_config, table_three_config
    ):
        """Testing for RDB.store_query_results()
        """
        rdb_mysql.update_all_database_tables(
            [["table_one"], ["table_two"], ["table_three"]],
            [table_one_config, table_two_config, table_three_config]
        )
        assert rdb_mysql.rdb_type.get_table_names() == ["table_one", "table_three", "table_two"]
        assert rdb_mysql.query_result_store.get_table_names() == ["result_zero"]

        csv_path = os.path.join(DATA_DIR, "queries.csv")
        rdb_mysql.store_query_results(csv_path)
        assert rdb_mysql.query_result_store.get_table_names() == [
            "result_one", "result_two", "result_zero"
        ]
