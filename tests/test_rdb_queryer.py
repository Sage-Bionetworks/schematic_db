"""Testing for Testing for RDBQueryer."""
from typing import Generator
import os
import pytest
from schematic_db.rdb_queryer import RDBQueryer
from schematic_db.query_store import QueryStore
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.rdb import MySQLDatabase, PostgresDatabase
from schematic_db.schema import Schema


@pytest.fixture(scope="module", name="rdb_updater_mysql_test")
def fixture_rdb_updater_mysql_test(
    mysql_database: MySQLDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=mysql_database, schema=test_schema)
    obj.build_database()
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_postgres_test")
def fixture_rdb_updater_postgres_test(
    postgres_database: PostgresDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a postgres database and test schema"""
    obj = RDBUpdater(rdb=postgres_database, schema=test_schema)
    obj.build_database()
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="function", name="query_store")
def fixture_query_store(synapse_test_query_store: QueryStore) -> Generator:
    """Yields a query store"""
    obj = synapse_test_query_store
    yield obj


@pytest.fixture(scope="function", name="mysql_test_rdb_queryer")
def fixture_mysql_test_rdb_queryer(
    rdb_updater_mysql_test: RDBUpdater,
    query_store: QueryStore,
    test_schema_table_names: list[str],
) -> Generator:
    """Yields a RDBQueryer with a mysql database with test schema tables added"""
    updater = rdb_updater_mysql_test
    assert updater.rdb.get_table_names() == test_schema_table_names

    assert query_store.get_table_names() == []

    obj = RDBQueryer(
        rdb=updater.rdb,
        query_store=query_store,
    )
    yield obj
    for table_name in query_store.get_table_names():
        query_store.delete_table(table_name)


@pytest.fixture(scope="function", name="postgres_test_rdb_queryer")
def fixture_postgres_test_rdb_queryer(
    rdb_updater_postgres_test: RDBUpdater,
    query_store: QueryStore,
    test_schema_table_names: list[str],
) -> Generator:
    """Yields a RDBQueryer with a postgres database with test schema tables added"""
    updater = rdb_updater_postgres_test
    assert updater.rdb.get_table_names() == test_schema_table_names

    assert query_store.get_table_names() == []

    obj = RDBQueryer(
        rdb=updater.rdb,
        query_store=query_store,
    )
    yield obj
    for table_name in query_store.get_table_names():
        query_store.delete_table(table_name)


@pytest.mark.synapse
@pytest.mark.schematic
class TestRDBQueryerTest:
    """Testing for RDBQueryer using the test schema database"""

    def test_store_query_results_mysql(
        self,
        mysql_test_rdb_queryer: RDBQueryer,
        test_schema_table_names: list[str],
        data_directory: str,
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        obj = mysql_test_rdb_queryer
        assert obj.query_store.get_table_names() == []
        path = os.path.join(data_directory, "test_queries_mysql.csv")
        obj.store_query_results(path)
        assert obj.query_store.get_table_names() == test_schema_table_names

    def test_store_query_results_postgres(
        self,
        postgres_test_rdb_queryer: RDBQueryer,
        test_schema_table_names: list[str],
        data_directory: str,
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        obj = postgres_test_rdb_queryer
        assert obj.query_store.get_table_names() == []
        path = os.path.join(data_directory, "test_queries_postgres.csv")
        obj.store_query_results(path)
        assert obj.query_store.get_table_names() == test_schema_table_names
