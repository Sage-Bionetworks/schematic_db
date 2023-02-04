"""Testing for RDBUpdater."""
from typing import Generator
import pytest
from schematic_db.rdb import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.schema import Schema


@pytest.fixture(scope="module", name="rdb_updater_mysql_test")
def fixture_rdb_updater_mysql_test(
    mysql_database: MySQLDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=mysql_database, schema=test_schema)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_postgres_test")
def fixture_rdb_updater_postgres_test(
    postgres_database: PostgresDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=postgres_database, schema=test_schema)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_updater_synapse_test")
def fixture_rdb_updater_synapse_test(
    synapse_database: SynapseDatabase, test_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a synapse database and test schema"""
    obj = RDBUpdater(rdb=synapse_database, schema=test_schema)
    yield obj
    obj.rdb.delete_all_tables()


@pytest.mark.schematic
class TestRDBUpdaterTestSchema:
    """Testing for RDBUpdater and test schema"""

    def test_mysql_update_all_database_tables(
        self, rdb_updater_mysql_test: RDBUpdater, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in MySQL"""
        obj = rdb_updater_mysql_test
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.update_database()
        assert obj.rdb.get_table_names() == test_schema_table_names

    def test_postgres_update_all_database_tables(
        self, rdb_updater_postgres_test: RDBUpdater, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in Postgres"""
        obj = rdb_updater_postgres_test
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.update_database()
        assert obj.rdb.get_table_names() == test_schema_table_names

    def test_synapse_update_all_database_tables(
        self, rdb_updater_synapse_test: RDBUpdater, test_schema_table_names: list[str]
    ) -> None:
        """Creates the test database in Synapse"""
        obj = rdb_updater_synapse_test
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
        obj.update_database()
        assert obj.rdb.get_table_names() == test_schema_table_names
