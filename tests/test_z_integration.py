"""Testing for RDBUpdater."""
from typing import Generator
import os
import pytest
from schematic_db.rdb.mysql import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.rdb_builder.rdb_builder import RDBBuilder
from schematic_db.schema.schema import Schema
from schematic_db.rdb_updater.rdb_updater import RDBUpdater
from schematic_db.manifest_store.manifest_store import ManifestStore
from schematic_db.query_store.query_store import QueryStore
from schematic_db.rdb_queryer.rdb_queryer import RDBQueryer


@pytest.fixture(scope="module", name="rdb_builder_mysql")
def fixture_rdb_builder_mysql(
    mysql_database: MySQLDatabase, test_schema2: Schema
) -> Generator:
    """Yields a RDBBuilder with a mysql database and test schema"""
    obj = RDBBuilder(rdb=mysql_database, schema=test_schema2)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_builder_postgres")
def fixture_rdb_builder_postgres(
    postgres_database: PostgresDatabase, test_schema2: Schema
) -> Generator:
    """Yields a RDBBuilder with a mysql database and test schema"""
    obj = RDBBuilder(rdb=postgres_database, schema=test_schema2)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="module", name="rdb_builder_synapse")
def fixture_rdb_builder_synapse(
    synapse_database: SynapseDatabase, test_schema2: Schema
) -> Generator:
    """Yields a RDBBuilder with a synapse database and test schema"""
    obj = RDBBuilder(rdb=synapse_database, schema=test_schema2)
    yield obj
    synapse_database.delete_all_tables()


@pytest.fixture(scope="class", name="rdb_updater_mysql")
def fixture_rdb_updater_mysql(
    mysql_database: MySQLDatabase, manifest_store: ManifestStore
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=mysql_database, manifest_store=manifest_store)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="class", name="rdb_updater_postgres")
def fixture_rdb_updater_postgres(
    postgres_database: PostgresDatabase, manifest_store: ManifestStore
) -> Generator:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = RDBUpdater(rdb=postgres_database, manifest_store=manifest_store)
    yield obj
    obj.rdb.drop_all_tables()


@pytest.fixture(scope="class", name="rdb_updater_synapse")
def fixture_rdb_updater_synapse(
    synapse_database: SynapseDatabase, manifest_store: ManifestStore
) -> Generator:
    """Yields a RDBUpdater with a synapse database and test schema"""
    obj = RDBUpdater(rdb=synapse_database, manifest_store=manifest_store)
    yield obj
    synapse_database.delete_all_tables()


@pytest.fixture(scope="function", name="query_store")
def fixture_query_store(synapse_test_query_store: QueryStore) -> Generator:
    """Yields a query store"""
    obj = synapse_test_query_store
    yield obj


@pytest.fixture(scope="function", name="rdb_queryer_mysql")
def fixture_rdb_queryer_mysql(
    mysql_database: MySQLDatabase,
    query_store: QueryStore,
) -> Generator:
    """Yields a RDBQueryer with a mysql database with test schema tables added"""
    obj = RDBQueryer(
        rdb=mysql_database,
        query_store=query_store,
    )
    yield obj
    for table_name in query_store.get_table_names():
        query_store.delete_table(table_name)


@pytest.fixture(scope="function", name="rdb_queryer_postgres")
def fixture_rdb_queryer_postgres(
    postgres_database: MySQLDatabase,
    query_store: QueryStore,
) -> Generator:
    """Yields a RDBQueryer with a postgres database with test schema tables added"""
    obj = RDBQueryer(
        rdb=postgres_database,
        query_store=query_store,
    )
    yield obj
    for table_name in query_store.get_table_names():
        query_store.delete_table(table_name)


class TestIntegration1:
    """Integration tests with upserts"""

    def test_mysql(  # pylint: disable=too-many-arguments
        self,
        rdb_builder_mysql: RDBBuilder,
        rdb_updater_mysql: RDBUpdater,
        rdb_queryer_mysql: RDBQueryer,
        data_directory: str,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        rdb_builder = rdb_builder_mysql
        assert rdb_builder.rdb.get_table_names() == []
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_mysql
        rdb_updater.update_database()
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0

        rdb_queryer = rdb_queryer_mysql
        assert rdb_queryer.query_store.get_table_names() == []
        path = os.path.join(data_directory, "test_queries_mysql.csv")
        rdb_queryer.store_query_results(path)
        assert rdb_queryer.query_store.get_table_names() == test_schema_table_names

    def test_postgres(  # pylint: disable=too-many-arguments
        self,
        rdb_builder_postgres: RDBBuilder,
        rdb_updater_postgres: RDBUpdater,
        rdb_queryer_postgres: RDBQueryer,
        data_directory: str,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Postgres"""
        rdb_builder = rdb_builder_postgres
        assert rdb_builder.rdb.get_table_names() == []
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_postgres
        rdb_updater.update_database()
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0

        rdb_queryer = rdb_queryer_postgres
        assert rdb_queryer.query_store.get_table_names() == []
        path = os.path.join(data_directory, "test_queries_postgres.csv")
        rdb_queryer.store_query_results(path)
        assert rdb_queryer.query_store.get_table_names() == test_schema_table_names

    def test_synapse_update_all_database_tables(
        self,
        rdb_updater_synapse: RDBUpdater,
        rdb_builder_synapse: RDBBuilder,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Synapse"""
        rdb_builder = rdb_builder_synapse
        assert rdb_builder.rdb.get_table_names() == []
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_synapse
        rdb_updater.update_database()
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0


class TestIntegration2:
    """Integration tests with inserts"""

    def test_mysql(  # pylint: disable=too-many-arguments
        self,
        rdb_builder_mysql: RDBBuilder,
        rdb_updater_mysql: RDBUpdater,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        rdb_builder = rdb_builder_mysql
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_mysql
        rdb_updater.update_database(method="insert")
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0

    def test_postgres(  # pylint: disable=too-many-arguments
        self,
        rdb_builder_postgres: RDBBuilder,
        rdb_updater_postgres: RDBUpdater,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Postgres"""
        rdb_builder = rdb_builder_postgres
        rdb_builder.build_database()
        assert rdb_builder.rdb.get_table_names() == test_schema_table_names

        rdb_updater = rdb_updater_postgres
        rdb_updater.update_database(method="insert")
        for name in test_schema_table_names:
            table = rdb_updater.rdb.query_table(name)
            assert len(table.index) > 0
