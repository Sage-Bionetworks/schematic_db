"""Testing for RDBUpdater."""

from typing import Generator
import os
import pytest
from schematic_db.databases.mysql import MySQLDatabase
from schematic_db.databases.postgres import PostgresDatabase
from schematic_db.databases.synapse_database import SynapseDatabase
from schematic_db.db_builder import DBBuilder
from schematic_db.schema_generator.schema_generator import SchemaGenerator
from schematic_db.db_updater import DBUpdater
from schematic_db.manifest_store.api_manifest_store import APIManifestStore
from schematic_db.query_store.query_store import QueryStore
from schematic_db.db_queryer import DBQueryer

# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments


@pytest.fixture(scope="module", name="db_builder_mysql")
def fixture_db_builder_mysql(
    mysql_database: MySQLDatabase, schema_generator2: SchemaGenerator
) -> Generator[DBBuilder, None, None]:
    """Yields a DBBuilder with a mysql database and test schema"""
    obj = DBBuilder(db=mysql_database, schema_generator=schema_generator2)
    yield obj
    obj.db.drop_all_tables()


@pytest.fixture(scope="module", name="db_builder_postgres")
def fixture_db_builder_postgres(
    postgres_database: PostgresDatabase, schema_generator2: SchemaGenerator
) -> Generator[DBBuilder, None, None]:
    """Yields a RDBBuilder with a mysql database and test schema"""
    obj = DBBuilder(db=postgres_database, schema_generator=schema_generator2)
    yield obj
    obj.db.drop_all_tables()


@pytest.fixture(scope="module", name="db_builder_synapse")
def fixture_db_builder_synapse(
    synapse_database: SynapseDatabase, schema_generator2: SchemaGenerator
) -> Generator[DBBuilder, None, None]:
    """Yields a RDBBuilder with a synapse database and test schema"""
    obj = DBBuilder(db=synapse_database, schema_generator=schema_generator2)
    yield obj
    synapse_database.delete_all_tables()


@pytest.fixture(scope="class", name="db_updater_mysql")
def fixture_db_updater_mysql(
    mysql_database: MySQLDatabase, api_manifest_store: APIManifestStore
) -> Generator[DBUpdater, None, None]:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = DBUpdater(db=mysql_database, manifest_store=api_manifest_store)
    yield obj
    obj.db.drop_all_tables()


@pytest.fixture(scope="class", name="db_updater_postgres")
def fixture_db_updater_postgres(
    postgres_database: PostgresDatabase, api_manifest_store: APIManifestStore
) -> Generator[DBUpdater, None, None]:
    """Yields a RDBUpdater with a mysql database and test schema"""
    obj = DBUpdater(db=postgres_database, manifest_store=api_manifest_store)
    yield obj
    obj.db.drop_all_tables()


@pytest.fixture(scope="class", name="db_updater_synapse")
def fixture_db_updater_synapse(
    synapse_database: SynapseDatabase, api_manifest_store: APIManifestStore
) -> Generator[DBUpdater, None, None]:
    """Yields a RDBUpdater with a synapse database and test schema"""
    obj = DBUpdater(db=synapse_database, manifest_store=api_manifest_store)
    yield obj
    synapse_database.delete_all_tables()


@pytest.fixture(scope="function", name="query_store")
def fixture_query_store(
    synapse_test_query_store: QueryStore,
) -> Generator[QueryStore, None, None]:
    """Yields a query store"""
    obj = synapse_test_query_store
    yield obj


@pytest.fixture(scope="function", name="db_queryer_mysql")
def fixture_db_queryer_mysql(
    mysql_database: MySQLDatabase,
    query_store: QueryStore,
) -> Generator[DBQueryer, None, None]:
    """Yields a RDBQueryer with a mysql database with test schema tables added"""
    obj = DBQueryer(
        db=mysql_database,
        query_store=query_store,
    )
    yield obj
    for table_name in query_store.get_table_names():
        query_store.delete_table(table_name)


@pytest.fixture(scope="function", name="db_queryer_postgres")
def fixture_db_queryer_postgres(
    postgres_database: MySQLDatabase,
    query_store: QueryStore,
) -> Generator[DBQueryer, None, None]:
    """Yields a RDBQueryer with a postgres database with test schema tables added"""
    obj = DBQueryer(
        db=postgres_database,
        query_store=query_store,
    )
    yield obj
    for table_name in query_store.get_table_names():
        query_store.delete_table(table_name)


class TestIntegration1:
    """Integration tests with upserts"""

    def test_mysql(
        self,
        db_builder_mysql: DBBuilder,
        db_updater_mysql: DBUpdater,
        db_queryer_mysql: DBQueryer,
        data_directory: str,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        db_builder = db_builder_mysql
        assert db_builder.db.get_table_names() == []
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_mysql
        db_updater.update_database()
        for name in test_schema_table_names:
            table = db_updater.db.query_table(name)
            assert len(table.index) > 0

        db_updater.update_table("Patient")

        db_queryer = db_queryer_mysql
        assert db_queryer.query_store.get_table_names() == []
        path = os.path.join(data_directory, "test_queries_mysql.csv")
        db_queryer.store_query_results(path)
        assert db_queryer.query_store.get_table_names() == test_schema_table_names

    def test_postgres(
        self,
        db_builder_postgres: DBBuilder,
        db_updater_postgres: DBUpdater,
        db_queryer_postgres: DBQueryer,
        data_directory: str,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Postgres"""
        db_builder = db_builder_postgres
        assert db_builder.db.get_table_names() == []
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_postgres
        db_updater.update_database()
        for name in test_schema_table_names:
            table = db_updater.db.query_table(name)
            assert len(table.index) > 0

        db_updater.update_table("Patient")

        db_queryer = db_queryer_postgres
        assert db_queryer.query_store.get_table_names() == []
        path = os.path.join(data_directory, "test_queries_postgres.csv")
        db_queryer.store_query_results(path)
        assert db_queryer.query_store.get_table_names() == test_schema_table_names

    def test_synapse_update_all_database_tables(
        self,
        db_updater_synapse: DBUpdater,
        db_builder_synapse: DBBuilder,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Synapse"""
        db_builder = db_builder_synapse
        assert db_builder.db.get_table_names() == []
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_synapse
        db_updater.update_database()
        for name in test_schema_table_names:
            table = db_updater.db.query_table(name)
            assert len(table.index) > 0

        db_updater.update_table("Patient")


class TestIntegration2:
    """Integration tests with inserts"""

    def test_mysql(
        self,
        db_builder_mysql: DBBuilder,
        db_updater_mysql: DBUpdater,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        db_builder = db_builder_mysql
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_mysql
        db_updater.update_database(method="insert")
        for name in test_schema_table_names:
            table = db_updater.db.query_table(name)
            assert len(table.index) > 0

    def test_postgres(
        self,
        db_builder_postgres: DBBuilder,
        db_updater_postgres: DBUpdater,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in Postgres"""
        db_builder = db_builder_postgres
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_postgres
        db_updater.update_database(method="insert")
        for name in test_schema_table_names:
            table = db_updater.db.query_table(name)
            assert len(table.index) > 0


class TestIntegration3:  # pylint: disable=too-few-public-methods
    """Integration tests with upserts only update one table"""

    def test_mysql(
        self,
        db_builder_mysql: DBBuilder,
        db_updater_mysql: DBUpdater,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        db_builder = db_builder_mysql
        assert db_builder.db.get_table_names() == []
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_mysql
        db_updater.update_database(table_names=["Patient"])

        for name in test_schema_table_names:
            table = db_updater.db.query_table(name)
            if name == "Patient":
                assert len(table.index) > 0
            else:
                assert len(table.index) == 0


class TestIntegration4:  # pylint: disable=too-few-public-methods
    """Integration tests with chunking patient table"""

    def test_mysql(
        self,
        db_builder_mysql: DBBuilder,
        db_updater_mysql: DBUpdater,
        test_schema_table_names: list[str],
    ) -> None:
        """Creates the test database in MySQL"""
        db_builder = db_builder_mysql
        assert db_builder.db.get_table_names() == []
        db_builder.build_database()
        assert db_builder.db.get_table_names() == test_schema_table_names

        db_updater = db_updater_mysql
        db_updater.update_database(table_names=["Patient"], chunk_size=1)

        table = db_updater.db.query_table("Patient")
        assert len(table.index) == 8
