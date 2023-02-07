"""Testing for GFF"""
from typing import Generator
from copy import copy
import pytest
from sqlalchemy import exc
from schematic_db.rdb import (
    MySQLDatabase,
    MySQLConfig,
    SynapseDatabase,
    PostgresDatabase,
)
from schematic_db.synapse import SynapseConfig
from schematic_db.schema import Schema, SchemaConfig, get_key_attribute
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.query_store import QueryStore, SynapseQueryStore
from schematic_db.rdb_queryer import RDBQueryer
from schematic_db.db_config import DBConfig


@pytest.fixture(scope="module", name="gff_database_table_names")
def fixture_gff_database_table_names() -> Generator:
    """
    Yields a list of table names the gff database should have.
    """
    table_names = [
        "AnimalModel",
        "Antibody",
        "Biobank",
        "CellLine",
        "Development",
        "Donor",
        "Funder",
        "GeneticReagent",
        "Investigator",
        "Mutation",
        "MutationDetails",
        "Observation",
        "Publication",
        "Resource",
        "ResourceApplication",
        "Usage",
        "Vendor",
        "VendorItem",
    ]
    yield table_names


@pytest.fixture(scope="module", name="gff_synapse_project_id")
def fixture_gff_synapse_project_id() -> Generator:
    """Yields the synapse id for the gff schema project id"""
    yield "syn38296792"


@pytest.fixture(scope="module", name="gff_synapse_asset_view_id")
def fixture_gff_synapse_asset_view_id() -> Generator:
    """Yields the synapse id for the gff schema project id"""
    yield "syn38308526"


# Databases -------------------------------------------------------------------


@pytest.fixture(scope="module", name="mysql_database")
def fixture_mysql_database(mysql_config: MySQLConfig) -> Generator:
    """
    Yields a MYSQL object with database named 'gff_test_schema'
    """
    config = copy(mysql_config)
    config.name = "gff_test_schema"
    obj = MySQLDatabase(config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="module", name="postgres_database")
def fixture_postgres_database(postgres_config: MySQLConfig) -> Generator:
    """
    Yields a Postgres object with database named 'gff_test_schema'
    """
    config = copy(postgres_config)
    config.name = "gff_test_schema"
    obj = PostgresDatabase(config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="module", name="synapse_database")
def fixture_synapse_database(secrets_dict: dict) -> Generator:
    """
    Yields a Synapse object used for testing databases
    """
    obj = SynapseDatabase(
        SynapseConfig(
            project_id="syn42838499",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


# Schema ----------------------------------------------------------------------


@pytest.fixture(scope="module", name="schema")
def fixture_schema(
    gff_synapse_project_id: str, gff_synapse_asset_view_id: str, secrets_dict: dict
) -> Generator:
    """Yields a Schema using the GFF tools schema"""
    schema_url = (
        "https://raw.githubusercontent.com/nf-osi/"
        "nf-research-tools-schema/main/nf-research-tools.jsonld"
    )
    config = SchemaConfig(
        schema_url,
        gff_synapse_project_id,
        gff_synapse_asset_view_id,
        secrets_dict["synapse"]["auth_token"],
    )
    obj = Schema(config, primary_key_getter=get_key_attribute)
    yield obj


@pytest.fixture(scope="module", name="gff_db_config")
def fixture_gff_db_config(schema: Schema) -> Generator:
    """Yields a config from the GFF tools schema"""
    yield schema.db_config


@pytest.mark.gff
@pytest.mark.schematic
class TestSchema:
    """Testing for GFF Schema"""

    def test_create_db_config(self, gff_db_config: DBConfig) -> None:
        """Testing for Schema.create_db_config()"""
        assert gff_db_config.get_config_names() == [
            "Donor",
            "AnimalModel",
            "CellLine",
            "Antibody",
            "GeneticReagent",
            "Funder",
            "Publication",
            "Investigator",
            "Resource",
            "MutationDetails",
            "Vendor",
            "Development",
            "Mutation",
            "ResourceApplication",
            "Observation",
            "VendorItem",
            "Biobank",
            "Usage",
        ]

    def test_get_manifests(self, schema: Schema, gff_db_config: DBConfig) -> None:
        """Testing for Schema.get_manifests()"""
        manifests1 = schema.get_manifests(gff_db_config.configs[0])
        assert len(manifests1) == 2
        assert list(manifests1[0].columns) == [
            "age",
            "donorId",
            "parentDonorId",
            "race",
            "sex",
            "species",
        ]

        manifests2 = schema.get_manifests(gff_db_config.configs[1])
        assert len(manifests2) == 1
        assert list(manifests2[0].columns) == [
            "animalModelDisease",
            "animalModelofManifestation",
            "animalModelId",
            "animalState",
            "backgroundStrain",
            "backgroundSubstrain",
            "donorId",
            "generation",
            "strainNomenclature",
            "transplantationDonorId",
            "transplantationType",
        ]


# RDBUpdater ------------------------------------------------------------------


@pytest.fixture(scope="module", name="rdb_updater_mysql")
def fixture_rdb_updater_mysql(
    mysql_database: MySQLDatabase, schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and gff schema with tables added"""
    obj = RDBUpdater(rdb=mysql_database, schema=schema)
    yield obj


@pytest.fixture(scope="module", name="rdb_updater_postgres")
def fixture_rdb_updater_postgres(
    postgres_database: PostgresDatabase, schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a postgres database and gff schema with tables added"""
    obj = RDBUpdater(rdb=postgres_database, schema=schema)
    yield obj


@pytest.fixture(scope="module", name="rdb_updater_synapse")
def fixture_rdb_updater_synapse(
    synapse_database: SynapseDatabase, schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a synapse database and gff schema with tables added"""
    obj = RDBUpdater(rdb=synapse_database, schema=schema)
    yield obj


@pytest.mark.gff
@pytest.mark.schematic
@pytest.mark.synapse
class TestRDBUpdater:
    """Testing for RDB with Synapse database"""

    def test_build_and_update_mysql(
        self, rdb_updater_mysql: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Builds and updates the gff database in MySQL"""
        with pytest.raises(exc.OperationalError, match="Row size too large"):
            obj = rdb_updater_mysql
            assert obj.rdb.get_table_names() == []
            obj.build_database()
            assert obj.rdb.get_table_names() == gff_database_table_names
            obj.update_database()
            assert obj.rdb.get_table_names() == gff_database_table_names

    def test_build_and_update_postgres(
        self, rdb_updater_postgres: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Builds and updates gff database in Postgres"""
        obj = rdb_updater_postgres
        assert obj.rdb.get_table_names() == []
        obj.build_database()
        assert obj.rdb.get_table_names() == gff_database_table_names
        obj.update_database()
        assert obj.rdb.get_table_names() == gff_database_table_names

    def test_build_and_update_synapse(
        self, rdb_updater_synapse: RDBUpdater, gff_database_table_names: list[str]
    ) -> None:
        """Builds and updates database in Synapse"""
        obj = rdb_updater_synapse
        obj.build_database()
        assert obj.rdb.get_table_names() == gff_database_table_names
        obj.update_database()
        assert obj.rdb.get_table_names() == gff_database_table_names


# RDBQueryer ------------------------------------------------------------------


@pytest.fixture(scope="module", name="query_store")
def fixture_query_store(secrets_dict: dict) -> Generator:
    """
    Yields a Synapse Query Store for gff
    """
    obj = SynapseQueryStore(
        SynapseConfig(
            project_id="syn39024404",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_mysql")
def fixture_rdb_queryer_mysql(
    rdb_updater_mysql: RDBUpdater,
    query_store: QueryStore,
) -> Generator:
    """Yields a RDBQueryer with a mysql database with gff tables added"""
    rdb_updater_mysql.build_database()
    obj = RDBQueryer(
        rdb=rdb_updater_mysql.rdb,
        query_store=query_store,
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_postgres")
def fixture_rdb_queryer_postgres(
    rdb_updater_postgres: RDBUpdater,
    query_store: QueryStore,
) -> Generator:
    """Yields a RDBQueryer with a postgres database with gff tables added"""
    rdb_updater_postgres.build_database()
    obj = RDBQueryer(
        rdb=rdb_updater_postgres.rdb,
        query_store=query_store,
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_queryer_synapse")
def fixture_rdb_queryer_synapse(
    synapse_database: SynapseDatabase,
    query_store: QueryStore,
    gff_database_table_names: list[str],
) -> Generator:
    """Yields a RDBQueryer with the gff synapse database"""
    assert synapse_database.get_table_names() == gff_database_table_names
    obj = RDBQueryer(
        rdb=synapse_database,
        query_store=query_store,
    )
    yield obj


@pytest.mark.gff
@pytest.mark.synapse
@pytest.mark.schematic
class TestRDBQueryer:  # pylint: disable=too-few-public-methods
    """Testing for RDBQueryer using the gff database"""

    def test_store_query_results_postgres(
        self,
        rdb_queryer_postgres: RDBQueryer,
    ) -> None:
        """Testing for RDBQueryer.store_query_results()"""
        rdb_queryer_postgres.store_query_results("tests/data/gff_queries_postgres.csv")
