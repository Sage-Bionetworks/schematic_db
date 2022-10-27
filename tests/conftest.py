"""Fixtures for all tests"""
import os
from datetime import datetime
from typing import Generator
import pytest
import pandas as pd
import numpy as np
from yaml import safe_load
from schematic_db.db_config import (
    DBConfig,
    DBObjectConfig,
    DBAttributeConfig,
    DBDatatype,
    DBForeignKey,
)

from schematic_db.query_store import QueryStore, SynapseQueryStore
from schematic_db.rdb import MySQLDatabase
from schematic_db.rdb_updater import RDBUpdater
from schematic_db.rdb_queryer import RDBQueryer
from schematic_db.synapse import Synapse
from schematic_db.schema import Schema

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
SECRETS_PATH = os.path.join(DATA_DIR, "local_secrets.yml")
if not os.path.exists(SECRETS_PATH):
    SECRETS_PATH = os.path.join(DATA_DIR, "secrets.yml")

# files -----------------------------------------------------------------------
@pytest.fixture(scope="session", name="secrets_dict")
def fixture_secrets_dict() -> Generator:
    """
    Yields a dict with various secrets, either locally or from a github action
    """
    with open(SECRETS_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config


@pytest.fixture(scope="session", name="query_csv_path")
def fixture_query_csv_path() -> Generator:
    """Yields a path to a file of test SQL queries"""
    path = os.path.join(DATA_DIR, "queries.csv")
    yield path


# gff database objects --------------------------------------------------------


@pytest.fixture(scope="session", name="gff_mysql")
def fixture_gff_mysql(secrets_dict: dict) -> Generator:
    """
    Yields a MYSQL object with database named 'gff_test_schema'
    """
    obj = MySQLDatabase(
        {
            "username": secrets_dict["mysql"]["username"],
            "password": secrets_dict["mysql"]["password"],
            "host": secrets_dict["mysql"]["host"],
            "schema": "gff_test_schema",
        }
    )
    yield obj
    table_names = [
        "Usage",
        "Biobank",
        "VendorItem",
        "Observation",
        "ResourceApplication",
        "Mutation",
        "Development",
        "Vendor",
        "MutationDetails",
        "Resource",
        "Investigator",
        "Publication",
        "Funder",
        "GeneticReagent",
        "Antibody",
        "CellLine",
        "AnimalModel",
        "Donor",
    ]
    for table_name in table_names:
        if table_name in obj.get_table_names():
            obj.drop_table(table_name)
    assert obj.get_table_names() == []


@pytest.fixture(scope="session", name="gff_synapse_project_id")
def fixture_gff_synapse_project_id() -> Generator:
    """Yields the synapse id for the gff schema project id"""
    yield "syn38296792"


@pytest.fixture(scope="session", name="gff_synapse_asset_view_id")
def fixture_gff_synapse_asset_view_id() -> Generator:
    """Yields the synapse id for the gff schema project id"""
    yield "syn38308526"


@pytest.fixture(scope="session", name="gff_schema")
def fixture_gff_schema(
    gff_synapse_project_id: str, gff_synapse_asset_view_id: str, secrets_dict: dict
) -> Generator:
    """Yields a Schema using the GFF tools schema"""
    schema_url = (
        "https://raw.githubusercontent.com/nf-osi/"
        "nf-research-tools-schema/main/nf-research-tools.jsonld"
    )
    obj = Schema(
        schema_url,
        gff_synapse_project_id,
        gff_synapse_asset_view_id,
        secrets_dict["synapse"]["auth_token"],
    )
    yield obj


@pytest.fixture(scope="module", name="rdb_updater_mysql_gff")
def fixture_rdb_updater_mysql_gff(
    gff_mysql: MySQLDatabase, gff_schema: Schema
) -> Generator:
    """Yields a RDBUpdater with a mysql database and gff schema with tables added"""
    obj = RDBUpdater(rdb=gff_mysql, schema=gff_schema)
    obj.update_all_database_tables()
    yield obj


@pytest.fixture(scope="session", name="synapse_gff_query_store")
def fixture_synapse_gff_query_store(secrets_dict: dict) -> Generator:
    """
    Yields a Synapse Query Store for gff
    """
    obj = SynapseQueryStore(
        {
            "project_id": "syn39024404",
            "username": secrets_dict["synapse"]["username"],
            "auth_token": secrets_dict["synapse"]["auth_token"],
        }
    )
    yield obj


@pytest.fixture(scope="session", name="gff_query_csv_path")
def fixture_gff_query_csv_path() -> Generator:
    """Yields a path to a file of test SQL queries for gff"""
    path = os.path.join(DATA_DIR, "gff_queries.csv")
    yield path


@pytest.fixture(scope="module", name="rdb_queryer_mysql_gff")
def fixture_rdb_queryer_mysql_gff(
    rdb_updater_mysql_gff: RDBUpdater, synapse_gff_query_store: QueryStore
) -> Generator:
    """Yields a RDBQueryer with a mysql database with gff tables added"""
    obj = RDBQueryer(
        rdb=rdb_updater_mysql_gff.rdb,
        query_store=synapse_gff_query_store,
    )
    yield obj


# test database objects -------------------------------------------------------


@pytest.fixture(scope="session", name="mysql")
def fixture_mysql(secrets_dict: dict) -> Generator:
    """
    Yields a MYSQL object
    """
    obj = MySQLDatabase(
        {
            "username": secrets_dict["mysql"]["username"],
            "password": secrets_dict["mysql"]["password"],
            "host": secrets_dict["mysql"]["host"],
            "schema": "test_schema",
        }
    )
    yield obj
    test_table_names = ["table_three", "table_one", "table_two"]
    for table_name in test_table_names:
        if table_name in obj.get_table_names():
            obj.drop_table(table_name)
    assert obj.get_table_names() == []


@pytest.fixture(scope="session", name="test_synapse_project_id")
def fixture_test_synapse_project_id() -> Generator:
    """Yields the synapse id for the test schema project id"""
    yield "syn23643250"


@pytest.fixture(scope="session", name="test_synapse_asset_view_id")
def fixture_test_synapse_asset_view_id() -> Generator:
    """Yields the synapse id for the test schema project id"""
    yield "syn23643253"


@pytest.fixture(scope="session", name="test_schema")
def fixture_test_schema(
    test_synapse_project_id: str, test_synapse_asset_view_id: str, secrets_dict: dict
) -> Generator:
    """Yields a Schema using the database specific test schema"""
    schema_url = (
        "https://raw.githubusercontent.com/Sage-Bionetworks/"
        "schematic/develop-rdb-export-joins/tests/data/example.rdb.model.jsonld"
    )
    obj = Schema(
        schema_url,
        test_synapse_project_id,
        test_synapse_asset_view_id,
        secrets_dict["synapse"]["auth_token"],
    )
    yield obj


# database objects ------------------------------------------------------------


@pytest.fixture(scope="session", name="synapse_database_table_names")
def fixture_synapse_database_table_names() -> Generator:
    """
    Yields a list of table names the database testing project should start out with.
    """
    yield ["test_table_one"]


@pytest.fixture(scope="session", name="synapse_database_project")
def fixture_synapse(
    secrets_dict: dict, synapse_database_table_names: list[str]
) -> Generator:
    """
    Yields a Synapse object used for testing databases
    """
    obj = Synapse(
        {
            "project_id": "syn33832432",
            "username": secrets_dict["synapse"]["username"],
            "auth_token": secrets_dict["synapse"]["auth_token"],
        }
    )
    if obj.get_table_names() != synapse_database_table_names:
        raise ValueError(
            "Synapse_database_project has incorrect table names;",
            f"Actual: {obj.get_table_names()}",
            f"Expected: {synapse_database_table_names}",
        )
    yield obj
    if obj.get_table_names() != synapse_database_table_names:
        raise ValueError(
            "Synapse_database_project has incorrect table names;",
            f"Actual: {obj.get_table_names()}",
            f"Expected: {synapse_database_table_names}",
        )


@pytest.fixture(scope="module", name="rdb_queryer_mysql")
def fixture_rdb_queryer_mysql(
    mysql: MySQLDatabase, synapse_query_store: QueryStore
) -> Generator:
    """Yields a RDBQueryer"""
    obj = RDBQueryer(
        rdb=mysql,
        query_store=synapse_query_store,
    )
    yield obj


# config objects --------------------------------------------------------------
@pytest.fixture(scope="session", name="gff_db_config")
def fixture_gff_db_config(gff_schema: Schema) -> Generator:
    """Yields a config from the GFF tools schema"""
    yield gff_schema.create_db_config()


@pytest.fixture(scope="session")
def table_one() -> Generator:
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_one_col": ["key1", "key2", "key3"],
            "string_one_col": ["a", "b", np.nan],
            "int_one_col": [1, pd.NA, 3],
            "double_one_col": [1.1, 2.2, np.nan],
            "date_one_col": [datetime(2022, 8, 2), np.nan, datetime(2022, 8, 2)],
            "bool_one_col": [pd.NA, True, False],
        }
    )
    dataframe = dataframe.astype({"int_one_col": "Int64", "bool_one_col": "boolean"})
    dataframe["date_one_col"] = pd.to_datetime(dataframe["date_one_col"]).dt.date
    yield dataframe


@pytest.fixture(scope="session", name="table_one_config")
def fixture_table_one_config() -> Generator:
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name="table_one",
        attributes=[
            DBAttributeConfig(name="pk_one_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_one_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="int_one_col", datatype=DBDatatype.INT),
            DBAttributeConfig(name="double_one_col", datatype=DBDatatype.FLOAT),
            DBAttributeConfig(name="date_one_col", datatype=DBDatatype.DATE),
            DBAttributeConfig(name="bool_one_col", datatype=DBDatatype.BOOLEAN),
        ],
        primary_keys=["pk_one_col"],
        foreign_keys=[],
    )
    yield table_config


@pytest.fixture(scope="session")
def table_two() -> Generator:
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_two_col": ["key1", "key2", "key3", "key4"],
            "string_two_col": ["a", "b", "c", "d"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session")
def table_two_b() -> Generator:
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_two_col": ["key5", "key6", "key7", "key8"],
            "string_two_col": ["a", "b", "c", "d"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session", name="table_two_config")
def fixture_table_two_config() -> Generator:
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name="table_two",
        attributes=[
            DBAttributeConfig(name="pk_two_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_two_col", datatype=DBDatatype.TEXT),
        ],
        primary_keys=["pk_two_col"],
        foreign_keys=[],
    )
    yield table_config


@pytest.fixture(scope="session", name="table_two_config_combined")
def fixture_table_two_config_combined() -> Generator:
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name="table_two",
        attributes=[
            DBAttributeConfig(name="pk_two_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_two_col", datatype=DBDatatype.TEXT),
        ],
        primary_keys=["pk_two_col"],
        foreign_keys=[],
    )
    yield table_config


@pytest.fixture(scope="session")
def table_three() -> Generator:
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame(
        {
            "pk_one_col": ["key1", "key1", "key2", "key2"],
            "pk_two_col": ["key1", "key2", "key1", "key2"],
            "string_three_col": ["a", "b", "c", "d"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session", name="table_three_config")
def fixture_table_three_config() -> Generator:
    """
    Yields a DBObjectConfig object with two keys that are both primary and foreign
    """
    table_config = DBObjectConfig(
        name="table_three",
        attributes=[
            DBAttributeConfig(name="pk_one_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="pk_two_col", datatype=DBDatatype.TEXT),
            DBAttributeConfig(name="string_three_col", datatype=DBDatatype.TEXT),
        ],
        primary_keys=["pk_one_col", "pk_two_col"],
        foreign_keys=[
            DBForeignKey(
                name="pk_one_col",
                foreign_object_name="table_one",
                foreign_attribute_name="pk_one_col",
            ),
            DBForeignKey(
                name="pk_two_col",
                foreign_object_name="table_two",
                foreign_attribute_name="pk_two_col",
            ),
        ],
    )
    yield table_config


@pytest.fixture(scope="session")
def table_123_unormalized() -> Generator:
    """
    Yields a pd.Dataframe. This is what a merged table might look like.
    """
    dataframe = pd.DataFrame(
        {
            "pk_one_col": ["key1", "key1", "key2", "key2"],
            "pk_two_col": ["key1", "key2", "key1", "key2"],
            "string_three_col": ["a", "b", "c", "d"],
            "string_one_col": ["a", "a", "b", "a"],
        }
    )
    yield dataframe


@pytest.fixture(scope="session", name="table_configs")
def fixture_table_configs(
    table_one_config: DBObjectConfig,
    table_two_config: DBObjectConfig,
    table_three_config: DBObjectConfig,
) -> Generator:
    """Yields a DBObjectConfigList"""
    yield DBConfig([table_one_config, table_two_config, table_three_config])
