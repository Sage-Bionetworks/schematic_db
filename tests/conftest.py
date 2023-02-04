"""Fixtures for all tests"""
import os
from datetime import datetime
from typing import Generator, Any
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
from schematic_db.rdb import MySQLDatabase, MySQLConfig
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.rdb_queryer import RDBQueryer
from schematic_db.synapse import Synapse, SynapseConfig
from schematic_db.schema import Schema, SchemaConfig

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
SECRETS_PATH = os.path.join(DATA_DIR, "local_secrets.yml")
if not os.path.exists(SECRETS_PATH):
    SECRETS_PATH = os.path.join(DATA_DIR, "secrets.yml")

# files -----------------------------------------------------------------------


@pytest.fixture(scope="session", name="data_directory")
def fixture_data_directory() -> Generator:
    """
    Yields the path to the testing data directory
    """
    yield DATA_DIR


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


# test schema database objects ------------------------------------------------
# objects involving the main schematic test schema


@pytest.fixture(scope="session", name="test_schema_table_names")
def fixture_test_schema_table_names() -> Generator:
    """
    Yields a list of table names the test schema database should have.
    """
    table_names = [
        "Biospecimen",
        "BulkRnaSeq",
        "Patient",
    ]
    yield table_names


@pytest.fixture(scope="session", name="test_schema_json_url")
def fixture_test_schema_json_url() -> Generator:
    """Yields the url for the main test schema json"""
    url = (
        "https://raw.githubusercontent.com/Sage-Bionetworks/"
        "Schematic-DB-Test-Schemas/main/test_schema.jsonld"
    )
    yield url


@pytest.fixture(scope="session", name="test_schema_json_url2")
def fixture_test_schema_json_url2() -> Generator:
    """
    Yields the url for the secondary test schema json.
    This schema has display names that cna be used in a database.
    """
    url = (
        "https://raw.githubusercontent.com/Sage-Bionetworks/"
        "Schematic-DB-Test-Schemas/main/test_schema2.jsonld"
    )
    yield url


@pytest.fixture(scope="session", name="mysql_config")
def fixture_mysql_config(secrets_dict: dict) -> Generator:
    """Yields a MYSQlConfig object"""
    yield MySQLConfig(
        username=secrets_dict["mysql"]["username"],
        password=secrets_dict["mysql"]["password"],
        host=secrets_dict["mysql"]["host"],
        name="test_schema",
    )


@pytest.fixture(scope="session", name="postgres_config")
def fixture_postgres_config(secrets_dict: dict) -> Generator:
    """Yields a MYSQlConfig object"""
    yield MySQLConfig(
        username=secrets_dict["postgres"]["username"],
        password=secrets_dict["postgres"]["password"],
        host=secrets_dict["postgres"]["host"],
        name="test_schema",
    )


@pytest.fixture(scope="session", name="mysql_database")
def fixture_mysql_database(mysql_config: MySQLConfig) -> Generator:
    """
    Yields a MYSQL object
    """
    obj = MySQLDatabase(mysql_config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="session", name="postgres_database")
def fixture_postgres_database(postgres_config: MySQLConfig) -> Generator:
    """
    Yields a Postgres object
    """
    obj = PostgresDatabase(postgres_config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="session", name="test_synapse_project_id")
def fixture_test_synapse_project_id() -> Generator:
    """Yields the synapse id for the test schema project id"""
    yield "syn47994571"


@pytest.fixture(scope="session", name="test_synapse_asset_view_id")
def fixture_test_synapse_asset_view_id() -> Generator:
    """Yields the synapse id for the test schema asset view"""
    yield "syn47997084"


@pytest.fixture(scope="session", name="test_schema")
def fixture_test_schema(
    test_synapse_project_id: str,
    test_synapse_asset_view_id: str,
    secrets_dict: dict,
    test_schema_json_url: str,
) -> Generator:
    """Yields a Schema using the database specific test schema"""
    config = SchemaConfig(
        test_schema_json_url,
        test_synapse_project_id,
        test_synapse_asset_view_id,
        secrets_dict["synapse"]["auth_token"],
    )
    obj = Schema(config)
    yield obj


@pytest.fixture(scope="session", name="test_schema2")
def fixture_test_schema2(
    test_synapse_project_id: str,
    test_synapse_asset_view_id: str,
    secrets_dict: dict,
    test_schema_json_url2: str,
) -> Generator:
    """
    Yields a Schema using the database specific test schema where display names are intended to be
     used
    """
    config = SchemaConfig(
        test_schema_json_url2,
        test_synapse_project_id,
        test_synapse_asset_view_id,
        secrets_dict["synapse"]["auth_token"],
    )
    obj = Schema(config)
    yield obj


@pytest.fixture(scope="session", name="synapse_test_query_store")
def fixture_synapse_test_query_store(secrets_dict: dict) -> Generator:
    """
    Yields a Synapse Query Store for the test schema
    """
    obj = SynapseQueryStore(
        SynapseConfig(
            project_id="syn34178981",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


# other test objects ----------------------------------------------------------
# objects that don't have a test schema or manifests, but interact with
# config objects and pandas dataframes


@pytest.fixture(scope="session", name="synapse_database_project")
def fixture_synapse_project(secrets_dict: dict[str, Any]) -> Generator:
    """
    Yields a Synapse object used for testing databases
    """
    obj = Synapse(
        SynapseConfig(
            project_id="syn33832432",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


@pytest.fixture(scope="session", name="synapse_database")
def fixture_synapse_database(secrets_dict: dict[str, Any]) -> Generator:
    """
    Yields a SynapseDatabase object used for testing databases
    """
    obj = SynapseDatabase(
        SynapseConfig(
            project_id="syn33832432",
            username=secrets_dict["synapse"]["username"],
            auth_token=secrets_dict["synapse"]["auth_token"],
        )
    )
    yield obj


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
            DBAttributeConfig(
                name="pk_one_col", datatype=DBDatatype.TEXT, required=True
            ),
            DBAttributeConfig(
                name="string_one_col", datatype=DBDatatype.TEXT, required=False
            ),
            DBAttributeConfig(
                name="int_one_col", datatype=DBDatatype.INT, required=False
            ),
            DBAttributeConfig(
                name="double_one_col", datatype=DBDatatype.FLOAT, required=False
            ),
            DBAttributeConfig(
                name="date_one_col", datatype=DBDatatype.DATE, required=False
            ),
            DBAttributeConfig(
                name="bool_one_col", datatype=DBDatatype.BOOLEAN, required=False
            ),
        ],
        primary_key="pk_one_col",
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
            DBAttributeConfig(
                name="pk_two_col", datatype=DBDatatype.TEXT, required=True
            ),
            DBAttributeConfig(
                name="string_two_col", datatype=DBDatatype.TEXT, required=False
            ),
        ],
        primary_key="pk_two_col",
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
            DBAttributeConfig(
                name="pk_two_col", datatype=DBDatatype.TEXT, required=True
            ),
            DBAttributeConfig(
                name="string_two_col", datatype=DBDatatype.TEXT, required=False
            ),
        ],
        primary_key="pk_two_col",
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
            "pk_zero_col": ["keyA", "keyB", "keyC", "keyD"],
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
            DBAttributeConfig(
                name="pk_zero_col", datatype=DBDatatype.TEXT, required=True
            ),
            DBAttributeConfig(
                name="pk_one_col", datatype=DBDatatype.TEXT, required=False
            ),
            DBAttributeConfig(
                name="pk_two_col", datatype=DBDatatype.TEXT, required=False
            ),
            DBAttributeConfig(
                name="string_three_col", datatype=DBDatatype.TEXT, required=False
            ),
        ],
        primary_key="pk_zero_col",
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
