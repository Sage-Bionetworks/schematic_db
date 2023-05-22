"""Fixtures for all tests"""
import os
from datetime import datetime
from typing import Generator, Any
import pytest
import pandas as pd
import numpy as np
from yaml import safe_load
import synapseclient as sc  # type: ignore
from schematic_db.db_schema.db_schema import (
    DatabaseSchema,
    TableSchema,
    ColumnSchema,
    ColumnDatatype,
    ForeignKeySchema,
)

from schematic_db.manifest_store.manifest_store import (
    ManifestStore,
    ManifestStoreConfig,
)

from schematic_db.query_store.query_store import QueryStore
from schematic_db.query_store.synapse_query_store import SynapseQueryStore
from schematic_db.rdb.sql_alchemy_database import SQLConfig
from schematic_db.rdb.mysql import MySQLDatabase
from schematic_db.rdb.postgres import PostgresDatabase
from schematic_db.rdb.synapse_database import SynapseDatabase
from schematic_db.rdb_queryer.rdb_queryer import RDBQueryer
from schematic_db.synapse.synapse import Synapse, SynapseConfig
from schematic_db.schema.schema import Schema, SchemaConfig
from schematic_db.schema.database_config import DatabaseConfig

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
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


@pytest.fixture(scope="session", name="mysql_config")
def fixture_mysql_config(secrets_dict: dict) -> Generator:
    """Yields a MYSQlConfig object"""
    yield SQLConfig(
        username=secrets_dict["mysql"]["username"],
        password=secrets_dict["mysql"]["password"],
        host=secrets_dict["mysql"]["host"],
        name="test_schema",
    )


@pytest.fixture(scope="session", name="postgres_config")
def fixture_postgres_config(secrets_dict: dict) -> Generator:
    """Yields a SQlConfig object"""
    yield SQLConfig(
        username=secrets_dict["postgres"]["username"],
        password=secrets_dict["postgres"]["password"],
        host=secrets_dict["postgres"]["host"],
        name="test_schema",
    )


@pytest.fixture(scope="session", name="synapse_config")
def fixture_synapse_config(secrets_dict: dict[str, Any]) -> Generator:
    """
    Yields a Synapse Config
    """
    yield SynapseConfig(
        project_id=secrets_dict["synapse"]["project_id"],
        username=secrets_dict["synapse"]["username"],
        auth_token=secrets_dict["synapse"]["auth_token"],
    )


@pytest.fixture(scope="session", name="mysql_database")
def fixture_mysql_database(mysql_config: SQLConfig) -> Generator:
    """Yields a SQlConfig object"""
    obj = MySQLDatabase(mysql_config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="session", name="postgres_database")
def fixture_postgres_database(postgres_config: SQLConfig) -> Generator:
    """
    Yields a Postgres object
    """
    obj = PostgresDatabase(postgres_config)
    yield obj
    obj.drop_database()


@pytest.fixture(scope="session", name="synapse_object")
def fixture_synapse_object(synapse_config: SynapseConfig) -> Generator:
    """
    Yields a Synapse object
    """
    yield Synapse(synapse_config)


@pytest.fixture(scope="session", name="synapse_database")
def fixture_synapse_database(synapse_config: SynapseConfig) -> Generator:
    """
    Yields a SynapseDatabase
    """
    yield SynapseDatabase(synapse_config)


@pytest.fixture(scope="session", name="test_synapse_project_id")
def fixture_test_synapse_project_id() -> Generator:
    """Yields the synapse id for the test schema project id"""
    yield "syn47994571"


@pytest.fixture(scope="session", name="test_synapse_asset_view_id")
def fixture_test_synapse_asset_view_id() -> Generator:
    """Yields the synapse id for the test schema asset view"""
    yield "syn47997084"


@pytest.fixture(scope="session", name="test_schema1")
def fixture_test_schema1(
    test_schema_json_url: str,
) -> Generator:
    """Yields a Schema using the database specific test schema"""
    config = SchemaConfig(test_schema_json_url)
    obj = Schema(config)
    yield obj


@pytest.fixture(scope="session", name="test_schema2")
def fixture_test_schema2(test_schema_json_url: str) -> Generator:
    """Yields a Schema using the database specific test schema"""
    config = SchemaConfig(test_schema_json_url)
    database_config = DatabaseConfig(
        [
            {
                "name": "Patient",
                "primary_key": "id",
                "columns": [
                    {
                        "column_name": "sex",
                        "datatype": "str",
                        "required": True,
                        "index": True,
                    },
                ],
            },
            {
                "name": "BulkRnaSeq",
                "primary_key": "id",
                "foreign_keys": [
                    {
                        "column_name": "biospecimenId",
                        "foreign_table_name": "Biospecimen",
                        "foreign_column_name": "id",
                    }
                ],
            },
        ]
    )
    obj = Schema(config, database_config=database_config)
    yield obj


@pytest.fixture(scope="session", name="manifest_store")
def fixture_manifest_store(
    test_synapse_project_id: str,
    test_synapse_asset_view_id: str,
    secrets_dict: dict,
    test_schema_json_url: str,
) -> Generator:
    """Yields a ManifestStore object"""
    yield ManifestStore(
        ManifestStoreConfig(
            test_schema_json_url,
            test_synapse_project_id,
            test_synapse_asset_view_id,
            secrets_dict["synapse"]["auth_token"],
        )
    )


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


@pytest.fixture(scope="session", name="table_one_schema")
def fixture_table_one_schema() -> Generator:
    """
    Yields a TableSchema object with one primary and no foreign keys
    """
    schema = TableSchema(
        name="table_one",
        columns=[
            ColumnSchema(
                name="pk_one_col",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=True,
            ),
            ColumnSchema(
                name="string_one_col",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=True,
            ),
            ColumnSchema(
                name="int_one_col", datatype=ColumnDatatype.INT, required=False
            ),
            ColumnSchema(
                name="double_one_col", datatype=ColumnDatatype.FLOAT, required=False
            ),
            ColumnSchema(
                name="date_one_col", datatype=ColumnDatatype.DATE, required=False
            ),
            ColumnSchema(
                name="bool_one_col", datatype=ColumnDatatype.BOOLEAN, required=False
            ),
        ],
        primary_key="pk_one_col",
        foreign_keys=[],
    )
    yield schema


@pytest.fixture(name="table_one_columns", scope="session")
def fixture_table_one_columns() -> Generator:
    """Yields a list of synapse columns for table one"""
    yield [
        sc.Column(name="pk_one_col", columnType="LARGETEXT"),
        sc.Column(name="string_one_col", columnType="LARGETEXT"),
        sc.Column(name="int_one_col", columnType="INTEGER"),
        sc.Column(name="double_one_col", columnType="DOUBLE"),
        sc.Column(name="date_one_col", columnType="DATE"),
        sc.Column(name="bool_one_col", columnType="BOOLEAN"),
    ]


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


@pytest.fixture(name="table_two_columns", scope="session")
def fixture_table_two_columns() -> Generator:
    """Yields a list of synapse columns for table two"""
    yield [
        sc.Column(name="pk_two_col", columnType="LARGETEXT"),
        sc.Column(name="string_two_col", columnType="LARGETEXT"),
    ]


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


@pytest.fixture(scope="session", name="table_two_schema")
def fixture_table_two_schema() -> Generator:
    """
    Yields a TableSchema object with one primary and no foreign keys
    """
    schema = TableSchema(
        name="table_two",
        columns=[
            ColumnSchema(
                name="pk_two_col", datatype=ColumnDatatype.TEXT, required=True
            ),
            ColumnSchema(
                name="string_two_col", datatype=ColumnDatatype.TEXT, required=False
            ),
        ],
        primary_key="pk_two_col",
        foreign_keys=[],
    )
    yield schema


@pytest.fixture(scope="session", name="table_two_schema_combined")
def fixture_table_two_schema_combined() -> Generator:
    """
    Yields a TableSchema object with one primary and no foreign keys
    """
    schema = TableSchema(
        name="table_two",
        columns=[
            ColumnSchema(
                name="pk_two_col", datatype=ColumnDatatype.TEXT, required=True
            ),
            ColumnSchema(
                name="string_two_col", datatype=ColumnDatatype.TEXT, required=False
            ),
        ],
        primary_key="pk_two_col",
        foreign_keys=[],
    )
    yield schema


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


@pytest.fixture(scope="session", name="table_three_schema")
def fixture_table_three_schema() -> Generator:
    """
    Yields a TableSchema object with two keys that are both primary and foreign
    """
    schema = TableSchema(
        name="table_three",
        columns=[
            ColumnSchema(
                name="pk_zero_col", datatype=ColumnDatatype.TEXT, required=True
            ),
            ColumnSchema(
                name="pk_one_col", datatype=ColumnDatatype.TEXT, required=False
            ),
            ColumnSchema(
                name="pk_two_col", datatype=ColumnDatatype.TEXT, required=False
            ),
            ColumnSchema(
                name="string_three_col", datatype=ColumnDatatype.TEXT, required=False
            ),
        ],
        primary_key="pk_zero_col",
        foreign_keys=[
            ForeignKeySchema(
                name="pk_one_col",
                foreign_table_name="table_one",
                foreign_column_name="pk_one_col",
            ),
            ForeignKeySchema(
                name="pk_two_col",
                foreign_table_name="table_two",
                foreign_column_name="pk_two_col",
            ),
        ],
    )
    yield schema


@pytest.fixture(name="table_three_columns", scope="session")
def fixture_table_three_columns() -> Generator:
    """Yields a list of synapse columns for table three"""
    yield [
        sc.Column(name="pk_zero_col", columnType="LARGETEXT"),
        sc.Column(name="pk_one_col", columnType="LARGETEXT"),
        sc.Column(name="pk_two_col", columnType="LARGETEXT"),
        sc.Column(name="string_three_col", columnType="LARGETEXT"),
    ]


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


@pytest.fixture(scope="session", name="database_schema")
def fixture_database_schema(
    table_one_schema: TableSchema,
    table_two_schema: TableSchema,
    table_three_schema: TableSchema,
) -> Generator:
    """Yields a TableSchemaList"""
    yield DatabaseSchema([table_one_schema, table_two_schema, table_three_schema])
