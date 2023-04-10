"""Testing for Schema."""
from typing import Generator
import pytest
from pydantic import ValidationError
from schematic_db.db_schema.db_schema import (
    DatabaseSchema,
    ForeignKeySchema,
    ColumnSchema,
    ColumnDatatype,
)
from schematic_db.schema.schema import Schema, DatabaseConfig, SchemaConfig
from schematic_db.schema.database_config import DatabaseObjectConfig


@pytest.fixture(name="database_config")
def fixture_database_config() -> Generator:
    """Yields a DatabaseConfig"""
    data = [
        {
            "name": "object1",
            "primary_key": "att1",
            "foreign_keys": [
                {
                    "column_name": "att2",
                    "foreign_table_name": "object2",
                    "foreign_column_name": "att1",
                },
                {
                    "column_name": "att3",
                    "foreign_table_name": "object3",
                    "foreign_column_name": "att1",
                },
            ],
            "columns": [
                {
                    "column_name": "att2",
                    "datatype": "str",
                    "required": True,
                    "index": True,
                },
                {
                    "column_name": "att3",
                    "datatype": "int",
                    "required": False,
                    "index": False,
                },
            ],
        },
        {"name": "object2", "primary_key": "att1"},
        {"name": "object3", "primary_key": "att1"},
    ]
    obj = DatabaseConfig(data)  # type: ignore
    yield obj


@pytest.fixture(name="database_object_config")
def fixture_database_object_config() -> Generator:
    """Yields a DatabaseObjectConfig"""
    data = {
        "name": "object1",
        "primary_key": "att1",
        "foreign_keys": [
            {
                "column_name": "att2",
                "foreign_table_name": "object2",
                "foreign_column_name": "att1",
            },
            {
                "column_name": "att3",
                "foreign_table_name": "object3",
                "foreign_column_name": "att1",
            },
        ],
    }
    obj = DatabaseObjectConfig(**data)  # type: ignore
    yield obj


@pytest.mark.fast
class TestSchemaConfig:
    """Testing for SchemaConfig"""

    def test_url_validator(self) -> None:
        """Testing for validators"""
        with pytest.raises(ValidationError):
            SchemaConfig(schema_url="xxx.jsonld")

    def test_jsonld_validator(self) -> None:
        """Testing for validators"""
        with pytest.raises(ValidationError):
            SchemaConfig(
                schema_url="https://raw.githubusercontent.com/Sage-Bionetworks/"
                "Schematic-DB-Test-Schemas/main/test_schema.csv"
            )


@pytest.mark.fast
class TestDatabaseConfig:
    """Testing for DatabaseConfig"""

    def test_init1(self, database_object_config: DatabaseObjectConfig) -> None:
        """Testing for init"""
        obj1 = database_object_config
        assert obj1

    def test_get_primary_key(self, database_config: DatabaseConfig) -> None:
        """Testing for get_primary_key"""
        obj = database_config
        assert obj.get_primary_key("object1") == "att1"

    def test_get_foreign_keys(self, database_config: DatabaseConfig) -> None:
        """Testing for get_foreign_keys"""
        obj = database_config
        assert obj.get_foreign_keys("object1") is not None
        assert obj.get_foreign_keys("object2") is None
        assert obj.get_foreign_keys("object3") is None

    def test_get_attributes(self, database_config: DatabaseConfig) -> None:
        """Testing for get_attributes"""
        obj = database_config
        assert obj.get_columns("object1") is not None
        assert obj.get_columns("object2") is None
        assert obj.get_columns("object3") is None


@pytest.mark.schematic
class TestSchema:
    """Testing for Schema"""

    def test_init(self, test_schema1: Schema) -> None:
        """Testing for Schema.__init__"""
        obj = test_schema1
        config = obj.get_database_schema()
        assert isinstance(config, DatabaseSchema)
        assert config.get_schema_names() == [
            "Patient",
            "Biospecimen",
            "BulkRnaSeq",
        ]

    def test_create_column_schemas(self, test_schema1: Schema) -> None:
        """Testing for Schema.attributes()"""
        obj = test_schema1
        assert obj.create_column_schemas("Patient") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="sex", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="yearofBirth",
                datatype=ColumnDatatype.INT,
                required=False,
                index=False,
            ),
            ColumnSchema(name="weight", datatype=ColumnDatatype.FLOAT, required=False),
            ColumnSchema(
                name="diagnosis",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(name="date", datatype=ColumnDatatype.DATE, required=False),
        ]
        assert obj.create_column_schemas("Biospecimen") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="patientId",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=False,
            ),
            ColumnSchema(
                name="tissueStatus",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]
        assert obj.create_column_schemas("BulkRnaSeq") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="biospecimenId",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=False,
            ),
            ColumnSchema(name="filename", datatype=ColumnDatatype.TEXT, required=True),
            ColumnSchema(
                name="fileFormat",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]

    def test_create_foreign_keys(self, test_schema1: Schema) -> None:
        """Testing for Schema.create_foreign_keys()"""
        obj = test_schema1
        assert obj.create_foreign_keys("Patient") == []
        assert obj.create_foreign_keys("Biospecimen") == [
            ForeignKeySchema(
                name="patientId",
                foreign_table_name="Patient",
                foreign_column_name="id",
            )
        ]
        assert obj.create_foreign_keys("BulkRnaSeq") == [
            ForeignKeySchema(
                name="biospecimenId",
                foreign_table_name="Biospecimen",
                foreign_column_name="id",
            )
        ]


@pytest.mark.schematic
class TestSchema2:
    """Testing for Schema"""

    def test_init(self, test_schema2: Schema) -> None:
        """Testing for Schema.__init__"""
        obj = test_schema2
        database_schema = obj.get_database_schema()
        assert isinstance(database_schema, DatabaseSchema)
        assert database_schema.get_schema_names() == [
            "Patient",
            "Biospecimen",
            "BulkRnaSeq",
        ]

    def test_create_column_schemas(self, test_schema2: Schema) -> None:
        """Testing for Schema.attributes()"""
        obj = test_schema2
        assert obj.create_column_schemas("Patient") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="sex", datatype=ColumnDatatype.TEXT, required=True, index=True
            ),
            ColumnSchema(
                name="yearofBirth",
                datatype=ColumnDatatype.INT,
                required=False,
                index=False,
            ),
            ColumnSchema(name="weight", datatype=ColumnDatatype.FLOAT, required=False),
            ColumnSchema(
                name="diagnosis",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(name="date", datatype=ColumnDatatype.DATE, required=False),
        ]
        assert obj.create_column_schemas("Biospecimen") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="patientId",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=False,
            ),
            ColumnSchema(
                name="tissueStatus",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]
        assert obj.create_column_schemas("BulkRnaSeq") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="biospecimenId",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=False,
            ),
            ColumnSchema(name="filename", datatype=ColumnDatatype.TEXT, required=True),
            ColumnSchema(
                name="fileFormat",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]

    def test_create_foreign_keys(self, test_schema2: Schema) -> None:
        """Testing for Schema.create_foreign_keys()"""
        obj = test_schema2
        assert obj.create_foreign_keys("Patient") == []
        assert obj.create_foreign_keys("Biospecimen") == [
            ForeignKeySchema(
                name="patientId",
                foreign_table_name="Patient",
                foreign_column_name="id",
            )
        ]
        assert obj.create_foreign_keys("BulkRnaSeq") == [
            ForeignKeySchema(
                name="biospecimenId",
                foreign_table_name="Biospecimen",
                foreign_column_name="id",
            )
        ]
