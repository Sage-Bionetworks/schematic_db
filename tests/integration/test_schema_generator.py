"""Testing for Schema."""

from typing import Generator, Any
import pytest
from pydantic import ValidationError
from schematic_db.db_schema.db_schema import (
    DatabaseSchema,
    ForeignKeySchema,
    ColumnSchema,
    ColumnDatatype,
)
from schematic_db.schema_generator.schema_generator import (
    SchemaGenerator,
    SchemaGeneratorConfig,
    ColumnSchematicError,
)
from schematic_db.schema_generator.database_config import (
    DatabaseConfig,
    TableConfig,
    ColumnConfig,
    DATATYPES,
)

# pylint: disable=protected-access


@pytest.fixture(name="database_config")
def fixture_database_config() -> Generator[DatabaseConfig, None, None]:
    """Yields a DatabaseConfig"""
    data: list[dict[str, Any]] = [
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
                    "name": "att2",
                    "datatype": "str",
                    "required": True,
                    "index": True,
                },
                {
                    "name": "att3",
                    "datatype": "int",
                    "required": False,
                    "index": False,
                },
            ],
        },
        {"name": "object2", "primary_key": "att1"},
        {"name": "object3", "primary_key": "att1"},
        {
            "name": "datasets",
            "primary_key": "id",
            "foreign_keys": None,
            "columns": [
                {
                    "name": "dataset_type",
                    "datatype": "str",
                    "required": True,
                    "index": True,
                },
            ],
        },
    ]
    obj = DatabaseConfig(data)
    yield obj


@pytest.fixture(name="table_config")
def fixture_table_config() -> Generator[TableConfig, None, None]:
    """Yields a TableConfig"""
    obj = TableConfig(
        name="object1",
        primary_key="att1",
        foreign_keys=[
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
    )
    yield obj


@pytest.fixture(name="column_config1")
def fixture_column_config1() -> Generator[ColumnConfig, None, None]:
    """Yields a ColumnConfig"""
    obj = ColumnConfig(
        name="col1",
        datatype=DATATYPES["str"],
        required=False,
        index=False,
    )
    yield obj


@pytest.fixture(name="column_config2")
def fixture_column_config2() -> Generator[ColumnConfig, None, None]:
    """Yields a ColumnConfig"""
    obj = ColumnConfig(
        name="col2",
    )
    yield obj


@pytest.mark.fast
class TestSchemaGeneratorConfig:
    """Testing for SchemaGeneratorConfig"""

    def test_url_validator(self) -> None:
        """Testing for validators"""
        with pytest.raises(ValidationError):
            SchemaGeneratorConfig(schema_url="xxx.jsonld")

    def test_jsonld_validator(self) -> None:
        """Testing for validators"""
        with pytest.raises(ValidationError):
            SchemaGeneratorConfig(
                schema_url="https://raw.githubusercontent.com/Sage-Bionetworks/"
                "Schematic-DB-Test-Schemas/main/README.md"
            )


@pytest.mark.fast
class TestDatabaseConfig:
    """Testing for DatabaseConfig"""

    def test_column_config(
        self, column_config1: ColumnConfig, column_config2: ColumnConfig
    ) -> None:
        """Testing for ColumnConfig"""
        assert column_config1
        assert column_config1.index is False
        assert column_config2
        assert column_config2.index is None

    def test_table_cofnig(self, table_config: TableConfig) -> None:
        """Testing for TableConfig"""
        obj1 = table_config
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

    def test_max_type_values(self) -> None:
        """Testing for string_size_max and list_length_max"""
        data = [
            {
                "name": "object1",
                "columns": [
                    {
                        "name": "att",
                        "datatype": "synapse_string_list",
                        "required": True,
                        "index": True,
                        "string_size_max": 1,
                        "list_length_max": 1,
                    },
                ],
            },
        ]
        obj = DatabaseConfig(data)
        assert obj


class TestSchemaGenerator:
    """Testing for SchemaGenerator"""

    def test_init(self, schema_generator1: SchemaGenerator) -> None:
        """Testing for SchemaGenerator.__init__"""
        obj = schema_generator1
        config = obj.get_database_schema()
        assert isinstance(config, DatabaseSchema)
        assert config.get_schema_names() == [
            "Patient",
            "Biospecimen",
            "BulkRnaSeq",
        ]

    def test_create_column_schemas(self, schema_generator1: SchemaGenerator) -> None:
        """Testing for SchemaGenerator.attributes()"""
        obj = schema_generator1
        assert obj._create_column_schemas("Patient") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="sex", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="yearOfBirth",
                datatype=ColumnDatatype.INT,
                required=False,
                index=False,
            ),
            ColumnSchema(
                name="diagnosis",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(name="weight", datatype=ColumnDatatype.FLOAT, required=False),
            ColumnSchema(name="date", datatype=ColumnDatatype.DATE, required=False),
        ]
        assert obj._create_column_schemas("Biospecimen") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="patientId",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(
                name="tissueStatus",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=False,
            ),
        ]
        assert obj._create_column_schemas("BulkRnaSeq") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(name="filename", datatype=ColumnDatatype.TEXT, required=True),
            ColumnSchema(
                name="biospecimenId",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(
                name="fileFormat",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]

    def test_create_foreign_keys(self, schema_generator1: SchemaGenerator) -> None:
        """Testing for SchemaGenerator.create_foreign_keys()"""
        obj = schema_generator1
        assert obj._create_foreign_keys("Patient") == []
        assert obj._create_foreign_keys("Biospecimen") == [
            ForeignKeySchema(
                name="patientId",
                foreign_table_name="Patient",
                foreign_column_name="id",
            )
        ]
        assert obj._create_foreign_keys("BulkRnaSeq") == [
            ForeignKeySchema(
                name="biospecimenId",
                foreign_table_name="Biospecimen",
                foreign_column_name="id",
            )
        ]

    def test_is_column_required(self, schema_generator1: SchemaGenerator) -> None:
        """Testing for SchemaGenerator.is_column_required"""
        obj = schema_generator1
        assert obj._is_column_required("id", "Patients")
        assert not obj._is_column_required("weight", "Patients")
        with pytest.raises(
            ColumnSchematicError,
            match=(
                "There was an issue getting data from the Schematic API for the "
                "column: column name: NOT_A_COLUMN; table_name: Patients"
            ),
        ):
            obj._is_column_required("NOT_A_COLUMN", "Patients")

    def test_get_column_datatype(self, schema_generator1: SchemaGenerator) -> None:
        """Testing for SchemaGenerator.get_column_datatype"""
        obj = schema_generator1
        assert obj._get_column_datatype("id", "Patients") == ColumnDatatype.TEXT
        assert obj._get_column_datatype("weight", "Patients") == ColumnDatatype.FLOAT


class TestSchema2:
    """Testing for Schema"""

    def test_init(self, schema_generator2: SchemaGenerator) -> None:
        """Testing for Schema.__init__"""
        obj = schema_generator2
        database_schema = obj.get_database_schema()
        assert isinstance(database_schema, DatabaseSchema)
        assert database_schema.get_schema_names() == [
            "Patient",
            "Biospecimen",
            "BulkRnaSeq",
        ]

    def test_create_column_schemas(self, schema_generator2: SchemaGenerator) -> None:
        """Testing for Schema.attributes()"""
        obj = schema_generator2
        assert obj._create_column_schemas("Patient") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="sex", datatype=ColumnDatatype.TEXT, required=True, index=True
            ),
            ColumnSchema(
                name="yearOfBirth",
                datatype=ColumnDatatype.INT,
                required=False,
                index=False,
            ),
            ColumnSchema(
                name="diagnosis",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(name="weight", datatype=ColumnDatatype.FLOAT, required=False),
            ColumnSchema(name="date", datatype=ColumnDatatype.DATE, required=False),
        ]
        assert obj._create_column_schemas("Biospecimen") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(
                name="patientId",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(
                name="tissueStatus",
                datatype=ColumnDatatype.TEXT,
                required=False,
                index=False,
            ),
        ]
        assert obj._create_column_schemas("BulkRnaSeq") == [
            ColumnSchema(
                name="id", datatype=ColumnDatatype.TEXT, required=True, index=False
            ),
            ColumnSchema(name="filename", datatype=ColumnDatatype.TEXT, required=True),
            ColumnSchema(
                name="biospecimenId",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
            ColumnSchema(
                name="fileFormat",
                datatype=ColumnDatatype.TEXT,
                required=True,
                index=False,
            ),
        ]

    def test_create_foreign_keys(self, schema_generator2: SchemaGenerator) -> None:
        """Testing for SchemaGenerator.create_foreign_keys()"""
        obj = schema_generator2
        assert obj._create_foreign_keys("Patient") == []
        assert obj._create_foreign_keys("Biospecimen") == [
            ForeignKeySchema(
                name="patientId",
                foreign_table_name="Patient",
                foreign_column_name="id",
            )
        ]
        assert obj._create_foreign_keys("BulkRnaSeq") == [
            ForeignKeySchema(
                name="biospecimenId",
                foreign_table_name="Biospecimen",
                foreign_column_name="id",
            )
        ]
