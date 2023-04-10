"""
Testing for DatabaseSchema.
"""
from typing import Generator
import pytest
from pydantic import ValidationError
from schematic_db.db_schema.db_schema import (
    DatabaseSchema,
    TableSchema,
    ColumnSchema,
    ColumnDatatype,
    ForeignKeySchema,
    TableColumnError,
    SchemaMissingTableError,
    TableKeyError,
    SchemaMissingColumnError,
)


@pytest.fixture(name="pk_col1_schema", scope="module")
def fixture_pk_col1_schema() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(name="pk_col1", datatype=ColumnDatatype.TEXT, required=True)
    yield att


@pytest.fixture(name="pk_col1b_schema", scope="module")
def fixture_pk_col1b_schema() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(
        name="pk_col1", datatype=ColumnDatatype.TEXT, required=True, index=True
    )
    yield att


@pytest.fixture(name="pk_col2_schema", scope="module")
def fixture_pk_col2_schema() -> Generator:
    """
    Yields a ColumnSchema
    """
    att = ColumnSchema(name="pk_col2", datatype=ColumnDatatype.TEXT, required=True)
    yield att


@pytest.mark.fast
class TestColumnSchema:  # pylint: disable= too-few-public-methods
    """Testing for ColumnSchema"""

    def test_validation_error(self) -> None:
        """Testing for ForeignKeySchema pydantic error"""
        with pytest.raises(ValidationError):
            ColumnSchema(name="", datatype=ColumnDatatype.TEXT)


@pytest.mark.fast
class TestForeignKeySchema:
    """Testing for ForeignKeySchema"""

    def test_validation_error(self) -> None:
        """Testing for ForeignKeySchema pydantic error"""
        with pytest.raises(ValidationError):
            ForeignKeySchema(
                name="",
                foreign_table_name="test_object",
                foreign_column_name="test_name",
            )

    def test_get_column_dict(self) -> None:
        """Testing for ForeignKeySchema.get_column_dict"""
        obj1 = ForeignKeySchema(
            name="test",
            foreign_table_name="test_object",
            foreign_column_name="test_name",
        )
        assert isinstance(obj1, ForeignKeySchema)
        assert obj1.get_column_dict() == {
            "name": "test",
            "foreign_table_name": "test_object",
            "foreign_column_name": "test_name",
        }


@pytest.mark.fast
class TestTableSchema:
    """Testing for TableSchema"""

    def test_equality(
        self,
        pk_col1_schema: ColumnSchema,
        pk_col1b_schema: ColumnSchema,
    ) -> None:
        """Testing for TableSchema equality"""
        obj1 = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        obj2 = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        obj3 = TableSchema(
            name="table",
            columns=[pk_col1b_schema],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        # obj1 and 2 are the same
        assert obj1 == obj2

        # obj1 and 3 are the same except the index
        assert obj1 != obj3

    def test_get_foreign_key_dependencies(self, pk_col1_schema: ColumnSchema) -> None:
        """Testing for TableSchema.get_foreign_key_dependencies()"""
        obj1 = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert obj1.get_foreign_key_dependencies() == []

        obj2 = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[
                ForeignKeySchema(
                    name="pk_col1",
                    foreign_table_name="table_two",
                    foreign_column_name="pk_two_col",
                )
            ],
        )
        assert obj2.get_foreign_key_dependencies() == ["table_two"]

    def test_get_foreign_key_names(self, pk_col1_schema: ColumnSchema) -> None:
        """Testing for TableSchema.get_foreign_key_names()"""
        obj1 = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert obj1.get_foreign_key_names() == []

        obj2 = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[
                ForeignKeySchema(
                    name="pk_col1",
                    foreign_table_name="table_two",
                    foreign_column_name="pk_two_col",
                )
            ],
        )
        assert obj2.get_foreign_key_names() == ["pk_col1"]

    def test_get_foreign_key_by_name(self, pk_col1_schema: ColumnSchema) -> None:
        """Testing for TableSchema.get_foreign_key_by_name()"""

        foreign_key = ForeignKeySchema(
            name="pk_col1",
            foreign_table_name="table_two",
            foreign_column_name="pk_two_col",
        )

        obj = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[foreign_key],
        )
        assert obj.get_foreign_key_by_name("pk_col1") == foreign_key

    def test_get_column_by_name(self, pk_col1_schema: ColumnSchema) -> None:
        """Testing for TableSchema.get_column_by_name()"""

        obj = TableSchema(
            name="table",
            columns=[pk_col1_schema],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert obj.get_column_by_name("pk_col1") == pk_col1_schema

    def test_exceptions(self, pk_col1_schema: ColumnSchema) -> None:
        """Tests for TableSchema() that raise exceptions"""
        with pytest.raises(TableColumnError, match="There are no columns: table_name"):
            TableSchema(
                name="table_name",
                columns=[],
                primary_key="pk_col1",
                foreign_keys=[],
            )

        with pytest.raises(
            TableColumnError, match="There are duplicate columns: table_name"
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_schema, pk_col1_schema],
                primary_key="pk_col1",
                foreign_keys=[],
            )

        with pytest.raises(
            TableKeyError,
            match="Primary key is missing from columns: table_name; pk_col2",
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_schema],
                primary_key="pk_col2",
                foreign_keys=[],
            )
        with pytest.raises(
            TableKeyError,
            match="Foreign key is missing from columns: table_name",
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_schema],
                primary_key="pk_col1",
                foreign_keys=[
                    ForeignKeySchema(
                        name="pk_col2",
                        foreign_table_name="table_two",
                        foreign_column_name="pk_one_col",
                    )
                ],
            )

        with pytest.raises(
            TableKeyError,
            match="Foreign key references its own table: table_name",
        ):
            TableSchema(
                name="table_name",
                columns=[pk_col1_schema],
                primary_key="pk_col1",
                foreign_keys=[
                    ForeignKeySchema(
                        name="pk_col1",
                        foreign_table_name="table_name",
                        foreign_column_name="pk_one_col",
                    )
                ],
            )

        with pytest.raises(ValidationError):
            TableSchema(
                name="",
                columns=[pk_col1_schema],
                primary_key="pk_col1",
                foreign_keys=[],
            )


@pytest.mark.fast
class TestDatabaseSchema:
    """Testing for DatabaseSchema"""

    def test_equality(
        self,
        pk_col1_schema: ColumnSchema,
        pk_col2_schema: ColumnSchema,
    ) -> None:
        """Testing for DatabaseSchema.__eq__"""
        obj1 = DatabaseSchema(
            [
                TableSchema(
                    name="table1",
                    columns=[pk_col1_schema],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table2",
                    columns=[pk_col2_schema],
                    primary_key="pk_col2",
                    foreign_keys=[],
                ),
            ]
        )
        obj2 = DatabaseSchema(
            [
                TableSchema(
                    name="table2",
                    columns=[pk_col2_schema],
                    primary_key="pk_col2",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table1",
                    columns=[pk_col1_schema],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
            ]
        )
        assert obj1 == obj2

    def test_get_dependencies(
        self, pk_col1_schema: ColumnSchema, pk_col2_schema: ColumnSchema
    ) -> None:
        """Testing for DatabaseSchema.get_dependencies"""
        obj = DatabaseSchema(
            [
                TableSchema(
                    name="table1",
                    columns=[pk_col1_schema],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table2",
                    columns=[pk_col2_schema],
                    primary_key="pk_col2",
                    foreign_keys=[
                        ForeignKeySchema(
                            name="pk_col2",
                            foreign_table_name="table1",
                            foreign_column_name="pk_col1",
                        )
                    ],
                ),
            ]
        )
        assert obj.get_dependencies("table1") == []
        assert obj.get_dependencies("table2") == ["table1"]

    def test_get_reverse_dependencies(
        self, pk_col1_schema: ColumnSchema, pk_col2_schema: ColumnSchema
    ) -> None:
        """Testing for DatabaseSchema.get_reverse_dependencies"""
        obj = DatabaseSchema(
            [
                TableSchema(
                    name="table1",
                    columns=[pk_col1_schema],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                TableSchema(
                    name="table2",
                    columns=[pk_col2_schema],
                    primary_key="pk_col2",
                    foreign_keys=[
                        ForeignKeySchema(
                            name="pk_col2",
                            foreign_table_name="table1",
                            foreign_column_name="pk_col1",
                        )
                    ],
                ),
            ]
        )
        assert obj.get_reverse_dependencies("table1") == ["table2"]
        assert obj.get_reverse_dependencies("table2") == []

    def test_db_object_config_list_exceptions(
        self, pk_col1_schema: ColumnSchema, pk_col2_schema: ColumnSchema
    ) -> None:
        """Tests for DatabaseSchema() that raise exceptions"""

        with pytest.raises(
            SchemaMissingTableError,
            match=(
                "Foreign key 'pk_col2' in table 'table2' references "
                "table 'table' which does not exist in schema."
            ),
        ):
            DatabaseSchema(
                [
                    TableSchema(
                        name="table2",
                        columns=[pk_col2_schema],
                        primary_key="pk_col2",
                        foreign_keys=[
                            ForeignKeySchema(
                                name="pk_col2",
                                foreign_table_name="table",
                                foreign_column_name="pk_col1",
                            )
                        ],
                    )
                ]
            )

        with pytest.raises(
            SchemaMissingColumnError,
            match=(
                "Foreign key 'pk_col2' in table 'table2' references column "
                "'pk_col3' which does not exist in table 'table'"
            ),
        ):
            DatabaseSchema(
                [
                    TableSchema(
                        name="table",
                        columns=[pk_col1_schema],
                        primary_key="pk_col1",
                        foreign_keys=[],
                    ),
                    TableSchema(
                        name="table2",
                        columns=[pk_col2_schema],
                        primary_key="pk_col2",
                        foreign_keys=[
                            ForeignKeySchema(
                                name="pk_col2",
                                foreign_table_name="table",
                                foreign_column_name="pk_col3",
                            )
                        ],
                    ),
                ]
            )
