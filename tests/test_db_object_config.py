"""
Testing for DBConfig.
"""
from typing import Generator
import pytest
from schematic_db.db_config.db_config import (
    DBConfig,
    DBObjectConfig,
    DBAttributeConfig,
    DBDatatype,
    DBForeignKey,
    ConfigAttributeError,
    ConfigForeignKeyMissingObjectError,
    ConfigKeyError,
    ConfigForeignKeyMissingAttributeError,
)


@pytest.fixture(name="pk_col1_attribute", scope="module")
def fixture_pk_col1_attribute() -> Generator:
    """
    Yields a DBAttributeConfig
    """
    att = DBAttributeConfig(name="pk_col1", datatype=DBDatatype.TEXT, required=True)
    yield att


@pytest.fixture(name="pk_col1b_attribute", scope="module")
def fixture_pk_col1b_attribute() -> Generator:
    """
    Yields a DBAttributeConfig
    """
    att = DBAttributeConfig(
        name="pk_col1", datatype=DBDatatype.TEXT, required=True, index=True
    )
    yield att


@pytest.fixture(name="pk_col2_attribute", scope="module")
def fixture_pk_col2_attribute() -> Generator:
    """
    Yields a DBAttributeConfig
    """
    att = DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT, required=True)
    yield att


@pytest.fixture(name="pk_col3_attribute", scope="module")
def fixture_pk_col3_attribute() -> Generator:
    """
    Yields a DBAttributeConfig
    """
    att = DBAttributeConfig(name="pk_col3", datatype=DBDatatype.TEXT, required=True)
    yield att


@pytest.mark.fast
class TestDBForeignKey:  # pylint: disable=too-few-public-methods
    """Testing for DBForeignKey"""

    def test_get_attribute_dict(self) -> None:
        """Testing for DBForeignKey.get_attribute_dict"""
        obj = DBForeignKey(
            name="test",
            foreign_object_name="test_object",
            foreign_attribute_name="test_name",
        )
        assert isinstance(obj, DBForeignKey)
        assert obj.get_attribute_dict() == {
            "name": "test",
            "foreign_object_name": "test_object",
            "foreign_attribute_name": "test_name",
        }


@pytest.mark.fast
class TestDBObjectConfig:
    """Testing for DBObjectConfig"""

    def test_is_equivalent(
        self,
        pk_col1_attribute: DBAttributeConfig,
        pk_col1b_attribute: DBAttributeConfig,
    ) -> None:
        """Testing for DBObjectConfig.is_equivalent"""
        obj1 = DBObjectConfig(
            name="table",
            attributes=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        obj2 = DBObjectConfig(
            name="table",
            attributes=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        obj3 = DBObjectConfig(
            name="table",
            attributes=[pk_col1b_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )

        # obj1 and 2 are the same
        assert obj1 == obj2
        assert obj1.is_equivalent(obj2)

        # obj1 and 3 are the same except the index
        assert obj1 != obj3

    def test_get_foreign_key_dependencies(
        self, pk_col1_attribute: DBAttributeConfig
    ) -> None:
        """Testing for DBObjectConfig.get_foreign_key_dependencies()"""
        obj1 = DBObjectConfig(
            name="table",
            attributes=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert obj1.get_foreign_key_dependencies() == []

        obj2 = DBObjectConfig(
            name="table",
            attributes=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[
                DBForeignKey(
                    name="pk_col1",
                    foreign_object_name="table_two",
                    foreign_attribute_name="pk_two_col",
                )
            ],
        )
        assert obj2.get_foreign_key_dependencies() == ["table_two"]

    def test_db_object_config_success(
        self, pk_col1_attribute: DBAttributeConfig
    ) -> None:
        """Successful tests for DBObjectConfig()"""
        obj1 = DBObjectConfig(
            name="table",
            attributes=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[],
        )
        assert isinstance(obj1, DBObjectConfig)

        obj2 = DBObjectConfig(
            name="table",
            attributes=[pk_col1_attribute],
            primary_key="pk_col1",
            foreign_keys=[
                DBForeignKey(
                    name="pk_col1",
                    foreign_object_name="table_two",
                    foreign_attribute_name="pk_two_col",
                )
            ],
        )
        assert isinstance(obj2, DBObjectConfig)

    def test_db_object_config_exceptions(
        self, pk_col1_attribute: DBAttributeConfig
    ) -> None:
        """Tests for DBObjectConfig() that raise exceptions"""
        # test attributes
        with pytest.raises(
            ConfigAttributeError, match="Attributes is empty: table_name"
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[],
                primary_key="pk_col1",
                foreign_keys=[],
            )

        with pytest.raises(
            ConfigKeyError,
            match="Primary key is missing from attributes: table_name; pk_col2",
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[pk_col1_attribute],
                primary_key="pk_col2",
                foreign_keys=[],
            )
        # test foreign_keys
        with pytest.raises(
            ConfigKeyError,
            match="Foreign key is missing from attributes: table_name",
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[pk_col1_attribute],
                primary_key="pk_col1",
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col2",
                        foreign_object_name="table_two",
                        foreign_attribute_name="pk_one_col",
                    )
                ],
            )

        with pytest.raises(
            ConfigKeyError,
            match="Foreign key references its own object: table_name",
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[pk_col1_attribute],
                primary_key="pk_col1",
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col1",
                        foreign_object_name="table_name",
                        foreign_attribute_name="pk_one_col",
                    )
                ],
            )


@pytest.mark.fast
class TestDBConfig:
    """Testing for DBConfig"""

    def test_equality(
        self,
        pk_col1_attribute: DBAttributeConfig,
        pk_col2_attribute: DBAttributeConfig,
    ) -> None:
        """Testing for DBConfig.__eq__"""
        obj1 = DBConfig(
            [
                DBObjectConfig(
                    name="table1",
                    attributes=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                DBObjectConfig(
                    name="table2",
                    attributes=[pk_col2_attribute],
                    primary_key="pk_col2",
                    foreign_keys=[],
                ),
            ]
        )
        obj2 = DBConfig(
            [
                DBObjectConfig(
                    name="table2",
                    attributes=[pk_col2_attribute],
                    primary_key="pk_col2",
                    foreign_keys=[],
                ),
                DBObjectConfig(
                    name="table1",
                    attributes=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
            ]
        )
        assert obj1 == obj2
        assert obj1.is_equivalent(obj2)

    def test_db_object_config_list_success(
        self, pk_col1_attribute: DBAttributeConfig, pk_col2_attribute: DBAttributeConfig
    ) -> None:
        """Successful tests for DBConfig()"""
        obj1 = DBConfig(
            [
                DBObjectConfig(
                    name="table",
                    attributes=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                )
            ]
        )
        assert isinstance(obj1, DBConfig)

        obj2 = DBConfig(
            [
                DBObjectConfig(
                    name="table",
                    attributes=[pk_col1_attribute],
                    primary_key="pk_col1",
                    foreign_keys=[],
                ),
                DBObjectConfig(
                    name="table2",
                    attributes=[pk_col2_attribute],
                    primary_key="pk_col2",
                    foreign_keys=[
                        DBForeignKey(
                            name="pk_col2",
                            foreign_object_name="table",
                            foreign_attribute_name="pk_col1",
                        )
                    ],
                ),
            ]
        )
        assert isinstance(obj2, DBConfig)

    def test_db_object_config_list_exceptions(
        self, pk_col1_attribute: DBAttributeConfig, pk_col2_attribute: DBAttributeConfig
    ) -> None:
        """Tests for DBConfig() that raise exceptions"""

        with pytest.raises(
            ConfigForeignKeyMissingObjectError, match="Foreign key 'DBForeignKey"
        ):
            DBConfig(
                [
                    DBObjectConfig(
                        name="table2",
                        attributes=[pk_col2_attribute],
                        primary_key="pk_col2",
                        foreign_keys=[
                            DBForeignKey(
                                name="pk_col2",
                                foreign_object_name="table",
                                foreign_attribute_name="pk_col1",
                            )
                        ],
                    )
                ]
            )

        with pytest.raises(
            ConfigForeignKeyMissingAttributeError,
            match="Foreign key 'DBForeignKey",
        ):
            DBConfig(
                [
                    DBObjectConfig(
                        name="table",
                        attributes=[pk_col1_attribute],
                        primary_key="pk_col1",
                        foreign_keys=[],
                    ),
                    DBObjectConfig(
                        name="table2",
                        attributes=[pk_col2_attribute],
                        primary_key="pk_col2",
                        foreign_keys=[
                            DBForeignKey(
                                name="pk_col2",
                                foreign_object_name="table",
                                foreign_attribute_name="pk_col3",
                            )
                        ],
                    ),
                ]
            )
