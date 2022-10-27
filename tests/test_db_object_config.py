"""
Testing for DBConfig.
"""
import pytest
from schematic_db.db_config import (
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


@pytest.mark.fast
class TestDBObjectConfig:
    """
    Testing for DBObjectConfig
    """

    def test_db_object_config_success(self) -> None:
        """
        Successful tests for DBObjectConfig()
        """
        obj1 = DBObjectConfig(
            name="table",
            attributes=[
                DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
            ],
            primary_keys=["pk_col"],
            foreign_keys=[],
        )
        assert isinstance(obj1, DBObjectConfig)

        obj2 = DBObjectConfig(
            name="table",
            attributes=[
                DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
            ],
            primary_keys=["pk_col"],
            foreign_keys=[
                DBForeignKey(
                    name="pk_col",
                    foreign_object_name="table_two",
                    foreign_attribute_name="pk_two_col",
                )
            ],
        )
        assert isinstance(obj2, DBObjectConfig)

    def test_db_object_config_exceptions(self) -> None:
        """
        Tests for DBObjectConfig() that raise exceptions
        """
        # test attributes
        with pytest.raises(
            ConfigAttributeError, match="Attributes is empty: table_name"
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[],
                primary_keys=["pk_col"],
                foreign_keys=[],
            )

        # test primary_keys
        with pytest.raises(ConfigKeyError, match="Primary keys is empty: table_name"):
            DBObjectConfig(
                name="table_name",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=[],
                foreign_keys=[],
            )

        with pytest.raises(
            ConfigKeyError,
            match="Primary key is missing from attributes: table_name; pk_col1",
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col1"],
                foreign_keys=[],
            )
        # test foreign_keys
        with pytest.raises(
            ConfigKeyError,
            match="Foreign key is missing from attributes: table_name",
        ):
            DBObjectConfig(
                name="table_name",
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col1",
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
                attributes=[DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT)],
                primary_keys=["pk_col"],
                foreign_keys=[
                    DBForeignKey(
                        name="pk_col",
                        foreign_object_name="table_name",
                        foreign_attribute_name="pk_one_col",
                    )
                ],
            )


@pytest.mark.fast
class TestDBConfig:
    """
    Testing for DBObjectConfigList
    """

    def test_db_object_config_list_success(self) -> None:
        """
        Successful tests for DBObjectConfigList()
        """
        obj1 = DBConfig(
            [
                DBObjectConfig(
                    name="table",
                    attributes=[
                        DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col"],
                    foreign_keys=[],
                )
            ]
        )
        assert isinstance(obj1, DBConfig)

        obj2 = DBConfig(
            [
                DBObjectConfig(
                    name="table",
                    attributes=[
                        DBAttributeConfig(name="pk_col1", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col1"],
                    foreign_keys=[],
                ),
                DBObjectConfig(
                    name="table2",
                    attributes=[
                        DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT),
                    ],
                    primary_keys=["pk_col2"],
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

    def test_db_object_config_list_exceptions(self) -> None:
        """
        Tests for DBObjectConfigList() that raise exceptions
        """

        with pytest.raises(
            ConfigForeignKeyMissingObjectError, match="Foreign key 'DBForeignKey"
        ):
            DBConfig(
                [
                    DBObjectConfig(
                        name="table2",
                        attributes=[
                            DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col2"],
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
                        attributes=[
                            DBAttributeConfig(name="pk_col", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col"],
                        foreign_keys=[],
                    ),
                    DBObjectConfig(
                        name="table2",
                        attributes=[
                            DBAttributeConfig(name="pk_col2", datatype=DBDatatype.TEXT),
                        ],
                        primary_keys=["pk_col2"],
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
