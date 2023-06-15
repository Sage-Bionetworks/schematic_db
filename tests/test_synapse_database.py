"""Fixtures for all tests"""

# pylint: disable=protected-access

from typing import Generator
from unittest.mock import patch, call, Mock
import pytest
import pandas as pd
import numpy as np
import synapseclient as sc  # type: ignore
from schematic_db.rdb.synapse_database import (
    SynapseDatabase,
    SynapseDatabaseDropTableError,
)
from schematic_db.synapse.synapse import Synapse
from schematic_db.db_schema.db_schema import TableSchema
from schematic_db.db_schema.db_schema import DatabaseSchema


@pytest.fixture(name="mock_synapse_database")
def fixture_mock_synapse_database() -> Generator:
    """Yields a SynapseDatabase object with a mocked Synapse object"""
    with patch.object(sc, "Synapse", return_value=Mock()):
        with patch.object(sc.Synapse, "login", return_value=None):
            obj = SynapseDatabase("y", "syn1")
            yield obj


@pytest.fixture(name="synapse_database")
def fixture_synapse_no_extra_tables(synapse_database: SynapseDatabase) -> Generator:
    """Yields a SynapseDatabase object"""
    obj = synapse_database
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        synapse_id = obj.synapse.get_synapse_id_from_table_name(name)
        obj.synapse.delete_table(synapse_id)


@pytest.fixture(name="synapse_with_empty_tables")
def fixture_synapse_with_empty_tables(  # pylint: disable=too-many-arguments
    synapse_database: SynapseDatabase,
    table_one_schema: TableSchema,
    table_two_schema: TableSchema,
    table_three_schema: TableSchema,
    table_one_columns: list[sc.Column],
    table_two_columns: list[sc.Column],
    table_three_columns: list[sc.Column],
) -> Generator:
    """Yields a SynapseDatabase object with tables added"""
    obj = synapse_database
    obj.synapse.add_table("table_one", table_one_columns)
    obj.synapse.add_table("table_two", table_two_columns)
    obj.synapse.add_table("table_three", table_three_columns)
    obj.annotate_table("table_one", table_one_schema)
    obj.annotate_table("table_two", table_two_schema)
    obj.annotate_table("table_three", table_three_schema)
    yield obj


@pytest.fixture(name="synapse_with_filled_tables")
def fixture_synapse_with_filled_tables(
    synapse_with_empty_tables: SynapseDatabase,
    table_one: pd.DataFrame,
    table_two: pd.DataFrame,
    table_three: pd.DataFrame,
) -> Generator:
    """Yields a SynapseDatabase object with tables added and filled"""
    obj = synapse_with_empty_tables
    synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
    synapse_id2 = obj.synapse.get_synapse_id_from_table_name("table_two")
    synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")
    obj.synapse.insert_table_rows(synapse_id1, table_one)
    obj.synapse.insert_table_rows(synapse_id2, table_two)
    obj.synapse.insert_table_rows(synapse_id3, table_three)
    yield obj


class TestMockSynapseDatabase:
    """Testing for SynapseDatabase where the Synapse object is mocked"""

    def test_query_table(self, mock_synapse_database: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.query_table"""
        obj = mock_synapse_database

        with patch.object(
            Synapse, "get_synapse_id_from_table_name", return_value="syn1"
        ) as mock_method1:
            with patch.object(
                Synapse, "query_table", return_value=pd.DataFrame()
            ) as mock_method2:
                obj.query_table("table_one")
                mock_method1.assert_called_once_with("table_one")
                mock_method2.assert_called_once_with("syn1")

    def test_drop_all_tables(
        self, mock_synapse_database: SynapseDatabase, database_schema: DatabaseSchema
    ) -> None:
        """Testing for SynapseDatabase.drop_all_tables"""
        obj = mock_synapse_database
        with patch.object(
            SynapseDatabase, "get_database_schema", return_value=database_schema
        ):
            with patch.object(
                SynapseDatabase, "_drop_table_and_dependencies"
            ) as mock_method:
                obj.drop_all_tables()
                assert mock_method.call_args_list == [
                    call("table_one", database_schema),
                    call("table_two", database_schema),
                ]

    def test_drop_table_and_dependencies(
        self, mock_synapse_database: SynapseDatabase
    ) -> None:
        """Testing for SynapseDatabase.drop_table_and_dependencies"""

        with patch.object(
            SynapseDatabase, "get_database_schema", return_value="table_schema"
        ):
            with patch.object(
                SynapseDatabase, "_drop_table_and_dependencies"
            ) as mock_method:
                mock_synapse_database.drop_table_and_dependencies("table_one")
                mock_method.assert_called_once_with("table_one", "table_schema")

    def test__drop_table_and_dependencies(
        self, mock_synapse_database: SynapseDatabase, database_schema: DatabaseSchema
    ) -> None:
        """Testing for SynapseDatabase._drop_table_and_dependencies"""

        with patch.object(
            SynapseDatabase, "_drop_all_table_dependencies"
        ) as mock_method1:
            with patch.object(
                Synapse, "get_synapse_id_from_table_name", return_value="syn1"
            ) as mock_method2:
                with patch.object(SynapseDatabase, "_drop_table") as mock_method3:
                    mock_synapse_database._drop_table_and_dependencies(
                        "table_one", database_schema
                    )
                    mock_method1.assert_called_once_with("table_one", database_schema)
                    mock_method2.assert_called_once_with("table_one")
                    mock_method3.assert_called_once_with("syn1")

    def test__drop_all_table_dependencies(
        self, mock_synapse_database: SynapseDatabase, database_schema: DatabaseSchema
    ) -> None:
        """Testing for SynapseDatabase._drop_all_table_dependencies"""

        with patch.object(
            DatabaseSchema,
            "get_reverse_dependencies",
            return_value=["table_one", "table_two"],
        ) as mock_method1:
            with patch.object(
                SynapseDatabase, "_drop_table_and_dependencies"
            ) as mock_method2:
                mock_synapse_database._drop_all_table_dependencies(
                    "table_one", database_schema
                )
                mock_method1.assert_called_once_with("table_one")
                assert mock_method2.call_args_list == [
                    call("table_one", database_schema),
                    call("table_two", database_schema),
                ]


class TestSynapseDatabase:
    """Testing for SynapseDatabase"""

    def test_drop_all_tables(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.drop_all_tables()"""
        obj = synapse_with_empty_tables
        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        synapse_id2 = obj.synapse.get_synapse_id_from_table_name("table_two")
        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")

        annos1a = obj.synapse.get_entity_annotations(synapse_id1)
        annos2a = obj.synapse.get_entity_annotations(synapse_id2)
        annos3a = obj.synapse.get_entity_annotations(synapse_id3)
        assert "primary_key" in list(annos1a.keys())
        assert "primary_key" in list(annos2a.keys())
        assert "primary_key" in list(annos3a.keys())
        assert "foreign_keys" in list(annos3a.keys())

        obj.drop_all_tables()
        annos1b = obj.synapse.get_entity_annotations(synapse_id1)
        annos2b = obj.synapse.get_entity_annotations(synapse_id2)
        annos3b = obj.synapse.get_entity_annotations(synapse_id3)
        assert not list(annos1b.keys())
        assert not list(annos2b.keys())
        assert not list(annos3b.keys())

    def test_drop_table_and_dependencies(
        self, synapse_with_empty_tables: SynapseDatabase
    ) -> None:
        """Testing for SynapseDatabase.drop_table_and_dependencies()"""
        obj = synapse_with_empty_tables
        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        synapse_id2 = obj.synapse.get_synapse_id_from_table_name("table_two")
        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")

        annos1a = obj.synapse.get_entity_annotations(synapse_id1)
        annos2a = obj.synapse.get_entity_annotations(synapse_id2)
        annos3a = obj.synapse.get_entity_annotations(synapse_id3)
        assert "primary_key" in list(annos1a.keys())
        assert "primary_key" in list(annos2a.keys())
        assert "primary_key" in list(annos3a.keys())
        assert "foreign_keys" in list(annos3a.keys())

        obj.drop_table_and_dependencies("table_one")
        annos1b = obj.synapse.get_entity_annotations(synapse_id1)
        annos2b = obj.synapse.get_entity_annotations(synapse_id2)
        annos3b = obj.synapse.get_entity_annotations(synapse_id3)
        assert not list(annos1b.keys())
        assert "primary_key" in list(annos2b.keys())
        assert not list(annos3b.keys())

    def test_drop_table(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.drop_table()"""
        obj = synapse_with_empty_tables
        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")

        with pytest.raises(
            SynapseDatabaseDropTableError,
            match="Can not drop database table, other tables exists that depend on it",
        ):
            obj.drop_table("table_one")

        annos1a = obj.synapse.get_entity_annotations(synapse_id1)
        assert list(annos1a.keys())

        obj.drop_table("table_three")
        annos3 = obj.synapse.get_entity_annotations(synapse_id3)
        assert not list(annos3.keys())

    def test_annotate_table(  # pylint: disable=too-many-arguments
        self,
        synapse_database: SynapseDatabase,
        table_one_schema: TableSchema,
        table_three_schema: TableSchema,
        table_one_columns: list[sc.Column],
        table_three_columns: list[sc.Column],
    ) -> None:
        """Testing for SynapseDatabase.annotate_table()"""
        obj = synapse_database
        assert obj.get_table_names() == []

        obj.synapse.add_table("table_one", table_one_columns)
        obj.synapse.add_table("table_three", table_three_columns)

        synapse_id1 = obj.synapse.get_synapse_id_from_table_name("table_one")
        annotations = obj.synapse.get_entity_annotations(synapse_id1)
        assert annotations == {}

        obj.annotate_table("table_one", table_one_schema)
        annotations2 = obj.synapse.get_entity_annotations(synapse_id1)
        assert list(annotations2.keys()) == [
            "attribute0",
            "attribute1",
            "attribute2",
            "attribute3",
            "attribute4",
            "attribute5",
            "primary_key",
        ]

        synapse_id3 = obj.synapse.get_synapse_id_from_table_name("table_three")
        annotations3 = obj.synapse.get_entity_annotations(synapse_id3)
        assert annotations3 == {}

        obj.annotate_table("table_three", table_three_schema)
        annotations4 = obj.synapse.get_entity_annotations(synapse_id3)
        assert list(annotations4.keys()) == [
            "attribute0",
            "attribute1",
            "attribute2",
            "attribute3",
            "primary_key",
            "foreign_keys",
        ]

    def get_database_schema(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.get_database_schema"""
        obj = synapse_with_empty_tables
        database_schema = obj.get_database_schema()
        assert database_schema.get_schema_names == [
            "table_one",
            "table_three",
            "table_two",
            "test_table_one",
        ]

    def test_get_table_schema(self, synapse_with_empty_tables: SynapseDatabase) -> None:
        """Testing for SynapseDatabase.get_table_schema()"""
        obj = synapse_with_empty_tables
        table_schema1 = obj.get_table_schema("table_one")
        assert table_schema1 is not None
        assert table_schema1.name == "table_one"
        assert table_schema1.primary_key == "pk_one_col"
        assert table_schema1.foreign_keys == []
        assert table_schema1.columns != []

        table_schema3 = obj.get_table_schema("table_three")
        assert table_schema3 is not None
        assert table_schema3.name == "table_three"
        assert table_schema3.primary_key == "pk_zero_col"
        assert table_schema3.foreign_keys != []
        assert table_schema3.columns != []

    def test_delete_table_rows(
        self,
        synapse_with_filled_tables: SynapseDatabase,
        table_three_schema: TableSchema,
    ) -> None:
        """Testing for SynapseDatabase.delete_table_rows()"""
        obj = synapse_with_filled_tables
        synapse_id = obj.synapse.get_synapse_id_from_table_name("table_three")
        query = f"SELECT {table_three_schema.primary_key} FROM {synapse_id}"
        table = obj.execute_sql_query(query)
        assert table["pk_zero_col"].tolist() == ["keyA", "keyB", "keyC", "keyD"]

        obj.delete_table_rows("table_three", table.iloc[[0]])
        table2 = obj.execute_sql_query(query)
        assert table2["pk_zero_col"].tolist() == ["keyB", "keyC", "keyD"]

    def test_delete_table_rows2(
        self,
        synapse_with_filled_tables: SynapseDatabase,
    ) -> None:
        """Testing for SynapseDatabase.delete_table_rows()"""
        obj = synapse_with_filled_tables

        table1a = obj.query_table("table_one")
        table3a = obj.query_table("table_three")
        assert table1a["pk_one_col"].tolist() == ["key1", "key2", "key3"]
        assert table3a["pk_zero_col"].tolist() == ["keyA", "keyB", "keyC", "keyD"]

        obj.delete_table_rows("table_one", table1a.iloc[[2]])
        table1b = obj.query_table("table_one")
        table3b = obj.query_table("table_three")
        assert table1b["pk_one_col"].tolist() == ["key1", "key2"]
        assert table3b["pk_zero_col"].tolist() == ["keyA", "keyB", "keyC", "keyD"]

        obj.delete_table_rows("table_one", table1a.iloc[[0]])
        table1b = obj.query_table("table_one")
        table3b = obj.query_table("table_three")
        assert table1b["pk_one_col"].tolist() == ["key2"]
        assert table3b["pk_zero_col"].tolist() == ["keyC", "keyD"]

    def test_insert_table_rows(
        self,
        synapse_with_empty_tables: SynapseDatabase,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for SynapseDatabase.upsert_table_rows()"""
        obj = synapse_with_empty_tables

        query_result1 = obj.query_table("table_one")
        assert query_result1["pk_one_col"].tolist() == []

        obj.insert_table_rows("table_one", table_one)
        query_result2 = obj.query_table("table_one")
        assert query_result2["pk_one_col"].tolist() == ["key1", "key2", "key3"]

    def test_upsert_table_rows(
        self,
        synapse_with_filled_tables: SynapseDatabase,
    ) -> None:
        """Testing for SynapseDatabase.upsert_table_rows()"""
        obj = synapse_with_filled_tables

        table1 = obj.query_table("table_one")
        assert table1["pk_one_col"].tolist() == ["key1", "key2", "key3"]
        assert table1["string_one_col"].tolist() == ["a", "b", np.nan]

        upsert_table1 = pd.DataFrame({"pk_one_col": ["key1"], "string_one_col": ["a"]})
        obj.upsert_table_rows("table_one", upsert_table1)
        table2 = obj.query_table("table_one")
        assert table2["pk_one_col"].tolist() == ["key1", "key2", "key3"]
        assert table2["string_one_col"].tolist() == ["a", "b", np.nan]

        upsert_table2 = pd.DataFrame(
            {"pk_one_col": ["key3", "key4"], "string_one_col": ["c", "d"]}
        )
        obj.upsert_table_rows("table_one", upsert_table2)
        table3 = obj.query_table("table_one")
        assert table3["pk_one_col"].tolist() == ["key1", "key2", "key3", "key4"]
        assert table3["string_one_col"].tolist() == ["a", "b", "c", "d"]

    def test_create_primary_key_table(
        self,
        synapse_with_filled_tables: SynapseDatabase,
    ) -> None:
        """Testing for SynapseDatabase._create_primary_key_table()"""
        obj = synapse_with_filled_tables
        synapse_id = obj.synapse.get_synapse_id_from_table_name("table_one")
        table = obj._create_primary_key_table(  # pylint: disable=protected-access
            synapse_id, "pk_one_col"
        )
        assert list(table.columns) == ["ROW_ID", "ROW_VERSION", "pk_one_col"]
