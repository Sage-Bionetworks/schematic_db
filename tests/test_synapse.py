"""Testing for Synapse."""
from typing import Any, Generator
import pytest
import pandas as pd
import synapseclient as sc  # type: ignore
from schematic_db.db_schema.db_schema import TableSchema
from schematic_db.synapse.synapse import Synapse


@pytest.fixture(name="synapse_with_test_table_one", scope="class")
def fixture_synapse_with_test_table_one(
    synapse_object: Synapse,
    table_one_columns: list[sc.Column],
    table_one: pd.DataFrame,
) -> Generator:
    """
    Yields a Synapse object with "test_table_one" added, used only for tests that
     don't alter the state of the Synapse project
    """
    obj = synapse_object
    obj.add_table("test_table_one", table_one_columns)
    synapse_id = obj.get_synapse_id_from_table_name("test_table_one")
    obj.insert_table_rows(synapse_id, table_one)
    obj.set_entity_annotations(synapse_id, {"test_annotation": "test_value"})
    yield obj
    obj.delete_table(synapse_id)


@pytest.fixture(name="synapse_with_no_tables")
def fixture_synapse_with_no_tables(synapse_object: Synapse) -> Generator:
    """
    Yields a Synapse object
    """
    obj = synapse_object
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        synapse_id = obj.get_synapse_id_from_table_name(name)
        obj.delete_table(synapse_id)


@pytest.fixture(name="synapse_with_empty_table_one")
def fixture_synapse_with_empty_table_one(
    synapse_with_no_tables: Synapse,
    table_one_columns: list[sc.Column],
) -> Generator:
    """
    Yields a Synapse object with table one added
    """
    obj = synapse_with_no_tables
    obj.add_table("table_one", table_one_columns)
    yield obj


@pytest.fixture(name="synapse_with_filled_table_one")
def fixture_synapse_with_filled_table_one(
    synapse_with_empty_table_one: Synapse, table_one: pd.DataFrame
) -> Generator:
    """
    Yields a Synapse object with table one filled
    """
    obj = synapse_with_empty_table_one
    synapse_id = obj.get_synapse_id_from_table_name("table_one")
    obj.insert_table_rows(synapse_id, table_one)
    yield obj


@pytest.mark.fast
class TestMockSynapse:
    """Testing for Synapse class with mocked methods"""

    def test_get_table_names(self, mocker: Any) -> None:
        """Testing for Synapse.get_table_names"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        obj = Synapse("", "")
        assert obj.get_table_names() == ["table1", "table2"]

    def test_get_synapse_id_from_table_name(self, mocker: Any) -> None:
        """Testing for Synapse.get_synapse_id_from_table_name"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        obj = Synapse("", "")
        assert obj.get_synapse_id_from_table_name("table1") == "syn1"
        assert obj.get_synapse_id_from_table_name("table2") == "syn2"

    def test_get_table_name_from_synapse_id(self, mocker: Any) -> None:
        """Testing for Synapse.get_table_name_from_synapse_id"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        obj = Synapse("", "")
        assert obj.get_table_name_from_synapse_id("syn1") == "table1"
        assert obj.get_table_name_from_synapse_id("syn2") == "table2"

    def test_query_table(self, mocker: Any) -> None:
        """Testing for Synapse.query_table"""
        tables = [{"name": "table1", "id": "syn1"}, {"name": "table2", "id": "syn2"}]
        query_result = pd.DataFrame({"col1": ["a", "b"], "col2": [1, 2]})
        mocker.patch("synapseclient.Synapse.login", return_value=None)
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse._get_tables", return_value=tables
        )
        mocker.patch(
            "schematic_db.synapse.synapse.Synapse.execute_sql_query",
            return_value=query_result,
        )
        obj = Synapse("", "")
        assert isinstance(obj.query_table("syn1"), pd.DataFrame)


class TestSynapseGetters:
    """Testing for Synapse class getters"""

    def test_get_table_names(self, synapse_with_test_table_one: Synapse) -> None:
        """Testing for Synapse.get_table_names()"""
        assert synapse_with_test_table_one.get_table_names() == ["test_table_one"]

    def test_get_column_table_names(
        self, synapse_with_test_table_one: Synapse, table_one_schema: TableSchema
    ) -> None:
        """Testing for Synapse.get_table_column_names()"""
        assert sorted(
            synapse_with_test_table_one.get_table_column_names("test_table_one")
        ) == sorted(table_one_schema.get_column_names())

    def test_get_table_id_and_name(self, synapse_with_test_table_one: Synapse) -> None:
        """Testing for Synapse.get_table_id_from_name()"""
        synapse_id = synapse_with_test_table_one.get_synapse_id_from_table_name(
            "test_table_one"
        )
        table_name = synapse_with_test_table_one.get_table_name_from_synapse_id(
            synapse_id
        )
        assert table_name == "test_table_one"

    def test_query_table(
        self,
        synapse_with_test_table_one: Synapse,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for synapse.query_table()"""
        obj = synapse_with_test_table_one
        synapse_id = obj.get_synapse_id_from_table_name("test_table_one")
        result = synapse_with_test_table_one.query_table(synapse_id)
        assert sorted(result.columns) == sorted(table_one.columns)
        assert (
            result["pk_one_col"].values.tolist()
            == table_one["pk_one_col"].values.tolist()
        )

    def test_get_entity_annotations(self, synapse_with_test_table_one: Synapse) -> None:
        """Testing for Synapse.get_entity_annotations"""
        obj = synapse_with_test_table_one
        synapse_id = obj.get_synapse_id_from_table_name("test_table_one")
        annotations = obj.get_entity_annotations(synapse_id)
        assert annotations.id == synapse_id
        assert annotations == {"test_annotation": ["test_value"]}


class TestSynapseModifyTables:
    """
    Testing for methods that add or drop tables
    """

    def test_build_table(
        self,
        synapse_with_no_tables: Synapse,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for Synapse.add_table()"""
        obj = synapse_with_no_tables
        assert obj.get_table_names() == []
        obj.build_table("table_one", table_one)
        assert obj.get_table_names() == ["table_one"]

    def test_add_table(
        self, synapse_with_no_tables: Synapse, table_one_columns: list[sc.Column]
    ) -> None:
        """Testing for Synapse.add_table()"""
        obj = synapse_with_no_tables
        assert obj.get_table_names() == []
        obj.add_table("table_one", table_one_columns)
        assert obj.get_table_names() == ["table_one"]

    def test_replace_table(
        self,
        synapse_with_filled_table_one: Synapse,
        table_two: pd.DataFrame,
    ) -> None:
        """Testing for synapse.replace_table()"""
        obj = synapse_with_filled_table_one
        table_id1 = obj.get_synapse_id_from_table_name("table_one")
        obj.replace_table("table_one", table_two)
        result1 = obj.query_table(table_id1)
        pd.testing.assert_frame_equal(result1, table_two)
        table_id2 = obj.get_synapse_id_from_table_name("table_one")
        assert table_id1 == table_id2


class TestSynapseModifyRows:
    """
    Testing for synapse methods that modify row data
    """

    def test_insert_table_rows(
        self,
        synapse_with_empty_table_one: Synapse,
        table_one: pd.DataFrame,
    ) -> None:
        """
        Testing for synapse.insert_table_rows()
        """
        obj = synapse_with_empty_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")

        result1 = obj.query_table(synapse_id)
        assert result1.empty

        obj.insert_table_rows(synapse_id, table_one)
        result2 = obj.query_table(synapse_id)
        assert not result2.empty

    def test_delete_table_rows(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one_schema: TableSchema,
    ) -> None:
        """Testing for Synapse.delete_table_rows()"""
        obj = synapse_with_filled_table_one
        table_id = obj.get_synapse_id_from_table_name("table_one")
        query = f"SELECT {table_one_schema.primary_key} FROM {table_id}"
        table = obj.execute_sql_query(query, include_row_data=True)
        assert table["ROW_ID"].tolist() == [1, 2, 3]

        obj.delete_table_rows(table_id, table.iloc[[0]])
        table2 = obj.execute_sql_query(query, include_row_data=True)
        assert table2["ROW_ID"].tolist() == [2, 3]

    def test_delete_all_table_rows(
        self,
        synapse_with_filled_table_one: Synapse,
    ) -> None:
        """
        Testing for synapse.delete_all_table_rows()
        """
        obj = synapse_with_filled_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")

        result1 = obj.query_table(synapse_id)
        assert not result1.empty

        obj.delete_all_table_rows(synapse_id)
        result2 = obj.query_table(synapse_id)
        assert result2.empty


class TestSynapseModifyColumns:
    """Testing for synapse methods that modify table columns"""

    def test_delete_all_table_columns(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
    ) -> None:
        """Testing for synapse.delete_all_table_columns()"""
        obj = synapse_with_filled_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        assert sorted(obj.get_table_column_names("table_one")) == sorted(
            table_one.columns
        )
        obj.delete_all_table_columns(synapse_id)
        assert obj.get_table_column_names("table_one") == []

    def test_add_table_columns(
        self,
        synapse_with_filled_table_one: Synapse,
        table_one: pd.DataFrame,
        table_one_columns: list[sc.Column],
    ) -> None:
        """Testing for synapse.add_table_columns()"""
        obj = synapse_with_filled_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        assert sorted(obj.get_table_column_names("table_one")) == sorted(
            table_one.columns
        )

        obj.delete_all_table_columns(synapse_id)
        assert obj.get_table_column_names("table_one") == []
        obj.add_table_columns(synapse_id, table_one_columns)
        assert sorted(obj.get_table_column_names("table_one")) == sorted(
            (table_one.columns)
        )


class TestSynapseAnnotations:
    """Testing for annotation methods"""

    def test_set_entity_annotations(
        self, synapse_with_empty_table_one: Synapse
    ) -> None:
        """Testing for Synapse.set_entity_annotations"""
        obj = synapse_with_empty_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        annotations = obj.get_entity_annotations(synapse_id)
        assert annotations.id == synapse_id
        assert annotations == {}

        obj.set_entity_annotations(synapse_id, {"test_annotation": "test_value"})
        annotations2 = obj.get_entity_annotations(synapse_id)
        assert annotations2.id == synapse_id
        assert annotations2 == {"test_annotation": ["test_value"]}

    def test_clear_entity_annotations(
        self, synapse_with_empty_table_one: Synapse
    ) -> None:
        """Testing for Synapse.clear_entity_annotations"""
        obj = synapse_with_empty_table_one
        synapse_id = obj.get_synapse_id_from_table_name("table_one")
        annotations = obj.get_entity_annotations(synapse_id)
        assert annotations.id == synapse_id
        assert annotations == {}

        obj.set_entity_annotations(synapse_id, {"test_annotation": "test_value"})
        annotations2 = obj.get_entity_annotations(synapse_id)
        assert annotations2.id == synapse_id
        assert annotations2 == {"test_annotation": ["test_value"]}

        obj.clear_entity_annotations(synapse_id)
        annotations3 = obj.get_entity_annotations(synapse_id)
        assert annotations3.id == synapse_id
        assert annotations3 == {}
