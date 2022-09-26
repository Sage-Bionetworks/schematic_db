"""Testing for Synapse."""
import pytest
import pandas as pd


@pytest.fixture(name="synapse_no_extra_tables")
def fixture_synapse_no_extra_tables(
    synapse_database_project, synapse_database_table_names
):
    """
    Yields a Synapse object
    """
    obj = synapse_database_project
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        if name not in synapse_database_table_names:
            obj.drop_table(name)


@pytest.fixture(name="synapse_with_empty_table_one")
def fixture_synapse_with_empty_table_one(synapse_no_extra_tables, table_one_config):
    """
    Yields a Synapse object with table one added
    """
    obj = synapse_no_extra_tables
    obj.add_table("table_one", table_one_config)
    yield obj


@pytest.fixture(name="synapse_with_filled_table_one")
def fixture_synapse_with_filled_table_one(synapse_with_empty_table_one, table_one):
    """
    Yields a Synapse object with table one filled
    """
    obj = synapse_with_empty_table_one
    obj.insert_table_rows("table_one", table_one)
    yield obj


class TestSynapseIDs:
    """Testing for id methods"""

    def test_get_table_id_from_name(self, synapse_database_project):
        """Testing for Synapse.get_table_id_from_name()"""
        assert (
            synapse_database_project.get_synapse_id_from_table_name("test_table_one")
            == "syn34532191"
        )

    def test_get_table_name_from_id(self, synapse_database_project):
        """Testing for Synapse.get_table_name_from_id()"""
        assert (
            synapse_database_project.get_table_name_from_synapse_id("syn34532191")
            == "test_table_one"
        )


class TestSynapseQueries:
    """Testing for query methods"""

    def test_query_table(self, synapse_database_project, table_one, table_one_config):
        """Testing for synapse.query_table()"""
        result = synapse_database_project.query_table(
            "test_table_one", table_one_config
        )
        pd.testing.assert_frame_equal(result, table_one)

    def test_read_csv_file(self, synapse_manifest_store, table_one):
        """Testing for synapse.read_csv_file()"""
        result = synapse_manifest_store.synapse.read_csv_file("syn35871897")
        assert result.shape == table_one.shape
        assert result["pk_one_col"].tolist() == table_one["pk_one_col"].tolist()


class TestSynapseModifyTables:
    """
    Testing for methods that add or drop tables
    """

    def test_add_table(
        self, synapse_no_extra_tables, table_one_config, synapse_database_table_names
    ):
        """Testing for Synapse.add_table()"""
        obj = synapse_no_extra_tables
        assert obj.get_table_names() == synapse_database_table_names
        obj.add_table("table_one", table_one_config)
        assert obj.get_table_names() == ["table_one"] + synapse_database_table_names

    def test_drop_table(
        self, synapse_with_empty_table_one, synapse_database_table_names
    ):
        """Testing for Synapse.drop_table()"""
        obj = synapse_with_empty_table_one
        assert obj.get_table_names() == ["table_one"] + synapse_database_table_names
        obj.drop_table("table_one")
        assert obj.get_table_names() == synapse_database_table_names


class TestSynapseModifyRows:
    """
    Testing for synapse methods that modify row data
    """

    def test_insert_table_rows(
        self, synapse_with_empty_table_one, table_one, table_one_config
    ):
        """
        Testing for synapse.insert_table_rows()
        """
        obj = synapse_with_empty_table_one
        obj.insert_table_rows("table_one", table_one)
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.insert_table_rows("table_one", table_one)
        result2 = obj.query_table("table_one", table_one_config)
        test_table = pd.concat(objs=[table_one, table_one])
        test_table.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result2, test_table)

    def test_delete_table_rows(
        self, synapse_with_filled_table_one, table_one, table_one_config
    ):
        """
        Testing for synapse.delete_table_rows()
        """
        obj = synapse_with_filled_table_one
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.delete_table_rows("table_one", table_one.iloc[[0]], table_one_config)
        result2 = obj.query_table("table_one", table_one_config)
        test_table = table_one.iloc[1:]
        test_table.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result2, test_table)

        obj.delete_table_rows("table_one", table_one.iloc[[0]], table_one_config)
        result3 = obj.query_table("table_one", table_one_config)
        test_table2 = table_one.iloc[1:]
        test_table2.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(result3, test_table2)

    def test_update_table_rows(
        self, synapse_with_filled_table_one, table_one, table_one_config
    ):
        """
        Testing for synapse.update_table_rows()
        """
        obj = synapse_with_filled_table_one
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.update_table_rows("table_one", table_one, table_one_config)
        result2 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result2, table_one)

        update_table = pd.DataFrame({"pk_one_col": ["key1"], "string_one_col": ["x"]})
        test_table = table_one
        test_table.at[0, "string_one_col"] = "x"
        obj.update_table_rows("table_one", update_table, table_one_config)
        result3 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result3, test_table)

    def test_upsert_table_rows(
        self, synapse_with_filled_table_one, table_one, table_one_config
    ):
        """
        Testing for synapse.upsert_table_rows()
        """
        obj = synapse_with_filled_table_one
        result1 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result1, table_one)

        obj.upsert_table_rows("table_one", table_one, table_one_config)
        result2 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result2, table_one)

        upsert_table = table_one
        upsert_table.at[0, "string_one_col"] = "x"
        obj.update_table_rows("table_one", upsert_table, table_one_config)
        result3 = obj.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result3, upsert_table)

# TODO fix this test
'''
class TestReplaceTable:
    """Testing for synapse.replace_table()"""

    def test_replace_table(
        self, synapse_with_filled_table_one, table_two, table_two_config
    ):
        """Testing for synapse.replace_table()"""
        obj = synapse_with_filled_table_one
        table_id1 = obj.get_synapse_id_from_table_name("table_one")
        obj.replace_table("table_one", table_two)
        result1 = obj.query_table("table_one", table_two_config)
        pd.testing.assert_frame_equal(result1, table_two)
        table_id2 = obj.get_synapse_id_from_table_name("table_one")
        assert table_id1 == table_id2
'''
