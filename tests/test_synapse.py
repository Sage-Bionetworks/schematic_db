"""
Testing for Synapse.

For testing locally there should be a synapse server running.
A config yaml should exist at 'tests/data/local_synapse_config.yml'.
This config should look like:
username: "root"
password: "root"
host: "localhost"
schema: "test_schema"
The schema should not exist on the database at the beginning of the test.
This file is ignored by git.

If the the config doesn't exist, the file at 'tests/data/synapse_config.yml'
will be used.
"""
import os
import pytest
import pandas as pd
from yaml import safe_load
from rdb_type import Synapse
from db_object_config import DBDatatype


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(TESTS_DIR, "data")
CONFIG_PATH = os.path.join(DATA_DIR, "local_synapse_config.yml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join(DATA_DIR, "synapse_config.yml")

@pytest.fixture(scope = "module", name = "config_dict")
def fixture_config_dict():
    """
    Yields a MYSQL config dict
    """
    with open(CONFIG_PATH, mode="rt", encoding="utf-8") as file:
        config = safe_load(file)
    yield config

@pytest.fixture(scope = "module", name = "test_project_table_names")
def fixture_test_project_table_names():
    """
    Yields a list of table names the testing project should start out with.
    """
    yield ["test_table_one"]

@pytest.fixture(scope = "module", name = "synapse")
def fixture_synapse(config_dict, test_project_table_names, table_one_config):
    """
    Yields a Synapse object
    """
    obj = Synapse(config_dict)
    assert obj.get_table_names() == test_project_table_names
    obj.add_table("table_one", table_one_config)
    yield obj
    table_names = obj.get_table_names()
    for name in table_names:
        if name not in test_project_table_names:
            obj.drop_table(name)
    assert obj.get_table_names() == test_project_table_names

class TestSynapseIDs:
    """Testing for  id methods
    """
    def test_get_table_id_from_name(self, synapse):
        """Testing for Synapse.get_table_id_from_name()
        """
        assert synapse.get_table_id_from_name("test_table_one") == "syn34073419"

    def test_get_table_name_from_id(self, synapse):
        """Testing for Synapse.get_table_name_from_id()
        """
        assert synapse.get_table_name_from_id("syn34073419") == "test_table_one"

class TestSynapseQueries:
    """Testing for query methods
    """
    def test_query_table(self, synapse, table_one, table_one_config):
        """Testing for synapse.query_table()
        """
        result = synapse.query_table("test_table_one", table_one_config)
        pd.testing.assert_frame_equal(result, table_one)

class TestSynapse:
    """
    Testing for synapse
    """

    def test_add_drop_table(self, synapse, table_one_config, test_project_table_names):
        """Testing for Synapse.add_table(), and Synapse.drop_table()
        """
        assert synapse.get_table_names() == ['table_one'] + test_project_table_names
        synapse.drop_table("table_one")
        assert synapse.get_table_names() == test_project_table_names
        synapse.add_table("table_one", table_one_config)
        assert synapse.get_table_names() == ['table_one'] + test_project_table_names

    def test_add_drop_table_column(
        self, synapse, table_one, table_one_config, test_project_table_names
    ):
        """Testing for synapse.add_table_column() and synapse.drop_table_column()
        """
        assert synapse.get_table_names() == ['table_one'] + test_project_table_names
        assert synapse.get_column_names_from_table("table_one") == list(table_one.columns)
        synapse.add_table_column("table_one", "test_add_col", DBDatatype.Float)
        assert synapse.get_column_names_from_table("table_one") == \
            list(table_one.columns) + ["test_add_col"]
        synapse.drop_table_column("table_one", "test_add_col")
        assert synapse.get_column_names_from_table("table_one") == list(table_one.columns)
        synapse.drop_table("table_one")
        synapse.add_table("table_one", table_one_config)

    def test_insert_delete_table_rows(
        self, synapse, table_one, table_one_config, test_project_table_names
    ):
        """
        Testing for synapse.insert_table_rows() and synapse.delete_table_rows()
        """
        assert synapse.get_table_names() == ['table_one'] + test_project_table_names
        synapse.insert_table_rows("table_one", table_one)
        result = synapse.query_table("table_one", table_one_config)
        pd.testing.assert_frame_equal(result, table_one)
        synapse.drop_table("table_one")
        synapse.add_table("table_one", table_one_config)
