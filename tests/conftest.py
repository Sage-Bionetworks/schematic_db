"""Fixtures for all tests
"""
from datetime import datetime
import pytest
import pandas as pd
import numpy as np
from db_object_config import DBObjectConfig, DBAttributeConfig, DBDatatype


@pytest.fixture(scope = "session")
def table_one():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame({
        "pk_col": ["key1", "key2", "key3"],
        "string_col": ["a","b", np.nan],
        "int_col": [1,pd.NA,3],
        "double_col": [1.1,2.2,np.nan],
        "date_col": [datetime(2022, 8, 2), np.nan, datetime(2022, 8, 2)],
        "bool_col": [pd.NA, True, False]
    })
    dataframe = dataframe.astype({"int_col": "Int64", "bool_col": "boolean"})
    dataframe['date_col'] = pd.to_datetime(dataframe['date_col']).dt.date
    yield dataframe

@pytest.fixture(scope = "session")
def table_one_config():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name = "table_one",
        attributes = [
            DBAttributeConfig(name="pk_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="string_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="int_col", datatype=DBDatatype.Int),
            DBAttributeConfig(name="double_col", datatype=DBDatatype.Float),
            DBAttributeConfig(name="date_col", datatype=DBDatatype.Date),
            DBAttributeConfig(name="bool_col", datatype=DBDatatype.Boolean)
        ],
        primary_keys = ["pk_col"],
        foreign_keys = {}
    )
    yield table_config

@pytest.fixture(scope = "session")
def table_two():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame({
        "pk_col": ["key1", "key2", "key3", "key4"],
        "string_col": ["a", "b", "c", "d"]
    })
    yield dataframe

@pytest.fixture(scope = "session")
def table_two_b():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame({
        "pk_col": ["key5", "key6", "key7", "key8"],
        "string_col": ["a", "b", "c", "d"]
    })
    yield dataframe

@pytest.fixture(scope = "session")
def table_two_config():
    """
    Yields a DBObjectConfig object with one primary and no foreign keys
    """
    table_config = DBObjectConfig(
        name = "table_two",
        attributes = [
            DBAttributeConfig(name="pk_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="string_col", datatype=DBDatatype.Text)
        ],
        primary_keys = ["pk_col"],
        foreign_keys = {}
    )
    yield table_config

@pytest.fixture(scope = "session")
def table_three():
    """
    Yields a pd.Dataframe.
    """
    dataframe = pd.DataFrame({
        "fk1_col": ["key1", "key1", "key2", "key2"],
        "fk2_col": ["key1", "key2", "key1", "key2"],
        "string_col": ["a", "b", "c", "d"]
    })
    yield dataframe

@pytest.fixture(scope = "session")
def table_three_config():
    """
    Yields a DBObjectConfig object with two keys that are both primary and foreign
    """
    table_config = DBObjectConfig(
        name = "table_three",
        attributes = [
            DBAttributeConfig(name="fk1_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="fk2_col", datatype=DBDatatype.Text),
            DBAttributeConfig(name="string_col", datatype=DBDatatype.Text)
        ],
        primary_keys = ["fk1_col", "fk2_col"],
        foreign_keys = {"fk1_col": "table_one.pk_col", "fk2_col": "table_two.pk_col"}
    )
    yield table_config
