"""Testing for Schematic API utils"""

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from schematic_db.utils.dataframe_utils import split_table_into_chunks


class TestSplitTableIntoChunks:  # pylint: disable=too-few-public-methods
    """Testing for split_table_into_chunks"""

    def test_success1(self) -> None:
        """Testing for successful split"""
        table = pd.DataFrame({"col1": ["A", "B", "C"]})
        result1 = split_table_into_chunks(table, 1)
        assert len(result1) == 3
        assert_frame_equal(
            result1[0].reset_index(drop=True), pd.DataFrame({"col1": ["A"]})
        )
        assert_frame_equal(
            result1[1].reset_index(drop=True), pd.DataFrame({"col1": ["B"]})
        )
        assert_frame_equal(
            result1[2].reset_index(drop=True), pd.DataFrame({"col1": ["C"]})
        )
        result2 = split_table_into_chunks(table, 3)
        assert len(result2) == 1
        assert_frame_equal(result2[0], table)
        result3 = split_table_into_chunks(table, 10)
        assert len(result3) == 1
        assert_frame_equal(result3[0], table)
        result4 = split_table_into_chunks(table)
        assert_frame_equal(result4[0], table)

    def test_success2(self) -> None:
        """Testing for successful split"""
        table = pd.DataFrame({"col1": ["A", "B", "C", "D", "E", "F"]})
        result1 = split_table_into_chunks(table, 4)
        assert len(result1) == 2
        assert_frame_equal(
            result1[0].reset_index(drop=True), pd.DataFrame({"col1": ["A", "B", "C"]})
        )
        assert_frame_equal(
            result1[1].reset_index(drop=True), pd.DataFrame({"col1": ["D", "E", "F"]})
        )

    def test_error(self) -> None:
        """Testing for value error when input is less than 1"""
        with pytest.raises(
            ValueError,
            match="Attempting to split input table using chunk size bewlow 1",
        ):
            split_table_into_chunks(pd.DataFrame({"col1": ["A", "B", "C"]}), 0)
