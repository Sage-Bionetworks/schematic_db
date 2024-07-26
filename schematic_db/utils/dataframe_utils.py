"""Utils for dataframe operations"""

import math
import pandas as pd
import numpy as np


def split_table_into_chunks(
    table: pd.DataFrame, chunk_size: int | None = None
) -> list[pd.DataFrame]:
    """
    Splits a dataframe into a lsit of smaller dataframes, where each dataframe has
    a row number equal to the chucnk size.

    Args:
        table (pd.DataFrame): The table to be split
        chunk_size (int | None, optional): How many rows per table after splitting.
          Defaults to None. If None, the table will be returned as a list.

    Raises:
        ValueError: If chunk size is below 1

    Returns:
        list[pd.DataFrame]: A list of tables that we split from the input table.
    """
    if chunk_size is None:
        return [table]
    if chunk_size < 1:
        raise ValueError(
            "Attempting to split input table using chunk size bewlow 1: ", chunk_size
        )
    n_rows = len(table.index)
    n_chunks = math.ceil(n_rows / chunk_size)
    return np.array_split(table, n_chunks)
