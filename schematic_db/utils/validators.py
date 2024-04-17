"""Functions for pydantic validators"""

import re
import validators


def string_is_not_empty(value: str) -> str:
    """Check if string  is not empty(has at least one char)

    Args:
        value (str): A string

    Raises:
        ValueError: If the value is zero characters long

    Returns:
        (str): The input value
    """
    if len(value) == 0:
        raise ValueError(f"{value} is an empty string")
    return value


def is_synapse_id(value: str) -> str:
    """Check if string is a valid synapse id

    Args:
        value (str): A string

    Raises:
        ValueError: If the value isn't a valid Synapse id

    Returns:
        (str): The input value
    """
    if not re.search("^syn[0-9]+", value):
        raise ValueError(f"{value} is not a valid Synapse id")
    return value


def is_valid_url(value: str) -> str:
    """Validates that the value is a valid URL

    Args:
        value (str) A string

    Raises:
        ValueError: If the value isn't a valid URL

    Returns:
        str: The input value
    """
    valid_url = validators.url(value)
    if not valid_url:
        raise ValueError(f"{value} is a valid url")
    return value


def is_data_model_file(value: str) -> str:
    """Validates that the value is a valid data model file type

    Args:
        value (str) A string

    Raises:
        ValueError: If the value isn't a valid data model file type

    Returns:
        str: The input value
    """
    is_valid_type = value.endswith(".jsonld") | value.endswith(".csv")
    if not is_valid_type:
        raise ValueError(f"{value} does end with '.jsonld', or '.csv")
    return value
