"""
A config for database specific items
"""

from typing import Any, Optional
from pydantic.dataclasses import dataclass
from schematic_db.db_schema.db_schema import (
    ForeignKeySchema,
    ColumnDatatype,
)

# used to translate types from the Schematic API or config to the geneirc ENUM
DATATYPES = {
    "str": ColumnDatatype.TEXT,
    "float": ColumnDatatype.FLOAT,
    "int": ColumnDatatype.INT,
    "date": ColumnDatatype.DATE,
    "boolean": ColumnDatatype.BOOLEAN,
    "synapse_string": ColumnDatatype.SYNAPSE_STRING,
    "synapse_file_handle_id": ColumnDatatype.SYNAPSE_FILE_HANDLE_ID,
    "synapse_entity_id": ColumnDatatype.SYNAPSE_ENTITY_ID,
    "synapse_link": ColumnDatatype.SYNAPSE_LINK,
    "synapse_user_id": ColumnDatatype.SYNAPSE_USER_ID,
    "synapse_string_list": ColumnDatatype.SYNAPSE_STRING_LIST,
    "synapse_date_list": ColumnDatatype.SYNAPSE_DATE_LIST,
    "synapse_int_list": ColumnDatatype.SYNAPSE_INT_LIST,
    "synapse_boolean_list": ColumnDatatype.SYNAPSE_BOOLEAN_LIST,
    "synapse_entity_id_list": ColumnDatatype.SYNAPSE_ENTITY_ID_LIST,
    "synapse_user_id_list": ColumnDatatype.SYNAPSE_USER_ID_LIST,
}


@dataclass()
class ColumnConfig:
    """
    A config for a table column (attribute).
    Properties:
        name (str): The name of the column
        datatype (ColumnDatatype | None): The data type of the column
        required: (bool | None): If the columne is required
        index: (bool | None): If the column should be indexed
        string_size_max: (int | None) Max size for string columns if used
        list_length_max: (int | None) Max size for list column ength if used


    """

    name: str
    datatype: ColumnDatatype | None = None
    required: bool | None = None
    index: bool | None = None
    string_size_max: int | None = None
    list_length_max: int | None = None


class TableConfig:  # pylint: disable=too-few-public-methods
    """A config for database specific items for one table"""

    def __init__(
        self,
        name: str,
        primary_key: str | None = None,
        foreign_keys: list[dict[str, str]] | None = None,
        columns: list[dict[str, Any]] | None = None,
    ) -> None:
        """
        Init
        """
        self.name = name
        self.primary_key = primary_key
        if foreign_keys is None:
            self.foreign_keys = None
        else:
            self.foreign_keys = [
                ForeignKeySchema(
                    name=key["column_name"],
                    foreign_table_name=key["foreign_table_name"],
                    foreign_column_name=key["foreign_column_name"],
                )
                for key in foreign_keys
            ]
        if columns is None:
            self.columns = None
        else:
            self.columns = [
                ColumnConfig(
                    name=column["name"],
                    datatype=DATATYPES[column["datatype"]],
                    required=column["required"],
                    index=column["index"],
                )
                for column in columns
            ]


class DatabaseConfig:
    """A config for database specific items"""

    def __init__(self, tables: list[dict[str, Any]]) -> None:
        """
        Init
        """
        self.tables: list[TableConfig] = [TableConfig(**table) for table in tables]
        self._check_table_names()

    def get_primary_key(self, table_name: str) -> str | None:
        """Gets the primary key for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[str]: The primary key
        """
        table = self._get_table_by_name(table_name)
        return None if table is None else table.primary_key

    def get_foreign_keys(self, table_name: str) -> list[ForeignKeySchema] | None:
        """Gets the foreign keys for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[list[ForeignKeySchema]]: The foreign keys
        """
        table = self._get_table_by_name(table_name)
        return None if table is None else table.foreign_keys

    def get_columns(self, table_name: str) -> list[ColumnConfig] | None:
        """Gets the columns for an table

        Args:
            table_name (str): The name of the table

        Returns:
            list[ColumnConfig] | None: The list of columns or None if the table doesn't exist
        """
        table = self._get_table_by_name(table_name)
        return None if table is None else table.columns

    def get_column(self, table_name: str, column_name: str) -> ColumnConfig | None:
        """Gets a column for a table

        Args:
            table_name (str): The name of the table to get the column for
            column_name (str): The name of the column to get

        Returns:
            Optional[list[ColumnSchema]]: The list of columns
        """
        columns = self.get_columns(table_name)
        if columns is None:
            return None
        columns = [column for column in columns if column.name == column_name]
        if len(columns) == 0:
            return None
        return columns[0]

    def _get_table_by_name(self, table_name: str) -> Optional[TableConfig]:
        """Gets the config for the table if it exists

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[TableConfig]: The config for the table if it exists
        """
        tables = [table for table in self.tables if table.name == table_name]
        if len(tables) == 0:
            return None
        return tables[0]

    def _get_table_names(self) -> list[str]:
        """Gets the list of tables names in the config

        Returns:
            list[str]: A list of table names
        """
        return [table.name for table in self.tables]

    def _check_table_names(self) -> None:
        """Checks that the table names are not duplicated

        Raises:
            ValueError: Raised when there are duplicate table names
        """
        n_table_names = len(self._get_table_names())
        n_unique_names = len(list(set(self._get_table_names())))
        if n_table_names != n_unique_names:
            raise ValueError("There are duplicate table names")
