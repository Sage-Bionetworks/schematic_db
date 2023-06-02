"""
A config for database specific items
"""
from typing import Optional, Any
from deprecation import deprecated
from schematic_db.db_schema.db_schema import (
    ForeignKeySchema,
    ColumnSchema,
    ColumnDatatype,
)


DATATYPES = {
    "str": ColumnDatatype.TEXT,
    "float": ColumnDatatype.FLOAT,
    "int": ColumnDatatype.INT,
    "date": ColumnDatatype.DATE,
}


@deprecated(
    deprecated_in="0.0.27",
    details="Functionality will be accomplished with future Schematic API calls.",
)
class DatabaseTableConfig:  # pylint: disable=too-few-public-methods
    """A config for database specific items for one table"""

    def __init__(
        self,
        name: str,
        primary_key: Optional[str] = None,
        foreign_keys: Optional[list[dict[str, str]]] = None,
        columns: Optional[list[dict[str, Any]]] = None,
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
                ColumnSchema(
                    name=column["column_name"],
                    datatype=DATATYPES[column["datatype"]],
                    required=column["required"],
                    index=column["index"],
                )
                for column in columns
            ]

    def _check_column_names(self) -> None:
        """Checks that column names are not duplicated

        Raises:
            ValueError: Raised when there are duplicate column names
        """
        column_names = self._get_column_names()
        if column_names is not None:
            if len(column_names) != len(list(set(column_names))):
                raise ValueError("There are duplicate column names")

    def _get_column_names(self) -> Optional[list[str]]:
        """Gets the list of column names in the config

        Returns:
            list[str]: A list of column names
        """
        if self.columns is not None:
            return [column.name for column in self.columns]
        return None

    def _check_foreign_key_name(self) -> None:
        """Checks that foreign keys are not duplicated

        Raises:
            ValueError: Raised when there are duplicate foreign keys
        """
        foreign_keys_names = self._get_foreign_key_names()
        if foreign_keys_names is not None:
            if len(foreign_keys_names) != len(list(set(foreign_keys_names))):
                raise ValueError("There are duplicate column names")

    def _get_foreign_key_names(self) -> Optional[list[str]]:
        """Gets the list of foreign key names in the config

        Returns:
            list[str]: A list of foreign key names
        """
        if self.foreign_keys is not None:
            return [key.name for key in self.foreign_keys]
        return None


class DatabaseConfig:
    """A config for database specific items"""

    def __init__(self, tables: list[dict[str, Any]]) -> None:
        """
        Init
        """
        self.tables: list[DatabaseTableConfig] = [
            DatabaseTableConfig(**table) for table in tables
        ]
        self._check_table_names()

    def get_primary_key(self, table_name: str) -> Optional[str]:
        """Gets the primary key for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[str]: The primary key
        """
        table = self._get_table_by_name(table_name)
        return None if table is None else table.primary_key

    def get_foreign_keys(self, table_name: str) -> Optional[list[ForeignKeySchema]]:
        """Gets the foreign keys for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[list[ForeignKeySchema]]: The foreign keys
        """
        table = self._get_table_by_name(table_name)
        return None if table is None else table.foreign_keys

    def get_columns(self, table_name: str) -> Optional[list[ColumnSchema]]:
        """Gets the columns for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[list[ColumnSchema]]: The list of columns
        """
        table = self._get_table_by_name(table_name)
        return None if table is None else table.columns

    def get_column(self, table_name: str, column_name: str) -> Optional[ColumnSchema]:
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

    def _get_table_by_name(self, table_name: str) -> Optional[DatabaseTableConfig]:
        """Gets the config for the table if it exists

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[DatabaseTableConfig]: The config for the table if it exists
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
