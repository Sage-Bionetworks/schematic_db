"""
A config for database specific items
"""
from typing import Optional, Any
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


class DatabaseObjectConfig:  # pylint: disable=too-few-public-methods
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


class DatabaseConfig:
    """A config for database specific items"""

    def __init__(self, tables: list[dict[str, Any]]) -> None:
        """
        Init
        """
        self.tables: list[DatabaseObjectConfig] = [
            DatabaseObjectConfig(**obj) for obj in tables
        ]

    def get_primary_key(self, table_name: str) -> Optional[str]:
        """Gets the primary key for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[str]: The primary key
        """
        obj = self._get_table_by_name(table_name)
        return None if obj is None else obj.primary_key

    def get_foreign_keys(self, table_name: str) -> Optional[list[ForeignKeySchema]]:
        """Gets the foreign keys for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[list[ForeignKeySchema]]: The foreign keys
        """
        obj = self._get_table_by_name(table_name)
        return None if obj is None else obj.foreign_keys

    def get_columns(self, table_name: str) -> Optional[list[ColumnSchema]]:
        """Gets the columns for an table

        Args:
            table_name (str): The name of the table

        Returns:
            Optional[list[ColumnSchema]]: The list of columns
        """
        obj = self._get_table_by_name(table_name)
        return None if obj is None else obj.columns

    def get_column(self, table_name: str, column_name: str) -> Optional[ColumnSchema]:
        """Gets the columns for an table

        Args:
            table_name (str): The name of the table

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

    def _get_table_by_name(self, table_name: str) -> Optional[DatabaseObjectConfig]:
        tables = [obj for obj in self.tables if obj.name == table_name]
        if len(tables) == 0:
            return None
        return tables[0]
