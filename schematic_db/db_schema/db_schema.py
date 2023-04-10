"""DB schema
These are a set of classes for defining a database table in a dialect agnostic way.
"""
from enum import Enum
from typing import Any, Optional, TypeVar
from pydantic.dataclasses import dataclass
from pydantic import validator


class ColumnDatatype(Enum):
    """A generic datatype that should be supported by all database types."""

    TEXT = "text"
    DATE = "date"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"


# mypy types so that a class can refer to its own type
X = TypeVar("X", bound="ColumnSchema")
Y = TypeVar("Y", bound="TableSchema")
T = TypeVar("T", bound="DatabaseSchema")


@dataclass()
class ColumnSchema:
    """A schema for a table column (attribute)."""

    name: str
    datatype: ColumnDatatype
    required: bool = False
    index: bool = False

    @validator("name")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string is not empty(has at least one char)

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


@dataclass()
class ForeignKeySchema:
    """A foreign key in a database schema."""

    name: str
    foreign_table_name: str
    foreign_column_name: str

    @validator("name", "foreign_table_name", "foreign_column_name")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
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

    def get_column_dict(self) -> dict[str, str]:
        """Returns the foreign key in dict form

        Returns:
            dict[str, str]: A dictionary of the foreign key columns
        """
        return {
            "name": self.name,
            "foreign_table_name": self.foreign_table_name,
            "foreign_column_name": self.foreign_column_name,
        }


class TableColumnError(Exception):
    """TableColumnError"""

    def __init__(self, message: str, table_name: str) -> None:
        self.message = message
        self.table_name = table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}: {self.table_name}"


class TableKeyError(Exception):
    """TableKeyError"""

    def __init__(
        self, message: str, table_name: str, key: Optional[str] = None
    ) -> None:
        self.message = message
        self.table_name = table_name
        self.key = key
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}: {self.table_name}; {self.key}"


@dataclass
class TableSchema:
    """A schema for a database table."""

    name: str
    columns: list[ColumnSchema]
    primary_key: str
    foreign_keys: list[ForeignKeySchema]

    @validator("name", "primary_key")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
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

    def __post_init__(self) -> None:
        self.columns.sort(key=lambda x: x.name)
        self.foreign_keys.sort(key=lambda x: x.name)
        self._check_columns()
        self._check_primary_key()
        self._check_foreign_keys()

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        return self.get_sorted_columns() == other.get_sorted_columns()

    def get_sorted_columns(self) -> list[ColumnSchema]:
        """Gets the tables columns sorted by name

        Returns:
            list[ColumnSchema]: Sorted list of columns
        """
        return sorted(self.columns, key=lambda x: x.name)

    def get_column_names(self) -> list[str]:
        """Returns a list of names of the columns

        Returns:
            List[str]: A list of names of the attributes
        """
        return [column.name for column in self.columns]

    def get_foreign_key_dependencies(self) -> list[str]:
        """Returns a list of table names the current table depends on

        Returns:
            list[str]: A list of table names
        """
        return [key.foreign_table_name for key in self.foreign_keys]

    def get_foreign_key_names(self) -> list[str]:
        """Returns a list of names of the foreign keys

        Returns:
            List[str]: A list of names of the foreign keys
        """
        return [key.name for key in self.foreign_keys]

    def get_foreign_key_by_name(self, name: str) -> ForeignKeySchema:
        """Returns foreign key

        Args:
            name (str): name of the foreign key

        Returns:
            ForeignKeySchema: The foreign key asked for
        """
        return [key for key in self.foreign_keys if key.name == name][0]

    def get_column_by_name(self, name: str) -> ColumnSchema:
        """Returns the column

        Args:
            name (str): name of the column

        Returns:
            ColumnSchema: The ColumnSchema asked for
        """
        return [column for column in self.columns if column.name == name][0]

    def _check_columns(self) -> None:
        if len(self.columns) == 0:
            raise TableColumnError("There are no columns", self.name)
        if len(self.get_column_names()) != len(set(self.get_column_names())):
            raise TableColumnError("There are duplicate columns", self.name)

    def _check_primary_key(self) -> None:
        if self.primary_key not in self.get_column_names():
            raise TableKeyError(
                "Primary key is missing from columns", self.name, self.primary_key
            )

    def _check_foreign_keys(self) -> None:
        for key in self.foreign_keys:
            self._check_foreign_key(key)

    def _check_foreign_key(self, key: ForeignKeySchema) -> None:
        if key.name not in self.get_column_names():
            raise TableKeyError(
                "Foreign key is missing from columns", self.name, key.name
            )
        if key.foreign_table_name == self.name:
            raise TableKeyError(
                "Foreign key references its own table", self.name, key.name
            )


class SchemaMissingTableError(Exception):
    """When a foreign key references an table that doesn't exist"""

    def __init__(
        self, foreign_key: str, table_name: str, foreign_table_name: str
    ) -> None:
        self.message = "Foreign key references table which does not exist in schema."
        self.foreign_key = foreign_key
        self.table_name = table_name
        self.foreign_table_name = foreign_table_name
        super().__init__(self.message)

    def __str__(self) -> str:
        msg = (
            f"Foreign key '{self.foreign_key}' in table '{self.table_name}' references table "
            f"'{self.foreign_table_name}' which does not exist in schema."
        )
        return msg


class SchemaMissingColumnError(Exception):
    """When a foreign key references an table column the table doesn't have"""

    def __init__(
        self,
        foreign_key: str,
        table_name: str,
        foreign_table_name: str,
        foreign_table_column: str,
    ) -> None:
        self.message = "Foreign key references column which does not exist."
        self.foreign_key = foreign_key
        self.table_name = table_name
        self.foreign_table_name = foreign_table_name
        self.foreign_table_column = foreign_table_column
        super().__init__(self.message)

    def __str__(self) -> str:
        msg = (
            f"Foreign key '{self.foreign_key}' in table '{self.table_name}' references "
            f"column '{self.foreign_table_column}' which does not exist in table "
            f"'{self.foreign_table_name}'"
        )
        return msg


@dataclass
class DatabaseSchema:
    """A database agnostic schema"""

    table_schemas: list[TableSchema]

    def __post_init__(self) -> None:
        for schema in self.table_schemas:
            self._check_foreign_keys(schema)

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        return self.get_sorted_table_schemas() == other.get_sorted_table_schemas()

    def get_sorted_table_schemas(self) -> list[TableSchema]:
        """Gets the table schemas sorted by name

        Returns:
            list[TableSchema]: The list of sorted table schemas
        """
        return sorted(self.table_schemas, key=lambda x: x.name)

    def get_dependencies(self, table_name: str) -> list[str]:
        """Gets the tables dependencies

        Args:
            table_name (str): The name of the table

        Returns:
            list[str]: A list of tables names the table depends on
        """
        return self.get_schema_by_name(table_name).get_foreign_key_dependencies()

    def get_reverse_dependencies(self, table_name: str) -> list[str]:
        """Gets the names of the tables that depend on the input table

        Args:
            table_name (str): The name of the table

        Returns:
            list[str]: A list of table names that depend on the input table
        """
        return [
            schema.name
            for schema in self.table_schemas
            if table_name in schema.get_foreign_key_dependencies()
        ]

    def get_schema_names(self) -> list[str]:
        """Returns a list of names of the schemas

        Returns:
            List[str]: A list of names of the schemas
        """
        return [schema.name for schema in self.table_schemas]

    def get_schema_by_name(self, name: str) -> TableSchema:
        """Returns the schema

        Args:
            name (str): name of the schema

        Returns:
            TableSchema: The TableSchema asked for
        """
        return [schema for schema in self.table_schemas if schema.name == name][0]

    def _check_foreign_keys(self, schema: TableSchema) -> None:
        for key in schema.foreign_keys:
            self._check_foreign_key_table(schema, key)
            self._check_foreign_key_column(schema, key)

    def _check_foreign_key_table(
        self, schema: TableSchema, key: ForeignKeySchema
    ) -> None:
        if key.foreign_table_name not in self.get_schema_names():
            raise SchemaMissingTableError(
                foreign_key=key.name,
                table_name=schema.name,
                foreign_table_name=key.foreign_table_name,
            )

    def _check_foreign_key_column(
        self, schema: TableSchema, key: ForeignKeySchema
    ) -> None:
        foreign_schema = self.get_schema_by_name(key.foreign_table_name)
        if key.foreign_column_name not in foreign_schema.get_column_names():
            raise SchemaMissingColumnError(
                foreign_key=key.name,
                table_name=schema.name,
                foreign_table_name=key.foreign_table_name,
                foreign_table_column=key.foreign_column_name,
            )
