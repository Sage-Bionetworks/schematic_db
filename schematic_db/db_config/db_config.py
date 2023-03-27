"""DB config
These are a set of classes for defining a database table in a dialect agnostic way.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, TypeVar

from sqlalchemy import ForeignKey


class DBDatatype(Enum):
    """A generic datatype that should be supported by all database types."""

    TEXT = "text"
    DATE = "date"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"


# mypy types so that a class can refer to its own type
X = TypeVar("X", bound="DBAttributeConfig")
Y = TypeVar("Y", bound="DBObjectConfig")
T = TypeVar("T", bound="DBConfig")


@dataclass
class DBAttributeConfig:
    """A config for a table attribute(column)."""

    name: str
    datatype: DBDatatype
    required: bool = False
    index: bool = False

    def is_equivalent(self, other: X) -> bool:
        """Use instead of == when determining if schema's are equivalent

        Args:
            other (DBAttributeConfig): Another DBAttributeConfig to compare to self

        Returns:
            bool: True if both DBAttributeConfigs are equivalent
        """

        return all(
            [
                self.name == other.name,
                self.datatype == other.datatype,
                self.required == other.required,
            ]
        )


@dataclass
class DBForeignKey:
    """A foreign key in a database object attribute."""

    name: str
    foreign_object_name: str
    foreign_attribute_name: str

    def get_attribute_dict(self) -> dict[str, str]:
        """Returns the foreign key in dict form

        Returns:
            dict[str, str]: A dictionary of the foreign key attributes
        """
        return {
            "name": self.name,
            "foreign_object_name": self.foreign_object_name,
            "foreign_attribute_name": self.foreign_attribute_name,
        }


class ConfigAttributeError(Exception):
    """ConfigAttributeError"""

    def __init__(self, message: str, object_name: str) -> None:
        self.message = message
        self.object_name = object_name
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message}: {self.object_name}"


class ConfigKeyError(Exception):
    """ConfigKeyError"""

    def __init__(
        self, message: str, object_name: str, key: Optional[str] = None
    ) -> None:
        self.message = message
        self.object_name = object_name
        self.key = key
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.key is None:
            return f"{self.message}: {self.object_name}"
        return f"{self.message}: {self.object_name}; {self.key}"


@dataclass
class DBObjectConfig:
    """A config for a generic database object."""

    name: str
    attributes: list[DBAttributeConfig]
    primary_key: str
    foreign_keys: list[DBForeignKey]

    def __post_init__(self) -> None:
        self.attributes.sort(key=lambda x: x.name)
        self.foreign_keys.sort(key=lambda x: x.name)
        self._check_attributes()
        self._check_primary_key()
        self._check_foreign_keys()

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        return self.get_sorted_attributes() == other.get_sorted_attributes()

    def is_equivalent(self, other: Y) -> bool:
        """
        Use instead of == when determining if schema's are equivalent
        Args:
            other (DBObjectConfig): Another instance of DBObjectConfig

        Returns:
            bool
        """
        attributes_equivalent = all(
            x.is_equivalent(y)
            for x, y in zip(
                self.get_sorted_attributes(),
                other.get_sorted_attributes(),
            )
        )

        return all(
            [
                attributes_equivalent,
                self.name == other.name,
                self.primary_key == other.primary_key,
                self.foreign_keys == other.foreign_keys,
            ]
        )

    def get_sorted_attributes(self) -> list[DBAttributeConfig]:
        """Gets the configs attributes sorted by name

        Returns:
            list[DBAttributeConfig]: Sorted list of attributes
        """
        return sorted(self.attributes, key=lambda x: x.name)

    def get_attribute_names(self) -> list[str]:
        """Returns a list of names of the attributes

        Returns:
            List[str]: A list of names of the attributes
        """
        return [att.name for att in self.attributes]

    def get_foreign_key_dependencies(self) -> list[str]:
        """Returns a list of object names the current object depends on

        Returns:
            list[str]: A list of object names
        """
        return [key.foreign_object_name for key in self.foreign_keys]

    def get_foreign_key_names(self) -> list[str]:
        """Returns a list of names of the foreign keys

        Returns:
            List[str]: A list of names of the foreign keys
        """
        return [key.name for key in self.foreign_keys]

    def get_foreign_key_by_name(self, name: str) -> DBForeignKey:
        """Returns foreign key

        Args:
            name (str): name of the foreign key

        Returns:
            DBForeignKey: The foreign key asked for
        """
        return [key for key in self.foreign_keys if key.name == name][0]

    def get_attribute_by_name(self, name: str) -> DBAttributeConfig:
        """Returns the attribute

        Args:
            name (str): name of the attribute

        Returns:
            DBAttributeConfig: The DBAttributeConfig asked for
        """
        return [att for att in self.attributes if att.name == name][0]

    def _check_attributes(self) -> None:
        if len(self.attributes) == 0:
            raise ConfigAttributeError("Attributes is empty", self.name)
        if len(self.get_attribute_names()) != len(set(self.get_attribute_names())):
            raise ConfigAttributeError("Attributes has duplicates", self.name)

    def _check_primary_key(self) -> None:
        if self.primary_key not in self.get_attribute_names():
            raise ConfigKeyError(
                "Primary key is missing from attributes", self.name, self.primary_key
            )

    def _check_foreign_keys(self) -> None:
        for key in self.foreign_keys:
            self._check_foreign_key(key)

    def _check_foreign_key(self, key: ForeignKey) -> None:
        if key.name not in self.get_attribute_names():
            raise ConfigKeyError(
                "Foreign key is missing from attributes", self.name, key
            )
        if key.foreign_object_name == self.name:
            raise ConfigKeyError(
                "Foreign key references its own object", self.name, key
            )


class ConfigForeignKeyMissingObjectError(Exception):
    """When a foreign key references an object that doesn't exist"""

    def __init__(
        self, foreign_key: str, object_name: str, foreign_object_name: str
    ) -> None:
        self.message = "Foreign key references object which does not exist in config."
        self.foreign_key = foreign_key
        self.object_name = object_name
        self.foreign_object_name = foreign_object_name
        super().__init__(self.message)

    def __str__(self) -> str:
        msg = (
            f"Foreign key '{self.foreign_key}' in object '{self.object_name}' references object"
            f"'{self.foreign_object_name}' which does not exist in config."
        )
        return msg


class ConfigForeignKeyMissingAttributeError(Exception):
    """When a foreign key references an object attribute the object doesn't have"""

    def __init__(
        self,
        foreign_key: str,
        object_name: str,
        foreign_object_name: str,
        foreign_object_attribute: str,
    ) -> None:
        self.message = "Foreign key references attribute which does not exist."
        self.foreign_key = foreign_key
        self.object_name = object_name
        self.foreign_object_name = foreign_object_name
        self.foreign_object_attribute = foreign_object_attribute
        super().__init__(self.message)

    def __str__(self) -> str:
        msg = (
            f"Foreign key '{self.foreign_key}' in object '{self.object_name}' references"
            f"attribute '{self.foreign_object_attribute}' which does not exist in object"
            f"'{self.foreign_object_name}'"
        )
        return msg


@dataclass
class DBConfig:
    """A group of configs for generic database tables."""

    configs: list[DBObjectConfig]

    def __post_init__(self) -> None:
        for config in self.configs:
            self._check_foreign_keys(config)

    def __eq__(self, other: Any) -> bool:
        """Overrides the default implementation"""
        return self.get_sorted_configs() == other.get_sorted_configs()

    def is_equivalent(self, other: T) -> bool:
        """
        Use instead of == when determining if schema's are equivalent
        Args:
            other (DBConfig): Another instance of DBConfig

        Returns:
            bool
        """
        return all(
            x.is_equivalent(y)
            for x, y in zip(
                self.get_sorted_configs(),
                other.get_sorted_configs(),
            )
        )

    def get_sorted_configs(self) -> list[DBObjectConfig]:
        """Gets the the configs sorted by name

        Returns:
            list[DBObjectConfig]: The list of sorted configs
        """
        return sorted(self.configs, key=lambda x: x.name)

    def get_dependencies(self, object_name: str) -> list[str]:
        """Gets the objects dependencies

        Args:
            object_name (str): The name of the object

        Returns:
            list[str]: A list of objects names the object depends on
        """
        return self.get_config_by_name(object_name).get_foreign_key_dependencies()

    def get_reverse_dependencies(self, object_name: str) -> list[str]:
        """Gets the names of Objects that depend on the input object

        Args:
            object_name (str): The name of the object

        Returns:
            list[str]: A list of object names that depend on the input object
        """
        return [
            config.name
            for config in self.configs
            if object_name in config.get_foreign_key_dependencies()
        ]

    def get_config_names(self) -> list[str]:
        """Returns a list of names of the configs

        Returns:
            List[str]: A list of names of the configs
        """
        return [config.name for config in self.configs]

    def get_config_by_name(self, name: str) -> DBObjectConfig:
        """Returns the config

        Args:
            name (str): name of the config

        Returns:
            DBObjectConfig: The DBObjectConfig asked for
        """
        return [config for config in self.configs if config.name == name][0]

    def _check_foreign_keys(self, config: DBObjectConfig) -> None:
        for key in config.foreign_keys:
            self._check_foreign_key_object(config, key)
            self._check_foreign_key_attribute(config, key)

    def _check_foreign_key_object(
        self, config: DBObjectConfig, key: ForeignKey
    ) -> None:
        if key.foreign_object_name not in self.get_config_names():
            raise ConfigForeignKeyMissingObjectError(
                foreign_key=key,
                object_name=config.name,
                foreign_object_name=key.foreign_object_name,
            )

    def _check_foreign_key_attribute(
        self, config: DBObjectConfig, key: ForeignKey
    ) -> None:
        foreign_config = self.get_config_by_name(key.foreign_object_name)
        if key.foreign_attribute_name not in foreign_config.get_attribute_names():
            raise ConfigForeignKeyMissingAttributeError(
                foreign_key=key,
                object_name=config.name,
                foreign_object_name=key.foreign_object_name,
                foreign_object_attribute=key.foreign_attribute_name,
            )
