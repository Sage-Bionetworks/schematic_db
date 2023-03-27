"""
A config for database specific items
"""
from typing import Optional, Any
from schematic_db.db_config.db_config import DBForeignKey, DBAttributeConfig, DBDatatype


DATATYPES = {
    "str": DBDatatype.TEXT,
    "float": DBDatatype.FLOAT,
    "int": DBDatatype.INT,
    "date": DBDatatype.DATE,
}


class DatabaseObjectConfig:  # pylint: disable=too-few-public-methods
    """A config for database specific items for one object"""

    def __init__(
        self,
        name: str,
        primary_key: Optional[str] = None,
        foreign_keys: Optional[list[dict[str, str]]] = None,
        attributes: Optional[list[dict[str, Any]]] = None,
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
                DBForeignKey(
                    name=key["attribute_name"],
                    foreign_object_name=key["foreign_object_name"],
                    foreign_attribute_name=key["foreign_attribute_name"],
                )
                for key in foreign_keys
            ]
        if attributes is None:
            self.attributes = None
        else:
            self.attributes = [
                DBAttributeConfig(
                    name=attribute["attribute_name"],
                    datatype=DATATYPES[attribute["datatype"]],
                    required=attribute["required"],
                    index=attribute["index"],
                )
                for attribute in attributes
            ]


class DatabaseConfig:
    """A config for database specific items"""

    def __init__(self, objects: list[dict[str, Any]]) -> None:
        """
        Init
        """
        self.objects: list[DatabaseObjectConfig] = [
            DatabaseObjectConfig(**obj) for obj in objects
        ]

    def get_primary_key(self, object_name: str) -> Optional[str]:
        """Gets the primary key for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[str]: The primary key
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.primary_key

    def get_foreign_keys(self, object_name: str) -> Optional[list[DBForeignKey]]:
        """Gets the foreign keys for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[DBForeignKey]]: The foreign keys
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.foreign_keys

    def get_attributes(self, object_name: str) -> Optional[list[DBAttributeConfig]]:
        """Gets the attributes for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[DBAttributeConfig]]: The list of attributes
        """
        obj = self._get_object_by_name(object_name)
        return None if obj is None else obj.attributes

    def get_attribute(
        self, object_name: str, attribute_name: str
    ) -> Optional[DBAttributeConfig]:
        """Gets the attributes for an object

        Args:
            object_name (str): The name of the object

        Returns:
            Optional[list[DBAttributeConfig]]: The list of attributes
        """
        attributes = self.get_attributes(object_name)
        if attributes is None:
            return None
        attributes = [
            attribute for attribute in attributes if attribute.name == attribute_name
        ]
        if len(attributes) == 0:
            return None
        return attributes[0]

    def _get_object_by_name(self, object_name: str) -> Optional[DatabaseObjectConfig]:
        objects = [obj for obj in self.objects if obj.name == object_name]
        if len(objects) == 0:
            return None
        return objects[0]
