"""Schema class"""

from typing import Optional
from dataclasses import dataclass
import warnings

from schematic_db.db_config.db_config import (
    DBConfig,
    DBObjectConfig,
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)

from schematic_db.api_utils.api_utils import (
    find_class_specific_properties,
    get_property_label_from_display_name,
    is_node_required,
    get_node_validation_rules,
)

from schematic_db.schema_graph.schema_graph import SchemaGraph

from .database_config import DatabaseConfig


SCHEMATIC_TYPE_DATATYPES = {
    "str": DBDatatype.TEXT,
    "float": DBDatatype.FLOAT,
    "num": DBDatatype.FLOAT,
    "int": DBDatatype.INT,
    "date": DBDatatype.DATE,
}


class NoAttributesWarning(Warning):
    """
    Occurs when a database object has no attributes returned from find_class_specific_properties().
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class MoreThanOneTypeRule(Exception):
    """Raised when an attribute has more than one validation type rule"""

    def __init__(
        self,
        attribute_name: str,
        type_rules: list[str],
    ):
        self.message = "Attribute has more than one validation type rule"
        self.attribute_name = attribute_name
        self.type_rules = type_rules
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; attribute name:{self.attribute_name}; "
            f"type_rules:{self.type_rules}"
        )


@dataclass
class SchemaConfig:
    """
    A config for a Schema.
    Properties:
        schema_url (str): A url to the jsonld schema file
        synapse_project_id (str): The synapse id to the project where the manifests are stored.
        synapse_asset_view_id (str): The synapse id to the asset view that tracks the manifests.
        synapse_input_token (str): A synapse token with download permissions for both the
         synapse_project_id and synapse_asset_view_id
    """

    schema_url: str
    synapse_project_id: str
    synapse_asset_view_id: str
    synapse_input_token: str


class Schema:  # pylint: disable=too-many-instance-attributes
    """
    The Schema class interacts with the Schematic API to create a DBConfig
     object or to get a list of manifests for the schema.
    """

    def __init__(
        self,
        config: SchemaConfig,
        database_config: DatabaseConfig = DatabaseConfig([]),
        use_display_names_as_labels: bool = False,
    ) -> None:
        """
        The Schema class handles interactions with the schematic API.
        The main responsibilities are creating the database schema, and retrieving manifests.

        Args:
            config (SchemaConfig): A config describing the basic inputs for the schema object
            database_config (DatabaseConfig): Experimental and will be deprecated in the near
             future. A config describing optional database specific attributes.
            use_display_names_as_labels(bool): Experimental and will be deprecated in the near
             future. Use when display names and labels are the same in the schema.
        """
        self.database_config = database_config
        self.schema_url = config.schema_url
        self.synapse_project_id = config.synapse_project_id
        self.synapse_asset_view_id = config.synapse_asset_view_id
        self.synapse_input_token = config.synapse_input_token
        self.use_display_names_as_labels = use_display_names_as_labels
        self.schema_graph = SchemaGraph(config.schema_url)
        self.update_db_config()

    def get_db_config(self) -> DBConfig:
        "Gets the currents objects DBConfig"
        return self.db_config

    def update_db_config(self) -> None:
        """Updates the DBConfig object."""
        object_names = self.schema_graph.create_sorted_object_name_list()
        object_configs = [
            config
            for config in [self.create_db_object_config(name) for name in object_names]
            if config is not None
        ]
        self.db_config = DBConfig(object_configs)

    def create_db_object_config(self, object_name: str) -> Optional[DBObjectConfig]:
        """Creates the config for one object in the database.

        Args:
            object_name (str): The name of the object the config will be created for.

        Returns:
            Optional[DBObjectConfig]: The config for the object if the object has attributes
              otherwise None.
        """
        # Some components will not have any attributes for various reasons
        attributes = self.create_attributes(object_name)
        if not attributes:
            return None

        return DBObjectConfig(
            name=object_name,
            attributes=attributes,
            primary_key=self.get_primary_key(object_name),
            foreign_keys=self.get_foreign_keys(object_name),
        )

    def create_attributes(
        self,
        object_name: str,
    ) -> Optional[list[DBAttributeConfig]]:
        """Create the attributes for the object

        Args:
            object_name (str): The name of the object to create the attributes for

        Returns:
            Optional[list[DBAttributeConfig]]: A list of attributes in DBAttributeConfig form
        """
        # the names of the attributes to be created, in label(not display) form
        attribute_names = find_class_specific_properties(self.schema_url, object_name)
        attributes = [
            self.create_attribute(name, object_name) for name in attribute_names
        ]
        # Some components will not have any attributes for various reasons
        if not attributes:
            warnings.warn(
                NoAttributesWarning(
                    f"Object {object_name} has no attributes, and will be skipped."
                )
            )
            return None
        return attributes

    def create_attribute(
        self, attribute_name: str, object_name: str
    ) -> DBAttributeConfig:
        """Creates an attribute

        Args:
            attribute_name (str): The name of the attribute
            object_name (str): The name of the object to create the attributes for

        Returns:
            DBAttributeConfig: The DBAttributeConfig for the attribute
        """
        attribute = self.database_config.get_attribute(object_name, attribute_name)
        # Use attribute config if provided
        if attribute is not None:
            return attribute
        # Create attribute config if not provided
        return DBAttributeConfig(
            name=attribute_name,
            datatype=self.get_attribute_datatype(attribute_name),
            required=is_node_required(self.schema_url, attribute_name),
            index=False,
        )

    def get_attribute_datatype(self, attribute_name: str) -> DBDatatype:
        """Gets the datatype for the attribute

        Args:
            attribute_name (str): The name of the attribute

        Raises:
            MoreThanOneTypeRule: Raised when the Schematic API returns more than one rule that
             indicate the attributes datatype

        Returns:
            DBDatatype: The attributes datatype
        """
        # Use schematic API to get validation rules
        rules = get_node_validation_rules(self.schema_url, attribute_name)
        # Filter for rules that indicate the datatype
        type_rules = [rule for rule in rules if rule in SCHEMATIC_TYPE_DATATYPES]
        # Raise error if there is more than one type of validation type rule
        if len(type_rules) > 1:
            raise MoreThanOneTypeRule(attribute_name, type_rules)
        if len(type_rules) == 1:
            return SCHEMATIC_TYPE_DATATYPES[type_rules[0]]
        # Use text if there are no validation type rules
        return DBDatatype.TEXT

    def get_primary_key(self, object_name: str) -> str:
        """Get the primary key for the attribute

        Args:
            object_name (str): The name of the attribute

        Returns:
            str: The primary key of the attribute
        """
        # Attempt to get the primary key from the config
        primary_key_attempt = self.database_config.get_primary_key(object_name)
        # Check if the primary key is in the config, otherwise assume "id"
        if primary_key_attempt is None:
            return "id"

        return primary_key_attempt

    def get_foreign_keys(self, object_name: str) -> list[DBForeignKey]:
        """Gets a list of foreign keys for an object in the database

        Args:
            object_name (str): The name of the object the config will be created for.

        Returns:
            list[DBForeignKey]: A list of foreign keys for the object.
        """
        # Attempt to get foreign keys from config
        foreign_keys_attempt = self.database_config.get_foreign_keys(object_name)
        # If there are no foreign keys in config use schema graph to create foreign keys
        if foreign_keys_attempt is None:
            return self.create_foreign_keys(object_name)

        return foreign_keys_attempt

    def create_foreign_keys(self, object_name: str) -> list[DBForeignKey]:
        """Create a list of foreign keys an object in the database using the schema graph

        Args:
            object_name (str): The name of the object

        Returns:
            list[DBForeignKey]: A list of foreign
        """
        # Uses the schema graph to find objects the current object depends on
        parent_object_names = self.schema_graph.get_neighbors(object_name)
        # Each parent of the current object needs a foreign key to that parent
        return [self.create_foreign_key(name) for name in parent_object_names]

    def create_foreign_key(self, foreign_object_name: str) -> DBForeignKey:
        """Creates a foreign key object

        Args:
            foreign_object_name (str): The name of the object the foreign key is referring to.

        Returns:
            DBForeignKey: A foreign key object.
        """
        # Assume the foreign key name is <object_name>_id where the object name is the
        #  name of the object the attribute the foreign key is in
        attribute_name = self.get_attribute_name(f"{foreign_object_name}_id")

        attempt = self.database_config.get_primary_key(foreign_object_name)
        foreign_attribute_name = "id" if attempt is None else attempt

        return DBForeignKey(attribute_name, foreign_object_name, foreign_attribute_name)

    def get_attribute_name(self, column_name: str) -> str:
        """Gets the attribute name of a manifest column

        Args:
            column_name (str): The name of the column

        Returns:
            str: The attribute name of the column
        """
        if self.use_display_names_as_labels:
            return column_name
        return get_property_label_from_display_name(self.schema_url, column_name)
