"""Schema class"""

from typing import Optional, Callable, Union
import warnings
import networkx
import pandas as pd
from schematic_db.db_config import (
    DBConfig,
    DBObjectConfig,
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)

from .api_utils import (
    get_graph_by_edge_type,
    find_class_specific_properties,
    get_property_label_from_display_name,
    get_project_manifests,
    get_manifest,
    ManifestSynapseConfig,
)


class NoAttributesWarning(Warning):
    """
    Occurs when a database object has no attributes returned from find_class_specific_properties().
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class ManifestMissingPrimaryKeyError(Exception):
    """Raised when a manifest is missing one or more primary keys"""

    def __init__(
        self,
        object_name: str,
        dataset_id: str,
        primary_keys: list[str],
        manifest_columns: list[str],
    ):
        self.message = "Manifest is missing one or more primary keys"
        self.object_name = object_name
        self.dataset_id = dataset_id
        self.primary_keys = primary_keys
        self.manifest_columns = manifest_columns
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; object name:{self.object_name}; "
            f"dataset_id:{self.dataset_id}; primary keys:{self.primary_keys}; "
            f"manifest columns:{self.manifest_columns}"
        )


def get_key_attribute(object_name: str) -> str:
    """
    Standard function for getting a key's name(primary or foreign) based on the objects name.
    The Schema class uses this function to get both primary and foreign keys by default.
    Users may want to use a different function.

    Args:
        object_name (str): The name of the object in the database

    Returns:
        str: The name of the key attribute for that database
    """
    return f"{object_name}_id"


def get_manifest_ids_for_object(
    object_name: str, manifests: list[ManifestSynapseConfig]
) -> list[str]:
    """Gets the manifest ids from a list of manifests matching the object name

    Args:
        object_name (str): The name of the object to get the manifests for
        manifests (list[ManifestSynapseConfig]): A list of manifests in Synapse

    Returns:
        list[str]: A list of synapse ids for the manifests
    """
    return [
        manifest.manifest_id
        for manifest in manifests
        if manifest.component_name == object_name and manifest.manifest_id != ""
    ]


def get_dataset_ids_for_object(
    object_name: str, manifests: list[ManifestSynapseConfig]
) -> list[str]:
    """Gets the dataset ids from a list of manifests matching the object name

    Args:
        object_name (str): The name of the object to get the manifests for
        manifests (list[ManifestSynapseConfig]): A list of manifests in Synapse

    Returns:
        list[str]: A list of synapse ids for the manifest datasets
    """
    return [
        manifest.dataset_id
        for manifest in manifests
        if manifest.component_name == object_name and manifest.manifest_id != ""
    ]


class Schema:
    """
    The Schema class interacts with the Schematic API to create a DBConfig
     object or to get a list of manifests for the schema.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        schema_url: str,
        synapse_project_id: str,
        synapse_asset_view_id: str,
        synapse_input_token: str,
        primary_key_getter: Callable[[str], str] = get_key_attribute,
        foreign_key_getter: Callable[[str], str] = get_key_attribute,
    ) -> None:
        """Init

        The Schema class has two optional arguments primary_key_getter, and foreign_key_getter.
        These are used to determine the names of the attributes that are primary and foreign keys.
        It is assumed that all objects(tables) have one primary key that can be systematically
        determined from the objects name, and that the primary_key_getter will do that.

        By default get_key_attribute is used for primary keys. This assumes that all primary keys
        are of the form "<object_name>_id". For example if the object was named "patient" then the
        primary key would be named "patient_id".

        Also by default get_key_attribute is used for foreign keys. This assumes that all foreign
        keys match the name of the the primary key they refer to.

        If foreign keys do not match the primary key they refer to then the functions would need
        to be different to reflect that.

        Args:
            schema_url (str): A url to the jsonld schema file
            synapse_project_id (str): The synapse id to the project where the manifests are stored.
            synapse_asset_view_id (str): The synapse id to the asset view that tracks the manifests.
            synapse_input_token (str): A synapse token with download permissions for both the
                synapse_project_id and synapse_asset_view_id
            primary_key_getter (Callable[[str], str], optional):
                Defaults to get_key_attribute.
            foreign_key_getter (Callable[[str], str], optional):
                Defaults to get_key_attribute.
        """
        self.schema_url = schema_url
        self.schema_graph = self.create_schema_graph()
        self.synapse_asset_view_id = synapse_asset_view_id
        self.synapse_input_token = synapse_input_token
        self.primary_key_getter = primary_key_getter
        self.foreign_key_getter = foreign_key_getter
        self.manifests = get_project_manifests(
            input_token=synapse_input_token,
            project_id=synapse_project_id,
            asset_view=synapse_asset_view_id,
        )

    def create_schema_graph(self) -> networkx.DiGraph:
        """Retrieve the edges from schematic API and store in networkx.DiGraph()

        Returns:
            networkx.DiGraph: The edges of the graph
        """
        subgraph = get_graph_by_edge_type(self.schema_url, "requiresComponent")
        schema_graph = networkx.DiGraph()
        schema_graph.add_edges_from(subgraph)
        return schema_graph

    def create_db_config(self) -> DBConfig:
        """Creates the configs for all objects in the database.

        Returns:
            DBObjectConfigList: Configs for all objects in the database.
        """
        # order objects so that ones with dependencies come after they depend on
        object_names = list(
            reversed(list(networkx.topological_sort(self.schema_graph)))
        )
        object_configs: list[Optional[DBObjectConfig]] = [
            self.create_db_object_config(name) for name in object_names
        ]
        filtered_object_configs: list[DBObjectConfig] = [
            config for config in object_configs if config is not None
        ]
        return DBConfig(filtered_object_configs)

    def create_db_object_config(self, object_name: str) -> Optional[DBObjectConfig]:
        """Creates the config for one object in the database.

        Args:
            object_name (str): The name of the object the config will be created for.
            manifests (list[dict[str:str]]): A list of manifests in dictionary form

        Returns:
            Optional[DBObjectConfig]: The config for the object if the object has attributes
              otherwise None.
        """
        # Some components will not have any attributes for various reasons
        attributes = self.create_attributes(object_name)
        if not attributes:
            return None
        primary_key = get_property_label_from_display_name(
            self.schema_url, self.primary_key_getter(object_name)
        )
        # primary keys don't always appear in the attributes endpoint
        if primary_key not in [att.name for att in attributes]:
            attributes.append(
                DBAttributeConfig(name=primary_key, datatype=DBDatatype.TEXT)
            )
        # foreign keys don't always appear in the attributes endpoint
        foreign_keys = self.create_foreign_keys(object_name)
        for key in foreign_keys:
            name = key.name
            if name not in [att.name for att in attributes]:
                attributes.append(
                    DBAttributeConfig(name=name, datatype=DBDatatype.TEXT)
                )
        return DBObjectConfig(
            name=object_name,
            attributes=attributes,
            primary_keys=[primary_key],
            foreign_keys=foreign_keys,
        )

    def create_attributes(
        self, object_name: str
    ) -> Union[list[DBAttributeConfig], None]:
        """Create the attributes for the object

        Args:
            object_name (str): The name of the object to create the attributes for

        Returns:
            Union[list[DBAttributeConfig], None]: A list of attributes in DBAttributeConfig form
        """
        attribute_names = find_class_specific_properties(self.schema_url, object_name)
        attributes = [
            DBAttributeConfig(name=name, datatype=DBDatatype.TEXT)
            for name in attribute_names
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

    def create_foreign_keys(self, object_name: str) -> list[DBForeignKey]:
        """Creates a list of foreign keys for an object in the database

        Args:
            object_name (str): The name of the object the config will be created for.

        Returns:
            list[DBForeignKey]: A list of foreign keys for the object.
        """
        neighbor_object_names = list(self.schema_graph.neighbors(object_name))
        foreign_keys = [self.create_foreign_key(name) for name in neighbor_object_names]
        return foreign_keys

    def create_foreign_key(self, foreign_object_name: str) -> DBForeignKey:
        """Creates a foreign key object

        Args:
            foreign_object_name (str): The name of the object the foreign key is referring to.

        Returns:
            DBForeignKey: A foreign key object.
        """
        attribute_name = get_property_label_from_display_name(
            self.schema_url, self.foreign_key_getter(foreign_object_name)
        )
        foreign_attribute_name = get_property_label_from_display_name(
            self.schema_url, self.primary_key_getter(foreign_object_name)
        )
        return DBForeignKey(
            attribute_name,
            foreign_object_name,
            foreign_attribute_name,
        )

    def get_manifests(self, config: DBObjectConfig) -> list[pd.DataFrame]:
        """Gets the manifests associated with a db object config

        Args:
            config (DBObjectConfig): The config for the database object

        Returns:
            list[pd.DataFrame]: A list manifests in dataframe form
        """
        dataset_ids = get_dataset_ids_for_object(config.name, self.manifests)
        manifests = [
            self.get_manifest(dataset_id, config) for dataset_id in dataset_ids
        ]
        return manifests

    def get_manifest(self, dataset_id: str, config: DBObjectConfig) -> pd.DataFrame:
        """Gets and formats a manifest

        Args:
            dataset_id (str): The Synapse id of the dataset the manifest belongs to
            config (DBObjectConfig): The config for the db object the manifest belongs to

        Raises:
            ManifestMissingPrimaryKeyError: Raised when the manifest is missing a primary key

        Returns:
            pd.DataFrame: The manifest in dataframe form
        """
        manifest = get_manifest(
            self.synapse_input_token,
            dataset_id,
            self.synapse_asset_view_id,
        )
        # create dict with columns names as keys and attribute names as values
        attribute_names: dict[str, str] = {
            col_name: self.get_attribute_name(col_name)
            for col_name in list(manifest.columns)
        }
        # filter dict for attribute names that appear in the config
        attribute_names = {
            col_name: att_name
            for (col_name, att_name) in attribute_names.items()
            if att_name in config.get_attribute_names()
        }
        # Raise error if all primary keys do not appear
        if not all(key in attribute_names.values() for key in config.primary_keys):
            raise ManifestMissingPrimaryKeyError(
                config.name,
                dataset_id,
                config.primary_keys,
                list(attribute_names.values()),
            )
        # select rename columns manifest and select those in the config
        manifest = manifest.rename(columns=attribute_names)[
            list(attribute_names.values())
        ]
        return manifest

    def get_attribute_name(self, column_name: str) -> str:
        """Gets the attribute name of a manifest column

        Args:
            column_name (str): The name of the column

        Returns:
            str: The attribute name of the column
        """
        return get_property_label_from_display_name(self.schema_url, column_name)
