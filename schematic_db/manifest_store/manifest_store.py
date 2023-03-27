"""The ManifestStore class interacts with the Schematic API download manifests."""

from dataclasses import dataclass
import pandas as pd
from schematic_db.api_utils.api_utils import (
    get_project_manifests,
    get_manifest,
    ManifestSynapseConfig,
)

from schematic_db.schema_graph.schema_graph import SchemaGraph


class ManifestMissingPrimaryKeyError(Exception):
    """Raised when a manifest is missing its primary key"""

    def __init__(
        self,
        object_name: str,
        dataset_id: str,
        primary_key: str,
        manifest_columns: list[str],
    ):
        self.message = "Manifest is missing its primary key"
        self.object_name = object_name
        self.dataset_id = dataset_id
        self.primary_key = primary_key
        self.manifest_columns = manifest_columns
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; object name:{self.object_name}; "
            f"dataset_id:{self.dataset_id}; primary keys:{self.primary_key}; "
            f"manifest columns:{self.manifest_columns}"
        )


def get_dataset_ids_for_component(
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


@dataclass
class ManifestStoreConfig:
    """
    A config for a ManifestStore.
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


class ManifestStore:  # pylint: disable=too-many-instance-attributes
    """
    The ManifestStore class interacts with the Schematic API download manifests.
    """

    def __init__(
        self,
        config: ManifestStoreConfig,
    ) -> None:
        """
        The Schema class handles interactions with the schematic API.
        The main responsibilities are creating the database schema, and retrieving manifests.

        Args:
            config (SchemaConfig): A config describing the basic inputs for the schema object
        """
        self.schema_url = config.schema_url
        self.synapse_project_id = config.synapse_project_id
        self.synapse_asset_view_id = config.synapse_asset_view_id
        self.synapse_input_token = config.synapse_input_token
        self.schema_graph = SchemaGraph(config.schema_url)
        self.update_manifest_configs()

    def create_sorted_object_name_list(self) -> list[str]:
        """
        Uses the schema graph to create a object name list such objects always come after ones they
         depend on.
        This order is how objects in a database should be built and/or updated.

        Returns:
            list[str]: A list of objects names
        """
        return self.schema_graph.create_sorted_object_name_list()

    def update_manifest_configs(self) -> None:
        """Updates the current objects manifest_configs."""
        self.manifest_configs = get_project_manifests(
            input_token=self.synapse_input_token,
            project_id=self.synapse_project_id,
            asset_view=self.synapse_asset_view_id,
        )

    def get_manifest_configs(self) -> list[ManifestSynapseConfig]:
        """Gets the currents objects manifest_configs"""
        return self.manifest_configs

    def get_manifests(self, name: str) -> list[pd.DataFrame]:
        """Gets the manifests associated with a component

        Args:
            name (str): The name of a component in the schema

        Returns:
            list[pd.DataFrame]: A list of manifests in dataframe form for the component
        """
        dataset_ids = get_dataset_ids_for_component(name, self.manifest_configs)
        manifests = [
            get_manifest(
                self.synapse_input_token,
                dataset_id,
                self.synapse_asset_view_id,
            )
            for dataset_id in dataset_ids
        ]
        return manifests
