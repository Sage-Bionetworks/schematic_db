"""The APIManifestStore class interacts with the Schematic API download manifests."""
# pylint: disable=duplicate-code

from typing import Optional
import pandas
from schematic_db.api_utils.api_utils import (
    get_project_manifests,
    download_manifest,
    ManifestMetadataList,
)
from schematic_db.schema_graph.schema_graph import SchemaGraph
from .manifest_store import ManifestStore, ManifestStoreConfig


class ManifestMissingPrimaryKeyError(Exception):
    """Raised when a manifest is missing its primary key"""

    def __init__(
        self,
        table_name: str,
        dataset_id: str,
        primary_key: str,
        manifest_columns: list[str],
    ):
        """
        Args:
            table_name (str): The name of the table
            dataset_id (str): The dataset id for the component
            primary_key (str): The name of the primary key
            manifest_columns (list[str]): The columns in the manifest
        """
        self.message = "Manifest is missing its primary key"
        self.table_name = table_name
        self.dataset_id = dataset_id
        self.primary_key = primary_key
        self.manifest_columns = manifest_columns
        super().__init__(self.message)

    def __str__(self) -> str:
        """String representation"""
        return (
            f"{self.message}; table name:{self.table_name}; "
            f"dataset_id:{self.dataset_id}; primary keys:{self.primary_key}; "
            f"manifest columns:{self.manifest_columns}"
        )


class APIManifestStore(ManifestStore):
    """
    The APIManifestStore class interacts with the Schematic API download manifests.
    """

    def __init__(self, config: ManifestStoreConfig) -> None:
        """
        The Schema class handles interactions with the schematic API.
        The main responsibilities are creating the database schema, and retrieving manifests.

        Args:
            config (SchemaConfig): A config describing the basic inputs for the schema object
        """
        self.synapse_project_id = config.synapse_project_id
        self.synapse_asset_view_id = config.synapse_asset_view_id
        self.synapse_auth_token = config.synapse_auth_token
        self.schema_graph = SchemaGraph(config.schema_url)
        self.manifest_metadata: Optional[ManifestMetadataList] = None

    def create_sorted_table_name_list(self) -> list[str]:
        """
        Uses the schema graph to create a table name list such tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of tables names
        """
        return self.schema_graph.create_sorted_table_name_list()

    def get_manifest_metadata(self) -> ManifestMetadataList:
        """Gets the manifest metadata

        Returns:
            ManifestMetadataList: the manifest metadata
        """
        # When first initialized, manifest metadata is None
        if self.manifest_metadata is None:
            self.manifest_metadata = get_project_manifests(
                access_token=self.synapse_auth_token,
                project_id=self.synapse_project_id,
                asset_view=self.synapse_asset_view_id,
            )
        assert self.manifest_metadata is not None
        return self.manifest_metadata

    def get_manifest_ids(self, name: str) -> list[str]:
        """Gets the manifest ids for a table(component)

        Args:
            name (str): The name of the table

        Returns:
            list[str]: The manifest ids for the table
        """
        return self.get_manifest_metadata().get_manifest_ids_for_component(name)

    def download_manifest(self, manifest_id: str) -> pandas.DataFrame:
        """Downloads the manifest

        Args:
            manifest_id (str): The synapse id of the manifest

        Returns:
            pandas.DataFrame: The manifest in dataframe form
        """
        manifest = download_manifest(self.synapse_auth_token, manifest_id)
        return manifest
