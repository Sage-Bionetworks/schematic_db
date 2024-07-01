"""
SynapseManifestStore implements the ManifestStore interface.
It interacts with Synapse through the Python client.
This is used to interact with manifests
"""

import pandas
from schematic_db.schema_graph.schema_graph import SchemaGraph
from schematic_db.utils.api_utils import ManifestMetadataList
from schematic_db.synapse.synapse import Synapse
from schematic_db.utils.types import DisplayLabelType
from .manifest_store import ManifestStore, ManifestStoreConfig


class SynapseManifestStore(ManifestStore):
    """An interface for interacting with manifests"""

    def __init__(
        self,
        config: ManifestStoreConfig,
        display_label_type: DisplayLabelType = "class_label",
        purge_synapse_cache: bool = False,
    ) -> None:
        """
        Args:
            config (ManifestStoreConfig): A config with setup values
            display_model_type (DisplayLabelType): The type of label used for display purposes
            purge_synapse_cache (bool): If True, the synapse cache is purged after downloading a csv
        """
        self.synapse_asset_view_id = config.synapse_asset_view_id
        self.synapse = Synapse(config.synapse_auth_token, config.synapse_project_id)
        self.schema_graph = SchemaGraph(config.schema_url, display_label_type)
        self.manifest_metadata: ManifestMetadataList | None = None
        self.purge_synapse_cache = purge_synapse_cache

    def create_sorted_table_name_list(self) -> list[str]:
        """
        Creates a table name list such tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of tables names
        """
        return self.schema_graph.create_sorted_table_name_list()

    def get_manifest_metadata(self) -> ManifestMetadataList:
        """Gets the current objects manifest metadata."""
        query = (
            "SELECT id, name, parentId, Component FROM "
            f"{self.synapse_asset_view_id} "
            "WHERE type = 'file' AND Component IS NOT NULL AND name LIKE '%csv'"
        )
        dataframe = self.synapse.execute_sql_query(query)
        manifest_list = []
        for _, row in dataframe.iterrows():
            manifest_list.append(
                {
                    "dataset_id": row["parentId"],
                    "dataset_name": "none",
                    "manifest_id": row["id"],
                    "manifest_name": row["name"],
                    "component_name": row["Component"],
                }
            )
        return ManifestMetadataList(manifest_list)

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
        return self.synapse.download_csv_as_dataframe(
            manifest_id, purge_cache=self.purge_synapse_cache
        )
