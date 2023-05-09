"""The ManifestStore class interacts with the Schematic API download manifests."""
# pylint: disable=duplicate-code

import re
from pydantic.dataclasses import dataclass
from pydantic import validator
import validators
import pandas as pd
from schematic_db.api_utils.api_utils import get_project_manifests, download_manifest
from schematic_db.schema_graph.schema_graph import SchemaGraph


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


@dataclass()
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

    @validator("schema_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        """Validates that the value is a valid URL"""
        valid_url = validators.url(value)
        if not valid_url:
            raise ValueError(f"{value} is a valid url")
        return value

    @validator("schema_url")
    @classmethod
    def validate_is_jsonld(cls, value: str) -> str:
        """Validates that the value is a jsonld file"""
        is_jsonld = value.endswith(".jsonld")
        if not is_jsonld:
            raise ValueError(f"{value} does end with '.jsonld'")
        return value

    @validator("synapse_project_id", "synapse_asset_view_id")
    @classmethod
    def validate_synapse_id(cls, value: str) -> str:
        """Check if string is a valid synapse id"""
        if not re.search("^syn[0-9]+", value):
            raise ValueError(f"{value} is not a valid Synapse id")
        return value

    @validator("synapse_input_token")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)"""
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value


class ManifestStore:
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
        self.update_manifest_metadata()

    def create_sorted_table_name_list(self) -> list[str]:
        """
        Uses the schema graph to create a table name list such tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of tables names
        """
        return self.schema_graph.create_sorted_table_name_list()

    def update_manifest_metadata(self) -> None:
        """Updates the current objects manifest_configs."""
        self.manifest_metadata = get_project_manifests(
            access_token=self.synapse_input_token,
            project_id=self.synapse_project_id,
            asset_view=self.synapse_asset_view_id,
        )

    def get_manifest_ids(self, name: str) -> list[str]:
        """Gets the manifest ids for a table(component)

        Args:
            name (str): The name of the table

        Returns:
            list[str]: The manifest ids for the table
        """
        return self.manifest_metadata.get_manifest_ids_for_component(name)

    def download_manifest(self, manifest_id: str) -> pd.DataFrame:
        """Downloads the manifest

        Args:
            manifest_id (str): The synapse id of the manifest

        Returns:
            pd.DataFrame: The manifest in dataframe form
        """
        manifest = download_manifest(self.synapse_input_token, manifest_id)
        return manifest
