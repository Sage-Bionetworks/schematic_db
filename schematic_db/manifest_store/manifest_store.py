"""
ManifestStore is an abstract base class that implements an interface.
The interface is used to interact with manifests
"""

# pylint: disable=duplicate-code
from abc import ABC, abstractmethod
import pandas
from pydantic.dataclasses import dataclass
from pydantic import field_validator
from schematic_db.utils.validators import (
    is_valid_url,
    is_data_model_file,
    is_synapse_id,
    string_is_not_empty,
)
from schematic_db.utils.api_utils import ManifestMetadataList


@dataclass()
class ManifestStoreConfig:
    """
    A config for a ManifestStore.
    Properties:
        schema_url (str): A url to the jsonld schema file
        synapse_project_id (str): The synapse id to the project where the manifests are stored.
        synapse_asset_view_id (str): The synapse id to the asset view that tracks the manifests.
        synapse_auth_token (str): A synapse token with download permissions for both the
         synapse_project_id and synapse_asset_view_id
    """

    schema_url: str
    synapse_project_id: str
    synapse_asset_view_id: str
    synapse_auth_token: str

    _validate_url = field_validator("schema_url")(is_valid_url)
    _validate_url2 = field_validator("schema_url")(is_data_model_file)
    _validate_project_id = field_validator("synapse_project_id")(is_synapse_id)
    _validate_asset_view_id = field_validator("synapse_asset_view_id")(is_synapse_id)
    _validate_auth_token = field_validator("synapse_auth_token")(string_is_not_empty)


class ManifestStore(ABC):
    """An interface for interacting with manifests"""

    @abstractmethod
    def create_sorted_table_name_list(self) -> list[str]:
        """
        Creates a table name list such tables always come after ones they
         depend on.
        This order is how tables in a database should be built and/or updated.

        Returns:
            list[str]: A list of tables names
        """

    @abstractmethod
    def get_manifest_metadata(self) -> ManifestMetadataList:
        """Gets the current objects manifest metadata."""

    @abstractmethod
    def get_manifest_ids(self, name: str) -> list[str]:
        """Gets the manifest ids for a table(component)

        Args:
            name (str): The name of the table

        Returns:
            list[str]: The manifest ids for the table
        """

    @abstractmethod
    def download_manifest(self, manifest_id: str) -> pandas.DataFrame:
        """Downloads the manifest

        Args:
            manifest_id (str): The synapse id of the manifest

        Returns:
            pandas.DataFrame: The manifest in dataframe form
        """
