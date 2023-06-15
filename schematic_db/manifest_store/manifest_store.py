"""
ManifestStore is an abstract base class that implements an interface.
The interface is used to interact with manifests
"""
# pylint: disable=duplicate-code
from abc import ABC, abstractmethod
import re
import pandas
from pydantic.dataclasses import dataclass
from pydantic import validator
import validators
from schematic_db.api_utils.api_utils import ManifestMetadataList


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

    @validator("synapse_auth_token")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)"""
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value


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
