"""Metadata for a manifest in Synapse."""
# pylint: disable=duplicate-code
from typing import Any
import json
import re
from pydantic.dataclasses import dataclass
from pydantic import validator


@dataclass()
class ManifestMetadata:
    """Metadata for a manifest in Synapse."""

    dataset_id: str
    dataset_name: str
    manifest_id: str
    manifest_name: str
    component_name: str

    @validator("dataset_id", "manifest_id")
    @classmethod
    def validate_synapse_id(cls, value: str) -> str:
        """Check if string is a valid synapse id

        Args:
            value (str): A string

        Raises:
            ValueError: If the value isn't a valid Synapse id

        Returns:
            (str): The input value
        """
        if not re.search("^syn[0-9]+", value):
            raise ValueError(f"{value} is not a valid Synapse id")
        return value

    @validator("dataset_name", "manifest_name", "component_name")
    @classmethod
    def validate_string_is_not_empty(cls, value: str) -> str:
        """Check if string  is not empty(has at least one char)

        Args:
            value (str): A string

        Raises:
            ValueError: If the value is zero characters long

        Returns:
            (str): The input value
        """
        if len(value) == 0:
            raise ValueError(f"{value} is an empty string")
        return value

    def to_dict(self) -> dict[str, str]:
        """Returns object attributes as dict

        Returns:
            dict[str, str]: dict of object attributes
        """
        attribute_dict = vars(self)
        attribute_names = [
            "dataset_id",
            "dataset_name",
            "manifest_id",
            "manifest_name",
            "component_name",
        ]
        return {key: attribute_dict[key] for key in attribute_names}

    def __repr__(self) -> str:
        """Prints object as dict"""
        return json.dumps(self.to_dict(), indent=4)


class ManifestMetadataList:
    """A list of Manifest Metadata"""

    def __init__(self, metadata_input: list[dict[str, Any]]) -> None:
        """
        Args:
            metadata_input (list[dict[str, Any]]): A list of dicts where each dict has key values
             pairs that correspond to the arguments of ManifestMetadata.
        """
        metadata_list: list[ManifestMetadata] = []
        for item in metadata_input.copy():
            try:
                metadata = ManifestMetadata(**item)
            except ValueError:
                pass
            else:
                metadata_list.append(metadata)
        self.metadata_list = metadata_list

    def __repr__(self) -> str:
        """Prints each metadata object as dict"""
        return json.dumps(
            [metadata.to_dict() for metadata in self.metadata_list], indent=4
        )

    def get_dataset_ids_for_component(self, component_name: str) -> list[str]:
        """Gets the dataset ids from the manifest metadata matching the component name

        Args:
            component_name (str): The name of the component to get the manifest datasets ids for

        Returns:
            list[str]: A list of synapse ids for the manifest datasets
        """
        return [
            metadata.dataset_id
            for metadata in self.metadata_list
            if metadata.component_name == component_name
        ]

    def get_manifest_ids_for_component(self, component_name: str) -> list[str]:
        """Gets the manifest ids from the manifest metadata matching the component name

        Args:
            component_name (str): The name of the component to get the manifest ids for

        Returns:
            list[str]: A list of synapse ids for the manifests
        """
        return [
            metadata.manifest_id
            for metadata in self.metadata_list
            if metadata.component_name == component_name
        ]
