"""Metadata for a manifest in Synapse."""

# pylint: disable=duplicate-code
from typing import Any
import json
from pydantic.dataclasses import dataclass
from pydantic import field_validator

from schematic_db.utils.validators import is_synapse_id, string_is_not_empty


@dataclass()
class ManifestMetadata:
    """Metadata for a manifest in Synapse."""

    dataset_id: str
    dataset_name: str
    manifest_id: str
    manifest_name: str
    component_name: str

    _validate_dataset_id = field_validator("dataset_id")(is_synapse_id)
    _validate_manifest_id = field_validator("manifest_id")(is_synapse_id)
    _validate_dataset_name = field_validator("dataset_name")(string_is_not_empty)
    _validate_manifest_name = field_validator("manifest_name")(string_is_not_empty)
    _validate_component_name = field_validator("component_name")(string_is_not_empty)

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
