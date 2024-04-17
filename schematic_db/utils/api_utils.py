"""Functions that interact with the schematic API"""

# pylint: disable=duplicate-code

from typing import Any
from os import getenv
from datetime import datetime
import pytz
import requests
import pandas
from schematic_db.manifest_store.manifest_metadata_list import ManifestMetadataList
from schematic_db.utils.types import DisplayLabelType


class SchematicAPIError(Exception):
    """When schematic API response status code is anything other than 200"""

    def __init__(  # pylint:disable=too-many-arguments
        self,
        endpoint_url: str,
        status_code: int,
        reason: str,
        time: datetime,
        params: dict[str, Any],
    ) -> None:
        """
        Args:
            endpoint_url (str): The url of the endpoint
            status_code (int): The status code given in the response
            reason (str): The reason given in the response
            time (datetime): The time the API was called
            params (dict[str, Any]): The parameters sent with the API call
        """
        self.message = "Error accessing Schematic endpoint"
        self.endpoint_url = endpoint_url
        self.status_code = status_code
        self.reason = reason
        self.time = time
        self.params = params
        super().__init__(self.message)

    def __str__(self) -> str:
        """
        Returns:
            str: The description of the error
        """
        return (
            f"{self.message}; "
            f"URL: {self.endpoint_url}; "
            f"Code: {self.status_code}; "
            f"Reason: {self.reason}; "
            f"Time (PST): {self.time}; "
            f"Parameters: {self.params}"
        )


class SchematicAPITimeoutError(Exception):
    """When schematic API timed out"""

    def __init__(
        self,
        endpoint_url: str,
        time: datetime,
        params: dict[str, Any],
    ) -> None:
        """
        Args:
            endpoint_url (str): The url of the endpoint
            time (datetime): The time the API was called
            params (dict[str, Any]): The parameters sent with the API call
        """
        self.message = "Schematic endpoint timed out"
        self.endpoint_url = endpoint_url
        self.time = time
        self.params = params
        super().__init__(self.message)

    def __str__(self) -> str:
        """
        Returns:
            str: The description of the error
        """
        return (
            f"{self.message}; "
            f"URL: {self.endpoint_url}; "
            f"Time (PST): {self.time}; "
            f"Parameters: {self.params}"
        )


def create_schematic_api_response(
    endpoint_path: str,
    params: dict[str, Any],
    timeout: float = 30,
    access_token: str | None = None,
) -> requests.Response:
    """Performs a GET request on the schematic API

    Args:
        endpoint_path (str): The path for the endpoint in the schematic API
        params (dict): The parameters in dict form for the requested endpoint
        timeout (float): The amount of seconds the API call has to run
        access_token (str): Access token for storage related endpoints

    Raises:
        SchematicAPIError: When response code is anything other than 200
        SchematicAPITimeoutError: When API call times out

    Returns:
        requests.Response: The response from the API
    """
    api_url = getenv("API_URL", "https://schematic.api.sagebionetworks.org/v1/")
    endpoint_url = f"{api_url}/{endpoint_path}"
    start_time = datetime.now(pytz.timezone("US/Pacific"))
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(
            endpoint_url, params=params, headers=headers, timeout=timeout
        )
    except requests.exceptions.Timeout as exc:
        raise SchematicAPITimeoutError(endpoint_url, start_time, params) from exc
    if response.status_code != 200:
        raise SchematicAPIError(
            endpoint_url,
            response.status_code,
            response.reason,
            start_time,
            params,
        )
    return response


def find_class_specific_properties(
    schema_url: str,
    schema_class: str,
    data_model_labels: DisplayLabelType = "class_label",
) -> list[str]:
    """Find properties specifically associated with a given class

    Args:
        schema_url (str): Data Model URL
        schema_class (str): The class/name fo the component
        data_model_labels (DisplayLabelType): The type of label to use as display

    Returns:
        list[str]: A list of properties of a given class/component.
    """
    params = {
        "schema_url": schema_url,
        "schema_class": schema_class,
        "data_model_labels": data_model_labels,
    }
    response = create_schematic_api_response(
        "/schemas/find_class_specific_properties", params
    )
    return response.json()


def get_property_label_from_display_name(
    display_name: str, strict_camel_case: bool = True
) -> str:
    """Converts a given display name string into a proper property label string

    Args:
        display_name (str): The display name to be converted
        strict_camel_case (bool, optional): If true the more strict way of converting
            to camel case is used. Defaults to True.

    Returns:
        str: the property label name
    """
    params = {
        "display_name": display_name,
        "strict_camel_case": strict_camel_case,
    }
    response = create_schematic_api_response(
        "/utils/get_property_label_from_display_name", params
    )
    return response.json()


def get_graph_by_edge_type(
    schema_url: str,
    relationship: str,
    data_model_labels: DisplayLabelType = "class_label",
) -> list[tuple[str, str]]:
    """Get a subgraph containing all edges of a given type (aka relationship)

    Args:
        schema_url (str): Data Model URL
        relationship (str): Relationship (i.e. parentOf, requiresDependency,
            rangeValue, domainValue)
        data_model_labels (DisplayLabelType): The type of label to use as display

    Returns:
        list[tuple[str, str]]: A subgraph in the form of a list of tuples.
    """
    params = {
        "schema_url": schema_url,
        "relationship": relationship,
        "data_model_labels": data_model_labels,
    }
    response = create_schematic_api_response("schemas/get/graph_by_edge_type", params)
    return response.json()


def get_project_manifests(
    access_token: str, project_id: str, asset_view: str
) -> ManifestMetadataList:
    """Gets all metadata manifest files across all datasets in a specified project.

    Args:
        access_token (str): access token
        project_id (str): Project ID
        asset_view (str): ID of view listing all project data assets. For example,
            for Synapse this would be the Synapse ID of the fileview listing all
            data assets for a given project.(i.e. master_fileview in config.yml)

    Returns:
        ManifestMetadataList: A list of manifests in Synapse
    """
    params = {
        "project_id": project_id,
        "asset_view": asset_view,
    }
    response = create_schematic_api_response(
        "storage/project/manifests", params, timeout=1000, access_token=access_token
    )
    metadata_list = []
    for item in response.json():
        metadata_list.append(
            {
                "dataset_id": item[0][0],
                "dataset_name": item[0][1],
                "manifest_id": item[1][0],
                "manifest_name": item[1][1],
                "component_name": item[2][0],
            }
        )
    return ManifestMetadataList(metadata_list)


def download_manifest(access_token: str, manifest_id: str) -> pandas.DataFrame:
    """Downloads a manifest as a pd.dataframe

    Args:
        access_token (str): Access token
        manifest_id (str): The synapse id of the manifest

    Returns:
        pd.DataFrame: The manifest in dataframe form
    """
    params = {
        "manifest_id": manifest_id,
        "as_json": True,
    }
    response = create_schematic_api_response(
        "manifest/download", params, timeout=1000, access_token=access_token
    )
    manifest = pandas.DataFrame(response.json())
    return manifest


def is_node_required(
    schema_url: str,
    node_display_name: str,
    data_model_labels: DisplayLabelType = "class_label",
) -> bool:
    """Checks if node is required

    Args:
        schema_url (str): Data Model URL
        node_display_name (str): Label/display name for the node to check
        data_model_labels (DisplayLabelType): The type of label to use as display

    Returns:
        bool: Wether or not the node is required
    """

    params = {
        "schema_url": schema_url,
        "node_display_name": node_display_name,
        "data_model_labels": data_model_labels,
    }
    response = create_schematic_api_response("schemas/is_node_required", params)
    return response.json()


def get_node_validation_rules(
    schema_url: str,
    node_display_name: str,
    data_model_labels: DisplayLabelType = "class_label",
) -> list[str]:
    """Gets the validation rules for the node

    Args:
        schema_url (str): Data Model URL
        node_display_name (str): Label/display name for the node to check
        data_model_labels (DisplayLabelType): The type of label to use as display

    Returns:
        list[str]: A list of validation rules
    """
    params = {
        "schema_url": schema_url,
        "node_display_name": node_display_name,
        "data_model_labels": data_model_labels,
    }
    response = create_schematic_api_response(
        "schemas/get_node_validation_rules", params
    )
    return response.json()
