"""Functions that interact with the schematic API"""

from dataclasses import dataclass
import pickle
import requests
import networkx
import pandas

# Currently this is the url for the API when running locally.
API_URL = "http://0.0.0.0:3001"
API_SERVER = "v1"


class SchematicAPIError(Exception):
    """When schematic API response status code is anything other than 200"""

    def __init__(self, endpoint_url: str, status_code: int, reason: str) -> None:
        self.message = "Error accessing Schematic endpoint"
        self.endpoint_url = endpoint_url
        self.status_code = status_code
        self.reason = reason
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message}; "
            f"URL: {self.endpoint_url}; "
            f"Code: {self.status_code}; "
            f"Reason: {self.reason}"
        )


def create_schematic_api_response(
    endpoint_path: str, params: dict, timeout: int = 30
) -> requests.Response:
    """Performs a GET request on the schematic API

    Args:
        endpoint_path (str): The path for the endpoint in the schematic API
        params (dict): The parameters in dict form for the requested endpoint
        timeout (int): The amount of seconds the API call has to run

    Raises:
        SchematicAPIError: When response code is anything other than 200

    Returns:
        requests.Response: The response from the API
    """
    endpoint_url = f"{API_URL}/{API_SERVER}/{endpoint_path}"
    response = requests.get(endpoint_url, params=params, timeout=timeout)
    if response.status_code != 200:
        raise SchematicAPIError(endpoint_url, response.status_code, response.reason)
    return response


def find_class_specific_properties(schema_url: str, schema_class: str) -> list[str]:
    """Find properties specifically associated with a given class

    Args:
        schema_url (str): Data Model URL
        schema_class (str): The class/name fo the component

    Returns:
        list[str]: A list of properties of a given class/component.
    """
    params = {"schema_url": schema_url, "schema_class": schema_class}
    response = create_schematic_api_response(
        "explorer/find_class_specific_properties", params
    )
    return response.json()


def get_node_dependencies(
    schema_url: str,
    source_node: str,
    return_display_names: bool = True,
    return_schema_ordered: bool = True,
) -> list[str]:
    """Get the immediate dependencies that are related to a given source node

    Args:
        schema_url (str): Data Model URL
        source_node (str): The node whose dependencies are needed
        return_display_names (bool, optional): _description_. Defaults to True.
        return_schema_ordered (bool, optional): _description_. Defaults to True.

    Returns:
        list[str]: List of nodes that are dependent on the source node.
    """
    params = {
        "schema_url": schema_url,
        "source_node": source_node,
        "return_display_names": return_display_names,
        "return_schema_ordered": return_schema_ordered,
    }
    response = create_schematic_api_response("explorer/get_node_dependencies", params)
    return response.json()


def get_node_range(
    schema_url: str, node_label: str, return_display_names: bool = True
) -> list[str]:
    """Get all the valid values that are associated with a node label

    Args:
        schema_url (str): Data Model URL
        node_label (str): Label/display name for the node to get values for
        return_display_names (bool, optional): If true returns the display names of the nodes.
            Defaults to True.

    Returns:
        list[str]: Valid values that are associated with a node label
    """
    params = {
        "schema_url": schema_url,
        "node_label": node_label,
        "return_display_names": return_display_names,
    }
    response = create_schematic_api_response("explorer/get_node_range", params)
    return response.json()


def get_property_label_from_display_name(
    schema_url: str, display_name: str, strict_came_case: bool = True
) -> str:
    """Converts a given display name string into a proper property label string

    Args:
        schema_url (str): Data Model URL
        display_name (str): The display name to be converted
        strict_came_case (bool, optional): If true the more strict way of converting
            to camel case is used. Defaults to True.

    Returns:
        str: the property label name
    """
    params = {
        "schema_url": schema_url,
        "display_name": display_name,
        "strict_came_case": strict_came_case,
    }
    response = create_schematic_api_response(
        "explorer/get_property_label_from_display_name", params
    )
    return response.json()


def get_graph_by_edge_type(schema_url: str, relationship: str) -> list[tuple[str, str]]:
    """Get a subgraph containing all edges of a given type (aka relationship)

    Args:
        schema_url (str): Data Model URL
        relationship (str): Relationship (i.e. parentOf, requiresDependency,
            rangeValue, domainValue)

    Returns:
        list[tuple[str, str]]: A subgraph in the form of a list of tuples.
    """
    params = {"schema_url": schema_url, "relationship": relationship}
    response = create_schematic_api_response("schemas/get/graph_by_edge_type", params)
    return response.json()


def get_schema(schema_url: str) -> networkx.MultiDiGraph:
    """Return schema as a pickle file

    Args:
        schema_url (str): Data Model URL

    Returns:
        str: The path to the downloaded pickle file.
    """
    params = {"schema_url": schema_url}
    response = create_schematic_api_response("schemas/get/schema", params)
    schema_path = response.text
    with open(schema_path, "rb") as file:
        schema = pickle.load(file)
    return schema


@dataclass
class ManifestSynapseConfig:
    """A config for a manifest in Synapse."""

    dataset_id: str
    dataset_name: str
    manifest_id: str
    manifest_name: str
    component_name: str


def get_project_manifests(
    input_token: str, project_id: str, asset_view: str
) -> list[ManifestSynapseConfig]:
    """Gets all metadata manifest files across all datasets in a specified project.

    Args:
        input_token (str): access token
        project_id (str): Project ID
        asset_view (str): ID of view listing all project data assets. For example,
            for Synapse this would be the Synapse ID of the fileview listing all
            data assets for a given project.(i.e. master_fileview in config.yml)

    Returns:
        list[ManifestSynapseConfig]: A list of manifests in Synapse
    """
    params = {
        "input_token": input_token,
        "project_id": project_id,
        "asset_view": asset_view,
    }
    response = create_schematic_api_response(
        "storage/project/manifests", params, timeout=60
    )
    manifests = [
        ManifestSynapseConfig(
            dataset_id=item[0][0],
            dataset_name=item[0][1],
            manifest_id=item[1][0],
            manifest_name=item[1][1],
            component_name=item[2][0],
        )
        for item in response.json()
    ]
    return manifests


def get_manifest(
    input_token: str, dataset_id: str, asset_view: str
) -> pandas.DataFrame:
    """Downloads a manifest as a pd.dataframe

    Args:
        input_token (str): Access token
        dataset_id (str): The id of the dataset the manifest part of
        asset_view (str): The id of the view listing all project data assets. For example,
            for Synapse this would be the Synapse ID of the fileview listing all
            data assets for a given project.(i.e. master_fileview in config.yml)

    Returns:
        pd.DataFrame: The manifest in dataframe form
    """
    params = {
        "input_token": input_token,
        "dataset_id": dataset_id,
        "asset_view": asset_view,
        "as_json": True,
    }
    response = create_schematic_api_response("manifest/download", params, timeout=120)
    manifest = pandas.DataFrame(response.json())
    return manifest


def is_node_required(schema_url: str, node_label: str) -> bool:
    """Checks if node is required

    Args:
        schema_url (str): Data Model URL
        node_label (str): Label/display name for the node to check

    Returns:
        bool: Wether or not the node is required
    """

    params = {"schema_url": schema_url, "node_display_name": node_label}
    response = create_schematic_api_response("schemas/is_node_required", params)
    return response.json()


def get_manifest_datatypes(
    input_token: str, manifest_id: str, asset_view: str
) -> dict[str, str]:
    """Gets the datatypes for all  attributes in a manifest

    Args:
        input_token (str): Access token
        manifest_id (str): The id of the manifest
        asset_view (str): The id of the view listing all project data assets. For example,
            for Synapse this would be the Synapse ID of the fileview listing all
            data assets for a given project.(i.e. master_fileview in config.yml)

    Returns:
        dict[str, str]: A dict of attribute names and their datatype
    """
    params = {
        "input_token": input_token,
        "manifest_id": manifest_id,
        "asset_view": asset_view,
    }
    response = create_schematic_api_response(
        "get/datatype/manifest", params, timeout=120
    )
    return response.json()


def get_node_validation_rules(schema_url: str, node_display_name: str) -> list[str]:
    """Gets the validation rules for the node

    Args:
        schema_url (str): Data Model URL
        node_display_name (str): Label/display name for the node to check

    Returns:
        list[str]: A list of validation rules
    """
    params = {
        "schema_url": schema_url,
        "node_display_name": node_display_name,
    }
    response = create_schematic_api_response(
        "schemas/get_node_validation_rules", params
    )
    return response.json()
