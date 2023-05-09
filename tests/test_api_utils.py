"""Testing for Schematic API utils"""

import pytest
import pandas as pd
from pydantic import ValidationError
from schematic_db.api_utils.api_utils import (
    ManifestMetadata,
    ManifestMetadataList,
    create_schematic_api_response,
    filter_params,
    find_class_specific_properties,
    get_property_label_from_display_name,
    get_graph_by_edge_type,
    get_project_manifests,
    get_manifest,
    is_node_required,
    get_node_validation_rules,
    SchematicAPIError,
    SchematicAPITimeoutError,
)


@pytest.mark.fast
class TestManifestMetadata:
    """Testing for ManifestMetadata"""

    def test_validation_error1(self) -> None:
        """Testing for ManifestMetadata pydantic synapse id error"""
        with pytest.raises(
            ValidationError,
            match="2 validation errors for ManifestMetadata",
        ):
            ManifestMetadata(
                dataset_id="xxx",
                dataset_name="xxx",
                manifest_id="xxx",
                manifest_name="xxx",
                component_name="xxx",
            )

    def test_validation_error2(self) -> None:
        """Testing for ManifestMetadata pydantic string error"""
        with pytest.raises(
            ValidationError,
            match="3 validation errors for ManifestMetadata",
        ):
            ManifestMetadata(
                dataset_id="syn1",
                dataset_name="",
                manifest_id="syn1",
                manifest_name="",
                component_name="",
            )

    def test_to_dict(self) -> None:
        """Testing for ManifestMetadata.to_dict"""
        dct = {
            "dataset_id": "syn1",
            "dataset_name": "dataset",
            "manifest_id": "syn2",
            "manifest_name": "manifest",
            "component_name": "component",
        }
        manifest = ManifestMetadata(**dct)
        assert manifest.to_dict() == dct

    def test_repr(self) -> None:
        """Testing for ManifestMetadata.__repr__"""
        dct = {
            "dataset_id": "syn1",
            "dataset_name": "dataset",
            "manifest_id": "syn2",
            "manifest_name": "manifest",
            "component_name": "component",
        }
        manifest = ManifestMetadata(**dct)
        print(manifest)


@pytest.mark.fast
class TestManifestMetadataList:
    """Testing for ManifestMetadataList"""

    def test_init(self) -> None:
        """Test ManifestMetadataList init"""
        mml = ManifestMetadataList(
            [
                [["", ""], ["", ""], ["", ""]],
                [["syn1", "xxx"], ["syn2", "xxx"], ["xxx", "xxx"]],
            ]
        )
        assert len(mml.metadata_list) == 1

    def test_repr(self) -> None:
        """Testing for ManifestMetadataList.__repr__"""
        mml = ManifestMetadataList(
            [
                [["syn1", "xxx"], ["syn2", "xxx"], ["component1", "component1"]],
                [["syn3", "xxx"], ["syn4", "xxx"], ["component2", "component2"]],
            ]
        )
        print(mml)

    def test_get_dataset_ids_for_component(self) -> None:
        """Test ManifestMetadataList.get_dataset_ids_for_component"""
        mml = ManifestMetadataList(
            [
                [["syn1", "xxx"], ["syn2", "xxx"], ["component1", "component1"]],
                [["syn3", "xxx"], ["syn4", "xxx"], ["component2", "component2"]],
            ]
        )
        assert mml.get_dataset_ids_for_component("component1") == ["syn1"]
        assert mml.get_dataset_ids_for_component("component2") == ["syn3"]

    def test_get_manifest_ids_for_component(self) -> None:
        """Test ManifestMetadataList.get_manifest_ids_for_component"""
        mml = ManifestMetadataList(
            [
                [["syn1", "xxx"], ["syn2", "xxx"], ["component1", "component1"]],
                [["syn3", "xxx"], ["syn4", "xxx"], ["component2", "component2"]],
            ]
        )
        assert mml.get_manifest_ids_for_component("component1") == ["syn2"]
        assert mml.get_manifest_ids_for_component("component2") == ["syn4"]


@pytest.mark.schematic
class TestAPIUtilHelpers:
    """Testing for API util helpers"""

    def test_create_schematic_api_response(
        self,
        test_schema_json_url: str,
        secrets_dict: dict,
        test_synapse_asset_view_id: str,
    ) -> None:
        """Testing for create_schematic_api_response"""
        response = create_schematic_api_response(
            endpoint_path="explorer/get_property_label_from_display_name",
            params={
                "schema_url": test_schema_json_url,
                "display_name": "year_of_birth",
            },
        )
        assert response.status_code == 200

        with pytest.raises(
            SchematicAPIError,
            match="Error accessing Schematic endpoint",
        ):
            create_schematic_api_response(
                endpoint_path="explorer/get_property_label_from_display_name",
                params={
                    "schema_url": "NOT_A_URL",
                    "display_name": "NOT_A_COMPONENT",
                },
            )

        with pytest.raises(
            SchematicAPITimeoutError,
            match="Schematic endpoint timed out",
        ):
            create_schematic_api_response(
                endpoint_path="manifest/download",
                params={
                    "input_token": secrets_dict["synapse"]["auth_token"],
                    "dataset_id": "syn47996410",
                    "asset_view": test_synapse_asset_view_id,
                    "as_json": True,
                },
                timeout=1,
            )

    def test_filter_params(self) -> None:
        """Testing for filter_params"""
        assert not filter_params({})
        assert not filter_params({"input_token": "xxx"})
        assert filter_params({"attribute": 1}) == {"attribute": 1}
        assert filter_params({"attribute": 1, "input_token": "xxx"}) == {"attribute": 1}


@pytest.mark.schematic
class TestAPIUtils:
    """Testing for API utils"""

    def test_find_class_specific_properties(self, test_schema_json_url: str) -> None:
        "Testing for find_class_specific_properties"
        assert find_class_specific_properties(test_schema_json_url, "Patient") == [
            "id",
            "sex",
            "yearofBirth",
            "weight",
            "diagnosis",
            "date",
        ]

    def test_get_property_label_from_display_name(
        self, test_schema_json_url: str
    ) -> None:
        "Testing for get_property_label_from_display_name"
        assert get_property_label_from_display_name(test_schema_json_url, "id") == "id"
        assert (
            get_property_label_from_display_name(test_schema_json_url, "year_of_birth")
            == "yearOfBirth"
        )

    def test_get_graph_by_edge_type(self, test_schema_json_url: str) -> None:
        "Testing for get_graph_by_edge_type"
        assert get_graph_by_edge_type(test_schema_json_url, "requiresComponent") == [
            ["Biospecimen", "Patient"],
            ["BulkRnaSeq", "Biospecimen"],
        ]

    def test_get_project_manifests(
        self,
        secrets_dict: dict,
        test_synapse_project_id: str,
        test_synapse_asset_view_id: str,
    ) -> None:
        "Testing for get_project_manifests"
        manifest_metadata = get_project_manifests(
            input_token=secrets_dict["synapse"]["auth_token"],
            project_id=test_synapse_project_id,
            asset_view=test_synapse_asset_view_id,
        )
        assert len(manifest_metadata.metadata_list) == 5

    def test_get_manifest(
        self, secrets_dict: dict, test_synapse_asset_view_id: str
    ) -> None:
        "Testing for get_manifest"
        manifest = get_manifest(
            secrets_dict["synapse"]["auth_token"],
            "syn47996410",
            test_synapse_asset_view_id,
        )
        assert isinstance(manifest, pd.DataFrame)

    def test_is_node_required(self, test_schema_json_url: str) -> None:
        """Testing for is_node_required"""
        assert is_node_required(test_schema_json_url, "sex")

    def test_get_node_validation_rules(self, test_schema_json_url: str) -> None:
        """Testing for get_node_validation_rules"""
        rules = get_node_validation_rules(test_schema_json_url, "Family History")
        assert isinstance(rules, list)
