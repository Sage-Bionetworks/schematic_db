"""Testing for Schematic API utils"""

import pytest
import pandas
from schematic_db.utils.api_utils import (
    create_schematic_api_response,
    find_class_specific_properties,
    get_property_label_from_display_name,
    get_graph_by_edge_type,
    get_project_manifests,
    download_manifest,
    is_node_required,
    get_node_validation_rules,
    SchematicAPIError,
    SchematicAPITimeoutError,
)


class TestAPIUtilHelpers:  # pylint: disable=too-few-public-methods
    """Testing for API util helpers"""

    def test_create_schematic_api_response(
        self,
        test_schema_csv_url: str,
        secrets_dict: dict[str, dict[str, str]],
    ) -> None:
        """Testing for create_schematic_api_response"""
        response = create_schematic_api_response(
            endpoint_path="utils/get_property_label_from_display_name",
            params={
                "schema_url": test_schema_csv_url,
                "display_name": "year_of_birth",
            },
        )
        assert response.status_code == 200

        with pytest.raises(
            SchematicAPIError,
            match="Error accessing Schematic endpoint",
        ):
            create_schematic_api_response(
                endpoint_path="schemas/get_node_validation_rules",
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
                    "access_token": secrets_dict["synapse"]["auth_token"],
                    "manifest_id": "syn47996491",
                    "as_json": True,
                },
                timeout=0.1,
            )


class TestAPIUtils:
    """Testing for API utils"""

    def test_find_class_specific_properties(
        self,
        test_schema_csv_url: str,
    ) -> None:
        "Testing for find_class_specific_properties"
        assert find_class_specific_properties(test_schema_csv_url, "Patient") == [
            "id",
            "sex",
            "yearOfBirth",
            "diagnosis",
            "weight",
            "date",
        ]

        assert find_class_specific_properties(
            test_schema_csv_url, "Patient", data_model_labels="display_label"
        ) == [
            "id",
            "Sex",
            "Year_Of_Birth",
            "Diagnosis",
            "Weight",
            "Date",
        ]

    def test_get_property_label_from_display_name(self) -> None:
        "Testing for get_property_label_from_display_name"
        assert get_property_label_from_display_name("id") == "id"
        assert get_property_label_from_display_name("year_of_birth") == "yearOfBirth"

    def test_get_graph_by_edge_type(self, test_schema_csv_url: str) -> None:
        "Testing for get_graph_by_edge_type"
        assert get_graph_by_edge_type(test_schema_csv_url, "requiresComponent") == [
            ["Biospecimen", "Patient"],
            ["BulkRnaSeq", "Biospecimen"],
        ]
        assert get_graph_by_edge_type(
            test_schema_csv_url, "requiresComponent", data_model_labels="display_label"
        ) == [
            ["Biospecimen", "Patient"],
            ["Bulk_Rna_Seq", "Biospecimen"],
        ]

    def test_get_project_manifests(
        self,
        secrets_dict: dict[str, dict[str, str]],
        test_synapse_project_id: str,
        test_synapse_asset_view_id: str,
    ) -> None:
        "Testing for get_project_manifests"
        manifest_metadata = get_project_manifests(
            access_token=secrets_dict["synapse"]["auth_token"],
            project_id=test_synapse_project_id,
            asset_view=test_synapse_asset_view_id,
        )
        assert len(manifest_metadata.metadata_list) == 5

    def test_download_manifest(self, secrets_dict: dict[str, dict[str, str]]) -> None:
        "Testing for download_manifest"
        manifest = download_manifest(
            secrets_dict["synapse"]["auth_token"],
            "syn47996491",
        )
        assert isinstance(manifest, pandas.DataFrame)

    def test_is_node_required(self, test_schema_csv_url: str) -> None:
        """Testing for is_node_required"""
        assert is_node_required(test_schema_csv_url, "sex")
        assert is_node_required(
            test_schema_csv_url, "Sex", data_model_labels="display_label"
        )

    def test_get_node_validation_rules(self, test_schema_csv_url: str) -> None:
        """Testing for get_node_validation_rules"""
        assert get_node_validation_rules(test_schema_csv_url, "sex") == ["str"]
        assert get_node_validation_rules(
            test_schema_csv_url, "Sex", data_model_labels="display_label"
        ) == ["str"]
