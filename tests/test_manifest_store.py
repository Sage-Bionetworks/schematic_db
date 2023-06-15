"""Testing for ManifestStore."""
from typing import Any
import pytest
from pydantic import ValidationError
from schematic_db.manifest_store.manifest_store import ManifestStore
from schematic_db.manifest_store.manifest_metadata_list import (
    ManifestMetadata,
    ManifestMetadataList,
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
                {
                    "dataset_id": "",
                    "dataset_name": "",
                    "manifest_id": "",
                    "manifest_name": "",
                    "component_name": "",
                },
                {
                    "dataset_id": "syn1",
                    "dataset_name": "x",
                    "manifest_id": "syn2",
                    "manifest_name": "x",
                    "component_name": "x",
                },
            ]
        )
        assert len(mml.metadata_list) == 1

    def test_repr(self) -> None:
        """Testing for ManifestMetadataList.__repr__"""
        mml = ManifestMetadataList(
            [
                {
                    "dataset_id": "syn1",
                    "dataset_name": "x",
                    "manifest_id": "syn2",
                    "manifest_name": "x",
                    "component_name": "x",
                },
                {
                    "dataset_id": "syn3",
                    "dataset_name": "x",
                    "manifest_id": "syn4",
                    "manifest_name": "x",
                    "component_name": "x",
                },
            ]
        )
        print(mml)

    def test_get_dataset_ids_for_component(self) -> None:
        """Test ManifestMetadataList.get_dataset_ids_for_component"""
        mml = ManifestMetadataList(
            [
                {
                    "dataset_id": "syn1",
                    "dataset_name": "x",
                    "manifest_id": "syn2",
                    "manifest_name": "x",
                    "component_name": "component1",
                },
                {
                    "dataset_id": "syn3",
                    "dataset_name": "x",
                    "manifest_id": "syn4",
                    "manifest_name": "x",
                    "component_name": "component2",
                },
            ]
        )
        assert mml.get_dataset_ids_for_component("component1") == ["syn1"]
        assert mml.get_dataset_ids_for_component("component2") == ["syn3"]

    def test_get_manifest_ids_for_component(self) -> None:
        """Test ManifestMetadataList.get_manifest_ids_for_component"""
        mml = ManifestMetadataList(
            [
                {
                    "dataset_id": "syn1",
                    "dataset_name": "x",
                    "manifest_id": "syn2",
                    "manifest_name": "x",
                    "component_name": "component1",
                },
                {
                    "dataset_id": "syn3",
                    "dataset_name": "x",
                    "manifest_id": "syn4",
                    "manifest_name": "x",
                    "component_name": "component2",
                },
            ]
        )
        assert mml.get_manifest_ids_for_component("component1") == ["syn2"]
        assert mml.get_manifest_ids_for_component("component2") == ["syn4"]


@pytest.mark.parametrize(
    "manifest_store", ["api_manifest_store", "synapse_manifest_store"]
)
class TestManifestStore:
    """Testing for ManifestStore"""

    def test_init(self, request: Any, manifest_store: str) -> None:
        """Testing for Schema.__init__"""
        obj: ManifestStore = request.getfixturevalue(manifest_store)
        for item in obj.get_manifest_metadata().metadata_list:
            assert isinstance(item, ManifestMetadata)

    def test_get_manifest_ids(self, request: Any, manifest_store: str) -> None:
        """Testing for Schema.get_get_manifest_ids"""
        obj: ManifestStore = request.getfixturevalue(manifest_store)
        assert obj.get_manifest_ids("Patient") == ["syn47996020", "syn47996172"]

    def test_download_manifest(self, request: Any, manifest_store: str) -> None:
        """Testing for Schema.download_manifest"""
        obj: ManifestStore = request.getfixturevalue(manifest_store)
        manifest = obj.download_manifest("syn47996020")
        assert sorted(list(manifest.columns)) == sorted(
            [
                "patientId",
                "sex",
                "yearOfBirth",
                "diagnosis",
                "Component",
                "cancerType",
                "familyHistory",
                "entityId",
                "id",
            ]
        )
