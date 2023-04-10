"""Testing for ManifestStore."""
import pytest
from schematic_db.manifest_store.manifest_store import ManifestStore
from schematic_db.api_utils.api_utils import ManifestMetadata


@pytest.mark.schematic
class TestSchema:
    """Testing for ManifestStore"""

    def test_init(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.__init__"""
        obj = manifest_store
        for item in obj.manifest_metadata.metadata_list:
            assert isinstance(item, ManifestMetadata)

    def test_get_dataset_ids(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.get_get_dataset_ids()"""
        obj = manifest_store
        assert obj.get_dataset_ids("Patient") == ["syn47994831", "syn47996086"]

    def test_get_manifest(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.get_manifest()"""
        obj = manifest_store
        manifest = obj.get_manifest("syn47994831")
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
