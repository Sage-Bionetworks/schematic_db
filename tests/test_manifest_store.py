"""Testing for ManifestStore."""
from schematic_db.manifest_store.manifest_store import ManifestStore
from schematic_db.api_utils.api_utils import ManifestMetadata


class TestSchema:
    """Testing for ManifestStore"""

    def test_init(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.__init__"""
        obj = manifest_store
        for item in obj.manifest_metadata.metadata_list:
            assert isinstance(item, ManifestMetadata)

    def test_get_manifest_ids(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.get_get_manifest_ids"""
        obj = manifest_store
        assert obj.get_manifest_ids("Patient") == ["syn47996020", "syn47996172"]

    def test_download_manifest(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.download_manifest"""
        obj = manifest_store
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
