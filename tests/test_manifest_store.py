"""Testing for ManifestStore."""
import pytest
from schematic_db.manifest_store.manifest_store import ManifestStore

from schematic_db.api_utils.api_utils import ManifestSynapseConfig


@pytest.mark.schematic
class TestSchema:
    """Testing for ManifestStore"""

    def test_init(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.__init__"""
        obj = manifest_store
        for item in obj.manifest_configs:
            assert isinstance(item, ManifestSynapseConfig)

    def test_get_manifests(self, manifest_store: ManifestStore) -> None:
        """Testing for Schema.get_manifests()"""
        obj = manifest_store
        manifests = obj.get_manifests("Patient")
        assert len(manifests) == 2
