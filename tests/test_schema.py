"""Testing for Schema."""
from typing import Generator, Any
import pytest
import pandas as pd
from schematic_db.db_config import (
    DBForeignKey,
    DBAttributeConfig,
    DBDatatype,
)
from schematic_db.schema import (
    Schema,
    ManifestSynapseConfig,
    SchematicAPIError,
    get_project_manifests,
    get_manifest_ids_for_object,
    get_dataset_ids_for_object,
    get_manifest,
    get_manifest_datatypes,
)


@pytest.fixture(name="test_manifests")
def fixture_test_manifests() -> Generator:
    """Yields a test set of manifests"""
    yield [
        ManifestSynapseConfig(
            manifest_id="syn1",
            dataset_id="syn6",
            component_name="C1",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="syn2",
            dataset_id="syn7",
            component_name="C2",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="syn3",
            dataset_id="syn8",
            component_name="",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="",
            dataset_id="syn9",
            component_name="C3",
            dataset_name="",
            manifest_name="",
        ),
        ManifestSynapseConfig(
            manifest_id="syn5",
            dataset_id="syn10",
            component_name="C1",
            dataset_name="",
            manifest_name="",
        ),
    ]


@pytest.mark.fast
class TestUtils:
    """Testing for Schema utils"""

    def test_get_manifest_ids_for_object(
        self, test_manifests: list[ManifestSynapseConfig]
    ) -> None:
        """Testing for get_manifest_ids_for_object"""
        assert get_manifest_ids_for_object("C1", test_manifests) == ["syn1", "syn5"]
        assert get_manifest_ids_for_object("C2", test_manifests) == ["syn2"]
        assert get_manifest_ids_for_object("C3", test_manifests) == []

    def test_get_dataset_ids_for_object(
        self, test_manifests: list[ManifestSynapseConfig]
    ) -> None:
        """Testing for get_manifest_ids_for_object"""
        assert get_dataset_ids_for_object("C1", test_manifests) == ["syn6", "syn10"]
        assert get_dataset_ids_for_object("C2", test_manifests) == ["syn7"]
        assert get_dataset_ids_for_object("C3", test_manifests) == []


@pytest.mark.schematic
class TestAPIUtils:
    """Testing for API utils"""

    def test_get_project_manifests(
        self,
        secrets_dict: dict,
        test_synapse_project_id: str,
        test_synapse_asset_view_id: str,
    ) -> None:
        "Testing for get_project_manifests"
        manifests = get_project_manifests(
            input_token=secrets_dict["synapse"]["auth_token"],
            project_id=test_synapse_project_id,
            asset_view=test_synapse_asset_view_id,
        )
        assert len(manifests) == 5

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

        with pytest.raises(
            SchematicAPIError,
            match="Error accessing Schematic endpoint",
        ):
            get_manifest(
                secrets_dict["synapse"]["auth_token"],
                "1",
                test_synapse_asset_view_id,
            )

    def test_get_manifest_datatypes(
        self, secrets_dict: dict, test_synapse_asset_view_id: str
    ) -> None:
        """Testing for get_manifest_datatypes()"""
        datatypes = get_manifest_datatypes(
            secrets_dict["synapse"]["auth_token"],
            "syn30988380",
            test_synapse_asset_view_id,
        )
        assert isinstance(datatypes, dict)


@pytest.mark.fast
class TestMockSchema:  # pylint: disable=too-few-public-methods
    """Testing for Schema with schematic endpoint mocked"""

    def test_init(self, mocker: Any) -> None:
        """Testing for Schema.create_foreign_keys()"""
        subgraph = [["Patient", "PatientID"], ["Patient", "Sex"]]
        mocker.patch(
            "schematic_db.schema.schema.get_graph_by_edge_type", return_value=subgraph
        )
        mocker.patch(
            "schematic_db.schema.schema.get_project_manifests", return_value=[]
        )
        obj = Schema("url", "project_id", "asset_id", "token")
        assert isinstance(obj, Schema)


@pytest.mark.schematic
class TestSchema:
    """Testing for Schema"""

    def test_create_attributes(self, test_schema: Schema) -> None:
        """Testing for Schema.attributes()"""
        obj = test_schema
        assert obj.create_attributes("Patient") == [
            DBAttributeConfig(name="sex", datatype=DBDatatype.TEXT, required=True),
            DBAttributeConfig(
                name="yearofBirth", datatype=DBDatatype.INT, required=False
            ),
            DBAttributeConfig(
                name="diagnosis", datatype=DBDatatype.TEXT, required=True
            ),
        ]
        assert obj.create_attributes("Biospecimen") == [
            DBAttributeConfig(
                name="patientId", datatype=DBDatatype.TEXT, required=True
            ),
            DBAttributeConfig(
                name="tissueStatus", datatype=DBDatatype.TEXT, required=True
            ),
        ]

    def test_create_foreign_keys(self, test_schema: Schema) -> None:
        """Testing for Schema.create_foreign_keys()"""
        obj = test_schema
        assert obj.create_foreign_keys("Patient") == []
        assert obj.create_foreign_keys("Biospecimen") == [
            DBForeignKey(
                name="patientId",
                foreign_object_name="Patient",
                foreign_attribute_name="patientId",
            )
        ]
        assert obj.create_foreign_keys("BulkRNA-seqAssay") == [
            DBForeignKey(
                name="biospecimenId",
                foreign_object_name="Biospecimen",
                foreign_attribute_name="biospecimenId",
            )
        ]

    def test_create_db_config(self, test_schema: Schema) -> None:
        """Testing for Schema.create_db_config()"""
        obj = test_schema
        config = obj.create_db_config()
        assert config.get_config_names() == [
            "Patient",
            "Biospecimen",
            "BulkRNA-seqAssay",
        ]

    def test_get_manifests(self, test_schema: Schema) -> None:
        """Testing for Schema.get_manifests()"""
        obj = test_schema
        db_config = obj.create_db_config()
        patient_config = db_config.get_config_by_name("Patient")
        manifests = test_schema.get_manifests(patient_config)
        assert len(manifests) == 2
