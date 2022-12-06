"""Testing for Schema."""
from typing import Generator, Any
import pytest
import pandas as pd
from schematic_db.db_config import (
    DBConfig,
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
)


@pytest.fixture(name="test_synapse_folder_id")
def fixture_test_synapse_folder_id() -> Generator:
    """Yields a synapse id for a folder in the test project"""
    yield "syn30988314"


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
        test_synapse_folder_id: str,
        test_synapse_asset_view_id: str,
    ) -> None:
        "Testing for get_project_manifests"
        manifests = get_project_manifests(
            input_token=secrets_dict["synapse"]["auth_token"],
            project_id=test_synapse_folder_id,
            asset_view=test_synapse_asset_view_id,
        )
        assert manifests == [
            ManifestSynapseConfig(
                dataset_id="syn30988361",
                dataset_name="TestFolder",
                manifest_id="syn30988380",
                manifest_name="synapse_storage_manifest (2).csv",
                component_name="Biospecimen",
            )
        ]

    def test_get_project_manifests2(
        self,
        secrets_dict: dict,
        gff_synapse_project_id: str,
        gff_synapse_asset_view_id: str,
    ) -> None:
        "Testing for get_project_manifests"
        manifests = get_project_manifests(
            input_token=secrets_dict["synapse"]["auth_token"],
            project_id=gff_synapse_project_id,
            asset_view=gff_synapse_asset_view_id,
        )
        assert len(manifests) == 31

    def test_get_manifest(
        self, secrets_dict: dict, gff_synapse_asset_view_id: str
    ) -> None:
        "Testing for get_manifest"
        manifest = get_manifest(
            secrets_dict["synapse"]["auth_token"],
            "syn38306654",
            gff_synapse_asset_view_id,
        )
        assert isinstance(manifest, pd.DataFrame)

        with pytest.raises(
            SchematicAPIError,
            match="Error accessing Schematic endpoint",
        ):
            get_manifest(
                secrets_dict["synapse"]["auth_token"],
                "1",
                gff_synapse_asset_view_id,
            )


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
                name="yearofBirth", datatype=DBDatatype.TEXT, required=False
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


@pytest.mark.schematic
class TestGFFSchema:
    """Testing for GFF Schema"""

    def test_create_db_config(self, gff_db_config: DBConfig) -> None:
        """Testing for Schema.create_db_config()"""
        assert gff_db_config.get_config_names() == [
            "Donor",
            "AnimalModel",
            "CellLine",
            "Antibody",
            "GeneticReagent",
            "Funder",
            "Publication",
            "Investigator",
            "Resource",
            "MutationDetails",
            "Vendor",
            "Development",
            "Mutation",
            "ResourceApplication",
            "Observation",
            "VendorItem",
            "Biobank",
            "Usage",
        ]

    def test_get_manifests(self, gff_schema: Schema, gff_db_config: DBConfig) -> None:
        """Testing for Schema.get_manifests()"""
        manifests1 = gff_schema.get_manifests(gff_db_config.configs[0])
        assert len(manifests1) == 2
        assert list(manifests1[0].columns) == [
            "age",
            "donorId",
            "parentDonorId",
            "race",
            "sex",
            "species",
        ]

        manifests2 = gff_schema.get_manifests(gff_db_config.configs[1])
        assert len(manifests2) == 1
        assert list(manifests2[0].columns) == [
            "animalModelDisease",
            "animalModelofManifestation",
            "animalModelId",
            "animalState",
            "backgroundStrain",
            "backgroundSubstrain",
            "donorId",
            "generation",
            "strainNomenclature",
            "transplantationDonorId",
            "transplantationType",
        ]
