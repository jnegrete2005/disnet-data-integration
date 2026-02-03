import unittest
from unittest.mock import MagicMock, patch

from domain.models import Drug, ForeignMap

# Adjust imports to match your structure
from pipeline.DCDB.drug_pipeline import (
    NOT_FOUND_IN_CHEMBL_CODE,
    NOT_FOUND_IN_DCDB_CODE,
    NOT_FOUND_IN_UNICHEM_CODE,
    DrugNotResolvableError,
    DrugPipeline,
)


class TestDrugPipeline(unittest.TestCase):
    def setUp(self):
        # 1. Mock DB & Repo
        self.db = MagicMock()
        self.drug_repo = MagicMock()

        # 2. Mock APIs
        self.dcdb_api = MagicMock()
        self.unichem_api = MagicMock()

        # 3. Instantiate pipeline
        self.pipeline = DrugPipeline(
            db=self.db,
            chembl_source_id=1,
            pubchem_source_id=2,
            dcdb_api=self.dcdb_api,
            unichem_api=self.unichem_api,
        )

        # 4. Inject mocked repo (Replacing the real one)
        self.pipeline.drug_repo = self.drug_repo

    @patch("pipeline.DCDB.drug_pipeline.new_client")
    def test_run_successful_full_translation(self, mock_chembl_client):
        """
        Scenario: DCDB -> UniChem -> ChEMBL -> Full Success
        """
        # --- Data Setup ---
        raw_drug = Drug(drug_id="12345", source_id=2, drug_name="Aspirin")
        chembl_id = "CHEMBL25"
        inchi_key = "BSYNRYMUTXBXSQ-UHFFFAOYSA-N"

        self.dcdb_api.get_drug_info.return_value = raw_drug
        self.unichem_api.get_compound_mappings.return_value = (chembl_id, inchi_key)

        # Mock ChEMBL Chain: client.molecule.filter().only()
        mock_chembl_client.molecule.filter.return_value.only.return_value = [
            {
                "molecule_chembl_id": chembl_id,
                "pref_name": "Aspirin",
                "molecule_type": "Small molecule",
                "molecule_structures": {
                    "canonical_smiles": "CC(=O)Oc1ccccc1C(=O)O",
                    "standard_inchi_key": inchi_key,
                },
            }
        ]

        # --- Fetch ---
        result_set = self.pipeline.fetch(["Aspirin"])
        self.pipeline.fetch(["Aspirin"])  # Run twice to test caching

        # --- Assertions ---
        # 1. APIs
        self.dcdb_api.get_drug_info.assert_called_with("Aspirin", self.pipeline.pubchem_source_id)
        self.unichem_api.get_compound_mappings.assert_called_with("12345")
        mock_chembl_client.molecule.filter.assert_called_with(molecule_chembl_id=chembl_id)

        # --- Persist ---
        self.pipeline.persist(result_set)

        # Repo Interactions (Using self.drug_repo)
        self.drug_repo.add_raw_drug.assert_called_once()
        self.drug_repo.add_chembl_drug.assert_called_once()
        self.drug_repo.map_foreign_to_chembl.assert_called_once()

        # 2. Check the mapping object passed to the repo
        mapping_arg = self.drug_repo.map_foreign_to_chembl.call_args[0][0]
        self.assertIsInstance(mapping_arg, ForeignMap)
        self.assertEqual(mapping_arg.foreign_id, "12345")
        self.assertEqual(mapping_arg.chembl_id, chembl_id)

        # 3. Result
        result_drug = result_set[0].chembl_drug
        self.assertIsNotNone(result_drug)
        self.assertEqual(result_drug.drug_id, chembl_id)

    def test_run_not_found_in_unichem(self):
        """
        Scenario: DCDB -> UniChem (No Match) -> Raises error
        """
        # --- Data Setup ---
        self.dcdb_api.get_drug_info.return_value = Drug(drug_id="999", source_id=2, drug_name="RareDrug")
        self.unichem_api.get_compound_mappings.return_value = (None, "SOME_INCHI")

        # --- Run ---
        with self.assertRaises(DrugNotResolvableError) as cm:
            self.pipeline.fetch(["RareDrug"])

        # --- Assertions ---
        self.assertEqual(cm.exception.code, NOT_FOUND_IN_UNICHEM_CODE)

    def test_run_handles_suffix_cleaning(self):
        """
        Scenario: 'Paracetamol (approved)' -> 'Paracetamol'
        """
        self.dcdb_api.get_drug_info.return_value = Drug(drug_id="1", drug_name="Paracetamol", source_id=2)
        self.unichem_api.get_compound_mappings.return_value = (None, None)

        with self.assertRaises(DrugNotResolvableError):
            self.pipeline.fetch(["Paracetamol (approved)"])
            self.dcdb_api.get_drug_info.assert_called_with("Paracetamol")

            self.pipeline.fetch(["Paracetamol(approved)"])
            self.dcdb_api.get_drug_info.assert_called_with("Paracetamol")

    def test_error_drug_not_in_dcdb(self):
        """
        Scenario: DCDB API returns None -> Raise Error Code 1
        """
        self.dcdb_api.get_drug_info.return_value = None

        with self.assertRaises(DrugNotResolvableError) as cm:
            self.pipeline.fetch(["GhostDrug"])

        self.assertEqual(cm.exception.code, NOT_FOUND_IN_DCDB_CODE)

    @patch("pipeline.DCDB.drug_pipeline.new_client")
    def test_error_mapped_but_not_in_chembl(self, mock_chembl_client):
        """
        Scenario: UniChem Maps -> ChEMBL returns empty -> Raise Error Code 2
        """
        # 1. DCDB
        self.dcdb_api.get_drug_info.return_value = Drug(drug_id="1", drug_name="X", source_id=2)
        # 2. UniChem
        self.unichem_api.get_compound_mappings.return_value = ("CHEMBL_OLD", "KEY")
        # 3. ChEMBL (Empty list)
        mock_chembl_client.molecule.filter.return_value.only.return_value = []

        with self.assertRaises(DrugNotResolvableError) as error:
            self.pipeline.fetch(["X"])

        self.assertEqual(error.exception.code, NOT_FOUND_IN_CHEMBL_CODE)
        self.assertEqual(error.exception.code, NOT_FOUND_IN_CHEMBL_CODE)

    @patch("pipeline.DCDB.drug_pipeline.new_client")
    def test_caching_mechanism(self, mock_chembl_client):
        """
        Test that repeated fetches for the same drug use the cache.
        """
        # Setup
        raw_drug = Drug(drug_id="123", source_id=2, drug_name="CachedDrug")
        chembl_id = "CHEMBL123"
        inchi_key = "KEY123"

        self.dcdb_api.get_drug_info.return_value = raw_drug
        self.unichem_api.get_compound_mappings.return_value = (chembl_id, inchi_key)

        mock_chembl_client.molecule.filter.return_value.only.return_value = [
            {
                "molecule_chembl_id": chembl_id,
                "pref_name": "CachedDrug",
                "molecule_type": "Small molecule",
                "molecule_structures": {
                    "canonical_smiles": "SMILES",
                    "standard_inchi_key": inchi_key,
                },
            }
        ]

        # First fetch
        result1 = self.pipeline.fetch(["CachedDrug"])
        # Second fetch (should hit cache)
        result2 = self.pipeline.fetch(["CachedDrug"])

        # Assertions
        self.assertEqual(result1[0].chembl_drug, result2[0].chembl_drug)
        self.assertTrue(result2[0].cached)
        self.dcdb_api.get_drug_info.assert_called_once_with("CachedDrug", self.pipeline.pubchem_source_id)
        self.unichem_api.get_compound_mappings.assert_called_once_with("123")
        mock_chembl_client.molecule.filter.assert_called_once_with(molecule_chembl_id=chembl_id)
