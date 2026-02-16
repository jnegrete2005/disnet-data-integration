import unittest
import sqlite3
from unittest.mock import MagicMock, patch, call

from domain.models import Drug, ForeignMap
# Adjust this import to match your actual file location
from pipeline.DCDB.drug_pipeline import (
    DrugPipeline,
    NOT_FOUND_IN_DCDB_CODE,
    NOT_FOUND_IN_UNICHEM_CODE,
    NOT_FOUND_IN_CHEMBL_CODE
)


class TestStagedDrugPipeline(unittest.TestCase):
    def setUp(self):
        # 1. Setup In-Memory SQLite DB (Fresh for every test)
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE drugs (
                id INTEGER PRIMARY KEY,
                drug_id TEXT,
                drugName TEXT,
                chemical_structure TEXT,
                drugNameOfficial TEXT
            )
        """)
        self.conn.commit()

        # 2. Mock External Dependencies
        self.mock_db_manager = MagicMock()
        self.mock_repo = MagicMock()
        self.mock_dcdb_api = MagicMock()
        self.mock_unichem_api = MagicMock()

        # 3. Instantiate Pipeline
        self.pipeline = DrugPipeline(
            db=self.mock_db_manager,
            chembl_source_id=1,
            pubchem_source_id=2,
            dcdb_api=self.mock_dcdb_api,
            unichem_api=self.mock_unichem_api,
            conn=self.conn,
        )

        # Inject the mock repo explicitly
        self.pipeline.drug_repo = self.mock_repo

    def tearDown(self):
        self.conn.close()

    def _get_row(self, drug_name):
        """Helper to fetch a row from the staging table for assertions."""
        cursor = self.conn.execute("SELECT * FROM staging_drugs WHERE drug_name=?", (drug_name,))
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None

    # =========================================================================
    # TEST: Happy Path (Full Success)
    # =========================================================================

    @patch("pipeline.DCDB.drug_pipeline.new_client")
    def test_full_pipeline_success(self, mock_chembl_client):
        """
        Scenario: Aspirin -> PubChem(123) -> ChEMBL(CHEMBL25) -> Success -> Persist
        """
        drug_name = "Aspirin"
        pubchem_id = "12345"
        chembl_id = "CHEMBL25"
        inchi = "INCHI_KEY_ABC"

        # --- Mocks Setup ---
        # Stage 1: DCDB
        self.mock_dcdb_api.get_drug_info.return_value = Drug(
            drug_id=pubchem_id, drug_name=drug_name, chemical_structure="SMILES_1", source_id=2
        )
        # Stage 2: UniChem
        self.mock_unichem_api.get_compound_mappings.return_value = (chembl_id, inchi)
        # Stage 3: ChEMBL
        mock_chembl_client.molecule.filter.return_value.only.return_value = [{
            "molecule_chembl_id": chembl_id,
            "pref_name": "Aspirin",
            "molecule_type": "Small molecule",
            "molecule_structures": {
                "canonical_smiles": "SMILES_FINAL",
                "standard_inchi_key": inchi
            }
        }]

        # --- EXECUTION ---

        # 1. Stage 0: Init
        self.pipeline.stage_0([drug_name])
        row = self._get_row(drug_name)
        self.assertIsNotNone(row)
        self.assertEqual(row['status'], 0)

        # 2. Stage 1: PubChem
        self.pipeline.stage_1()
        row = self._get_row(drug_name)
        self.assertEqual(row['status'], 1)
        self.assertEqual(row['pubchem_id'], pubchem_id)

        # 3. Stage 2: UniChem
        self.pipeline.stage_2()
        row = self._get_row(drug_name)
        self.assertEqual(row['status'], 2)
        self.assertEqual(row['chembl_id'], chembl_id)
        self.assertEqual(row['inchi_key'], inchi)

        # 4. Stage 3: ChEMBL
        self.pipeline.stage_3()
        row = self._get_row(drug_name)
        self.assertEqual(row['status'], 3)
        self.assertEqual(row['molecular_type'], "Small molecule")
        self.assertEqual(row['chemical_structure'], "SMILES_FINAL")

        # 5. Persist
        self.pipeline.persist()

        # --- ASSERTIONS (Repo) ---
        # Verify ChEMBL Drug Saved
        self.mock_repo.add_chembl_drug.assert_called()
        saved_chembl = self.mock_repo.add_chembl_drug.call_args[0][0]
        self.assertEqual(saved_chembl.drug_id, chembl_id)

    # =========================================================================
    # TEST: Error Handling Scenarios
    # =========================================================================

    def test_stage_1_not_found_in_dcdb(self):
        """
        Scenario: Drug not in DCDB -> Status -1, Error Code DCDB
        """
        drug_name = "GhostDrug"
        self.mock_dcdb_api.get_drug_info.return_value = None

        self.pipeline.stage_0([drug_name])
        self.pipeline.stage_1()

        row = self._get_row(drug_name)
        self.assertEqual(row['status'], -1)
        self.assertEqual(row['error_code'], NOT_FOUND_IN_DCDB_CODE)
        self.assertIsNone(row['pubchem_id'])

    def test_stage_2_not_found_in_unichem(self):
        """
        Scenario: PubChem ID found, but no ChEMBL ID in UniChem -> Status -1
        """
        drug_name = "RareDrug"
        pubchem_id = "999"

        # Setup: Pass Stage 1
        self.mock_dcdb_api.get_drug_info.return_value = Drug(drug_id=pubchem_id, drug_name=drug_name, source_id=2)
        # Setup: Fail Stage 2
        self.mock_unichem_api.get_compound_mappings.return_value = (None, None)

        self.pipeline.stage_0([drug_name])
        self.pipeline.stage_1()  # Status -> 1
        self.pipeline.stage_2()  # Status -> -1

        row = self._get_row(drug_name)
        self.assertEqual(row['status'], -1)
        self.assertEqual(row['error_code'], NOT_FOUND_IN_UNICHEM_CODE)
        self.assertEqual(row['pubchem_id'], pubchem_id)  # Should still be there

    @patch("pipeline.DCDB.drug_pipeline.new_client")
    def test_stage_3_not_found_in_chembl(self, mock_chembl_client):
        """
        Scenario: ChEMBL ID known, but ChEMBL API returns empty -> Status -1
        """
        drug_name = "OldDrug"

        # Setup: Pass Stage 1 & 2
        self.mock_dcdb_api.get_drug_info.return_value = Drug(drug_id="1", drug_name=drug_name, source_id=2)
        self.mock_unichem_api.get_compound_mappings.return_value = ("CHEMBL_OLD", "KEY")

        # Setup: Fail Stage 3
        mock_chembl_client.molecule.filter.return_value.only.return_value = []

        self.pipeline.stage_0([drug_name])
        self.pipeline.stage_1()
        self.pipeline.stage_2()
        self.pipeline.stage_3()

        row = self._get_row(drug_name)
        self.assertEqual(row['status'], -1)
        self.assertEqual(row['error_code'], NOT_FOUND_IN_CHEMBL_CODE)
        self.assertEqual(row['chembl_id'], "CHEMBL_OLD")

    # =========================================================================
    # TEST: Robustness & Idempotency
    # =========================================================================

    def test_idempotency_skip_processed_rows(self):
        """
        Scenario: If Stage 1 is run twice, it should not call the API for rows 
        that are already done or failed.
        """
        drug_name = "DoneDrug"
        self.mock_dcdb_api.get_drug_info.return_value = Drug(drug_id="1", drug_name=drug_name, source_id=2)

        self.pipeline.stage_0([drug_name])

        # Run Stage 1 Once
        self.pipeline.stage_1()
        self.assertEqual(self.mock_dcdb_api.get_drug_info.call_count, 1)

        # Run Stage 1 Again (Should ignore "DoneDrug" because status is 1)
        self.pipeline.stage_1()
        self.assertEqual(self.mock_dcdb_api.get_drug_info.call_count, 1)

    def test_pipeline_resumes_from_crash(self):
        """
        Scenario: Manually insert a row in 'Stage 2 pending' state (simulating a resume after crash).
        Verify Stage 1 skips it and Stage 2 picks it up.
        """
        drug_name = "ResumedDrug"
        pubchem_id = "555"

        # Simulate a crash: Insert row directly with Status=1 (Passed Stage 1)
        self.pipeline.stage_0([drug_name])
        self.conn.execute(
            "UPDATE staging_drugs SET status=1, pubchem_id=? WHERE drug_name=?",
            (pubchem_id, drug_name)
        )
        self.conn.commit()

        # Setup Stage 2 success
        self.mock_unichem_api.get_compound_mappings.return_value = ("CHEMBL555", "KEY")

        # Run Stage 1 (Should skip)
        self.pipeline.stage_1()
        self.mock_dcdb_api.get_drug_info.assert_not_called()

        # Run Stage 2 (Should pick it up)
        self.pipeline.stage_2()
        self.mock_unichem_api.get_compound_mappings.assert_called_with(pubchem_id)

        row = self._get_row(drug_name)
        self.assertEqual(row['status'], 2)
        self.assertEqual(row['chembl_id'], "CHEMBL555")
