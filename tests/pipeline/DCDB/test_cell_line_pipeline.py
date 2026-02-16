import unittest
import sqlite3
from unittest.mock import MagicMock, call

from domain.models import CellLine, Disease
# Adjust the import path to match your actual project structure
from pipeline.DCDB.cell_line_pipeline import CellLinePipeline


class TestCellLinePipeline(unittest.TestCase):
    def setUp(self):
        # 1. Setup In-Memory SQLite DB
        self.conn = sqlite3.connect(":memory:")

        # Create the local 'cell_lines' table required by Stage 1 (Local Lookup)
        self.conn.execute("""
            CREATE TABLE cell_lines (
                name TEXT PRIMARY KEY,
                cosmic_id TEXT
            )
        """)
        self.conn.commit()

        # 2. Mock External Dependencies
        self.mock_db_manager = MagicMock()
        self.mock_repo = MagicMock()

        self.mock_dcdb_api = MagicMock()
        self.mock_cellosaurus_api = MagicMock()
        self.mock_umls_api = MagicMock()

        # 3. Instantiate Pipeline
        self.pipeline = CellLinePipeline(
            db=self.mock_db_manager,
            cellosaurus_source_id=3,
            dcdb_api=self.mock_dcdb_api,
            cellosaurus_api=self.mock_cellosaurus_api,
            umls_api=self.mock_umls_api,
            conn=self.conn
        )

        # Inject the mock repo
        self.pipeline.cell_line_repo = self.mock_repo

    def tearDown(self):
        self.conn.close()

    def _get_row(self, original_name):
        """Helper to fetch a row from the staging table."""
        cursor = self.conn.execute("SELECT * FROM staging_cell_lines WHERE original_name=?", (original_name,))
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None

    # =========================================================================
    # TEST: Path A - Fast Track (Local Table -> COSMIC -> Cellosaurus + NCIt)
    # =========================================================================

    def test_path_a_fast_track_success(self):
        """
        Scenario: 
        1. 'A2058' is found in local `cell_lines` table -> Gets COSMIC ID.
        2. Cellosaurus API maps COSMIC -> (Accession, Tissue, NCIt).
        3. Pipeline jumps straight to Status 2 (Skipping Stage 2).
        4. Stage 3 maps UMLS.
        """
        cell_name = "A2058"
        cosmic_id = "COSMIC_123"
        cellosaurus_id = "CVCL_1059"
        tissue = "Skin"
        ncit = "NCIT_C1234"
        cui = "C0001234"
        disease_name = "Melanoma"

        # 1. Setup Local Data
        self.conn.execute("INSERT INTO cell_lines (name, cosmic_id) VALUES (?, ?)", (cell_name, cosmic_id))
        self.conn.commit()

        # 2. Mock API (Fast Track returns triplet)
        self.mock_cellosaurus_api.get_cell_line_from_cosmic_id.return_value = (cellosaurus_id, tissue, ncit)
        self.mock_umls_api.ncit_to_umls_cui.return_value = (cui, disease_name)

        # --- EXECUTION ---
        self.pipeline.stage_0([cell_name])

        # Stage 1: Should resolve everything and Jump to Status 2
        self.pipeline.stage_1()

        row = self._get_row(cell_name)
        self.assertEqual(row['status'], 2)  # Important: Fast track skipped status 1
        self.assertEqual(row['cosmic_id'], cosmic_id)
        self.assertEqual(row['cellosaurus_accession'], cellosaurus_id)
        self.assertEqual(row['ncit_accession'], ncit)

        # Stage 2: Should do nothing (row is already status 2)
        self.pipeline.stage_2()
        self.mock_cellosaurus_api.get_cell_line_disease.assert_not_called()

        # Stage 3: UMLS Mapping
        self.pipeline.stage_3()
        row = self._get_row(cell_name)
        self.assertEqual(row['status'], 3)
        self.assertEqual(row['umls_cui'], cui)

        # Persist
        self.pipeline.persist()

        # Verify Repo Calls
        self.mock_repo.add_cell_line.assert_called_once()
        saved_cell = self.mock_repo.add_cell_line.call_args[0][0]
        self.assertEqual(saved_cell.name, cell_name)
        self.assertEqual(saved_cell.disease_id, cui)

        self.mock_repo.add_disease.assert_called_once()

    # =========================================================================
    # TEST: Path B - Fallback (No COSMIC -> DCDB API)
    # =========================================================================

    def test_path_b_fallback_success(self):
        """
        Scenario:
        1. 'UnknownLine' NOT in local table.
        2. Stage 1 falls back to DCDB API -> Gets (Accession, Tissue). Status -> 1.
        3. Stage 2 fetches NCIt. Status -> 2.
        4. Stage 3 fetches UMLS. Status -> 3.
        """
        cell_name = "UnknownLine"
        cellosaurus_id = "CVCL_9999"
        tissue = "Lung"
        ncit = "NCIT_C5678"

        # 1. Setup Mocks (DCDB fallback)
        self.mock_dcdb_api.get_cell_line_info.return_value = (cellosaurus_id, tissue)
        self.mock_cellosaurus_api.get_cell_line_disease.return_value = ncit
        self.mock_umls_api.ncit_to_umls_cui.return_value = (None, None)  # Valid case: No UMLS mapping

        # --- EXECUTION ---
        self.pipeline.stage_0([cell_name])

        # Stage 1: Fallback path -> Status 1
        self.pipeline.stage_1()
        row = self._get_row(cell_name)
        self.assertEqual(row['status'], 1)
        self.assertEqual(row['cellosaurus_accession'], cellosaurus_id)
        self.assertIsNone(row['ncit_accession'])  # Not fetched yet

        # Stage 2: Fetch NCIt
        self.pipeline.stage_2()
        row = self._get_row(cell_name)
        self.assertEqual(row['status'], 2)
        self.assertEqual(row['ncit_accession'], ncit)

        # Stage 3: UMLS (Returns None, still status 3)
        self.pipeline.stage_3()
        row = self._get_row(cell_name)
        self.assertEqual(row['status'], 3)
        self.assertIsNone(row['umls_cui'])

        # Persist
        self.pipeline.persist()
        self.mock_repo.add_cell_line.assert_called_once()
        saved_cell = self.mock_repo.add_cell_line.call_args[0][0]
        self.assertIsNone(saved_cell.disease_id)
        self.mock_repo.add_disease.assert_not_called()

    # =========================================================================
    # TEST: Error Handling
    # =========================================================================

    def test_stage_1_completely_unresolvable(self):
        """Scenario: Not in local table AND DCDB API returns None."""
        cell_name = "GhostCell"

        # Mocks return failure
        self.mock_dcdb_api.get_cell_line_info.return_value = (None, None)

        self.pipeline.stage_0([cell_name])
        self.pipeline.stage_1()

        row = self._get_row(cell_name)
        self.assertEqual(row['status'], -1)
        self.assertIn("Not found", row['error_msg'])

    def test_stage_2_api_error(self):
        """Scenario: Stage 1 succeeds, but Stage 2 API raises exception."""
        cell_name = "ErrorCell"

        # Stage 1 succeeds (Fallback)
        self.mock_dcdb_api.get_cell_line_info.return_value = ("CVCL_ERR", "Tissue")
        # Stage 2 fails
        self.mock_cellosaurus_api.get_cell_line_disease.side_effect = Exception("API Down")

        self.pipeline.stage_0([cell_name])
        self.pipeline.stage_1()  # Status 1
        self.pipeline.stage_2()  # Should fail to -1

        row = self._get_row(cell_name)
        self.assertEqual(row['status'], -1)
        self.assertEqual(row['error_msg'], "API Down")

    # =========================================================================
    # TEST: Resume / State Logic
    # =========================================================================

    def test_resume_from_stage_2(self):
        """
        Scenario: Row exists in DB with status=1. 
        Stage 1 should ignore it. Stage 2 should pick it up.
        """
        cell_name = "ResumeCell"
        ncit = "NCIT_RES"

        # Manually insert a row in 'Status 1' state
        self.pipeline.stage_0([cell_name])
        self.conn.execute(
            "UPDATE staging_cell_lines SET status=1, cellosaurus_accession='CVCL_RES' WHERE original_name=?",
            (cell_name,)
        )
        self.conn.commit()

        self.mock_cellosaurus_api.get_cell_line_disease.return_value = ncit

        # Run Stage 1 (Should verify it doesn't touch it)
        self.pipeline.stage_1()
        self.mock_dcdb_api.get_cell_line_info.assert_not_called()
        self.mock_cellosaurus_api.get_cell_line_from_cosmic_id.assert_not_called()

        # Run Stage 2
        self.pipeline.stage_2()
        self.mock_cellosaurus_api.get_cell_line_disease.assert_called_with("CVCL_RES")

        row = self._get_row(cell_name)
        self.assertEqual(row['status'], 2)
        self.assertEqual(row['ncit_accession'], ncit)
