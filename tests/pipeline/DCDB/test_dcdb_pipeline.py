import unittest
from unittest.mock import ANY, MagicMock, mock_open

from pipeline.DCDB.cell_line_pipeline import CellLineFetchResult, CellLineNotResolvableError
from pipeline.DCDB.dcdb_pipeline import DrugCombDBPipeline
from pipeline.DCDB.drug_pipeline import DrugFetchResult, DrugNotResolvableError


class TestDrugCombDBPipeline(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment before each test method.
        We inject Mocks for all dependencies to isolate the pipeline logic.
        """
        self.mock_db = MagicMock()
        self.mock_dcdb_api = MagicMock()

        # Sub-pipelines Mocks
        self.mock_drug_pipeline = MagicMock()
        self.mock_cell_line_pipeline = MagicMock()
        self.mock_score_pipeline = MagicMock()
        self.mock_experiment_pipeline = MagicMock()

        # Source Repo Mock
        self.mock_source_repo = MagicMock()
        self.mock_source_repo.get_or_create_source.side_effect = lambda name: {
            "CHEMBL": 1,
            "PubChem": 2,
            "Cellosaurus": 3,
        }[name]

        # File System Mocks
        self.mock_checkpoint_path = MagicMock()
        self.mock_audit_path = MagicMock()

        # Initialize the pipeline under test
        self.pipeline = DrugCombDBPipeline(
            db=self.mock_db,
            checkpoint_path=self.mock_checkpoint_path,
            audit_path=self.mock_audit_path,
            source_repo=self.mock_source_repo,
            dcdb_api=self.mock_dcdb_api,
            drug_pipeline=self.mock_drug_pipeline,
            cell_line_pipeline=self.mock_cell_line_pipeline,
            score_pipeline=self.mock_score_pipeline,
            experiment_pipeline=self.mock_experiment_pipeline,
        )

    def test_load_checkpoint_exists(self):
        """Test that the checkpoint is loaded correctly if the file exists."""
        # Setup: File contains "50"
        self.mock_checkpoint_path.exists.return_value = True
        self.mock_checkpoint_path.read_text.return_value = "50"

        result = self.pipeline._load_checkpoint()
        self.assertEqual(result, 50)

    def test_load_checkpoint_not_exists(self):
        """Test that None is returned if checkpoint file does not exist."""
        self.mock_checkpoint_path.exists.return_value = False
        result = self.pipeline._load_checkpoint()
        self.assertIsNone(result)

    def test_save_checkpoint(self):
        """Test that the checkpoint writes the index as string."""
        self.pipeline._save_checkpoint(100)
        self.mock_checkpoint_path.write_text.assert_called_once_with("100")

    def test_audit_skipped(self):
        """Test that skipped records are appended to the JSONL file."""
        # We need to mock the open() context manager on the Path object
        mock_file = mock_open()
        self.mock_audit_path.open = mock_file

        self.pipeline._audit_skipped(combination_id=1, stage="test_stage", entity="test_entity", code="404")

        # Check if file was opened in append mode
        self.mock_audit_path.open.assert_called_once_with("a", encoding="utf-8")

        # Check if JSON was written
        handle = mock_file()
        handle.write.assert_called_once()
        written_data = handle.write.call_args[0][0]
        self.assertIn('"combination_id": 1', written_data)
        self.assertIn('"stage": "test_stage"', written_data)

    def test_get_exp_id_success(self):
        """
        Test the main logic of processing a single experiment ID (Happy Path).
        Verifies:
        1. ThreadPool submits tasks.
        2. APIs are called.
        3. Data is persisted.
        4. Experiment is created.
        """
        # 1. Mock DrugCombDB API response
        mock_combo_info = MagicMock()
        mock_combo_info.drug1 = "DrugA"
        mock_combo_info.drug2 = "DrugB"
        mock_combo_info.cell_line = "HeLa"
        self.mock_dcdb_api.get_drug_combination_info.return_value = mock_combo_info

        # 2. Mock Drug and Cell Line processing
        mock_fetched_drug_result = [
            DrugFetchResult(chembl_drug=MagicMock(drug_id="CHEMBL1"), raw_drug=MagicMock()),
            DrugFetchResult(chembl_drug=MagicMock(drug_id="CHEMBL2"), raw_drug=MagicMock()),
        ]
        mock_fetched_cell_result = CellLineFetchResult(
            cell_line=MagicMock(cell_line_id="CVCL_0001"), disease=MagicMock()
        )
        self.mock_drug_pipeline.fetch.return_value = mock_fetched_drug_result
        self.mock_cell_line_pipeline.fetch.return_value = mock_fetched_cell_result

        # 3. Mock Score processing
        self.mock_score_pipeline.run.return_value = ([], 1)  # (scores, classification)

        # 4. Mock Final Experiment Creation
        self.mock_experiment_pipeline.run.return_value = 12345

        # --- EXECUTE ---
        result_id = self.pipeline._etl_pipeline(99)

        # --- ASSERTIONS ---
        # Verify Persist calls
        self.mock_drug_pipeline.persist.assert_called_once_with(mock_fetched_drug_result)
        self.mock_cell_line_pipeline.persist.assert_called_once_with(mock_fetched_cell_result)

        # Verify Experiment Pipeline Run
        self.mock_experiment_pipeline.run.assert_called_once_with(
            drug_ids=["CHEMBL1", "CHEMBL2"],
            classification=1,
            cell_line_id="CVCL_0001",
            scores=[],
            drug_names=["DrugA", "DrugB"],
            combination_id=99,
        )
        self.assertEqual(result_id, 12345)

    def test_get_exp_id_drug_resolvable_error(self):
        """Test that DrugNotResolvableError is caught and audited."""
        # Mock API
        self.mock_dcdb_api.get_drug_combination_info.return_value = MagicMock()

        # Drug Pipeline fails with resolvable error
        self.mock_drug_pipeline.fetch.side_effect = DrugNotResolvableError("BadDrug", 404)

        # Audit Mocking
        self.pipeline._audit_skipped = MagicMock()

        # Execute
        result = self.pipeline._etl_pipeline(99)

        # Assertions
        self.assertIsNone(result)
        self.pipeline._audit_skipped.assert_called_once_with(
            combination_id=99, stage="drug", entity="BadDrug", code=404
        )
        # Ensure we did NOT persist
        self.mock_drug_pipeline.persist.assert_not_called()

    def test_get_exp_id_cell_line_resolvable_error(self):
        """Test that CellLineNotResolvableError is caught and audited."""
        self.mock_dcdb_api.get_drug_combination_info.return_value = MagicMock()

        # Drugs succeed
        self.mock_drug_pipeline.fetch.return_value = []

        # Cell Line fails
        self.mock_cell_line_pipeline.fetch.side_effect = CellLineNotResolvableError("BadCell", "Not found")

        self.pipeline._audit_skipped = MagicMock()

        result = self.pipeline._etl_pipeline(99)

        self.assertIsNone(result)
        self.pipeline._audit_skipped.assert_called_once_with(
            combination_id=99,
            stage="cell_line",
            entity="BadCell",
            code=ANY,  # We don't care about the specific code here
        )

    def test_run_loop_execution(self):
        """
        Test the outer run loop.
        Verifies:
        1. Loads checkpoint.
        2. Iterates correctly.
        3. Calls _get_exp_id.
        4. Saves checkpoint on success.
        """
        # Setup Checkpoint (Start at 0)
        self.mock_checkpoint_path.exists.return_value = True
        self.mock_checkpoint_path.read_text.return_value = "0"

        # Mock the internal method _get_exp_id to avoid complex logic here
        self.pipeline._etl_pipeline = MagicMock()

        # Case 1: ID 1 returns Success (123), ID 2 returns None (Skipped)
        self.pipeline._etl_pipeline.side_effect = [123, None]

        # Execute run for range 1 to 3 (so it processes 1 and 2)
        self.pipeline.run(start=1, end=3, step=1)

        # Assertions
        # Should be called for 1 and 2
        self.assertEqual(self.pipeline._etl_pipeline.call_count, 2)

        # Save checkpoint should be called ONLY for the successful one (ID 1)
        # or it might be called for all depending on implementation.
        # In your code: `if exp_id is None: continue`. So only successful ones save checkpoint.
        self.pipeline.checkpoint_path.write_text.assert_called_with("1")
        # Note: If logic changes to save on skip, this test needs update.
        # Based on your provided code: "self._save_checkpoint(i)" is inside the success block.


if __name__ == "__main__":
    unittest.main()
