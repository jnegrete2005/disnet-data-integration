import unittest
import warnings
from unittest.mock import MagicMock

# Adjust imports based on your actual file structure
from pipeline.DCDB.experiment_pipeline import ExperimentPipeline, AdditiveExperimentWarning
from infraestructure.database import DisnetManager
from repo.drugcomb_repo import DrugCombRepo
from repo.experiment_repo import ExperimentRepo
from domain.models import Experiment, Score


class TestExperimentPipeline(unittest.TestCase):
    """
    Unit tests for ExperimentPipeline.
    It verifies the flow of creating combinations, classifications, sources, and experiments,
    including the handling of additive warnings.
    """

    def setUp(self):
        """
        Set up the test environment.
        We mock the database and the repositories to avoid actual DB connections.
        """
        # Mock the database manager
        self.mock_db = MagicMock(spec=DisnetManager)

        # Mock the repositories
        self.mock_drug_comb_repo = MagicMock(spec=DrugCombRepo)
        self.mock_experiment_repo = MagicMock(spec=ExperimentRepo)

        # Initialize the pipeline with injected mock repositories
        self.pipeline = ExperimentPipeline(
            db=self.mock_db,
            drug_comb_repo=self.mock_drug_comb_repo,
            experiment_repo=self.mock_experiment_repo
        )

        # Common test data
        self.dummy_drug_ids = ["D1", "D2"]
        self.dummy_drug_names = ["DrugA", "DrugB"]
        self.dummy_scores = [Score(score_id=1, score_name="ZIP", score_value=10.5)]
        self.dummy_cell_line = "CL-123"
        self.dummy_comb_id = 999

        # Setup default return values for IDs to track flow
        self.mock_drug_comb_repo.get_or_create_combination.return_value = 10  # drug_comb_id
        self.mock_experiment_repo.get_or_create_exp_source.return_value = 50  # source_id
        self.mock_experiment_repo.get_or_create_experiment.return_value = 100  # experiment_id

    def test_run_synergistic_flow(self):
        """
        Test the pipeline when classification is positive (Synergistic).
        It should map the class name correctly and create the experiment without warnings.
        """
        # Arrange
        classification_input = 1  # Positive = Synergistic
        self.mock_experiment_repo.get_or_create_exp_class.return_value = 20  # class_id

        # Act
        result_id = self.pipeline.run(
            drug_ids=self.dummy_drug_ids,
            classification=classification_input,
            cell_line_id=self.dummy_cell_line,
            scores=self.dummy_scores,
            drug_names=self.dummy_drug_names,
            combination_id=self.dummy_comb_id
        )

        # Assert
        # 1. Verify Repositories were called
        self.mock_drug_comb_repo.get_or_create_combination.assert_called_with(self.dummy_drug_ids)

        # 2. Verify Classification logic
        self.mock_experiment_repo.get_or_create_exp_class.assert_called_with("Synergistic")

        # 3. Verify Source logic (Hardcoded to DrugCombDB in your code)
        self.mock_experiment_repo.get_or_create_exp_source.assert_called_with("DrugCombDB")

        # 4. Verify Experiment creation
        # We capture the argument passed to get_or_create_experiment to inspect the object attributes
        args, _ = self.mock_experiment_repo.get_or_create_experiment.call_args
        experiment_obj = args[0]

        self.assertIsInstance(experiment_obj, Experiment)
        self.assertEqual(experiment_obj.dc_id, 10)  # From drug_comb_repo mock
        self.assertEqual(experiment_obj.experiment_classification_id, 20)  # From class repo mock
        self.assertEqual(experiment_obj.experiment_source_id, 50)  # From source repo mock
        self.assertEqual(experiment_obj.cell_line_id, self.dummy_cell_line)

        # 5. Verify return value
        self.assertEqual(result_id, 100)

    def test_run_antagonistic_flow(self):
        """
        Test the pipeline when classification is negative (Antagonistic).
        """
        # Arrange
        classification_input = -1  # Negative = Antagonistic

        # Act
        self.pipeline.run(
            drug_ids=self.dummy_drug_ids,
            classification=classification_input,
            cell_line_id=self.dummy_cell_line,
            scores=self.dummy_scores,
            drug_names=self.dummy_drug_names,
            combination_id=self.dummy_comb_id
        )

        # Assert
        self.mock_experiment_repo.get_or_create_exp_class.assert_called_with("Antagonistic")

    def test_run_additive_warning_triggered(self):
        """
        Test the pipeline when classification is 0 (Additive).
        It MUST trigger an AdditiveExperimentWarning but still proceed to create the experiment.
        """
        # Arrange
        classification_input = 0  # 0 = Additive

        # Act & Assert
        # We use strict context manager to catch warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")  # Cause all warnings to always be triggered

            self.pipeline.run(
                drug_ids=self.dummy_drug_ids,
                classification=classification_input,
                cell_line_id=self.dummy_cell_line,
                scores=self.dummy_scores,
                drug_names=self.dummy_drug_names,
                combination_id=self.dummy_comb_id
            )

            # Check if warning was caught
            self.assertEqual(len(w), 1, "Expected exactly one warning")
            self.assertTrue(issubclass(w[-1].category, AdditiveExperimentWarning))
            self.assertIn("classified as Additive", str(w[-1].message))
            self.assertIn("DrugA, DrugB", str(w[-1].message))

        # Even with warning, the pipeline should proceed:
        self.mock_experiment_repo.get_or_create_exp_class.assert_called_with("Additive")
        self.mock_experiment_repo.get_or_create_experiment.assert_called()

    def test_dependency_injection_default(self):
        """
        Test that the pipeline initializes its own repositories if None are provided.
        This verifies the __init__ logic: self.repo = repo or Repo(db)
        """
        # Act
        pipeline = ExperimentPipeline(db=self.mock_db)

        # Assert
        self.assertIsInstance(pipeline.drug_comb_repo, DrugCombRepo)
        self.assertIsInstance(pipeline.experiment_repo, ExperimentRepo)
        # Verify they were initialized with the DB manager
        self.assertEqual(pipeline.drug_comb_repo.db, self.mock_db)
