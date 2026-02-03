import unittest
from unittest.mock import MagicMock

from domain.models import CellLine, Disease
from pipeline.DCDB.cell_line_pipeline import CellLineDiseasePipeline, CellLineNotResolvableError


class TestCellLineDiseasePipeline(unittest.TestCase):
    def setUp(self):
        # Mock DB & Repo
        self.db = MagicMock()
        self.cell_line_repo = MagicMock()

        # Mock APIs
        self.dcdb_api = MagicMock()
        self.cellosaurus_api = MagicMock()
        self.umls_api = MagicMock()

        # Instantiate pipeline
        self.pipeline = CellLineDiseasePipeline(
            db=self.db,
            cellosaurus_source_id=3,
            dcdb_api=self.dcdb_api,
            cellosaurus_api=self.cellosaurus_api,
            umls_api=self.umls_api,
        )

        # Inject mocked repo
        self.pipeline.cell_line_repo = self.cell_line_repo

    def test_run_successful(self):
        """Test the fetch method for a successful retrieval of cell line and disease info."""
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_1059", "Skin")
        self.cellosaurus_api.get_cell_line_disease.return_value = "NCIT_C1234"
        self.umls_api.ncit_to_umls_cui.return_value = ("C0001234", "Melanoma")

        cell_line_fetched = self.pipeline.fetch("A2058")
        cell_line = cell_line_fetched.cell_line
        disease = cell_line_fetched.disease

        self.assertIsInstance(cell_line, CellLine)
        self.assertEqual(cell_line.cell_line_id, "CVCL_1059")
        self.assertEqual(cell_line.name, "A2058")
        self.assertEqual(cell_line.disease_id, "C0001234")
        self.assertEqual(cell_line.source_id, self.pipeline.cellosaurus_source_id)

        # Disease inserted
        self.assertIsInstance(disease, Disease)
        self.assertEqual(disease.umls_cui, "C0001234")

        # Run persist
        self.pipeline.persist(cell_line_fetched)
        self.cell_line_repo.add_disease.assert_called_once_with(disease)
        self.cell_line_repo.add_cell_line.assert_called_once_with(cell_line)

    def test_fetch_no_cell_line_found(self):
        self.dcdb_api.get_cell_line_info.return_value = (None, None)

        with self.assertRaises(CellLineNotResolvableError):
            self.pipeline.fetch("UNKNOWN_LINE")

    def test_run_cell_line_without_disease(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_0001", "Tissue_X")
        self.cellosaurus_api.get_cell_line_disease.return_value = None

        cell_line_fetched = self.pipeline.fetch("TEST_LINE")

        cell_line = cell_line_fetched.cell_line
        self.assertIsInstance(cell_line, CellLine)
        self.assertIsNone(cell_line.disease_id)

        self.pipeline.persist(cell_line_fetched)
        self.cell_line_repo.add_disease.assert_not_called()
        self.cell_line_repo.add_cell_line.assert_called_once()

    def test_run_ncit_without_umls_mapping(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_9999", "Tissue_Y")
        self.cellosaurus_api.get_cell_line_disease.return_value = "NCIT_UNKNOWN"
        self.umls_api.ncit_to_umls_cui.return_value = (None, None)

        cell_line_fetched = self.pipeline.fetch("LINE_X")
        cell_line = cell_line_fetched.cell_line

        self.assertIsInstance(cell_line, CellLine)
        self.assertIsNone(cell_line.disease_id)

        self.pipeline.persist(cell_line_fetched)
        self.cell_line_repo.add_disease.assert_not_called()
        self.cell_line_repo.add_cell_line.assert_called_once()

    def test_api_calls_sequence(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_SEQ", "Tissue_SEQ")
        self.cellosaurus_api.get_cell_line_disease.return_value = None

        self.pipeline.fetch("SEQ_LINE")

        self.dcdb_api.get_cell_line_info.assert_called_once_with("SEQ_LINE")
        self.cellosaurus_api.get_cell_line_disease.assert_called_once_with("CVCL_SEQ")
        self.umls_api.ncit_to_umls_cui.assert_not_called()

    def test_caching_mechanism(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_CACHE", "Tissue_CACHE")
        self.cellosaurus_api.get_cell_line_disease.return_value = "NCIT_CACHE"
        self.umls_api.ncit_to_umls_cui.return_value = ("C000CACHE", "Disease_CACHE")

        # First fetch - should call APIs
        first = self.pipeline.fetch("CACHE_LINE")
        self.dcdb_api.get_cell_line_info.assert_called_once_with("CACHE_LINE")
        self.cellosaurus_api.get_cell_line_disease.assert_called_once_with("CVCL_CACHE")
        self.umls_api.ncit_to_umls_cui.assert_called_once_with("NCIT_CACHE")
        self.assertFalse(first.cached)

        # Reset mocks
        self.dcdb_api.get_cell_line_info.reset_mock()
        self.cellosaurus_api.get_cell_line_disease.reset_mock()
        self.umls_api.ncit_to_umls_cui.reset_mock()

        # Second fetch - should use cache, no API calls
        second = self.pipeline.fetch("CACHE_LINE")
        self.dcdb_api.get_cell_line_info.assert_not_called()
        self.cellosaurus_api.get_cell_line_disease.assert_not_called()
        self.umls_api.ncit_to_umls_cui.assert_not_called()
        self.assertTrue(second.cached)

        self.assertEqual(first.cell_line, second.cell_line)
        self.assertEqual(first.disease, second.disease)
