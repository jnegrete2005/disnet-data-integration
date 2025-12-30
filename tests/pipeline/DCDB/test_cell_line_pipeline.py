import unittest
from unittest.mock import MagicMock

from pipeline.DCDB.cell_line_pipeline import CellLineDiseasePipeline, CellLineNotResolvableError
from domain.models import CellLine, Disease, COSMIC_DISNET_SOURCE_ID


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
            dcdb_api=self.dcdb_api,
            cellosaurus_api=self.cellosaurus_api,
            umls_api=self.umls_api
        )

        # Inject mocked repo
        self.pipeline.cell_line_repo = self.cell_line_repo

    def test_run_successful_pipeline(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_1059", "Skin")
        self.cellosaurus_api.get_cell_line_disease.return_value = "NCIT_C1234"
        self.umls_api.ncit_to_umls_cui.return_value = ("C0001234", "Melanoma")

        cell_line = self.pipeline.run("A2058")

        self.assertIsInstance(cell_line, CellLine)
        self.assertEqual(cell_line.cell_line_id, "CVCL_1059")
        self.assertEqual(cell_line.name, "A2058")
        self.assertEqual(cell_line.disease_id, "C0001234")
        self.assertEqual(cell_line.source_id, COSMIC_DISNET_SOURCE_ID)

        # Disease inserted
        self.cell_line_repo.add_disease.assert_called_once()
        disease_arg = self.cell_line_repo.add_disease.call_args[0][0]
        self.assertIsInstance(disease_arg, Disease)
        self.assertEqual(disease_arg.umls_cui, "C0001234")

        # Cell line inserted
        self.cell_line_repo.add_cell_line.assert_called_once_with(cell_line)

    def test_run_no_cell_line_found(self):
        self.dcdb_api.get_cell_line_info.return_value = (None, None)

        with self.assertRaises(CellLineNotResolvableError) as context:
            self.pipeline.run("UNKNOWN_LINE")

        self.cell_line_repo.add_disease.assert_not_called()
        self.cell_line_repo.add_cell_line.assert_not_called()

    def test_run_cell_line_without_disease(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_0001", "Tissue_X")
        self.cellosaurus_api.get_cell_line_disease.return_value = None

        cell_line = self.pipeline.run("TEST_LINE")

        self.assertIsInstance(cell_line, CellLine)
        self.assertIsNone(cell_line.disease_id)

        self.cell_line_repo.add_disease.assert_not_called()
        self.cell_line_repo.add_cell_line.assert_called_once()

    def test_run_ncit_without_umls_mapping(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_9999", "Tissue_Y")
        self.cellosaurus_api.get_cell_line_disease.return_value = "NCIT_UNKNOWN"
        self.umls_api.ncit_to_umls_cui.return_value = (None, None)

        cell_line = self.pipeline.run("LINE_X")

        self.assertIsInstance(cell_line, CellLine)
        self.assertIsNone(cell_line.disease_id)

        self.cell_line_repo.add_disease.assert_not_called()
        self.cell_line_repo.add_cell_line.assert_called_once()

    def test_api_calls_sequence(self):
        self.dcdb_api.get_cell_line_info.return_value = ("CVCL_SEQ", "Tissue_SEQ")
        self.cellosaurus_api.get_cell_line_disease.return_value = None

        self.pipeline.run("SEQ_LINE")

        self.dcdb_api.get_cell_line_info.assert_called_once_with("SEQ_LINE")
        self.cellosaurus_api.get_cell_line_disease.assert_called_once_with("CVCL_SEQ")
        self.umls_api.ncit_to_umls_cui.assert_not_called()
