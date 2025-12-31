import unittest
from unittest.mock import patch, Mock

from apis.dcdb import DrugCombDBAPI
from apis.schemas.dcdb import DrugCombData
from domain.models import Drug, PUBCHEM_DISNET_SOURCE_ID


class TestDrugCombDBAPIUnit(unittest.TestCase):
    @patch("apis.dcdb.requests.get")
    def test_get_drug_combination_info_parsing(self, mock_get):
        """
        GIVEN a valid API response
        WHEN get_drug_combination_info is called
        THEN it returns a DrugCombData with correctly parsed fields
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "code": 200,
            "msg": "success",
            "data": {
                "id": 1,
                "drugCombination": "5-FU - ABT-888",
                "drug1": "5-FU(approved)",
                "drug2": "ABT-888",
                "source": "Oneil",
                "cellName": "A2058",
                "HSA": 5.53690281,
                "Bliss": 6.256583897,
                "ZIP": 1.718274208,
                "Loewe": -2.750699326,
            },
        }
        mock_get.return_value = mock_response

        api = DrugCombDBAPI()
        result = api.get_drug_combination_info(1)

        self.assertIsInstance(result, DrugCombData)
        self.assertEqual(result.drug_combination, "5-FU - ABT-888")
        self.assertEqual(result.cell_line, "A2058")
        self.assertAlmostEqual(result.hsa, 5.5369, places=4)
        self.assertAlmostEqual(result.bliss, 6.2566, places=4)

        mock_get.assert_called_once_with(
            "http://drugcombdb.denglab.org:8888/integration/list/1"
        )

    @patch("apis.dcdb.requests.get")
    def test_get_drug_info_transforms_to_domain_drug(self, mock_get):
        """
        GIVEN a valid chemical info API response
        WHEN get_drug_info is called
        THEN it returns a domain Drug with correct fields
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "code": 200,
            "msg": "success",
            "data": {
                "drugNameOfficial": "5-fluorouracil",
                "smilesString": "C1=NC=NC(=O)N1",
                "cIds": "CIDs0003385",
            },
        }
        mock_get.return_value = mock_response

        api = DrugCombDBAPI()
        drug = api.get_drug_info("5-FU")

        self.assertIsInstance(drug, Drug)
        self.assertEqual(drug.drug_name, "5-fluorouracil")
        self.assertEqual(drug.drug_id, "3385")
        self.assertEqual(drug.source_id, PUBCHEM_DISNET_SOURCE_ID)
        self.assertEqual(drug.chemical_structure, "C1=NC=NC(=O)N1")

        mock_get.assert_called_once_with(
            "http://drugcombdb.denglab.org:8888/chemical/info/5-FU"
        )

    @patch("apis.dcdb.requests.get")
    def test_get_cell_line_info_returns_accession(self, mock_get):
        """
        GIVEN a valid cell line API response
        WHEN get_cell_line_info is called
        THEN it returns the cellosaurus accession string
        """

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "code": 200,
            "msg": "success",
            "data": {"cellosaurus_assession": "CVCL_1059"},
        }
        mock_get.return_value = mock_response

        api = DrugCombDBAPI()
        accession = api.get_cell_line_info("A2058")

        self.assertEqual(accession, "CVCL_1059")

        mock_get.assert_called_once_with(
            "http://drugcombdb.denglab.org:8888/cellLine/cellName",
            params={"cellName": "A2058"},
        )
