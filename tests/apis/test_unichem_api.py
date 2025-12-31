import unittest
from unittest.mock import Mock, patch

from apis.unichem import UniChemAPI


class TestUniChemAPIUnit(unittest.TestCase):
    @patch("apis.unichem.requests.post")
    def test_get_compound_mappings_success(self, mock_post):
        """
        GIVEN a valid UniChem API response with ChEMBL source
        WHEN get_compound_mappings is called
        THEN it returns (chembl_id, inchi_key)
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "compounds": [
                {
                    "standardInchiKey": "ABCDEF-GHIJKL-MNOPQR",
                    "sources": [{"id": 1, "compoundId": "CHEMBL25"}],
                }
            ]
        }
        mock_post.return_value = mock_response

        api = UniChemAPI()
        chembl_id, inchi_key = api.get_compound_mappings("3385")

        self.assertEqual(chembl_id, "CHEMBL25")
        self.assertEqual(inchi_key, "ABCDEF-GHIJKL-MNOPQR")

        mock_post.assert_called_once_with(
            "https://www.ebi.ac.uk/unichem/api/v1/compounds/",
            json={"compound": "3385", "sourceID": 22, "type": "sourceID"},
        )

    @patch("apis.unichem.requests.post")
    def test_get_compound_mappings_no_compounds(self, mock_post):
        """
        GIVEN a UniChem API response with no compounds
        WHEN get_compound_mappings is called
        THEN it returns (None, None)
        """

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"compounds": []}
        mock_post.return_value = mock_response

        api = UniChemAPI()
        chembl_id, inchi_key = api.get_compound_mappings("999999")

        self.assertIsNone(chembl_id)
        self.assertIsNone(inchi_key)

    @patch("apis.unichem.requests.post")
    def test_get_compound_mappings_without_chembl_source(self, mock_post):
        """
        GIVEN a UniChem API response without ChEMBL source
        WHEN get_compound_mappings is called
        THEN it returns (None, inchi_key)
        """

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "compounds": [
                {
                    "standardInchiKey": "XYZ-123",
                    "sources": [{"id": 22, "compoundId": "3385"}],
                }
            ]
        }
        mock_post.return_value = mock_response

        api = UniChemAPI()
        chembl_id, inchi_key = api.get_compound_mappings("3385")

        self.assertIsNone(chembl_id)
        self.assertEqual(inchi_key, "XYZ-123")

    @patch("apis.unichem.requests.post")
    def test_get_compound_mappings_missing_fields(self, mock_post):
        """
        GIVEN a UniChem API response with missing optional fields
        WHEN get_compound_mappings is called
        THEN it handles it gracefully
        """

        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"compounds": [{"sources": []}]}
        mock_post.return_value = mock_response

        api = UniChemAPI()
        chembl_id, inchi_key = api.get_compound_mappings("3385")

        self.assertIsNone(chembl_id)
        self.assertIsNone(inchi_key)
