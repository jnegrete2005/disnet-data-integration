import unittest
from unittest.mock import patch, Mock

import requests

from apis.cellosaurus import CellosaurusAPI


class TestCellosaurusAPI(unittest.TestCase):
    def setUp(self):
        self.api = CellosaurusAPI()

    def test_none_cellosaurus_id_raises_value_error(self):
        with self.assertRaises(ValueError):
            self.api.get_cell_line_disease(None)

    @patch("requests.get")
    def test_cell_line_with_disease(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "Cellosaurus": {
                "cell-line-list": [
                    {
                        "disease-list": [
                            {
                                "accession": "C4878",
                                "category": "Medical resources",
                                "database": "NCIt",
                                "iri": "http://purl.obolibrary.org/obo/NCIT_C4878",
                                "label": "Lung carcinoma",
                                "url": "https://evsexplore.semantics.cancer.gov/evsexplore/concept/ncit/C4878",
                            }
                        ]
                    }
                ]
            }
        }
        mock_get.return_value = mock_response

        result = self.api.get_cell_line_disease("CVCL_1059")

        self.assertEqual(result, "C4878")
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_cell_line_without_disease(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "Cellosaurus": {"cell-line-list": [{"disease-list": []}]}
        }
        mock_get.return_value = mock_response

        result = self.api.get_cell_line_disease("CVCL_XXXX")

        self.assertIsNone(result)

    @patch("requests.get")
    def test_http_error_is_raised(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")
        mock_get.return_value = mock_response

        with self.assertRaises(requests.HTTPError):
            self.api.get_cell_line_disease("CVCL_FAIL")
