import unittest
from unittest.mock import patch, Mock

import requests

from apis.umls import UMLSAPI


class TestUMLSAPI(unittest.TestCase):
    def setUp(self):
        self.api = UMLSAPI()
        self.api.api_key = "FAKE_API_KEY"  # nunca usar la real en tests

    def test_none_ncit_id_raises(self):
        with self.assertRaises(ValueError):
            self.api.ncit_to_umls_cui(None)

    @patch("requests.get")
    def test_valid_ncit_returns_cui_and_name(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "result": {"results": [{"ui": "C0006142", "name": "Melanoma"}]}
        }
        mock_get.return_value = mock_response

        cui, name = self.api.ncit_to_umls_cui("C4878")

        self.assertEqual(cui, "C0006142")
        self.assertEqual(name, "Melanoma")
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_valid_ncit_without_results_returns_none_tuple(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"result": {"results": []}}
        mock_get.return_value = mock_response

        cui, name = self.api.ncit_to_umls_cui("NCIT:XXXX")

        self.assertIsNone(cui)
        self.assertIsNone(name)

    @patch("requests.get")
    def test_http_error_is_propagated(self, mock_get):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "401 Unauthorized"
        )
        mock_get.return_value = mock_response

        with self.assertRaises(requests.HTTPError):
            self.api.ncit_to_umls_cui("NCIT:C3058")
