import os

import requests
from dotenv import load_dotenv

from apis.api_interface import BaseAPI


class UMLSAPI(BaseAPI):
    def __init__(self):
        super().__init__(base_url="https://uts-ws.nlm.nih.gov/rest/")
        load_dotenv("../.env")
        self.api_key = os.getenv("UMLS_API_KEY")

    def ncit_to_umls_cui(self, ncit_id: str) -> tuple[str | None, str | None]:
        if ncit_id is None:
            raise ValueError("ncit_id must not be None")

        endpoint = "search/current"
        url = f"{self.base_url}{endpoint}"
        params = {
            "string": ncit_id,
            "inputType": "sourceUi",
            "searchType": "exact",
            "sabs": "NCI",
            "apiKey": self.api_key,
            "pageSize": 1,
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        result = data.get("result", {}).get("results", [{}])
        if not result or len(result) == 0:
            return None, None
        result = result[0]

        if not result:
            return None, None

        return result.get("ui", None), result.get("name", None)
