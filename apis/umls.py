from apis.api_interface import IAPI

import requests

from dotenv import load_dotenv
import os


class UMLSAPI(IAPI):
    def __init__(self):
        super().__init__(base_url="https://uts-ws.nlm.nih.gov/rest/")
        load_dotenv("../.env")
        self.api_key = os.getenv('UMLS_API_KEY')

    def get_data(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.base_url}{endpoint}"
        params['apiKey'] = self.api_key
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
