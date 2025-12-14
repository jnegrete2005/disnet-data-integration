from apis.api_interface import IAPI

import requests


class CellosaurusAPI(IAPI):
    def __init__(self):
        super().__init__(base_url="https://api.cellosaurus.org/")

    def get_data(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
