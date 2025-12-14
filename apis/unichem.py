from apis.api_interface import IAPI

import requests


class UniChemAPI(IAPI):
    def __init__(self):
        super().__init__(base_url="https://www.ebi.ac.uk/unichem/api/v1/")

    def get_data(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.base_url}{endpoint}"
        response = requests.post(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
