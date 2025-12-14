from apis.api_interface import IAPI

from .schemas.dcdb import DrugCombAPIResponse, DrugCombData

import requests


class DrugCombDBAPI(IAPI):
    def __init__(self):
        super().__init__(base_url="http://drugcombdb.denglab.org:8888/")

    def get_drug_combination(self, index: int) -> DrugCombData:
        endpoint = f"integration/list/{index}"
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url)
        response.raise_for_status()

        api_response = DrugCombAPIResponse.model_validate(response.json())

        if api_response.code != 200 or api_response.data is None:
            raise ValueError(f"API returned error code {api_response.code}: {api_response.msg}")

        return api_response.data
