from apis.api_interface import IAPI
from domain.models import Drug, PUBCHEM_DISNET_SOURCE_ID

from .schemas.dcdb import DrugCombDBAPIResponse, DrugCombData, DrugData

import requests


class DrugCombDBAPI(IAPI):
    def __init__(self):
        super().__init__(base_url="http://drugcombdb.denglab.org:8888/")

    def get_drug_combination(self, index: int) -> DrugCombData:
        endpoint = f"integration/list/{index}"
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url)
        response.raise_for_status()

        api_response = DrugCombDBAPIResponse[DrugCombData].model_validate(response.json())

        if api_response.code != 200 or api_response.data is None:
            raise ValueError(f"API returned error code {api_response.code}: {api_response.msg}")

        return api_response.data

    def get_drug_info(self, drug_name: str) -> Drug:
        endpoint = f"chemical/info/{drug_name}"
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url)
        response.raise_for_status()

        api_response = DrugCombDBAPIResponse[DrugData].model_validate(response.json())

        if api_response.code != 200 or api_response.data is None:
            raise ValueError(f"API returned error code {api_response.code}: {api_response.msg}")

        drug_data = api_response.data
        drug_id = str(int(drug_data.c_ids[4:]))  # CIDs000xxx -> xxx
        drug = Drug(
            drug_id=drug_id,
            drug_name=drug_data.drug_name_official,
            source_id=PUBCHEM_DISNET_SOURCE_ID,
            chemical_structure=drug_data.smiles_string,
        )
        return drug
