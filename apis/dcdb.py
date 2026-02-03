import requests

from apis.api_interface import APIInterface
from domain.models import Drug

from .schemas.dcdb import DrugCombData, DrugCombDBAPIResponse, DrugData


class DrugCombDBAPI(APIInterface):
    def __init__(self):
        super().__init__(base_url="http://drugcombdb.denglab.org:8888/")

    def get_drug_combination_info(self, index: int) -> DrugCombData:
        endpoint = f"integration/list/{index}"
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url)
        response.raise_for_status()

        api_response = DrugCombDBAPIResponse[DrugCombData].model_validate(response.json())

        if api_response.code != 200 or api_response.data is None:
            raise ValueError(f"API returned error code {api_response.code}: {api_response.msg}")

        return api_response.data

    def get_drug_info(self, drug_name: str, pubchem_source_id: int) -> Drug:
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
            source_id=pubchem_source_id,
            chemical_structure=drug_data.smiles_string,
        )
        return drug

    def get_cell_line_info(self, cell_line_name: str) -> tuple[str | None, str | None]:
        """
        Get the Cellosaurus accession and tissue for a given cell line name.

        :param cell_line_name: Name of the cell line.
        :type cell_line_name: str

        :return: A tuple containing the Cellosaurus accession and tissue.
        :rtype: tuple[str | None, str | None]
        """
        endpoint = "cellLine/cellName"
        url = f"{self.base_url}{endpoint}"
        params = {"cellName": cell_line_name}
        response = requests.get(url, params=params)
        response.raise_for_status()

        api_response = DrugCombDBAPIResponse[dict].model_validate(response.json())
        if api_response.code != 200 or api_response.data is None:
            raise ValueError(f"API returned error code {api_response.code}: {api_response.msg}")

        return api_response.data.get("cellosaurus_assession"), api_response.data.get("tissue")
