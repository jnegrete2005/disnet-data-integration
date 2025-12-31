from apis.api_interface import APIInterface

import requests


class UniChemAPI(APIInterface):
    def __init__(self):
        super().__init__(base_url="https://www.ebi.ac.uk/unichem/api/v1/")

    def get_compound_mappings(self, compound: str) -> tuple[str | None, str | None]:
        endpoint = "compounds/"
        url = f"{self.base_url}{endpoint}"
        body = {
            "compound": compound,
            "sourceID": 22,  # PubChem
            "type": "sourceID",
        }
        response = requests.post(url, json=body)
        response.raise_for_status()

        data = response.json()

        if not data.get("compounds"):
            return None, None

        compound_data = data["compounds"][
            0
        ]  # Only one compound expected since looking by ID
        inchi_key = compound_data.get("standardInchiKey")

        chembl_id = None
        source = compound_data.get("sources", [None])
        if source and source[0].get("id") == 1:  # ChEMBL source ID
            chembl_id = source[0].get("compoundId")

        return chembl_id, inchi_key
