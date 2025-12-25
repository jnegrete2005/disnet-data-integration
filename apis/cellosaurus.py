from apis.api_interface import APIInterface

import requests


class CellosaurusAPI(APIInterface):
    def __init__(self):
        super().__init__(base_url="https://api.cellosaurus.org/")

    def get_cell_line_disease(self, cellosaurus_id: str) -> str | None:
        if cellosaurus_id is None:
            return None
        endpoint = f"cell-line/{cellosaurus_id}"
        url = f"{self.base_url}{endpoint}"
        params = {
            "fields": "din",  # din is diseases from NCIt
            "format": "json"
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        disease = data.get("Cellosaurus", {}) \
            .get("cell-line-list", [{}])[0] \
            .get("disease-list", [{}])[0]

        if not disease:
            return None

        return disease.get("accession")
