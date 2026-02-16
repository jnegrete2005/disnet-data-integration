import requests

from apis.api_interface import BaseAPI
from apis.schemas.cellosaurus import CellosaurusResponse


class CellosaurusAPI(BaseAPI):
    def __init__(self):
        super().__init__(base_url="https://api.cellosaurus.org/")

    def _fetch_and_parse(self, endpoint: str, params: dict) -> dict:
        # Fetch
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, params=params)
        response.raise_for_status()

        # Parse
        parsed = CellosaurusResponse.model_validate(response.json())
        return parsed.cell_lines[0] if parsed.cell_lines else None

    def get_cell_line_disease(self, cellosaurus_id: str) -> str | None:
        endpoint = f"cell-line/{cellosaurus_id}"
        params = {
            "fields": "din",  # din is diseases from NCIt
            "format": "json",
        }
        entry = self._fetch_and_parse(endpoint, params)
        return entry.ncit_id if entry else None

    def get_cell_line_from_cosmic_id(self, cosmic_id: str) -> tuple[str | None, str | None, str | None]:
        endpoint = "search/cell-line"
        params = {
            "q": f"dr:(Cosmic:{cosmic_id} OR Cosmic-CLP:{cosmic_id})",
            "format": "json",
            "fields": "ac,din,derived-from-site"
        }
        entry = self._fetch_and_parse(endpoint, params)
        if not entry:
            return None, None, None

        return entry.cellosaurus_ac, entry.site, entry.ncit_id
