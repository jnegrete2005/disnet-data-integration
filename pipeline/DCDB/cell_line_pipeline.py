from dataclasses import dataclass, replace

from apis.cellosaurus import CellosaurusAPI
from apis.dcdb import DrugCombDBAPI
from apis.umls import UMLSAPI
from caching.cache import CacheDict
from domain.models import CellLine, Disease
from infraestructure.database import DisnetManager
from pipeline.base_pipeline import ParallelablePipeline
from repo.cell_line_repo import CellLineRepo


@dataclass(frozen=True)
class CellLineFetchResult:
    cell_line: CellLine
    disease: Disease | None
    cached: bool = False


class CellLineDiseasePipeline(ParallelablePipeline):
    """
    Process the cell line and the disease associated with a drug combination in DrugCombDB.

    The pipeline will follow these steps:
    1. Extract the cell line Cellosaurus ID from DrugCombDB using the DrugCombDBAPI.
    2. Get the cell line's associated disease with the Cellosaurus API.
    3. Transform the disease ID (NCIt) to UMLS CUI using the UMLS API.
    4. Load the disease into the DISNET database.
    5. Load the cell line into the DISNET database.
    """

    def __init__(
        self,
        db: DisnetManager,
        cellosaurus_source_id: int,
        dcdb_api: DrugCombDBAPI = None,
        cellosaurus_api: CellosaurusAPI = None,
        umls_api: UMLSAPI = None,
    ):
        self.cell_line_repo = CellLineRepo(db)
        self.dcdb_api = dcdb_api or DrugCombDBAPI()
        self.cellosaurus_source_id = cellosaurus_source_id
        self.cellosaurus_api = cellosaurus_api or CellosaurusAPI()
        self.umls_api = umls_api or UMLSAPI()

        # Cell line and disease caching systems
        self.cache: CacheDict[CellLineFetchResult] = CacheDict()
        self.error_cache: CacheDict[CellLineNotResolvableError] = CacheDict()

    def fetch(self, cell_line_name: str) -> CellLineFetchResult:
        # Check if the cell line is already cached
        if cell_line_name in self.cache:
            return self.cache[cell_line_name]
        elif cell_line_name in self.error_cache:
            raise self.error_cache[cell_line_name]

        # Step 1: Extract the cell line Cellosaurus ID from DrugCombDB
        cellosaurus_accession, tissue = self.dcdb_api.get_cell_line_info(cell_line_name)
        if cellosaurus_accession is None:
            error = CellLineNotResolvableError(cell_line_name, "not found in DrugCombDB database.")
            self.error_cache[cell_line_name] = error
            raise error

        umls_cui = None

        # Step 2: Get the cell line's associated disease with the Cellosaurus API
        ncit_accession = self.cellosaurus_api.get_cell_line_disease(cellosaurus_accession)

        if ncit_accession is not None:
            # Step 3: Transform the disease ID (NCIt) to UMLS CUI using the UMLS API
            umls_cui, disease_name = self.umls_api.ncit_to_umls_cui(ncit_accession)
            if umls_cui is not None:
                # Step 4: Load the disease into the DISNET database
                disease = Disease(umls_cui=umls_cui, name=disease_name)

        cell_line = CellLine(
            cell_line_id=cellosaurus_accession,
            source_id=self.cellosaurus_source_id,
            name=cell_line_name,
            tissue=tissue,
            disease_id=umls_cui,
        )
        result = CellLineFetchResult(cell_line=cell_line, disease=disease if umls_cui is not None else None)
        self.cache[cell_line_name] = replace(result, cached=True)
        return result

    def persist(self, fetch_result: CellLineFetchResult):
        if fetch_result.cached:
            return

        if fetch_result.disease:
            self.cell_line_repo.add_disease(fetch_result.disease)
        self.cell_line_repo.add_cell_line(fetch_result.cell_line)


class CellLineNotResolvableError(Exception):
    """
    Exception raised when a cell line cannot be resolved.
    """

    def __init__(self, cell_line_name: str, reason: str = None):
        msg = f"Cell line '{cell_line_name}' could not be resolved"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)
        self.cell_line_name = cell_line_name
        self.reason = reason
