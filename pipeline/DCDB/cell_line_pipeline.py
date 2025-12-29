from pipeline.base_pipeline import IntegrationPipeline

from apis.dcdb import DrugCombDBAPI
from apis.cellosaurus import CellosaurusAPI
from apis.umls import UMLSAPI

from domain.models import Disease, CellLine, COSMIC_DISNET_SOURCE_ID

from infraestructure.database import DisnetManager

from repo.cell_line_repo import CellLineRepo


class CellLineDiseasePipeline(IntegrationPipeline):
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
        dcdb_api: DrugCombDBAPI,
        cellosaurus_api: CellosaurusAPI,
        umls_api: UMLSAPI
    ):
        self.cell_line_repo = CellLineRepo(db)
        self.dcdb_api = dcdb_api or DrugCombDBAPI()
        self.cellosaurus_api = cellosaurus_api or CellosaurusAPI()
        self.umls_api = umls_api or UMLSAPI()

    def run(self, cell_line_name: str) -> CellLine | None:
        # Step 1: Extract the cell line Cellosaurus ID from DrugCombDB
        cellosaurus_accession, tissue = self.dcdb_api.get_cell_line_info(cell_line_name)
        if cellosaurus_accession is None:
            return None

        umls_cui = None

        # Step 2: Get the cell line's associated disease with the Cellosaurus API
        ncit_accession = self.cellosaurus_api.get_cell_line_disease(cellosaurus_accession)

        if ncit_accession is not None:
            # Step 3: Transform the disease ID (NCIt) to UMLS CUI using the UMLS API
            umls_cui, disease_name = self.umls_api.ncit_to_umls_cui(ncit_accession)
            if umls_cui is not None:
                # Step 4: Load the disease into the DISNET database
                disease = Disease(
                    umls_cui=umls_cui,
                    name=disease_name
                )
                self.cell_line_repo.add_disease(disease)

        # Step 5: Load the cell line into the DISNET database
        cell_line = CellLine(
            cell_line_id=cellosaurus_accession,
            source_id=COSMIC_DISNET_SOURCE_ID,
            name=cell_line_name,
            tissue=tissue,
            disease_id=umls_cui
        )
        self.cell_line_repo.add_cell_line(cell_line)
        return cell_line
