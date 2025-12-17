from pipeline.base_pipeline import IntegrationPipeline

from apis.dcdb import DrugCombDBAPI
from apis.cellosaurus import CellosaurusAPI
from apis.umls import UMLSAPI

from repo.metadata_repo import MetadataRepo

from domain.models import Disease, CellLine, COSMIC_DISNET_SOURCE_ID


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
    @classmethod
    def run(cls, cell_line_name: str) -> tuple[CellLine | None, Disease | None]:
        # Step 1: Extract the cell line Cellosaurus ID from DrugCombDB
        cell_line_info = DrugCombDBAPI().get_cell_line_info(cell_line_name)
        cosmic_id = cell_line_info.get("cosmic_id")
        cellosaurus_id = cell_line_info.get("cellosaurus_id")

        # Step 2: Get the cell line's associated disease with the Cellosaurus API
        ncit_id = CellosaurusAPI().get_cell_line_disease(cellosaurus_id)

        # Step 3: Transform the disease ID (NCIt) to UMLS CUI using the UMLS API
        umls_cui, disease_name = UMLSAPI().ncit_to_umls_cui(ncit_id)

        # Step 4: Load the disease into the DISNET database
        disease = Disease(
            umls_cui=umls_cui,
            name=disease_name
        )
        MetadataRepo().get_or_create_disease_id(disease)

        # Step 5: Load the cell line into the DISNET database
        cell_line = CellLine(
            cell_line_id=cosmic_id,
            source_id=COSMIC_DISNET_SOURCE_ID,
            name=cell_line_name,
            disease_id=umls_cui
        )
        MetadataRepo().get_or_create_cell_line_id(cell_line)

        return cell_line, disease
