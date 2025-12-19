from pipeline.base_pipeline import IntegrationPipeline

from apis.dcdb import DrugCombDBAPI

from domain.models import Drug

from .drug_pipeline import DrugPipeline


class DrugCombDBPipeline(IntegrationPipeline):
    """
    Integrate drug combination data from DrugCombDB into DISNET.

    The pipeline will follow these steps:
    1. Extract drug combination data from DrugCombDB using the DrugCombDBAPI.

    CELL_LINE AND DISEASE:
    2. Get the cell line's ID from Cellosaurus
    3. Transform the NCIt disease IDs to UMLS CUIs using the UMLS API.
    4. Load the transformed data into the DISNET database.

    DRUGS:
    2. Get the drug's ID from PubChem through the DrugCombDB API.
    3. Transform PubChem IDs to CHEMBL IDs using the ChEMBL API.
    4. Load the transformed data into the DISNET database.

    SCORES:
    2. Get the scores from the drug combination.

    FINALLY:
    5. Load all the data into the DISNET database.
    """

    def run(self, start: int = 1, end: int = 2, step: int = 1):
        # Step 1: Extract drug combination data from DrugCombDB
        dcdb_api = DrugCombDBAPI()

        # TODO: Optimize to save DCs in a dict to avoid quering the DB multiple times
        for i in range(start, end, step):
            drugcomb = dcdb_api.get_drug_combination(i)
            if not drugcomb:
                continue

            drugs = [drugcomb.drug1, drugcomb.drug2]
            chembl_drugs: list[Drug | None] = []

            chembl_drugs.append(DrugPipeline.run(drugs))
