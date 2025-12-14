from pipeline.base_pipeline import IntegrationPipeline

from apis.dcdb import DrugCombDBAPI

from domain.models import Drug


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
    2. Get the drug's ID from DrugBank through the DrugCombDB API.
    3. Transform DrugBank IDs to CHEMBL IDs using the ChEMBL API.
    4. Load the transformed data into the DISNET database.

    SCORES:
    2. Get the scores from the drug combination.

    FINALLY:
    5. Load all the data into the DISNET database.
    """

    def run(self, start: int = 1, end: int = 2, step: int = 1):
        # Step 1: Extract drug combination data from DrugCombDB
        dcdb_api = DrugCombDBAPI()

        for i in range(start, end, step):
            drugcomb = dcdb_api.get_drug_combination(i)
            if not drugcomb:
                continue

            drugs = [drugcomb.drug1, drugcomb.drug2]
            for drug in drugs:
                pass


class DrugPipeline(IntegrationPipeline):
    """
    Process the individual drugs from a combination in DrugCombDB.

    The pipeline will follow these steps:
    1. Extract the drug's data from DrugCombDB using the DrugCombDBAPI.
    2. Attempt to translate DrugBank IDs to CHEMBL IDs using the UniChem API.
    3. If successful, get the drug's data from ChEMBL.
    4. Load the raw drug (with DrugBank ID) into the DISNET database.
    5. If ChEMBL ID available, load the cured drug into the DISNET database.
    6. "                     " load the translation mapping into the DISNET database.
    """

    def run(self, drug_name: str):
        # Step 1: Extract the drug's data from DrugCombDB
        drug = DrugCombDBAPI().get_drug_info(drug_name)
        if not drug:
            return

        # Step 2: Translate DrugBank ID to CHEMBL ID using UniChem API
