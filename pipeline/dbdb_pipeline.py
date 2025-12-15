from pipeline.base_pipeline import IntegrationPipeline

from apis.dcdb import DrugCombDBAPI
from apis.unichem import UniChemAPI

from domain.models import Drug

from repo.drug_repo import DrugRepo, ForeignMap

from infraestructure.database import DisnetManager

from chembl_webresource_client.new_client import new_client


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

        for i in range(start, end, step):
            drugcomb = dcdb_api.get_drug_combination(i)
            if not drugcomb:
                continue

            drugs = [drugcomb.drug1, drugcomb.drug2]

            for drug in drugs:
                if "(approved)" in drug:
                    drug = drug.replace("(approved)", "").strip()

                DrugPipeline.run(drug.drug_name)


class DrugPipeline(IntegrationPipeline):
    """
    Process the individual drugs from a combination in DrugCombDB.

    The pipeline will follow these steps:
    1. Extract the drug's data from DrugCombDB using the DrugCombDBAPI.
    2. Attempt to translate PubChem IDs to CHEMBL IDs using the UniChem API.
    3. Load the raw drug (with PubChem ID) into the DISNET database.
    4. If CHEMBL ID available, get the drug's data from ChEMBL.
    5. "                     " load the cured drug into the DISNET database.
    6. "                     " load the translation mapping into the DISNET database.
    """
    @classmethod
    def run(cls, drug_name: str) -> Drug | None:
        # Step 1: Extract the drug's data from DrugCombDB
        raw_drug = DrugCombDBAPI().get_drug_info(drug_name)
        if not raw_drug:
            raise ValueError(f"Drug {drug_name} not found in DrugCombDB database.")

        # Step 2: Translate PubChem ID to CHEMBL ID using UniChem API
        chembl_id, inchi_key = UniChemAPI().get_compound_mappings(raw_drug.drug_id)
        raw_drug.inchi_key = inchi_key

        drug_repo = DrugRepo(DisnetManager())

        # Step 3: Load the raw drug (with PubChem ID) into the DISNET database
        drug_repo.get_or_create_raw_drug(raw_drug)

        if chembl_id:
            # Step 4: Get the drug's data from ChEMBL
            chembl_drug = cls.__get_drug_info_from_chembl(chembl_id)
            if not chembl_drug:
                raise ValueError(f"CHEMBL ID {chembl_id} not found in ChEMBL database.")

            # Step 5: Load the cured drug into the DISNET database
            drug_repo.get_or_create_chembl_drug(chembl_drug)

            # Step 6: Load the translation mapping into the DISNET database
            mapping = ForeignMap(
                foreign_id=raw_drug.drug_id,
                foreign_source_id=raw_drug.source_id,
                chembl_id=chembl_drug.drug_id
            )
            drug_repo.map_foreign_to_chembl(mapping)

            return chembl_drug

        return None

    @staticmethod
    def __get_drug_info_from_chembl(chembl_id: str) -> Drug | None:
        result = new_client.molecule.filter(molecule_chembl_id=chembl_id).only(
            "molecule_chembl_id",
            "molecule_structures",
            "molecule_type",
            "pref_name"
        )[0]

        if not result:
            return None

        return Drug(
            drug_id=result["molecule_chembl_id"],
            drug_name=result["pref_name"],
            source_id=1,  # ChEMBL source ID in DISNET
            molecular_type=result["molecule_type"],
            chemical_structure=result["molecule_structures"]["canonical_smiles"],
            inchi_key=result["molecule_structures"]["standard_inchi_key"]
        )
