from chembl_webresource_client.new_client import new_client

from pipeline.base_pipeline import IntegrationPipeline

from apis.dcdb import DrugCombDBAPI
from apis.unichem import UniChemAPI

from domain.models import Drug

from repo.drug_repo import DrugRepo, ForeignMap

from infraestructure.database import DisnetManager


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
    def run(cls, drug_combination: list[str]) -> set[Drug] | None:
        processed_drugs: set[Drug] = set()
        for drug_name in drug_combination:
            if "(approved)" in drug_name:
                drug_name = drug_name.replace("(approved)", "").strip()

            processed_drug = cls.__get_drug_info(drug_name)
            if processed_drug:
                processed_drugs.add(processed_drug)

        return processed_drugs if processed_drugs else None

    @classmethod
    def __get_drug_info(cls, drug_name: str) -> Drug:
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

        return raw_drug

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
