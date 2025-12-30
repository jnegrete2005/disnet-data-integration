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

    def __init__(
        self,
        db: DisnetManager,
        dcdb_api: DrugCombDBAPI = DrugCombDBAPI(),
        unichem_api: UniChemAPI = UniChemAPI()
    ):
        self.drug_repo = DrugRepo(db)
        self.dcdb_api = dcdb_api
        self.unichem_api = unichem_api
        self.drug_cache: dict[str, Drug] = {}

    def run(self, drug_combination: list[str]) -> set[Drug]:
        """
        Given a list of drug names from a DrugCombDB combination, transform
        them into ChEMBL drugs, loading all necessary data into the DISNET DB.

        :param drug_combination: List of drug names from a DrugCombDB combination.
        :type drug_combination: list[str]
        :return: Set of processed Drug objects. Must be chembl drugs.
        :rtype: set[Drug]

        :raises DrugNotResolvableError: If any drug cannot be resolved.
        """
        processed_drugs: set[Drug] = set()
        for drug_name in drug_combination:
            if "(approved)" in drug_name:
                drug_name = drug_name.replace("(approved)", "").strip()

            if drug_name in self.drug_cache:
                processed_drugs.add(self.drug_cache[drug_name])
                continue

            processed_drug = self.__get_drug_info(drug_name)

            processed_drugs.add(processed_drug)
            self.drug_cache[drug_name] = processed_drug
        return processed_drugs

    def __get_drug_info(self, drug_name: str) -> Drug:
        # Step 1: Extract the drug's data from DrugCombDB
        raw_drug = self.dcdb_api.get_drug_info(drug_name)
        if not raw_drug:
            raise DrugNotResolvableError(drug_name, NOT_FOUND_IN_DCDB_CODE)

        # Step 2: Translate PubChem ID to CHEMBL ID using UniChem API
        chembl_id, inchi_key = self.unichem_api.get_compound_mappings(raw_drug.drug_id)
        raw_drug.inchi_key = inchi_key

        # Step 3: Load the raw drug (with PubChem ID) into the DISNET database
        self.__persist_raw_drug(raw_drug)

        if not chembl_id:
            raise DrugNotResolvableError(drug_name, NOT_FOUND_IN_UNICHEM_CODE)

        # Step 4: Get the drug's data from ChEMBL
        chembl_drug = self.__get_drug_info_from_chembl(chembl_id)
        if not chembl_drug:
            raise DrugNotResolvableError(chembl_id, NOT_FOUND_IN_CHEMBL_CODE)

        # Step 5: Load the cured drug into the DISNET database
        # Step 6: Load the translation mapping into the DISNET database
        self.__persist_chembl_drug(raw_drug, chembl_drug)

        return chembl_drug

    @staticmethod
    def __get_drug_info_from_chembl(chembl_id: str) -> Drug | None:
        result = new_client.molecule.filter(molecule_chembl_id=chembl_id).only(
            "molecule_chembl_id",
            "molecule_structures",
            "molecule_type",
            "pref_name"
        )
        if not result:
            return None

        result = result[0]

        return Drug(
            drug_id=result["molecule_chembl_id"],
            drug_name=result["pref_name"],
            source_id=1,  # ChEMBL source ID in DISNET
            molecular_type=result["molecule_type"],
            chemical_structure=result["molecule_structures"]["canonical_smiles"],
            inchi_key=result["molecule_structures"]["standard_inchi_key"]
        )

    def __persist_raw_drug(self, raw_drug: Drug) -> None:
        self.drug_repo.get_or_create_raw_drug(raw_drug)

    def __persist_chembl_drug(self, raw_drug: Drug, chembl_drug: Drug) -> None:
        self.drug_repo.get_or_create_chembl_drug(chembl_drug)
        mapping = ForeignMap(
            foreign_id=raw_drug.drug_id,
            foreign_source_id=raw_drug.source_id,
            chembl_id=chembl_drug.drug_id
        )
        self.drug_repo.map_foreign_to_chembl(mapping)


NOT_FOUND_IN_DCDB_CODE = 1
NOT_FOUND_IN_UNICHEM_CODE = 2
NOT_FOUND_IN_CHEMBL_CODE = 3


class DrugNotResolvableError(Exception):
    def __init__(self, drug_name: str, code: int = 0):
        msg = f"Drug '{drug_name}' could not be resolved"
        reason = ""
        if code == NOT_FOUND_IN_DCDB_CODE:
            reason = "not found in DrugCombDB database, despite being in a combination"
        elif code == NOT_FOUND_IN_UNICHEM_CODE:
            reason = "could not find CHEMBL ID mapping in UniChem"
        elif code == NOT_FOUND_IN_CHEMBL_CODE:
            reason = "not found in ChEMBL database, despite being mapped in UniChem"

        if reason:
            msg += f": {reason}"

        super().__init__(msg)
        self.drug_name = drug_name
        self.code = code
