from dataclasses import dataclass

from chembl_webresource_client.new_client import new_client

from apis.dcdb import DrugCombDBAPI
from apis.unichem import UniChemAPI
from domain.models import Drug
from infraestructure.database import DisnetManager
from pipeline.base_pipeline import ParallelablePipeline
from repo.drug_repo import DrugRepo, ForeignMap


@dataclass(frozen=True)
class DrugFetchResult:
    raw_drug: Drug | None
    chembl_drug: Drug


class DrugPipeline(ParallelablePipeline):
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
        unichem_api: UniChemAPI = UniChemAPI(),
    ):
        self.drug_repo = DrugRepo(db)
        self.dcdb_api = dcdb_api
        self.unichem_api = unichem_api
        self.drug_cache: dict[str, Drug] = {}

    def fetch(self, drug_combination: list[str]) -> list[DrugFetchResult]:
        processed_drugs: list[DrugFetchResult] = []
        for drug_name in drug_combination:
            if "(approved)" in drug_name:
                drug_name = drug_name.replace("(approved)", "").strip()

            if drug_name in self.drug_cache:
                cached_drug = self.drug_cache[drug_name]
                processed_drugs.append(DrugFetchResult(raw_drug=None, chembl_drug=cached_drug))
                continue

            processed_drug = self.__fetch_drug_info(drug_name)
            processed_drugs.append(processed_drug)
            self.drug_cache[drug_name] = processed_drug.chembl_drug
        return processed_drugs

    def __fetch_drug_info(self, drug_name: str) -> DrugFetchResult:
        # Step 1: Extract the drug's data from DrugCombDB
        raw_drug = self.dcdb_api.get_drug_info(drug_name)
        if not raw_drug:
            raise DrugNotResolvableError(drug_name, NOT_FOUND_IN_DCDB_CODE)

        # Step 2: Translate PubChem ID to CHEMBL ID using UniChem API
        chembl_id, inchi_key = self.unichem_api.get_compound_mappings(raw_drug.drug_id)
        raw_drug.inchi_key = inchi_key

        if not chembl_id:
            raise DrugNotResolvableError(drug_name, NOT_FOUND_IN_UNICHEM_CODE)

        # Step 3: Get the drug's data from ChEMBL
        chembl_drug = self.__get_drug_info_from_chembl(chembl_id)
        if not chembl_drug:
            raise DrugNotResolvableError(chembl_id, NOT_FOUND_IN_CHEMBL_CODE)

        return DrugFetchResult(raw_drug=raw_drug, chembl_drug=chembl_drug)

    def persist(self, fetch_results: list[DrugFetchResult]) -> None:
        for result in fetch_results:
            if result.raw_drug:
                self.__persist_raw_drug(result.raw_drug)

            self.__persist_chembl_drug(result.raw_drug, result.chembl_drug)

    @staticmethod
    def __get_drug_info_from_chembl(chembl_id: str) -> Drug | None:
        result = new_client.molecule.filter(molecule_chembl_id=chembl_id).only(
            "molecule_chembl_id", "molecule_structures", "molecule_type", "pref_name"
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
            inchi_key=result["molecule_structures"]["standard_inchi_key"],
        )

    def __persist_raw_drug(self, raw_drug: Drug) -> None:
        self.drug_repo.add_raw_drug(raw_drug)

    def __persist_chembl_drug(self, raw_drug: Drug | None, chembl_drug: Drug) -> None:
        self.drug_repo.add_chembl_drug(chembl_drug)
        if raw_drug is None:
            return

        mapping = ForeignMap(
            foreign_id=raw_drug.drug_id,
            foreign_source_id=raw_drug.source_id,
            chembl_id=chembl_drug.drug_id,
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
        self.reason = reason
