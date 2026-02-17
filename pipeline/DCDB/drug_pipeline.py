import sqlite3
import logging

from chembl_webresource_client.new_client import new_client
from pathlib import Path

from apis.dcdb import DrugCombDBAPI
from apis.unichem import UniChemAPI
from domain.models import Drug
from infraestructure.database import DisnetManager
from repo.drug_repo import DrugRepo

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False

BATCH_SIZE = 1000


class DrugPipeline:
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
        chembl_source_id: int,
        pubchem_source_id: int,
        dcdb_api: DrugCombDBAPI = DrugCombDBAPI(),
        unichem_api: UniChemAPI = UniChemAPI(),
        conn: sqlite3.Connection = None,
        from_local: bool = False,
    ):
        self.drug_repo = DrugRepo(db)
        self.sqlite_conn = conn

        self.dcdb_api = dcdb_api
        self.unichem_api = unichem_api

        self.chembl_source_id = chembl_source_id
        self.pubchem_source_id = pubchem_source_id

        self.local = from_local

        log_path = Path("logs/drug_pipeline.log")
        if not log_path.parent.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def run(self, drugs: list[str]):
        self.stage_0(drugs)
        self.stage_1()
        self.stage_2()
        self.stage_3()
        self.persist()

    def _init_staging_table(self):
        cursor = self.sqlite_conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_drugs (
                drug_name TEXT PRIMARY KEY,

                -- STAGE 1: RAW DRUG (PUBCHEM) 
                pubchem_id TEXT,

                -- STAGE 2: CHEMBL DRUG ID
                chembl_id TEXT,

                -- STAGE 3: CHEMBL DRUG INFO
                molecular_type TEXT,
                chemical_structure TEXT,
                inchi_key TEXT,

                status INT DEFAULT 0, -- 0: not processed, 1: raw drug fetched, 2: chembl id mapped, 3: chembl drug fetched, -1: error
                error_code INT, -- if status is -1, this field will contain the error code for the failure

                UNIQUE(chembl_id)

            )
            """
        )
        self.sqlite_conn.commit()

    def stage_0(self, drugs: list[str]) -> None:
        """
        Stage 0: Extract the drug's data from the source and store it in the staging table.
        """
        self._init_staging_table()
        logger.info("Stage 0: Staging %d unique drugs from combinations into the staging table...", len(drugs))
        with self.sqlite_conn:
            for drug in drugs:
                self.sqlite_conn.execute(
                    """
                    INSERT OR IGNORE INTO staging_drugs (drug_name) VALUES (?)
                    """,
                    (drug,),
                )

    def stage_1(self):
        """
        Stage 1: For each drug in the staging table, attempt to fetch its PubChem ID from DrugCombDB.
        If successful, update the staging table with the PubChem ID and set status to 1.
        If not found, set status to -1 and error_code to NOT_FOUND_IN_DCDB_CODE.
        """
        logger.info("Stage 1: Fetching PubChem IDs from DrugCombDB...")

        success = 0
        skipped = 0

        while True:
            # 1. Fetch a Batch of 'pending' work
            rows = self.sqlite_conn.execute(
                "SELECT drug_name FROM staging_drugs WHERE status=0 LIMIT ?",
                (BATCH_SIZE,)
            ).fetchall()

            if not rows:
                break  # No more pending work

            updates = []
            for (drug_name,) in rows:
                try:
                    pubchem_drug = self.__get_pubchem_id(drug_name)
                    if pubchem_drug and pubchem_drug.drug_id:
                        # Status 1 -> Raw drug fetched successfully
                        updates.append((pubchem_drug.drug_id, pubchem_drug.chemical_structure, 1, None, drug_name))
                        success += 1

                    else:
                        # Status -1 -> Not found in DCDB
                        logger.warning("Drug '%s' not found in DrugCombDB", drug_name)
                        updates.append((None, None, -1, NOT_FOUND_IN_DCDB_CODE, drug_name))
                        skipped += 1
                except Exception as e:
                    logger.error("Error processing drug %s: %s", drug_name, e)
                    updates.append((None, None, -1, NOT_FOUND_IN_DCDB_CODE, drug_name))
                    skipped += 1

            # 2. Update the staging table with the results of the batch
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_drugs SET pubchem_id=?, chemical_structure=?, status=?, error_code=? WHERE drug_name=?",
                    updates
                )
        logger.info("Stage 1: Completed. Success: %d, Skipped: %d", success, skipped)

    def stage_2(self):
        """
        Stage 2: For each drug with a PubChem ID in the staging table, attempt to map it to a CHEMBL ID using UniChem.
        If successful, update the staging table with the CHEMBL ID and set status to 2.
        If not found, set status to -1 and error_code to NOT_FOUND_IN_UNICHEM_CODE.
        """
        logger.info("Stage 2: Mapping PubChem IDs to CHEMBL IDs using UniChem...")

        success = 0
        skipped = 0

        while True:
            # 1. Fetch a Batch of rows where status=1 (raw drug fetched successfully)
            rows = self.sqlite_conn.execute(
                "SELECT drug_name, pubchem_id FROM staging_drugs WHERE status=1 LIMIT ?",
                (BATCH_SIZE,)
            ).fetchall()

            if not rows:
                break  # No more pending work

            updates = []
            for drug_name, pubchem_id in rows:
                try:

                    chembl_id, inchi_key = self.unichem_api.get_compound_mappings(pubchem_id)
                    if chembl_id:
                        # Status 2 -> CHEMBL ID mapped successfully
                        updates.append((chembl_id, inchi_key, 2, None, drug_name))
                        success += 1
                    else:
                        # Status -1 -> Not found in UniChem
                        logger.warning(
                            "No CHEMBL ID mapping found in UniChem for PubChem ID '%s' (drug '%s')", pubchem_id, drug_name)
                        updates.append((None, None, -1, NOT_FOUND_IN_UNICHEM_CODE, drug_name))
                        skipped += 1
                except Exception as e:
                    logger.error("Error mapping PubChem ID '%s' for drug '%s': %s", pubchem_id, drug_name, e)
                    # Status -1 -> Not found in UniChem (or error during API call)
                    updates.append((None, None, -1, NOT_FOUND_IN_UNICHEM_CODE, drug_name))
                    skipped += 1

            # 2. Update the staging table with the results of the batch
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_drugs SET chembl_id=?, inchi_key=?, status=?, error_code=? WHERE drug_name=?",
                    updates
                )
        logger.info("Stage 2: Completed. Success: %d, Skipped: %d", success, skipped)

    def stage_3(self):
        """
        Stage 3: For each drug with a CHEMBL ID in the staging table, attempt to fetch its data from ChEMBL.
        If successful, update the staging table with the drug info and set status to 3.
        If not found, set status to -1 and error_code to NOT_FOUND_IN_CHEMBL_CODE.
        """
        logger.info("Stage 3: Fetching drug info from ChEMBL...")

        success = 0
        skipped = 0

        while True:
            # 1. Fetch a Batch rows where status=2 (CHEMBL ID mapped successfully)
            rows = self.sqlite_conn.execute(
                "SELECT chembl_id FROM staging_drugs WHERE status=2 LIMIT ?",
                (BATCH_SIZE,)
            ).fetchall()

            if not rows:
                break  # No more pending work

            updates = []
            for (chembl_id,) in rows:
                try:
                    chembl_drug = self.__get_drug_info_from_chembl(chembl_id)
                    if chembl_drug:
                        # Status 3 -> CHEMBL drug info fetched successfully. Extraction Stage done.
                        updates.append((chembl_drug.molecular_type, chembl_drug.chemical_structure,
                                       chembl_drug.inchi_key, 3, None, chembl_id))
                        success += 1
                    else:
                        # Status -1 -> Not found in CHEMBL
                        updates.append((None, None, None, -1, NOT_FOUND_IN_CHEMBL_CODE, chembl_id))
                        skipped += 1
                except Exception as e:
                    # Status -1 -> Not found in CHEMBL (or error during API call)
                    updates.append((None, None, None, -1, NOT_FOUND_IN_CHEMBL_CODE, chembl_id))
                    skipped += 1

            # 2. Update the staging table with the results of the batch
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_drugs SET molecular_type=?, chemical_structure=?, inchi_key=?, status=?, error_code=? WHERE chembl_id=?",
                    updates
                )

        logger.info("Stage 3: Completed. Success: %d, Skipped: %d", success, skipped)

    def persist(self):
        """
        After all stages are completed, persist the successfully processed drugs into the DISNET database using the DrugRepo.
        """
        logger.info("Persisting successfully processed drugs into DISNET database...")
        cursor = self.sqlite_conn.execute(
            """
            SELECT drug_name, chembl_id, molecular_type, chemical_structure, inchi_key 
            FROM staging_drugs 
            WHERE status=3
            """
        )
        n_to_process = self.sqlite_conn.execute("SELECT COUNT(*) FROM staging_drugs WHERE status=3").fetchone()[0]
        logger.info("Total drugs to persist: %d", n_to_process)
        counter = 0

        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break

            for row in batch:
                (name, cid, mol_type, cstructure, inchi_key) = row
                drug = Drug(
                    drug_id=cid,  # chembl_id
                    drug_name=name,  # drug_name
                    source_id=self.chembl_source_id,
                    molecular_type=mol_type,  # molecular_type
                    chemical_structure=cstructure,  # chemical_structure
                    inchi_key=inchi_key  # inchi_key
                )

                self.drug_repo.add_chembl_drug(drug)
                counter += 1

        logger.info("Persistence completed. Total drugs persisted: %d of %d", counter, n_to_process)

    def __get_pubchem_id(self, drug_name: str) -> Drug | None:
        query = "SELECT cIds, drugNameOfficial, smilesString FROM drugs WHERE drugName = ? OR drugNameOfficial = ?"
        params = (drug_name, drug_name)
        cursor = self.sqlite_conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        if result:
            name = result[1] if result[1] else drug_name
            pubchem_id = str(int(result[0][4:]))  # CIDs000xxx -> xxx
            return Drug(
                drug_id=pubchem_id,
                drug_name=name,
                chemical_structure=result[2],
                source_id=self.pubchem_source_id,
            )

        if self.local:
            return None
        return self.dcdb_api.get_drug_info(drug_name, self.pubchem_source_id)

    def __get_drug_info_from_chembl(self, chembl_id: str) -> Drug | None:
        result = new_client.molecule.filter(molecule_chembl_id=chembl_id).only(
            "molecule_chembl_id", "molecule_structures", "molecule_type", "pref_name"
        )
        if not result:
            return None

        result = result[0]

        return Drug(
            drug_id=result["molecule_chembl_id"],
            drug_name=result["pref_name"],
            source_id=self.chembl_source_id,
            molecular_type=result["molecule_type"],
            chemical_structure=result["molecule_structures"]["canonical_smiles"],
            inchi_key=result["molecule_structures"]["standard_inchi_key"],
        )


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
