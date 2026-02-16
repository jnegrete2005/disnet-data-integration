import sqlite3

from chembl_webresource_client.new_client import new_client

from apis.dcdb import DrugCombDBAPI
from apis.unichem import UniChemAPI
from domain.models import Drug
from infraestructure.database import DisnetManager
from repo.drug_repo import DrugRepo


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
        print("Stage 1: Fetching PubChem IDs from DrugCombDB...")

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

                    else:
                        # Status -1 -> Not found in DCDB
                        updates.append((None, None, -1, NOT_FOUND_IN_DCDB_CODE, drug_name))
                except Exception as e:
                    print(f"Error processing drug {drug_name}: {e}")
                    updates.append((None, None, -1, NOT_FOUND_IN_DCDB_CODE, drug_name))

            # 2. Update the staging table with the results of the batch
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_drugs SET pubchem_id=?, chemical_structure=?, status=?, error_code=? WHERE drug_name=?",
                    updates
                )

    def stage_2(self):
        """
        Stage 2: For each drug with a PubChem ID in the staging table, attempt to map it to a CHEMBL ID using UniChem.
        If successful, update the staging table with the CHEMBL ID and set status to 2.
        If not found, set status to -1 and error_code to NOT_FOUND_IN_UNICHEM_CODE.
        """
        print("Stage 2: Mapping PubChem IDs to CHEMBL IDs using UniChem...")

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
                    else:
                        # Status -1 -> Not found in UniChem
                        updates.append((None, None, -1, NOT_FOUND_IN_UNICHEM_CODE, drug_name))
                except Exception:
                    # Status -1 -> Not found in UniChem (or error during API call)
                    updates.append((None, None, -1, NOT_FOUND_IN_UNICHEM_CODE, drug_name))

            # 2. Update the staging table with the results of the batch
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_drugs SET chembl_id=?, inchi_key=?, status=?, error_code=? WHERE drug_name=?",
                    updates
                )

    def stage_3(self):
        """
        Stage 3: For each drug with a CHEMBL ID in the staging table, attempt to fetch its data from ChEMBL.
        If successful, update the staging table with the drug info and set status to 3.
        If not found, set status to -1 and error_code to NOT_FOUND_IN_CHEMBL_CODE.
        """
        print("Stage 3: Fetching drug info from ChEMBL...")

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
                    else:
                        # Status -1 -> Not found in CHEMBL
                        updates.append((None, None, None, -1, NOT_FOUND_IN_CHEMBL_CODE, chembl_id))
                except Exception as e:
                    # Status -1 -> Not found in CHEMBL (or error during API call)
                    updates.append((None, None, None, -1, NOT_FOUND_IN_CHEMBL_CODE, chembl_id))

            # 2. Update the staging table with the results of the batch
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_drugs SET molecular_type=?, chemical_structure=?, inchi_key=?, status=?, error_code=? WHERE chembl_id=?",
                    updates
                )

    def persist(self):
        """
        After all stages are completed, persist the successfully processed drugs into the DISNET database using the DrugRepo.
        """
        print("Persisting successfully processed drugs into DISNET database...")
        cursor = self.sqlite_conn.execute(
            """
            SELECT drug_name, chembl_id, molecular_type, chemical_structure, inchi_key 
            FROM staging_drugs 
            WHERE status=3
            """
        )

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

    def __get_pubchem_id(self, drug_name: str) -> Drug | None:
        query = "SELECT * FROM drugs WHERE drugName = ? OR drugNameOfficial = ?"
        params = (drug_name, drug_name)
        cursor = self.sqlite_conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        if result:
            return Drug(
                drug_id=result[1],
                drug_name=result[2],
                chemical_structure=result[3],
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
