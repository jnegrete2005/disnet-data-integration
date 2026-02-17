import sqlite3
import logging

from pathlib import Path

from typing import List
from apis.cellosaurus import CellosaurusAPI
from apis.dcdb import DrugCombDBAPI
from apis.umls import UMLSAPI
from domain.models import CellLine, Disease
from infraestructure.database import DisnetManager
from repo.cell_line_repo import CellLineRepo

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False


BATCH_SIZE = 1000


class CellLinePipeline:
    """
    Staged pipeline for extracting Cell Line data.

    Stages:
    1. Resolve COSMIC ID from local tables and translate to Cellosaurus ID.
    2. Get disease NCIT ID from Cellosaurus.
    3. Map NCIt to UMLS CUI.
    4. Persist to DISNET.
    """

    def __init__(
        self,
        db: DisnetManager,
        cellosaurus_source_id: int,
        dcdb_api: DrugCombDBAPI = None,
        cellosaurus_api: CellosaurusAPI = None,
        umls_api: UMLSAPI = None,
        conn: sqlite3.Connection = None,
        from_local=False,
    ):
        self.cell_line_repo = CellLineRepo(db)
        self.dcdb_api = dcdb_api or DrugCombDBAPI()
        self.cellosaurus_api = cellosaurus_api or CellosaurusAPI()
        self.umls_api = umls_api or UMLSAPI()
        self.cellosaurus_source_id = cellosaurus_source_id
        self.sqlite_conn = conn
        self.local = from_local

        log_path = Path("logs/cl_pipeline.log")
        if not log_path.parent.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def run(self, cell_line_names: List[str]):
        self.stage_0(cell_line_names)
        self.stage_1()
        self.stage_2()
        self.stage_3()
        self.persist()

    def _init_staging_table(self):
        cursor = self.sqlite_conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_cell_lines (
                original_name TEXT PRIMARY KEY,
                
                -- STAGE 1: IDENTIFICATION (Local Table -> COSMIC -> CELLOSAURUS)
                cosmic_id TEXT,
                cellosaurus_accession TEXT,
                tissue TEXT,

                -- STAGE 2: DISEASE ASSOCIATION (Cellosaurus)
                ncit_accession TEXT,

                -- STAGE 3: DISEASE MAPPING (UMLS)
                umls_cui TEXT,
                disease_name TEXT,

                status INT DEFAULT 0, 
                error_msg TEXT
            )
            """
        )
        self.sqlite_conn.commit()

    def stage_0(self, cell_line_names: List[str]):
        """
        Stage 0: Load raw cell line names into staging table.
        """
        self._init_staging_table()
        with self.sqlite_conn:
            for name in cell_line_names:
                self.sqlite_conn.execute(
                    "INSERT OR IGNORE INTO staging_cell_lines (original_name) VALUES (?)",
                    (name,),
                )

    def stage_1(self):
        """
        Stage 1:
        - Path A (COSMIC): Gets ID + Tissue + NCIt -> Jumps to Status 2.
        - Path B (Fallback): Gets ID + Tissue -> Goes to Status 1.
        """
        logger.info("Stage 1: Resolving COSMIC IDs and translating to Cellosaurus IDs...")

        success = 0
        skipped = 0
        while True:
            # Fetch a batch of unresolved cell lines
            rows = self.sqlite_conn.execute(
                "SELECT original_name FROM staging_cell_lines WHERE status = 0 LIMIT ?",
                (BATCH_SIZE,),
            ).fetchall()

            if not rows:
                break

            updates = []
            for (name,) in rows:
                try:
                    # Step 1: Try to resolve COSMIC ID from local 'cell_lines' table
                    cursor = self.sqlite_conn.execute(
                        "SELECT cosmicId FROM cell_lines WHERE cellName = ?", (name,)
                    )
                    result = cursor.fetchone()

                    cosmic_id = str(int(result[0])) if result else None
                    cellosaurus_id = None
                    tissue = None

                    # Path A: COSMIC Lookup.
                    if cosmic_id:
                        cellosaurus_id, tissue, ncit = self.cellosaurus_api.get_cell_line_from_cosmic_id(
                            cosmic_id
                        )

                    if cellosaurus_id:
                        # Success! Jump to status 2 since we got the ncit.
                        updates.append((cosmic_id, cellosaurus_id, tissue, ncit, 2, None, name))
                        success += 1

                    else:
                        if self.local:
                            # If we're running locally, we won't have access to the DCDB API, so we can skip the fallback and just mark as not found.
                            logger.warning(
                                "COSMIC ID not found for cell line '%s' and local mode is enabled, skipping DCDB fallback", name)
                            updates.append((cosmic_id, None, None, None, -1, "Not found (local mode)", name))
                            skipped += 1
                            continue

                        # Path B: Fallback to DrugCombDB
                        cellosaurus_id, tissue = self.dcdb_api.get_cell_line_info(name)
                        if cellosaurus_id:
                            updates.append((cosmic_id, cellosaurus_id, tissue, None, 1, None, name))

                        else:
                            # Could not resolve Cellosaurus ID, mark as failed with reason
                            logger.warning("Could not resolve Cellosaurus ID for cell line '%s'", name)
                            updates.append((cosmic_id, None, None, None, -1, "Not found", name))

                except Exception as e:
                    logger.error("Error processing cell line '%s': %s", name, str(e))
                    updates.append((None, None, None, None, -1, str(e), name))

            # Batch update the staging table
            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    """
                    UPDATE staging_cell_lines 
                    SET cosmic_id=?, cellosaurus_accession=?, tissue=?, ncit_accession=?, status=?, error_msg=? 
                    WHERE original_name=?
                    """,
                    updates,
                )

        logger.info("Stage 1 batch completed: %d resolved, %d skipped", success, skipped)

    def stage_2(self):
        """
        Stage 2: For rows with resolved Cellosaurus ID, query Cellosaurus API to get associated disease NCIT ID.
        """
        logger.info("Stage 2: Getting disease associations from Cellosaurus...")

        success = 0
        skipped = 0
        while True:
            rows = self.sqlite_conn.execute(
                "SELECT original_name, cellosaurus_accession FROM staging_cell_lines WHERE status=1 LIMIT ?",
                (BATCH_SIZE,)
            ).fetchall()

            if not rows:
                break

            updates = []
            for name, accession in rows:
                try:
                    ncit = self.cellosaurus_api.get_cell_line_disease(accession)
                    updates.append((ncit, 2, None, name))
                    success += 1
                except Exception as e:
                    logger.error("Error getting disease association for cell line '%s': %s", name, str(e))
                    updates.append((None, -1, str(e), name))
                    skipped += 1

            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_cell_lines SET ncit_accession=?, status=?, error_msg=? WHERE original_name=?",
                    updates
                )

        logger.info("Stage 2 batch completed: %d updated with NCIT, %d failed", success, skipped)

    def stage_3(self):
        """Stage 3: Map NCIt to UMLS CUI."""
        logger.info("Stage 3: Mapping to UMLS CUI...")

        success = 0
        skipped = 0
        while True:
            rows = self.sqlite_conn.execute(
                "SELECT original_name, ncit_accession FROM staging_cell_lines WHERE status=2 LIMIT ?",
                (BATCH_SIZE,)
            ).fetchall()

            if not rows:
                break

            updates = []
            for name, ncit in rows:
                try:
                    if ncit:
                        cui, disease_name = self.umls_api.ncit_to_umls_cui(ncit)
                        updates.append((cui, disease_name, 3, None, name))
                        success += 1
                    else:
                        # If no NCIt was found, we can still mark it as processed but with null CUI and disease name
                        logger.warning("No NCIT accession for cell line '%s', skipping UMLS mapping", name)
                        updates.append((None, None, 3, None, name))
                        skipped += 1
                except Exception as e:
                    logger.error("Error mapping NCIT to UMLS CUI for cell line '%s': %s", name, str(e))
                    updates.append((None, None, -1, str(e), name))

            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_cell_lines SET umls_cui=?, disease_name=?, status=?, error_msg=? WHERE original_name=?",
                    updates
                )

        logger.info("Stage 3 batch completed: %d mapped to UMLS, %d skipped", success, skipped)

    def persist(self):
        """Stage 4: Save fully processed rows to the Repo."""
        logger.info("Persisting to Production DB...")
        cursor = self.sqlite_conn.execute("""
            SELECT original_name, cellosaurus_accession, tissue, umls_cui, disease_name 
            FROM staging_cell_lines 
            WHERE status=3
        """)
        n_to_persist = self.sqlite_conn.execute("SELECT COUNT(*) FROM staging_cell_lines WHERE status=3").fetchone()[0]
        logger.info("Total cell lines to persist: %d", n_to_persist)
        counter = 0

        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break

            for row in batch:
                (name, accession, tissue, cui, disease_name) = row

                cell_line = CellLine(
                    cell_line_id=accession,
                    source_id=self.cellosaurus_source_id,
                    name=name,
                    tissue=tissue,
                    disease_id=cui,
                )

                disease = None
                if cui:
                    disease = Disease(umls_cui=cui, name=disease_name)

                try:
                    if disease:
                        self.cell_line_repo.add_disease(disease)
                    self.cell_line_repo.add_cell_line(cell_line)
                    counter += 1
                except Exception as e:
                    logger.error("Error persisting cell line '%s': %s", name, str(e))

        logger.info("Persistence completed. Total cell lines persisted: %d of %d", counter, n_to_persist)


class CellLineNotResolvableError(Exception):
    """
    Exception raised when a cell line cannot be resolved.
    """

    def __init__(self, cell_line_name: str, reason: str = None):
        msg = f"Cell line '{cell_line_name}' could not be resolved"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)
        self.cell_line_name = cell_line_name
        self.reason = reason
