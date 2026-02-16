import sqlite3

from typing import List, Optional, Tuple
from apis.cellosaurus import CellosaurusAPI
from apis.dcdb import DrugCombDBAPI
from apis.umls import UMLSAPI
from domain.models import CellLine, Disease
from infraestructure.database import DisnetManager
from repo.cell_line_repo import CellLineRepo

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
    ):
        self.cell_line_repo = CellLineRepo(db)
        self.dcdb_api = dcdb_api or DrugCombDBAPI()
        self.cellosaurus_api = cellosaurus_api or CellosaurusAPI()
        self.umls_api = umls_api or UMLSAPI()
        self.cellosaurus_source_id = cellosaurus_source_id
        self.sqlite_conn = conn

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
        print("Stage 1: Resolving COSMIC IDs and translating to Cellosaurus IDs...")

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
                        "SELECT cosmic_id FROM cell_lines WHERE name = ?", (name,)
                    )
                    result = cursor.fetchone()

                    cosmic_id = result[0] if result else None
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

                    else:
                        # Path B: Fallback to DrugCombDB
                        cellosaurus_id, tissue = self.dcdb_api.get_cell_line_info(name)
                        if cellosaurus_id:
                            updates.append((cosmic_id, cellosaurus_id, tissue, None, 1, None, name))

                        else:
                            # Could not resolve Cellosaurus ID, mark as failed with reason
                            updates.append((cosmic_id, None, None, None, -1, "Not found", name))

                except Exception as e:
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

    def stage_2(self):
        """
        Stage 2: For rows with resolved Cellosaurus ID, query Cellosaurus API to get associated disease NCIT ID.
        """
        print("Stage 2: Getting disease associations from Cellosaurus...")

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
                except Exception as e:
                    updates.append((None, -1, str(e), name))

            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_cell_lines SET ncit_accession=?, status=?, error_msg=? WHERE original_name=?",
                    updates
                )

    def stage_3(self):
        """Stage 3: Map NCIt to UMLS CUI."""
        print("Stage 3: Mapping to UMLS CUI...")
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
                    else:
                        # If no NCIt was found, we can still mark it as processed but with null CUI and disease name
                        updates.append((None, None, 3, None, name))
                except Exception as e:
                    updates.append((None, None, -1, str(e), name))

            with self.sqlite_conn:
                self.sqlite_conn.executemany(
                    "UPDATE staging_cell_lines SET umls_cui=?, disease_name=?, status=?, error_msg=? WHERE original_name=?",
                    updates
                )

    def persist(self):
        """Stage 4: Save fully processed rows to the Repo."""
        print("Persisting to Production DB...")
        cursor = self.sqlite_conn.execute("""
            SELECT original_name, cellosaurus_accession, tissue, umls_cui, disease_name 
            FROM staging_cell_lines 
            WHERE status=3
        """)

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
                except Exception as e:
                    print(f"Error persisting {name}: {e}")


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
