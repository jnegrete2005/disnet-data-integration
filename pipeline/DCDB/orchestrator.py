import logging
import sqlite3
from pathlib import Path

from infraestructure.database import DisnetManager
from pipeline.DCDB.cell_line_pipeline import CellLinePipeline
from pipeline.DCDB.drug_pipeline import DrugPipeline
from pipeline.DCDB.experiment_pipeline import ExperimentPipeline
from pipeline.DCDB.score_pipeline import ScorePipeline
from repo.source_repo import SourceRepo

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DrugCombDBOrchestrator:
    def __init__(
        self,
        disnet_db: DisnetManager,
        conn: sqlite3.Connection = None,
        log_path: Path = Path("logs/dcdb_pipeline.log"),
        source_repo: SourceRepo = None,
        drug_pipeline: DrugPipeline = None,
        cell_line_pipeline: CellLinePipeline = None,
        score_pipeline: ScorePipeline = None,
        experiment_pipeline: ExperimentPipeline = None,
        from_local: bool = False,
    ):
        self.disnet_db = disnet_db
        self.conn = conn
        self.from_local = from_local

        # Get sources
        source_repo = source_repo or SourceRepo(disnet_db)
        chembl_source_id = source_repo.get_or_create_source("CHEMBL")
        pubchem_source_id = source_repo.get_or_create_source("PubChem")
        cellosaurus_source_id = source_repo.get_or_create_source("Cellosaurus")

        # Logger path
        self.log_path = log_path
        self._setup_file_logger()

        # Pipelines
        self.drug_pipeline = drug_pipeline or DrugPipeline(
            db=disnet_db,
            chembl_source_id=chembl_source_id,
            pubchem_source_id=pubchem_source_id,
            conn=self.conn,
            from_local=from_local,
        )
        self.cell_line_pipeline = cell_line_pipeline or CellLinePipeline(
            db=disnet_db,
            cellosaurus_source_id=cellosaurus_source_id,
            conn=self.conn,
            from_local=from_local,
        )
        self.score_pipeline = score_pipeline or ScorePipeline(db=disnet_db)
        self.experiment_pipeline = experiment_pipeline or ExperimentPipeline(db=disnet_db)

    def run(self):
        logger.info("--- Starting ETL Orchestration ---")

        # Step 1: Extract drug combination data from DrugCombDB
        combinations = self._fetch_combinations()

        # Step 2: Extract unique entities (drugs and cell lines)
        unique_drugs, unique_cell_lines = self._extract_unique_entities(combinations)

        # Step 3: Process drugs and cell lines
        self._run_drug_pipeline(unique_drugs)
        self._run_cell_line_pipeline(unique_cell_lines)

        # Step 4: Asemble and persist scores and experiments
        self._persist_experiments(combinations)

        logger.info("--- ETL Orchestration Completed ---")

    def _fetch_combinations(self):
        if not self.from_local:
            raise NotImplementedError("DrugCombDB API is offline. Local processing is required.")

        query = """
        SELECT
            dc.id AS combination_id,
            dc.drug1 AS drug1_name,
            dc.drug2 AS drug2_name,
            dc.cell_line AS cell_line_name,
            dc.hsa, dc.zip, dc.bliss, dc.loewe,
            dc.classification
        FROM drug_combinations dc
        JOIN drugs d1 ON dc.drug1 = d1.drugName
        JOIN drugs d2 ON dc.drug2 = d2.drugName
        JOIN cell_lines cl ON dc.cell_line = cl.cellName
        WHERE dc.status = 'pending';
        """
        cursor = self.conn.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return results

    def _extract_unique_entities(self, combinations: list[dict]) -> tuple[set[str], set[str]]:
        unique_drugs = set()
        unique_cell_lines = set()

        for combo in combinations:
            unique_drugs.add(combo["drug1_name"])
            unique_drugs.add(combo["drug2_name"])
            unique_cell_lines.add(combo["cell_line_name"])

        return unique_drugs, unique_cell_lines

    def _run_drug_pipeline(self, unique_drugs: list[str]):
        logger.info("Processing %d unique drugs through DrugPipeline", len(unique_drugs))
        self.drug_pipeline.run(unique_drugs)

    def _run_cell_line_pipeline(self, unique_cell_lines: list[str]):
        logger.info("Processing %d unique cell lines through CellLinePipeline", len(unique_cell_lines))
        self.cell_line_pipeline.run(unique_cell_lines)

    def _persist_experiments(self, combinations: list[dict]):
        logger.info("Persisting experiments for %d drug combinations", len(combinations))

        drug_map = self._load_drug_map()
        cell_map = self._load_cell_map()

        skipped = 0
        success = 0

        for comb in combinations:
            d1_name = comb["drug1_name"]
            d2_name = comb["drug2_name"]
            cl_name = comb["cell_line_name"]

            # Resolve IDs
            d1_id = drug_map.get(d1_name)
            d2_id = drug_map.get(d2_name)
            cl_id = cell_map.get(cl_name)

            if not (d1_id and d2_id and cl_id):
                skipped += 1
                continue

            # Process scores
            scores = self.score_pipeline.run(
                hsa=comb["hsa"],
                bliss=comb["bliss"],
                loewe=comb["loewe"],
                zip=comb["zip"],
            )

            # Persist experiment
            try:
                self.experiment_pipeline.run(
                    drug_ids=[d1_id, d2_id],
                    class_name=comb["classification"],
                    cell_line_id=cl_id,
                    scores=scores,
                    drug_names=[d1_name, d2_name],
                    combination_id=comb["combination_id"],
                )
                success += 1
                self._set_processing_status(comb["combination_id"], "processed")
            except Exception as e:
                logger.error("Failed to persist experiment for combination ID %d: %s", comb["combination_id"], str(e))
                self._set_processing_status(comb["combination_id"], "error")

        logger.info("Experiment persistence completed: %d successful, %d skipped", success, skipped)

    def _load_drug_map(self) -> dict[str, str]:
        """ Reads staging drugs table to create a name to chembl_id mapping. """
        cursor = self.conn.execute("SELECT drug_name, chembl_id FROM staging_drugs WHERE status=3;")
        return {row[0]: row[1] for row in cursor.fetchall()}

    def _load_cell_map(self) -> dict[str, str]:
        """ Reads staging cell lines table to create a name to cell_line_id mapping. """
        cursor = self.conn.execute(
            "SELECT original_name, cellosaurus_accession FROM staging_cell_lines WHERE status=3;")
        return {row[0]: row[1] for row in cursor.fetchall()}

    def _setup_file_logger(self):
        if not self.log_path.parent.exists():
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(self.log_path)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def _set_processing_status(self, combination_id: int, status: str):
        self.conn.execute(
            "UPDATE drug_combinations SET status = ? WHERE id = ?", (status, combination_id)
        )
        self.conn.commit()
