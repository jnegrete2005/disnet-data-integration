import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import json

from concurrent.futures import ThreadPoolExecutor

from infraestructure.database import DisnetManager

from pipeline.DCDB.experiment_pipeline import ExperimentPipeline
from pipeline.base_pipeline import IntegrationPipeline
from pipeline.DCDB.drug_pipeline import DrugPipeline, DrugNotResolvableError
from pipeline.DCDB.cell_line_pipeline import (
    CellLineDiseasePipeline,
    CellLineNotResolvableError,
)
from pipeline.DCDB.score_pipeline import ScorePipeline

from apis.dcdb import DrugCombDBAPI

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


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

    def __init__(
        self,
        db: DisnetManager,
        checkpoint_path: Path = Path("checkpoints/dcdb_pipeline.chkpt"),
        audit_path: Path = Path("audit/skipped_dcdb.jsonl"),
        dcdb_api: DrugCombDBAPI = None,
        drug_pipeline: DrugPipeline = None,
        cell_line_pipeline: CellLineDiseasePipeline = None,
        score_pipeline: ScorePipeline = None,
        experiment_pipeline: ExperimentPipeline = None,
    ):
        self.db = db
        self.dcdb_api = dcdb_api or DrugCombDBAPI()

        # Checkpoints and audit paths
        self.checkpoint_path = checkpoint_path
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_path = audit_path
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)

        # Pipelines
        self.drug_pipeline = drug_pipeline or DrugPipeline(db)
        self.cell_line_pipeline = cell_line_pipeline or CellLineDiseasePipeline(db)
        self.score_pipeline = score_pipeline or ScorePipeline(db)
        self.experiment_pipeline = experiment_pipeline or ExperimentPipeline(
            db, logger=logger
        )

    def run(self, start: int = 1, end: int = 2, step: int = 1):
        last_done = self._load_checkpoint(self.checkpoint_path)
        skipped = 0
        failed = 0
        succeeded = 0

        start = last_done + 1 if last_done is not None else start
        logger.info(
            "Starting DCDB pipeline from %d to %d (step=%d). Resume from %s",
            start,
            end,
            step,
            last_done,
        )

        # Extract drug combination data from DrugCombDB
        for i in range(start, end, step):
            try:
                exp_id = self._get_exp_id(i)
                if exp_id is None:
                    skipped += 1
                    continue

                succeeded += 1
                logger.info(
                    "Processed drug combination %d -> Experiment ID %d", i, exp_id
                )

                self._save_checkpoint(i)

            except Exception as e:
                # Unexpected error, log and continue
                failed += 1
                logger.exception("Fatal error processing drug combination %d: %s", i, e)

        logger.info(
            "DCDB pipeline completed. Succeeded: %d, Skipped: %d, Failed: %d",
            succeeded,
            skipped,
            failed,
        )

    def _get_exp_id(self, i: int) -> int | None:
        drugcomb = self.dcdb_api.get_drug_combination_info(i)

        drugs = [drugcomb.drug1, drugcomb.drug2]

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_drugs = executor.submit(self.drug_pipeline.fetch, drugs)
            future_cell_line = executor.submit(
                self.cell_line_pipeline.fetch, drugcomb.cell_line
            )

            # DRUGS PROCESSING
            try:
                fetched_drugs = future_drugs.result()
            except DrugNotResolvableError as e:
                logger.warning(
                    "Skipping drug combination %d due to unresolved drug: %s (Reason: %s)",
                    i,
                    e.drug_name,
                    e.reason,
                )
                self._audit_skipped(
                    combination_id=i, stage="drug", entity=e.drug_name, code=e.code
                )
                return None
            except Exception:
                raise

            # CELL LINE AND DISEASE PROCESSING
            try:
                fetched_cell_line = future_cell_line.result()
            except CellLineNotResolvableError as e:
                logger.warning(
                    "Skipping drug combination %d due to unresolved cell line: %s",
                    i,
                    e.cell_line_name,
                )
                self._audit_skipped(
                    combination_id=i,
                    stage="cell_line",
                    entity=e.cell_line_name,
                    code=e.code,
                )
                return None
            except Exception:
                raise

        # Persist fetched data
        self.drug_pipeline.persist(fetched_drugs)
        self.cell_line_pipeline.persist(fetched_cell_line)

        processed_drugs = [result.chembl_drug for result in fetched_drugs]
        cell_line = fetched_cell_line.cell_line

        # SCORES PROCESSING
        scores, classification = self._process_scores(drugcomb)

        # Create the drug combination entry
        exp_id = self.experiment_pipeline.run(
            drug_ids=[drug.drug_id for drug in processed_drugs],
            classification=classification,
            cell_line_id=cell_line.cell_line_id,
            scores=scores,
            drug_names=drugs,
            combination_id=i,
        )
        return exp_id

    def _process_scores(self, drugcomb):
        return self.score_pipeline.run(
            hsa=drugcomb.hsa,
            bliss=drugcomb.bliss,
            loewe=drugcomb.loewe,
            zip=drugcomb.zip,
        )

    def _load_checkpoint(self) -> int | None:
        if not self.checkpoint_path.exists():
            return None

        try:
            return int(self.checkpoint_path.read_text().strip())
        except Exception as e:
            logger.error(f"Failed to read checkpoint from {self.checkpoint_path}: {e}")
            return None

    def _save_checkpoint(self, index: int):
        self.checkpoint_path.write_text(str(index))

    def _audit_skipped(
        self, *, combination_id: int, stage: str, entity: str, code: str
    ):
        record = {
            "combination_id": combination_id,
            "stage": stage,
            "entity": entity,
            "code": code,
            "timestamp": datetime.now(ZoneInfo("UTC")).isoformat(),
        }

        with self.audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
