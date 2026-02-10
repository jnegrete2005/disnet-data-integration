import sys
from time import time

from infraestructure.database import DisnetManager
from pipeline.DCDB.dcdb_pipeline import DrugCombDBPipeline
from repo.cell_line_repo import CellLineRepo
from repo.drug_repo import DrugRepo
from repo.drugcomb_repo import DrugCombRepo
from repo.experiment_repo import ExperimentRepo
from repo.score_repo import ScoreRepo


def create_tables(db: DisnetManager):
    ScoreRepo(db).create_tables()
    DrugRepo(db).create_tables()
    CellLineRepo(db).create_table()
    DrugCombRepo(db).create_tables()
    ExperimentRepo(db).create_tables()


def integrate_dcdb(start: int = 1, end: int = 2, step: int = 1):
    db = DisnetManager(test=True)

    create_tables(db)

    start_time = time()
    pipeline = DrugCombDBPipeline(db)
    pipeline.run(start=start, end=end, step=step)
    end_time = time()
    print(f"DCDB Integration Time: {end_time - start_time} seconds")
    db.disconnect()


if __name__ == "__main__":
    args = sys.argv[1:]
    start = int(args[0]) if len(args) > 0 else 1
    end = int(args[1]) if len(args) > 1 else 2
    step = int(args[2]) if len(args) > 2 else 1

    integrate_dcdb(start=start, end=end, step=step)
