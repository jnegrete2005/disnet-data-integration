import sys
import sqlite3
from time import time


from infraestructure.database import DisnetManager
from pipeline.DCDB.orchestrator import DrugCombDBOrchestrator
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


def integrate_dcdb():
    db = DisnetManager()
    sqlite_db_path = "./data/drugcombs.sqlite"
    conn = sqlite3.connect(sqlite_db_path)

    create_tables(db)

    start_time = time()
    orchestrator = DrugCombDBOrchestrator(disnet_db=db, conn=conn, from_local=True)
    orchestrator.run()
    end_time = time()
    print(f"DCDB Integration Time: {end_time - start_time} seconds")
    db.disconnect()


if __name__ == "__main__":
    integrate_dcdb()
