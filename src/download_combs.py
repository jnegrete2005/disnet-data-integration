import sqlite3
from pathlib import Path

import pandas as pd

from apis.schemas.dcdb import DrugCombData
from pipeline.DCDB.dcdb_pipeline import DrugCombDBPipeline

MAX_COMBINATION_ID = 476718


def get_combination_id(id: int):
    drugcomb = DrugCombDBPipeline().get_initial_drug_comb(id)
    return drugcomb


def format_combination(id: int, drugcomb: DrugCombData):
    data = drugcomb.model_dump()
    data["id"] = id
    del data["drug_combination"]

    for score in ["zip", "bliss", "loewe", "hsa"]:
        data[score] = round(float(data[score]), 4)

    zip_class = 1 if data["zip"] > 0 else -1
    bliss_class = 1 if data["bliss"] > 0 else -1
    loewe_class = 1 if data["loewe"] > 0 else -1
    hsa_class = 1 if data["hsa"] > 0 else -1
    classification = zip_class + bliss_class + loewe_class + hsa_class
    data["classification"] = "synergistic" if classification > 0 else "antagonistic"

    return data


def get_batch_combination(start: int, end: int):
    combinations = []
    for i in range(start, end):
        drugcomb = get_combination_id(i)
        formatted_comb = format_combination(i, drugcomb)
        combinations.append(formatted_comb)
    return combinations


def get_last_combination_id(sqlite_path: Path) -> int:
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM drug_combinations")
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else 0


def insert_batch_combinations(combinations: list[dict], conn: sqlite3.Connection):
    query = """
    INSERT INTO drug_combinations (id, drug1, drug2, cell_line, zip, bliss, loewe, hsa, classification)
    VALUES (:id, :drug1, :drug2, :cell_line, :zip, :bliss, :loewe, :hsa, :classification)
    """
    with conn:
        try:
            conn.executemany(query, combinations)
        except sqlite3.Error as e:
            print(f"Error inserting batch combinations: {e}")
            raise e


def migrate_csv_to_sqlite(csv_path: Path, sqlite_path: Path):
    df = pd.read_csv(
        csv_path, usecols=["ID", "Drug1", "Drug2", "Cell line", "ZIP", "Bliss", "Loewe", "HSA", "classification"]
    )

    df.columns = ["id", "drug1", "drug2", "cell_line", "zip", "bliss", "loewe", "hsa", "classification"]

    with sqlite3.connect(sqlite_path) as conn:
        df.to_sql("drug_combinations", conn, if_exists="replace", index=False)

    return


def main():
    sqlite_path = Path("data/drugcombs.sqlite")

    idx = get_last_combination_id(sqlite_path)
    print("Resuming data extraction from combination ID:", idx)

    conn = sqlite3.connect(sqlite_path)

    for start in range(idx + 1, MAX_COMBINATION_ID + 1, 100):
        end = min(start + 99, MAX_COMBINATION_ID)
        print(f"Processing combinations from {start} to {end}...")
        combinations = get_batch_combination(start, end)
        insert_batch_combinations(combinations, conn)

    conn.close()


if __name__ == "__main__":
    main()
