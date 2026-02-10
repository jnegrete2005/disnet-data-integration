from infraestructure.database import DisnetManager


def delete_tables():
    db = DisnetManager(test=True)

    cursor = db.get_cursor()

    try:
        tables = [
            "foreign_to_chembl",
            "drug_raw",
            "experiment_score",
            "experiment",
            "experiment_source",
            "experiment_classification",
            "drug_comb_drug",
            "drug_combination",
            "score",
            "cell_line",
        ]
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        db.conn.commit()

        cursor.execute("DELETE FROM drug")
        cursor.execute("DELETE FROM source")

        db.conn.commit()
    finally:
        cursor.close()
        db.disconnect()

    return


if __name__ == "__main__":
    delete_tables()
