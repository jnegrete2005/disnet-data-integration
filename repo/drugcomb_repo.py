from repo.base import sql_op
from repo.generic_repo import GenericRepo


class DrugCombRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)

        # Cache for existing drug combination
        self.drugcomb_cache: dict[tuple[str], int] = {}

    @sql_op()
    def __create_drug_combination_table(self, cursor) -> bool:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS drug_combination (
                dc_id INT AUTO_INCREMENT PRIMARY KEY
            );
        """
        cursor.execute(create_table_query)
        return True

    @sql_op()
    def __create_drug_comb_drug_table(self, cursor) -> bool:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS drug_comb_drug (
                dc_id INT,
                drug_id VARCHAR(25) CHARACTER SET utf8mb3 NOT NULL,
                PRIMARY KEY (dc_id, drug_id),
                FOREIGN KEY (dc_id) REFERENCES drug_combination(dc_id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (drug_id) REFERENCES drug(drug_id)
            );
        """
        cursor.execute(create_table_query)
        return True

    def create_tables(self) -> bool:
        if not self.__create_drug_combination_table():
            return False
        if not self.__create_drug_comb_drug_table():
            return False
        return True

    # TODO: This function will create a race condition if executed concurrently
    @sql_op(returns_bool=False)
    def get_or_create_combination(self, cursor, drug_ids: list[str]) -> int | None:
        drug_ids = sorted(set(drug_ids))
        if len(drug_ids) <= 1:
            raise ValueError("At least two unique drug IDs are required to form a combination.")

        if tuple(drug_ids) in self.drugcomb_cache:
            return self.drugcomb_cache[tuple(drug_ids)]

        placeholders = ', '.join(['%s'] * len(drug_ids))
        select_query = f"""
            SELECT dc_id
            FROM drug_comb_drug
            GROUP BY dc_id
            HAVING COUNT(*) = %s
                AND SUM(drug_id IN ({placeholders})) = %s;
        """
        cursor.execute(select_query, [len(drug_ids), *drug_ids, len(drug_ids)])
        result = cursor.fetchone()
        if result:
            self.drugcomb_cache[tuple(drug_ids)] = result['dc_id']
            return result['dc_id']

        insert_comb_query = "INSERT INTO drug_combination () VALUES ();"
        cursor.execute(insert_comb_query)
        new_dc_id = cursor.lastrowid
        insert_drug_query = """
            INSERT INTO drug_comb_drug (dc_id, drug_id) VALUES (%s, %s);
        """
        cursor.executemany(
            insert_drug_query,
            [(new_dc_id, drug_id) for drug_id in drug_ids]
        )
        self.drugcomb_cache[tuple(drug_ids)] = new_dc_id
        return new_dc_id
