from domain.models import Drug, ForeignMap

from repo.base import sql_op
from repo.generic_repo import GenericRepo


class DrugRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)

    @sql_op()
    def __create_drug_raw_table(self, cursor) -> bool:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS drug_raw (
            drug_id VARCHAR(50) PRIMARY KEY,
            source_id INT,
            drug_name VARCHAR(255) NOT NULL,
            molecular_type VARCHAR(50),
            chemical_structure TEXT,
            inchi_key VARCHAR(255),
            FOREIGN KEY (source_id) REFERENCES source(source_id)
        );
        """
        cursor.execute(create_table_query)
        return True

    @sql_op()
    def __create_foreign_to_chembl_table(self, cursor) -> bool:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS foreign_to_chembl (
            foreign_id VARCHAR(50),
            foreign_source_id INT,
            chembl_id VARCHAR(50) CHARACTER SET utf8mb3,
            PRIMARY KEY (foreign_id, foreign_source_id),
            FOREIGN KEY (foreign_id) REFERENCES drug_raw(drug_id),
            FOREIGN KEY (chembl_id) REFERENCES drug(drug_id)
        );
        """
        cursor.execute(create_table_query)
        return True

    def create_tables(self) -> bool:
        if not self.__create_drug_raw_table():
            return False
        if not self.__create_foreign_to_chembl_table():
            return False
        return True

    @sql_op(returns_bool=False)
    def get_or_create_raw_drug(self, cursor, drug: Drug) -> str | None:
        select_query = """
            SELECT drug_id
            FROM drug_raw
            WHERE drug_id = %s;
        """
        cursor.execute(select_query, (drug.drug_id,))
        result = cursor.fetchone()
        if result:
            return result['drug_id']

        insert_query = """
            INSERT INTO drug_raw (drug_id, source_id, drug_name, molecular_type, chemical_structure, inchi_key)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            drug.drug_id,
            drug.source_id,
            drug.drug_name,
            drug.molecular_type,
            drug.chemical_structure,
            drug.inchi_key
        ))

        return drug.drug_id

    @sql_op(returns_bool=False)
    def get_or_create_chembl_drug(self, cursor, drug: Drug) -> str | None:
        select_query = """
            SELECT drug_id
            FROM drug
            WHERE drug_id = %s;
        """
        cursor.execute(select_query, (drug.drug_id,))
        result = cursor.fetchone()
        if result:
            return result['drug_id']

        insert_query = """
            INSERT INTO drug (drug_id, source_id, drug_name, molecular_type, chemical_structure, inchi_key)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            drug.drug_id,
            drug.source_id,
            drug.drug_name,
            drug.molecular_type,
            drug.chemical_structure,
            drug.inchi_key
        ))

        return drug.drug_id

    @sql_op(returns_bool=False)
    def map_foreign_to_chembl(self, cursor, mapping: ForeignMap) -> bool:
        insert_query = """
            INSERT IGNORE INTO foreign_to_chembl (foreign_id, foreign_source_id, chembl_id)
            VALUES (%s, %s, %s);
        """
        cursor.execute(insert_query, (
            mapping.foreign_id,
            mapping.foreign_source_id,
            mapping.chembl_id
        ))
        return True
