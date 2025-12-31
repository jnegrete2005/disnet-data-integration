from domain.models import Drug, ForeignMap

from repo.base import sql_op, sql_insert_op
from repo.generic_repo import GenericRepo


class DrugRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)
        self.raw_drug_cache: set[Drug] = set()
        self.drug_cache: set[Drug] = set()
        self.foreign_map_cache: set[ForeignMap] = set()

    @sql_op
    def __create_drug_raw_table(self, cursor) -> bool:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS drug_raw (
                drug_id VARCHAR(50) PRIMARY KEY,
                source_id INT NOT NULL,
                drug_name VARCHAR(255) NOT NULL,
                molecular_type VARCHAR(50),
                chemical_structure TEXT,
                inchi_key VARCHAR(255),
                FOREIGN KEY (source_id) REFERENCES source(source_id)
            );
        """
        cursor.execute(create_table_query)
        return True

    @sql_op
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

    @sql_insert_op
    def add_raw_drug(self, cursor, drug: Drug) -> bool:
        """
        Insert a raw drug into the DB. If duplicate key, do nothing.
        """
        if drug in self.raw_drug_cache:
            return True

        insert_query = """
            INSERT INTO drug_raw (drug_id, source_id, drug_name, molecular_type, chemical_structure, inchi_key)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(
            insert_query,
            (
                drug.drug_id,
                drug.source_id,
                drug.drug_name,
                drug.molecular_type,
                drug.chemical_structure,
                drug.inchi_key,
            ),
        )
        self.raw_drug_cache.add(drug)

        return drug.drug_id

    @sql_insert_op
    def add_chembl_drug(self, cursor, drug: Drug) -> bool:
        if drug in self.drug_cache:
            return True

        insert_query = """
            INSERT INTO drug (drug_id, source_id, drug_name, molecular_type, chemical_structure, inchi_key)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(
            insert_query,
            (
                drug.drug_id,
                drug.source_id,
                drug.drug_name,
                drug.molecular_type,
                drug.chemical_structure,
                drug.inchi_key,
            ),
        )
        self.drug_cache.add(drug)

        return True

    @sql_insert_op
    def map_foreign_to_chembl(self, cursor, mapping: ForeignMap) -> bool:
        if mapping in self.foreign_map_cache:
            return True

        insert_query = """
            INSERT INTO foreign_to_chembl (foreign_id, foreign_source_id, chembl_id)
            VALUES (%s, %s, %s);
        """
        cursor.execute(
            insert_query,
            (mapping.foreign_id, mapping.foreign_source_id, mapping.chembl_id),
        )
        self.foreign_map_cache.add(mapping)

        return True
