from repo.base import sql_op, sql_insert_op
from repo.generic_repo import GenericRepo

from domain.models import Disease, CellLine


class CellLineRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)
        self.cell_line_cache: set[CellLine] = set()
        self.disease_cache: set[Disease] = set()

    @sql_op
    def __create_cell_line_table(self, cursor) -> bool:
        query = """
            CREATE TABLE IF NOT EXISTS cell_line (
                cell_line_id CHAR(9) PRIMARY KEY, -- CVCL_XXXX
                cell_line_name VARCHAR(100) NOT NULL UNIQUE,
                tissue VARCHAR(100),
                source_id INT,
                disease_id VARCHAR(50) CHARACTER SET utf8mb3, 
                FOREIGN KEY (disease_id) REFERENCES disease(disease_id),
                FOREIGN KEY (source_id) REFERENCES source(source_id)
            );
        """
        cursor.execute(query)
        return True

    def create_table(self) -> bool:
        if not self.__create_cell_line_table():
            return False
        return True

    @sql_insert_op
    def add_cell_line(self, cursor, cell_line: CellLine) -> bool:
        """
        Insert a cell line into the DB. If duplicate key, do nothing.
        """
        if cell_line in self.cell_line_cache:
            return True

        insert_query = """
            INSERT INTO cell_line (cell_line_id, cell_line_name, tissue, disease_id)
            VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            cell_line.cell_line_id,
            cell_line.name,
            cell_line.tissue,
            cell_line.disease_id
        ))
        self.cell_line_cache.add(cell_line)

        return True

    @sql_insert_op
    def add_disease(self, cursor, disease: Disease) -> bool:
        """
        Insert a disease into the DB. If duplicate key, do nothing.
        """
        if disease in self.disease_cache:
            return True

        insert_query = """
            INSERT INTO disease (disease_id, disease_name)
            VALUES (%s, %s);
        """
        cursor.execute(insert_query, (disease.umls_cui, disease.name))
        self.disease_cache.add(disease)

        return True
