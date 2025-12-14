from domain.models import CellLine, ExperimentClassification, ExperimentSource, Score
from repo.base import sql_op


class MetadataRepo:
    def __init__(self, db):
        self.db = db

    @sql_op()
    def __create_exp_class_table(self, cursor) -> bool:
        query = """
        CREATE TABLE IF NOT EXISTS experiment_classification (
            classification_id INT AUTO_INCREMENT PRIMARY KEY,
            classification_name VARCHAR(50) UNIQUE NOT NULL
        );
        """
        cursor.execute(query)
        return True

    @sql_op()
    def __create_exp_source_table(self, cursor) -> bool:
        query = """
        CREATE TABLE IF NOT EXISTS experiment_source (
            source_id INT AUTO_INCREMENT PRIMARY KEY,
            source_name VARCHAR(100) UNIQUE NOT NULL
        );
        """
        cursor.execute(query)
        return True

    @sql_op()
    def __create_score_table(self, cursor) -> bool:
        query = """
        CREATE TABLE IF NOT EXISTS score (
            score_id INT AUTO_INCREMENT PRIMARY KEY,
            score_name VARCHAR(30) UNIQUE NOT NULL
        );
        """
        cursor.execute(query)
        return True

    @sql_op()
    def __create_cell_line_table(self, cursor) -> bool:
        query = """
        CREATE TABLE IF NOT EXISTS cell_line (
                cell_line_id INT AUTO_INCREMENT PRIMARY KEY,
                cell_line_name VARCHAR(100) NOT NULL UNIQUE,
                disease_id VARCHAR(50) CHARACTER SET utf8mb3, 
                FOREIGN KEY (disease_id) REFERENCES disease(disease_id)
        );
        """
        cursor.execute(query)
        return True

    def create_tables(self) -> bool:
        if not self.__create_exp_class_table():
            return False
        if not self.__create_exp_source_table():
            return False
        if not self.__create_score_table():
            return False
        if not self.__create_cell_line_table():
            return False
        return True

    @sql_op(returns_bool=False)
    def get_or_create_class_id(self, cursor, classification: ExperimentClassification) -> int | None:
        select_query = """
        SELECT classification_id FROM experiment_classification
        WHERE classification_name = %s;
        """
        class_name = classification.classification_name
        cursor.execute(select_query, (class_name,))
        result = cursor.fetchone()
        if result:
            return result[0]

        insert_query = """
        INSERT INTO experiment_classification (classification_name)
        VALUES (%s);
        """
        cursor.execute(insert_query, (class_name,))
        return cursor.lastrowid

    @sql_op(returns_bool=False)
    def get_or_create_source_id(self, cursor, source: ExperimentSource) -> int | None:
        select_query = """
        SELECT source_id FROM experiment_source
        WHERE source_name = %s;
        """
        source_name = source.source_name
        cursor.execute(select_query, (source_name,))
        result = cursor.fetchone()
        if result:
            return result[0]

        insert_query = """
        INSERT INTO experiment_source (source_name)
        VALUES (%s);
        """
        cursor.execute(insert_query, (source_name,))
        return cursor.lastrowid

    @sql_op(returns_bool=False)
    def get_or_create_score_id(self, cursor, score: Score) -> int | None:
        select_query = """
        SELECT score_id FROM score
        WHERE score_name = %s;
        """
        score_name = score.score_name
        cursor.execute(select_query, (score_name,))
        result = cursor.fetchone()
        if result:
            return result[0]

        insert_query = """
        INSERT INTO score (score_name)
        VALUES (%s);
        """
        cursor.execute(insert_query, (score_name,))
        return cursor.lastrowid

    @sql_op(returns_bool=False)
    def get_or_create_cell_line_id(self, cursor, cell_line: CellLine) -> int | None:
        select_query = """
        SELECT cell_line_id FROM cell_line
        WHERE cell_line_name = %s;
        """
        cell_line_name = cell_line.name
        cursor.execute(select_query, (cell_line_name,))
        result = cursor.fetchone()
        if result:
            return result[0]

        insert_query = """
        INSERT INTO cell_line (cell_line_name, disease_id)
        VALUES (%s, %s);
        """
        cursor.execute(insert_query, (cell_line_name, cell_line.disease_id))
        return cursor.lastrowid
