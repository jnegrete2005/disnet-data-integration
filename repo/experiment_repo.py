from mysql.connector.errorcode import ER_DUP_ENTRY
from mysql.connector.errors import IntegrityError

from domain.models import Experiment
from repo.base import sql_op
from repo.generic_repo import GenericRepo


class ExperimentRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)

        self.experiment_cache: dict[str, int] = {}
        self.exp_class_cache: dict[str, int] = {}
        self.exp_source_cache: dict[str, int] = {}

    @sql_op
    def __create_exp_class_table(self, cursor) -> bool:
        query = """
            CREATE TABLE IF NOT EXISTS experiment_classification (
                classification_id INT AUTO_INCREMENT PRIMARY KEY,
                classification_name VARCHAR(50) UNIQUE NOT NULL
            );
        """
        cursor.execute(query)
        return True

    @sql_op
    def __create_exp_source_table(self, cursor) -> bool:
        query = """
            CREATE TABLE IF NOT EXISTS experiment_source (
                source_id INT AUTO_INCREMENT PRIMARY KEY,
                source_name VARCHAR(50) UNIQUE NOT NULL
            );
        """
        cursor.execute(query)
        return True

    @sql_op
    def __create_experiment_table(self, cursor) -> bool:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS experiment (
                experiment_id INT AUTO_INCREMENT PRIMARY KEY,
                dc_id INT NOT NULL,
                cell_line_id CHAR(9) NOT NULL,
                classification_id INT NOT NULL,
                source_id INT NOT NULL,
                experiment_hash CHAR(64) NOT NULL,
                UNIQUE KEY uq_experiment_hash (experiment_hash),
                FOREIGN KEY (dc_id) REFERENCES drug_combination(dc_id),
                FOREIGN KEY (cell_line_id) REFERENCES cell_line(cell_line_id),
                FOREIGN KEY (classification_id) REFERENCES experiment_classification(classification_id),
                FOREIGN KEY (source_id) REFERENCES experiment_source(source_id)
            );
        """
        cursor.execute(create_table_query)
        return True

    @sql_op
    def __create_experiment_score_table(self, cursor) -> bool:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS experiment_score (
                experiment_id INT,
                score_id INT,
                score_value FLOAT,
                PRIMARY KEY (experiment_id, score_id),
                FOREIGN KEY (experiment_id) REFERENCES experiment(experiment_id),
                FOREIGN KEY (score_id) REFERENCES score(score_id)
            )
        """
        cursor.execute(create_table_query)
        return True

    def create_tables(self) -> bool:
        if not self.__create_exp_class_table():
            return False
        if not self.__create_exp_source_table():
            return False
        if not self.__create_experiment_table():
            return False
        if not self.__create_experiment_score_table():
            return False
        return True

    @sql_op
    def get_or_create_exp_class(self, cursor, exp_class_name: str) -> int:
        if exp_class_name in self.exp_class_cache:
            return self.exp_class_cache[exp_class_name]

        select_query = """
            SELECT classification_id FROM experiment_classification
            WHERE classification_name = %s;
        """
        cursor.execute(select_query, (exp_class_name,))
        result = cursor.fetchone()
        if result:
            self.exp_class_cache[exp_class_name] = result[0]
            return result[0]

        insert_query = """
            INSERT INTO experiment_classification (classification_name)
            VALUES (%s);
        """
        cursor.execute(insert_query, (exp_class_name,))
        self.exp_class_cache[exp_class_name] = cursor.lastrowid
        return cursor.lastrowid

    @sql_op
    def get_or_create_exp_source(self, cursor, exp_source_name: str) -> int:
        if exp_source_name in self.exp_source_cache:
            return self.exp_source_cache[exp_source_name]

        select_query = """
            SELECT source_id FROM experiment_source
            WHERE source_name = %s;
        """
        cursor.execute(select_query, (exp_source_name,))
        result = cursor.fetchone()
        if result:
            self.exp_source_cache[exp_source_name] = result[0]
            return result[0]

        insert_query = """
            INSERT INTO experiment_source (source_name)
            VALUES (%s);
        """
        cursor.execute(insert_query, (exp_source_name,))
        self.exp_source_cache[exp_source_name] = cursor.lastrowid
        return cursor.lastrowid

    @sql_op
    def get_or_create_experiment(self, cursor, exp: Experiment) -> int:
        exp_hash = exp.experiment_hash
        if exp_hash in self.experiment_cache:
            return self.experiment_cache[exp_hash]

        try:
            # Insert experiment
            insert_exp_query = """
                INSERT INTO experiment (dc_id, cell_line_id, classification_id, source_id, experiment_hash)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(
                insert_exp_query,
                (
                    exp.dc_id,
                    exp.cell_line_id,
                    exp.experiment_classification_id,
                    exp.experiment_source_id,
                    exp_hash,
                ),
            )
            exp_id = cursor.lastrowid

        except IntegrityError as ie:
            if ie.errno != ER_DUP_ENTRY:
                raise

            # Duplicate entry, fetch existing experiment_id
            select_query = """
                SELECT experiment_id FROM experiment
                WHERE experiment_hash = %s;
            """
            cursor.execute(select_query, (exp_hash,))
            result = cursor.fetchone()
            if not result:
                raise RuntimeError("Failed to retrieve existing experiment after duplicate entry error.")
            exp_id = result[0]

            # Check if scores are already inserted
            check_scores_query = """
                SELECT COUNT(*) FROM experiment_score
                WHERE experiment_id = %s;
            """
            cursor.execute(check_scores_query, (exp_id,))
            score_count = cursor.fetchone()[0]
            if score_count == len(exp.scores):
                # Scores already inserted, return existing experiment_id
                self.experiment_cache[exp_hash] = exp_id
                return exp_id

        # Insert scores
        # Reaching here means either a new experiment was created
        # or an existing experiment was found but scores need to be inserted/updated
        # so we will try/except to handle potential duplicates
        insert_score_query = """
            INSERT INTO experiment_score (experiment_id, score_id, score_value)
            VALUES (%s, %s, %s);
        """
        for score in exp.scores:
            score_id = score.score_id
            score_value = score.score_value
            try:
                cursor.execute(insert_score_query, (exp_id, score_id, score_value))
            except IntegrityError as ie:
                if ie.errno != ER_DUP_ENTRY:
                    raise
                # If duplicate, we skip
                continue

        # Cache result
        self.experiment_cache[exp_hash] = exp_id
        return exp_id
