from domain.models import Experiment, Score

from repo.base import sql_op
from repo.metadata_repo import MetadataRepo
from repo.drugcomb_repo import DrugCombRepo
from repo.generic_repo import GenericRepo


class ExperimentRepo(GenericRepo):
    def __init__(self, db):
        super().__init__(db)
        self.metadata_repo = MetadataRepo(db)
        self.drugcomb_repo = DrugCombRepo(db)

    @sql_op()
    def __create_experiment_table(self, cursor) -> bool:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS experiment (
                experiment_id INT AUTO_INCREMENT PRIMARY KEY,
                dc_id INT,
                cell_line_id INT,
                classification_id INT,
                source_id INT,
                FOREIGN KEY (dc_id) REFERENCES drug_combination(dc_id),
                FOREIGN KEY (cell_line_id) REFERENCES cell_line(cell_line_id),
                FOREIGN KEY (classification_id) REFERENCES experiment_classification(classification_id),
                FOREIGN KEY (source_id) REFERENCES experiment_source(source_id)
            )
        """
        cursor.execute(create_table_query)
        return True

    @sql_op()
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
        if not self.__create_experiment_table():
            return False
        if not self.__create_experiment_score_table():
            return False
        return True

    @sql_op(returns_bool=False)
    def add_experiment(self, cursor, exp: Experiment) -> int | None:
        # Get metadata
        cl_id = self.metadata_repo.get_or_create_cell_line_id(exp.cell_line)
        if cl_id is None:
            return None

        class_id = self.metadata_repo.get_or_create_class_id(exp.experiment_classification)
        if class_id is None:
            return None

        source_id = self.metadata_repo.get_or_create_source_id(exp.experiment_source)
        if source_id is None:
            return None

        # Get or create drug combination
        dc_id = self.drugcomb_repo.get_or_create_combination(exp.drug_ids)
        if dc_id is None:
            return None

        # Insert experiment
        insert_exp_query = """
            INSERT INTO experiment (dc_id, cell_line_id, classification_id, source_id)
            VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_exp_query, (dc_id, cl_id, class_id, source_id))
        exp_id = cursor.lastrowid
        if exp_id is None:
            return None

        # Insert scores
        for score_name, score_value in exp.scores.items():
            score_id = self.metadata_repo.get_or_create_score_id(Score(score_name))
            if score_id is None:
                return None
            insert_score_query = """
                INSERT INTO experiment_score (experiment_id, score_id, score_value)
                VALUES (%s, %s, %s);
            """
            cursor.execute(insert_score_query, (exp_id, score_id, score_value))

        return exp_id
