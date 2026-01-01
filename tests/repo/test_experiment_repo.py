import unittest

from domain.models import CellLine, Disease, Drug, Experiment, Score
from infraestructure.database import DisnetManager
from repo.cell_line_repo import CellLineRepo
from repo.drug_repo import DrugRepo
from repo.drugcomb_repo import DrugCombRepo
from repo.experiment_repo import ExperimentRepo
from repo.score_repo import ScoreRepo
from tests.repo.delete_tables import delete_tables


class TestExperimentRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        delete_tables()
        cls.db = DisnetManager(test=True)
        cls.db.connect()
        cls.exp_repo = ExperimentRepo(cls.db)
        cls.dc_repo = DrugCombRepo(cls.db)
        cls.drug_repo = DrugRepo(cls.db)
        cls.cell_line_repo = CellLineRepo(cls.db)
        cls.score_repo = ScoreRepo(cls.db)

        # Create necessary tables
        cls.cell_line_repo.create_table()
        cls.score_repo.create_tables()
        cls.dc_repo.create_tables()
        cls.exp_repo.create_tables()

        # Create dummy source
        cursor = cls.db.get_cursor()
        cursor.execute('INSERT INTO source (name) VALUES ("Test Source")')
        cls.source_id = cursor.lastrowid

        # Create dummy drugs
        cls.drugs = [
            Drug(
                drug_id=f"DRUG0{i + 1}",
                drug_name=f"Test Drug {i + 1}",
                source_id=cls.source_id,
                molecular_type="Small molecule",
                chemical_structure=f"Structure 0{i + 1}",
                inchi_key=f"INCHIKEY0{i + 1}",
            )
            for i in range(3)
        ]
        for drug in cls.drugs:
            cls.drug_repo.add_chembl_drug(drug)
        cursor.close()

        cls.drug_ids = [drug.drug_id for drug in cls.drugs]

        # Create dummy drug combinations
        cls.dc_id_1 = cls.dc_repo.get_or_create_combination(cls.drug_ids[:2])
        cls.dc_id_2 = cls.dc_repo.get_or_create_combination(cls.drug_ids[1:])

        # Create dummy cell line
        cls.cell_line_repo.add_disease(Disease(umls_cui="D000001", name="Test Disease"))
        cls.cell_line_id = "CVCL_0001"
        cls.cell_line_repo.add_cell_line(
            CellLine(
                cell_line_id="CVCL_0001",
                source_id=cls.source_id,
                name="Test Cell Line",
                tissue="Lung",
                disease_id="D000001",
            )
        )

        # Create dummy scores
        cls.score_name_A = "Score A"
        cls.score_name_B = "Score B"
        cls.score_id_A = cls.score_repo.get_or_create_score(cls.score_name_A)
        cls.score_id_B = cls.score_repo.get_or_create_score(cls.score_name_B)

    def test_get_or_create_exp_class(self):
        class_name = "Test Classification"
        class_id_1 = self.exp_repo.get_or_create_exp_class(class_name)
        class_id_2 = self.exp_repo.get_or_create_exp_class(class_name)
        self.assertEqual(class_id_1, class_id_2)

        # Check cache
        self.assertIn(class_name, self.exp_repo.exp_class_cache)

        # Check DB directly
        cursor = self.db.get_cursor()
        cursor.execute(
            "SELECT classification_id FROM experiment_classification WHERE classification_name = %s",
            (class_name,),
        )
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(class_id_1, result[0])
        cursor.close()

    def test_get_or_create_exp_source(self):
        source_name = "Test Experiment Source"
        source_id_1 = self.exp_repo.get_or_create_exp_source(source_name)
        source_id_2 = self.exp_repo.get_or_create_exp_source(source_name)
        self.assertEqual(source_id_1, source_id_2)

        # Check cache
        self.assertIn(source_name, self.exp_repo.exp_source_cache)

        # Check DB directly
        cursor = self.db.get_cursor()
        cursor.execute(
            "SELECT source_id FROM experiment_source WHERE source_name = %s",
            (source_name,),
        )
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(source_id_1, result[0])
        cursor.close()

    def test_get_or_create_experiment(self):
        scores1 = [
            Score(
                score_id=self.score_id_A, score_name=self.score_name_A, score_value=4
            ),
            Score(
                score_id=self.score_id_B, score_name=self.score_name_B, score_value=1
            ),
        ]
        exp1 = Experiment(
            dc_id=self.dc_id_1,
            cell_line_id=self.cell_line_id,
            experiment_source_id=self.exp_repo.get_or_create_exp_source("Source A"),
            experiment_classification_id=self.exp_repo.get_or_create_exp_class(
                "Class A"
            ),
            scores=scores1,
        )
        exp1.experiment_id = self.exp_repo.get_or_create_experiment(exp1)
        self.assertIsInstance(exp1.experiment_id, int)

        # Check there are two experiment scores in DB
        cursor = self.db.get_cursor()
        query = """
            SELECT COUNT(*) FROM experiment_score;
        """
        cursor.execute(query)
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 2)
        cursor.close()

    @classmethod
    def tearDownClass(cls):
        cls.db.disconnect()
        delete_tables()
