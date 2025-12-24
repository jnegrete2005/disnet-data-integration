import unittest

from repo.cell_line_repo import CellLineRepo

from infraestructure.database import DisnetManager

from domain.models import CellLine, Disease


class TestCellLineRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DisnetManager(test=True)
        cls.db.connect()
        cls.repo = CellLineRepo(cls.db)

        # Create tables before testing
        cls.repo.create_table()

        # Create dummy source
        cursor = cls.db.get_cursor()
        query = """
            INSERT INTO source (name) VALUES ("TEST")
        """
        cursor.execute(query)
        cls.source_id = cursor.lastrowid
        cursor.close()

    def setUp(self):
        # Clean state before every test
        cursor = self.db.get_cursor()
        cursor.execute("DELETE FROM cell_line")
        self.db.conn.commit()

        cursor.execute("DELETE FROM disease")
        self.db.conn.commit()
        cursor.close()

        self.repo.cell_line_cache.clear()
        self.repo.disease_cache.clear()

    def test_add_disease(self):
        # Add the disease to the DB
        disease = Disease("C00001", "Test Disease")
        result = self.repo.add_disease(disease)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute("SELECT disease_id FROM disease")
        disease_id = cursor.fetchone()[0]
        cursor.close()
        self.assertEqual(disease_id, disease.umls_cui)

    def test_add_cell_line(self):
        # Add the disease to the DB
        disease = Disease(
            umls_cui="C00002",
            name="Test Disease"
        )
        self.repo.add_disease(disease)

        # Add the cell line to the DB
        cell_line = CellLine(
            cell_line_id="CVCL_0001",
            source_id=self.source_id,
            name="Test Cell Line",
            disease_id=disease.umls_cui,
            tissue="Test Cell Tissue",
        )
        result = self.repo.add_cell_line(cell_line)
        self.assertTrue(result)

        cursor = self.db.get_cursor()
        cursor.execute("SELECT cell_line_name FROM cell_line")
        row = cursor.fetchone()
        cursor.close()
        self.assertEqual(row[0], cell_line.name)

    def test_add_cell_line_cached(self):
        # Add the disease to the DB
        disease = Disease(
            umls_cui="C00003",
            name="Test Disease"
        )
        self.repo.add_disease(disease)

        # Add the cell line to the DB
        cell_line = CellLine(
            cell_line_id="CVCL_0003",
            source_id=self.source_id,
            name="Test Cell Line",
            disease_id=disease.umls_cui,
            tissue="Test Cell Tissue",
        )
        self.repo.add_cell_line(cell_line)
        self.repo.add_cell_line(cell_line)

        cursor = self.db.get_cursor()
        cursor.execute("SELECT COUNT(*) FROM cell_line")
        count = cursor.fetchone()[0]
        cursor.close()
        self.assertEqual(count, 1)

    @classmethod
    def tearDownClass(cls):
        cursor = cls.db.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS cell_line")
        cursor.execute("DELETE FROM source")
        cls.db.conn.commit()
        cursor.close()
        cls.db.disconnect()
