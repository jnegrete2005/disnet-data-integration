import unittest

from repo.drug_repo import DrugRepo
from infraestructure.database import DisnetManager
from domain.models import Drug, ForeignMap


class TestDrugRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DisnetManager(test=True)
        cls.db.connect()
        cls.repo = DrugRepo(cls.db)

        # Create tables before testing
        cls.repo.create_tables()

        # Create dummy source
        cursor = cls.db.get_cursor()
        cursor.execute("INSERT INTO source (name) VALUES (\"CHEMBL\")")
        cls.chembl_source_id = cursor.lastrowid

        cursor.execute("INSERT INTO source (name) VALUES (\"Test\")")
        cls.foreign_source_id = cursor.lastrowid
        cursor.close()

    def test_add_chembl_drug(self):
        drug = Drug(
            drug_id="CHEMBL0001",
            drug_name="Test Drug",
            source_id=self.chembl_source_id,
            molecular_type="Small molecule",
            chemical_structure="Test Structure",
            inchi_key="TESTINCHIKEY"
        )
        result = self.repo.add_chembl_drug(drug)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute("SELECT drug_id FROM drug")
        fetched_drug_id = cursor.fetchone()[0]
        cursor.close()
        self.assertEqual(fetched_drug_id, drug.drug_id)

    def test_add_raw_drug(self):
        drug = Drug(
            drug_id="RAW0001",
            drug_name="Raw Drug",
            source_id=self.foreign_source_id
        )
        result = self.repo.add_raw_drug(drug)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute("SELECT drug_id FROM drug_raw")
        fetched_drug_id = cursor.fetchone()[0]
        cursor.close()
        self.assertEqual(fetched_drug_id, drug.drug_id)

    def test_map_foreign_to_chembl(self):
        mapping = ForeignMap(
            foreign_id="RAW0001",
            chembl_id="CHEMBL0001",
            foreign_source_id=self.foreign_source_id
        )
        result = self.repo.map_foreign_to_chembl(mapping)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute("SELECT foreign_id, chembl_id, foreign_source_id FROM foreign_to_chembl")
        fetched_mapping = cursor.fetchone()
        cursor.close()
        self.assertEqual(fetched_mapping, (
            mapping.foreign_id,
            mapping.chembl_id,
            mapping.foreign_source_id
        ))

    @classmethod
    def tearDownClass(cls):
        cursor = cls.db.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS foreign_to_chembl")
        cursor.execute("DROP TABLE IF EXISTS drug_raw")
        cursor.execute("DELETE FROM drug")
        cursor.execute("DELETE FROM source")
        cls.db.conn.commit()
        cursor.close()
        cls.db.disconnect()
