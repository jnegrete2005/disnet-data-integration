import unittest

from tests.repo.delete_tables import delete_tables

from repo.drug_repo import DrugRepo
from infraestructure.database import DisnetManager
from domain.models import Drug, ForeignMap


class TestDrugRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        delete_tables()
        cls.db = DisnetManager(test=True)
        cls.db.connect()
        cls.repo = DrugRepo(cls.db)

        # Create tables before testing
        cls.repo.create_tables()

        # Create dummy source
        cursor = cls.db.get_cursor()
        cursor.execute('INSERT INTO source (name) VALUES ("CHEMBL")')
        cls.chembl_source_id = cursor.lastrowid

        cursor.execute('INSERT INTO source (name) VALUES ("Test")')
        cls.foreign_source_id = cursor.lastrowid
        cursor.close()

        # Create test objects
        cls.chembl_drug = Drug(
            drug_id="CHEMBL0001",
            drug_name="Test Drug",
            source_id=cls.chembl_source_id,
            molecular_type="Small molecule",
            chemical_structure="Test Structure",
            inchi_key="TESTINCHIKEY",
        )

        cls.raw_drug = Drug(
            drug_id="RAW0001", drug_name="Raw Drug", source_id=cls.foreign_source_id
        )

        cls.mapping = ForeignMap(
            foreign_id="RAW0001",
            chembl_id="CHEMBL0001",
            foreign_source_id=cls.foreign_source_id,
        )

    def test_add_chembl_drug(self):
        result = self.repo.add_chembl_drug(self.chembl_drug)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute("SELECT drug_id FROM drug")
        fetched_drug_id = cursor.fetchone()[0]
        cursor.close()
        self.assertEqual(fetched_drug_id, self.chembl_drug.drug_id)

    def test_add_raw_drug(self):
        result = self.repo.add_raw_drug(self.raw_drug)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute("SELECT drug_id FROM drug_raw")
        fetched_drug_id = cursor.fetchone()[0]
        cursor.close()
        self.assertEqual(fetched_drug_id, self.raw_drug.drug_id)

    def test_map_foreign_to_chembl(self):
        result = self.repo.map_foreign_to_chembl(self.mapping)
        self.assertTrue(result)

        # Check if it exists
        cursor = self.db.get_cursor()
        cursor.execute(
            "SELECT foreign_id, chembl_id, foreign_source_id FROM foreign_to_chembl"
        )
        fetched_mapping = cursor.fetchone()
        cursor.close()
        self.assertEqual(
            fetched_mapping,
            (
                self.mapping.foreign_id,
                self.mapping.chembl_id,
                self.mapping.foreign_source_id,
            ),
        )

    def test_cache_mechanism(self):
        # Add everything again to test cache
        self.repo.add_chembl_drug(self.chembl_drug)
        self.repo.add_raw_drug(self.raw_drug)
        self.repo.map_foreign_to_chembl(self.mapping)

        # There should only be one entry in each table
        cursor = self.db.get_cursor()
        cursor.execute("SELECT COUNT(*) FROM drug")
        chembl_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM drug_raw")
        raw_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM foreign_to_chembl")
        mapping_count = cursor.fetchone()[0]
        cursor.close()

        self.assertEqual(chembl_count, 1)
        self.assertEqual(raw_count, 1)
        self.assertEqual(mapping_count, 1)

    @classmethod
    def tearDownClass(cls):
        cls.db.disconnect()
        delete_tables()
