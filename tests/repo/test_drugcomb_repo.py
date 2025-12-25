import unittest

from repo.drugcomb_repo import DrugCombRepo
from repo.drug_repo import DrugRepo
from infraestructure.database import DisnetManager
from domain.models import Drug


class TestDrugCombRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DisnetManager(test=True)
        cls.db.connect()
        cls.repo = DrugCombRepo(cls.db)

        # Create tables before testing
        cls.repo.create_tables()

        # Create dummy source
        cursor = cls.db.get_cursor()
        cursor.execute("INSERT INTO source (name) VALUES (\"Test Source\")")
        cls.source_id = cursor.lastrowid

        # Create dummy drugs
        drug01 = Drug(
            drug_id="DRUG01",
            drug_name="Test Drug 01",
            source_id=cls.source_id,
            molecular_type="Small molecule",
            chemical_structure="Structure 01",
            inchi_key="INCHIKEY01"
        )
        drug02 = Drug(
            drug_id="DRUG02",
            drug_name="Test Drug 02",
            source_id=cls.source_id,
            molecular_type="Small molecule",
            chemical_structure="Structure 02",
            inchi_key="INCHIKEY02"
        )
        drug03 = Drug(
            drug_id="DRUG03",
            drug_name="Test Drug 03",
            source_id=cls.source_id,
            molecular_type="Small molecule",
            chemical_structure="Structure 03",
            inchi_key="INCHIKEY03"
        )
        drug_repo = DrugRepo(cls.db)
        drug_repo.add_chembl_drug(drug01)
        drug_repo.add_chembl_drug(drug02)
        drug_repo.add_chembl_drug(drug03)
        cursor.close()

        cls.drug_ids = [drug01.drug_id, drug02.drug_id, drug03.drug_id]

    def test_get_or_create_combination(self):
        # Create 3 DCs
        AB_ids = self.drug_ids[:2]
        dc_id_1 = self.repo.get_or_create_combination(AB_ids)
        self.assertIsNotNone(dc_id_1)

        BC_ids = self.drug_ids[1:]
        dc_id_2 = self.repo.get_or_create_combination(BC_ids)
        self.assertIsNotNone(dc_id_2)

        dc_id_3 = self.repo.get_or_create_combination(self.drug_ids)
        self.assertIsNotNone(dc_id_3)

        # Check the cache
        # self.assertIn(tuple(sorted(AB_ids)), self.repo.drugcomb_cache)
        # self.assertIn(tuple(sorted(BC_ids)), self.repo.drugcomb_cache)
        # self.assertIn(tuple(sorted(self.drug_ids)), self.repo.drugcomb_cache)

        # Retrieve existing DCs
        dc_id_1_retrieved = self.repo.get_or_create_combination(AB_ids)
        self.assertEqual(dc_id_1, dc_id_1_retrieved)

        dc_id_2_retrieved = self.repo.get_or_create_combination(BC_ids)
        self.assertEqual(dc_id_2, dc_id_2_retrieved)

        dc_id_3_retrieved = self.repo.get_or_create_combination(self.drug_ids)
        self.assertEqual(dc_id_3, dc_id_3_retrieved)

    @classmethod
    def tearDownClass(cls):
        # Clean up the database
        cursor = cls.db.get_cursor()
        cursor.execute("DROP TABLE IF EXISTS drug_comb_drug")
        cursor.execute("DROP TABLE IF EXISTS drug_combination")
        cursor.execute("DELETE FROM drug")
        cursor.execute("DELETE FROM source")
        cls.db.conn.commit()
        cursor.close()
        cls.db.disconnect()
