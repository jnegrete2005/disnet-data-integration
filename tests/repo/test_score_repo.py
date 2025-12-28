import unittest

from tests.repo.delete_tables import delete_tables

from repo.score_repo import ScoreRepo

from infraestructure.database import DisnetManager


class TestScoreRepo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        delete_tables()
        cls.db = DisnetManager(test=True)
        cls.db.connect()
        cls.repo = ScoreRepo(cls.db)

        # Create tables before testing
        cls.repo.create_tables()

    def test_get_or_create_score(self):
        score_name = "Test Score"
        score_id_1 = self.repo.get_or_create_score(score_name)
        self.assertIsInstance(score_id_1, int)

        # Retrieve the same score - idempotence
        score_id_2 = self.repo.get_or_create_score(score_name)
        self.assertEqual(score_id_1, score_id_2)

        # Check the cache
        self.assertIn(score_name, self.repo.score_cache)
        self.assertEqual(self.repo.score_cache[score_name], score_id_1)

        # Check the DB directly
        cursor = self.db.get_cursor()
        cursor.execute(
            "SELECT score_id FROM score WHERE score_name = %s;",
            (score_name,)
        )
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], score_id_1)
        cursor.close()

    @classmethod
    def tearDownClass(cls):
        cls.db.disconnect()
        delete_tables()
