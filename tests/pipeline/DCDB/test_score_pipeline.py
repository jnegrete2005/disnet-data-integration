import unittest
from unittest.mock import MagicMock

from pipeline.DCDB.score_pipeline import ScorePipeline


class TestScorePipeline(unittest.TestCase):
    def setUp(self):
        """Initial setup before each test."""
        # Mock DB & Repo
        self.db = MagicMock()
        self.score_repo = MagicMock()

        self.pipeline = ScorePipeline(self.db)

        # Inject mocked repo
        self.pipeline.score_repo = self.score_repo

        self.pipeline.score_repo.get_or_create_score.side_effect = lambda name: {
            "HSA": 1,
            "Bliss": 2,
            "Loewe": 3,
            "ZIP": 4,
        }.get(name, 0)

    def test_run_synergy_consensus(self):
        """Case 1: All scores are positive."""
        scores, classification = self.pipeline.run(
            hsa=10.0, bliss=5.5, loewe=2.0, zip=15.0
        )

        self.assertEqual(classification, 1)
        self.assertEqual(len(scores), 4)
        self.assertEqual(scores[0].score_name, "HSA")
        self.assertEqual(scores[0].score_value, 10.0)

    def test_run_antagonism_consensus(self):
        """Case 2: All scores are negative."""
        scores, classification = self.pipeline.run(
            hsa=-5.0, bliss=-10.0, loewe=-2.0, zip=-0.5
        )

        self.assertEqual(classification, -1)

    def test_run_additive_neutral(self):
        """Case 3: All scores are around zero (Additive)."""
        # Input: Values smaller than epsilon (1e-5)
        scores, classification = self.pipeline.run(
            hsa=0.000001, bliss=-0.000001, loewe=0.0, zip=0.0
        )

        self.assertEqual(classification, 0)
        self.assertEqual(len(scores), 4)

    def test_run_mixed_voting_tie(self):
        """Case 4: Mixed voting resulting in a tie (Additive)."""
        # Input: 2 positives vs 2 negatives
        scores, classification = self.pipeline.run(
            hsa=10.0,  # +1 vote
            bliss=10.0,  # +1 vote
            loewe=-5.0,  # -1 vote
            zip=-5.0,  # -1 vote
        )
        # Net balance: 0
        self.assertEqual(classification, 0, "Tie in votes should result in 0")

    def test_run_mixed_voting_winner(self):
        """Case 5: Mixed voting where the majority wins."""
        # Input: 3 positives vs 1 negative
        scores, classification = self.pipeline.run(
            hsa=10.0,  # +1
            bliss=10.0,  # +1
            loewe=10.0,  # +1
            zip=-50.0,  # -1
        )
        # Net balance: +2 -> Normalized to 1
        self.assertEqual(classification, 1, "Majority of positives should win")

    def test_run_with_none_values(self):
        """Case 6: Handling None values (missing data)."""
        # Input: We only have data for ZIP
        scores, classification = self.pipeline.run(
            hsa=None, bliss=None, loewe=None, zip=12.5
        )

        self.assertEqual(
            classification, 1, "Should classify based only on available data"
        )
        self.assertEqual(len(scores), 1, "There should only be 1 Score object")
        self.assertEqual(scores[0].score_name, "ZIP")

        # Verify that the repo was called only once for ZIP
        self.pipeline.score_repo.get_or_create_score.assert_called_with("ZIP")

    def test_run_all_none(self):
        """Case 7: All values are None."""
        scores, classification = self.pipeline.run(
            hsa=None, bliss=None, loewe=None, zip=None
        )

        self.assertEqual(classification, 0, "No data, default classification is 0")
        self.assertEqual(len(scores), 0)
