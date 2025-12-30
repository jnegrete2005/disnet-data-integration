from pipeline.base_pipeline import IntegrationPipeline

from domain.models import Score

from infraestructure.database import DisnetManager

from repo.score_repo import ScoreRepo


class ScorePipeline(IntegrationPipeline):
    """
    Get or create the scores' id of the drug combination from the DISNET database.
    """

    def __init__(self, db: DisnetManager):
        self.score_repo = ScoreRepo(db)

    def run(self, hsa: float | None, bliss: float | None,
            loewe: float | None, zip: float | None) -> tuple[list[Score], int]:
        scores: list[Score] = []

        score_mappings = {
            "HSA": hsa,
            "Bliss": bliss,
            "Loewe": loewe,
            "ZIP": zip
        }

        classification = 0
        eps = 1e-5
        for score_name, score_value in score_mappings.items():
            if score_value is None:
                continue
            score_id = self.score_repo.get_or_create_score(score_name)
            scores.append(Score(score_id=score_id, score_name=score_name, score_value=score_value))

            if score_value < -eps:
                classification -= 1
            elif score_value > eps:
                classification += 1

        classification = (classification > 0) - (classification < 0)
        return scores, classification
