from domain.models import Score
from infraestructure.database import DisnetManager
from pipeline.base_pipeline import IntegrationPipeline
from repo.score_repo import ScoreRepo


class ScorePipeline(IntegrationPipeline):
    """
    Get or create the scores' id of the drug combination from the DISNET database.
    """

    def __init__(self, db: DisnetManager):
        self.score_repo = ScoreRepo(db)

    def run(
        self,
        hsa: float | None,
        bliss: float | None,
        loewe: float | None,
        zip: float | None,
    ) -> tuple[list[Score], int]:
        scores: list[Score] = []

        score_mappings = {"HSA": hsa, "Bliss": bliss, "Loewe": loewe, "ZIP": zip}

        for score_name, score_value in score_mappings.items():
            if score_value is None:
                continue
            score_id = self.score_repo.get_or_create_score(score_name)
            scores.append(
                Score(score_id=score_id, score_name=score_name, score_value=score_value)
            )

        return scores
