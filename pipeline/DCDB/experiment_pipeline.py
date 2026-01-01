import logging

from domain.models import Experiment, Score
from infraestructure.database import DisnetManager
from pipeline.base_pipeline import IntegrationPipeline
from repo.drugcomb_repo import DrugCombRepo
from repo.experiment_repo import ExperimentRepo

logger = logging.getLogger(__name__)


class ExperimentPipeline(IntegrationPipeline):
    """
    Do the necessary operations to get or create an experiment entry in the DISNET database.

    The pipeline will follow these steps:
    1. Get or create the drug combination entry.
    2. Get or create the experiment classification entry.
    3. Get or create the experiment source entry.
    4. Get or create the experiment entry.
    """

    def __init__(
        self,
        db: DisnetManager,
        drug_comb_repo: DrugCombRepo = None,
        experiment_repo: ExperimentRepo = None,
    ):
        self.drug_comb_repo = drug_comb_repo or DrugCombRepo(db)
        self.experiment_repo = experiment_repo or ExperimentRepo(db)

    def run(
        self,
        drug_ids: list[str],
        classification: int,
        cell_line_id: str,
        scores: list[Score],
        drug_names: list[str],
        combination_id: int,
    ) -> int:
        """
        Given a list of drug IDs and a classification, get or create the experiment entry in the DISNET database.

        :param drug_ids: List of drug IDs forming the combination.
        :type drug_ids: list[str]
        :param classification: Classification of the experiment.
        :type classification: int
        :param scores: List of Score objects associated with the experiment.
        :type scores: list[Score]
        :param cell_line_id: ID of the cell line used in the experiment.
        :type cell_line_id: str

        :param drug_names: List of drug names forming the combination (for warning purposes).
        :type drug_names: list[str]
        :param combination_id: ID of the drug combination (for warning purposes).
        :type combination_id: int

        :return: ID of the created or existing experiment.
        :rtype: int
        """
        # Step 1: Get or create the drug combination entry
        drug_comb_id = self.drug_comb_repo.get_or_create_combination(drug_ids)

        # Step 2: Get or create the experiment classification entry
        class_name = self.__determine_classification_name(classification)
        if classification == 0:
            logger.warning(
                "Experiment with drug combination ID %d and drugs %s is classified as Additive.",
                combination_id,
                ", ".join(drug_names),
            )
        classification_id = self.experiment_repo.get_or_create_exp_class(class_name)

        # Step 3: Get or create the experiment source entry
        source_id = self.experiment_repo.get_or_create_exp_source("DrugCombDB")

        # Step 4: Get or create the experiment entry
        experiment = Experiment(
            dc_id=drug_comb_id,
            cell_line_id=cell_line_id,
            experiment_classification_id=classification_id,
            experiment_source_id=source_id,
            scores=scores,
        )
        experiment_id = self.experiment_repo.get_or_create_experiment(experiment)

        return experiment_id

    def __determine_classification_name(self, classification: int) -> str:
        """
        Determine the classification name based on the classification integer.

        :param classification: Classification integer.
        :type classification: int

        :return: Classification name.
        :rtype: str
        """
        if classification > 0:
            return "Synergistic"
        elif classification < 0:
            return "Antagonistic"
        else:
            return "Additive"
