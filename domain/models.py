import hashlib
import json
from dataclasses import dataclass


# Basic entities
@dataclass
class CellLine:
    cell_line_id: str
    source_id: int
    name: str
    disease_id: int
    tissue: str = None

    def __eq__(self, value: "CellLine"):
        return (
            self.cell_line_id == value.cell_line_id
            and self.name == value.name
            and self.tissue == value.tissue
        )

    def __hash__(self):
        return hash((self.cell_line_id, self.name, self.tissue))


@dataclass
class Score:
    score_name: str
    score_value: float
    score_id: int | None = None

    def __post_init__(self):
        self.score_value = round(self.score_value, 4)


@dataclass
class ExperimentClassification:
    classification_id: int
    classification_name: str


@dataclass
class ExperimentSource:
    source_id: int
    source_name: str


@dataclass
class Disease:
    umls_cui: str
    name: str

    def __eq__(self, value: "Disease"):
        return self.umls_cui == value.umls_cui and self.name == value.name

    def __hash__(self):
        return hash((self.umls_cui, self.name))


# Drug-related entities
UNKNOWN_SOURCE_ID = -1
PUBCHEM_DISNET_SOURCE_ID = 6
CELLOSAURUS_DISNET_SOURCE_ID = 7


@dataclass
class Drug:
    drug_id: str
    drug_name: str
    source_id: int = UNKNOWN_SOURCE_ID
    molecular_type: str | None = None
    chemical_structure: str | None = None
    inchi_key: str | None = None

    def __eq__(self, value: "Drug"):
        return (
            self.drug_id == value.drug_id
            and self.drug_name == value.drug_name
            and self.inchi_key == value.inchi_key
        )

    def __hash__(self):
        return hash((self.drug_id, self.drug_name))


@dataclass
class ForeignMap:
    foreign_id: str
    foreign_source_id: int
    chembl_id: str

    def __eq__(self, value: "ForeignMap"):
        return (
            self.foreign_id == value.foreign_id
            and self.foreign_source_id == value.foreign_source_id
            and self.chembl_id == value.chembl_id
        )

    def __hash__(self):
        return hash((self.foreign_id, self.foreign_source_id, self.chembl_id))


# Main entity
@dataclass
class Experiment:
    dc_id: int
    cell_line_id: str
    experiment_classification_id: int
    experiment_source_id: int
    scores: list[Score]
    experiment_id: int | None = None

    @property
    def experiment_hash(self) -> str:
        payload = {
            "dc_id": self.dc_id,
            "cell_line_id": self.cell_line_id,
            "experiment_classification_id": self.experiment_classification_id,
            "experiment_source_id": self.experiment_source_id,
            "scores": sorted(
                [(score.score_id, score.score_value) for score in self.scores]
            ),
        }
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
