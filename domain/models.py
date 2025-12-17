from dataclasses import dataclass

# Basic entities


@dataclass
class CellLine:
    cell_line_id: str
    source_id: int
    name: str
    disease_id: int
    tissue: str = None


@dataclass
class Score:
    score_name: str
    score_id: int | None = None


@dataclass
class ExperimentClassification:
    classification_name: str
    classification_id: int | None = None


@dataclass
class ExperimentSource:
    source_id: int
    source_name: str


@dataclass
class Disease:
    umls_cui: str
    name: str


# Drug-related entities
UNKNOWN_SOURCE_ID = -1
PUBCHEM_DISNET_SOURCE_ID = 6
COSMIC_DISNET_SOURCE_ID = 7


@dataclass
class Drug:
    drug_id: str
    drug_name: str
    source_id: int = UNKNOWN_SOURCE_ID
    molecular_type: str | None = None
    chemical_structure: str | None = None
    inchi_key: str | None = None


@dataclass
class ForeignMap:
    foreign_id: str
    foreign_source_id: int
    chembl_id: str


# Main entity
@dataclass
class Experiment:
    drugs_ids: list[Drug]  # List of Drug objects (N)
    cell_line: CellLine
    experiment_classification: ExperimentClassification
    experiment_source: ExperimentSource
    scores: dict[str, float]
    experiment_id: int | None = None
