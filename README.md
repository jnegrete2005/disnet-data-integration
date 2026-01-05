# DISNET drug combinations data integration

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Development-green)

This repository contains the **pharmacological data integration and processing system** developed as part of the research at the **MEDAL** (Medical Data Analytics Laboratory).

The main objective is the extraction, transformation, and loading (ETL) of drug combination data (sourced from platforms like **DrugCombDB** and **DrugComb**) to build a **heterogeneous graph**. This graph will serve as the foundation for training Artificial Intelligence models (specifically **GNNs**) capable of predicting drug synergy and adverse effects.

## DrugCombDB
[DrugCombDB](https://drugcombdb.denglab.org/) is a comprehensive database of drug combinations for cancer therapy, integrating data from high-throughput screening studies, external databases, and manual curation from scientific literature.

We use their API to extract drug combination data with the following pipeline:
</br>
![DCDB ETL](./pictures/DCDB_pipeline.png)

First, we extract the drug combination data from the DrugCombDB API. Then, we go into three main sub-pipelines:

### Drug Pipeline
DrugCombDB provides drug information using PubChem names. We map these names to **ChEMBL IDs** by extracting the PubChem IDs from DrugCombDB, then we use UniChem's API to map PubChem IDs to ChEMBL IDs. Finally, we extract drug features from ChEMBL using their API.

### Cell Line and Disease Pipeline
DrugCombDB provides the cell line's name used in the experiments. We extract the cellosaurus accession number using the DrugCombDB API. With this info, we can extract the associated disease's NCIt accession using the Cellosaurus API. With the NCIt accession, we extract the UMLS CUI using the UMLS API.

### Score Pipeline
DrugCombDB provides different synergy scores for each drug combination. We use these scores to calculate the experiment classification.

### Main Pipeline
Finally, we persist all the extracted and transformed data into DISNET, and create the drug_combination, drug_comb_drug, experiment, and experiment_score in DISNET.

## New tables in DISNET's Drugslayer
The following new tables have been created in DISNET to store the drug combination data:
- **cell_line**: Stores information about cell lines used in experiments.
    - `cell_line_id`: The cellosaurus accession number.
    - `cell_line_name`: The name of the cell line.
    - `source_id`: The source database from where the ID comes from. In this case it is always Cellosaurus.
    - `tissue`: The tissue associated with the cell line.
    - `disease_id`: The UMLS CUI of the disease associated with the cell line.
- **drug_raw**: Stores drug information from a source different from ChEMBL. It has the same fields as the existing drug table, but most of the fields are optional.
    - `drug_id`: The ID of the drug in the source database.
    - `source_id`: The source database from where the ID comes from. In DrugCombDB's case it is always PubChem.
    - `drug_name`: The name of the drug.
    - Other fields are optional and can be filled if the information is available.
- **foreign_to_chembl**: Maps external drug IDs to ChEMBL IDs.
    - `drug_id`: The ID of the drug in the source database.
    - `source_id`: The source database from where the ID comes from.
    - `chembl_id`: The corresponding ChEMBL ID of the drug.
- **drug_combination**: Stores information about drug combinations.
    - `dc_id`: The ID of the drug combination.
- **drug_comb_drug**: Joins N drugs to a drug combination.
    - `dc_id`: The `id` from `drug_combination` table.
    - `drug_id`: The `id` from `drug` table.
- **experiment_classification**: Stores the classification of experiments based on synergy scores (synergistic, antagonistic, or additive).
    - `classification_id`: The ID of the classification.
    - `classification_name`: The name of the classification.
- **experiment_source**: Stores information about the source of the experiments.
    - `source_id`: The ID of the source.
    - `source_name`: The name of the source.
- **experiment**: Stores information about drug combination experiments.
    - `experiment_id`: The ID of the experiment.
    - `dc_id`: The `id` from `drug_combination` table.
    - `cell_line_id`: The ID from the associated `cell_line`.
    - `classification_id`: The ID from the associated `experiment_classification`.
    - `source_id`: The ID from the associated `experiment_source`.
    - `experiment_hash`: Another key to identify a complete experiment, meaning their drug combination, cell line, classification, source, and all their scores.
- **experiment_score**: Stores the synergy scores for each experiment.
    - `experiment_id`: The ID of the associated experiment.
    - `score_id`: The ID of the score type.
    - `score_value`: The value of the score.
- **score**: Stores information about the different types of synergy scores.
    - `score_id`: The ID of the score type.
    - `score_name`: The name of the score type.

## Future work
We have successfully integrated drug combination data from DrugCombDB. However, DrugCombDb has over 500k drug combinations, so we have yet to plan how to integrate all this data efficiently.

We also plan to integrate data from other drug combination sources, such as DrugComb, to enrich our dataset and improve the performance of our AI models.