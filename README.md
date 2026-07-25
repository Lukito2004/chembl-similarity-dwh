# chembl-similarity-dwh

Finds the top-10 most similar ChEMBL molecules for each compound in a personal input set,
using Morgan fingerprints and Tanimoto similarity.

The pipeline ingests ChEMBL from its public REST API into a medallion data warehouse on
PostgreSQL, computes fingerprints and similarity scores, publishes the results to S3, and
serves a star-schema data mart. Everything is orchestrated with Apache Airflow and runs
from a single `docker compose up`.

Setup, usage and results are documented below as the project takes shape.