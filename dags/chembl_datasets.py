"""Datasets that link the pipeline DAGs together."""

from airflow.datasets import Dataset

SILVER_MOLECULE = Dataset("chembl://warehouse/silver.molecule")
