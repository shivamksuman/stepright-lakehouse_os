"""
Shared pytest fixtures for all tests in this repo.

- Adds the repo root to sys.path so tests can do:
      from gold.gold_customer_360_logic import compute_customer_360
  even though gold/ has no __init__.py (Python 3 namespace packages handle this
  fine as long as the repo root is on sys.path).

- Provides a single local PySpark session shared across all tests in a run
  (session-scoped) so we don't pay Spark's ~5-10s startup cost per test.
"""

import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-gold-logic-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
