"""Pytest fixtures for spark-anon test suite.

Provides fixtures that bridge the custom TestRunner-based test suite
with pytest's fixture injection system, enabling both `python test_spart.py`
and `python -m pytest test_spart.py` execution modes.
"""

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from test_spart import TestRunner, create_test_dataframe


@pytest.fixture(scope="session")
def spark():
    """Create a shared Spark session for all tests."""
    session = (
        SparkSession.builder.appName("spart-test")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def runner():
    """Provide a TestRunner instance for each test function."""
    return TestRunner()


@pytest.fixture
def temp_dir():
    """Provide a temporary directory, cleaned up after each test."""
    path = Path(tempfile.mkdtemp())
    yield path
    if path.exists():
        shutil.rmtree(path)


@pytest.fixture(scope="session")
def df(spark):
    """Create the standard test DataFrame."""
    return create_test_dataframe(spark)
