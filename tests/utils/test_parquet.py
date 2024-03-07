import os
import tempfile

import pytest

from target_hdfs.utils.parquet import get_parquet_files


@pytest.fixture
def temp_parquet_files():
    # Create a temporary directory and temporary parquet files for testing
    temp_dir = tempfile.TemporaryDirectory()
    temp_files = [
        "file2.parquet",
        "file1.parquet",
        "file3.csv",  # This file should not be included in the result
        "file4.txt",  # This file should not be included in the result
    ]
    for temp_file in temp_files:
        with open(os.path.join(temp_dir.name, temp_file), "w") as f:
            f.write("test")
    yield temp_dir
    # Cleanup: Close and remove the temporary directory and files
    temp_dir.cleanup()


def test_get_parquet_files(temp_parquet_files):
    # Get the list of parquet files from the temporary directory
    parquet_files = get_parquet_files(temp_parquet_files.name)

    # Expected list of parquet files
    expected_files = [
        os.path.join(temp_parquet_files.name, "file1.parquet"),
        os.path.join(temp_parquet_files.name, "file2.parquet"),
    ]

    # Assert that the retrieved parquet files match the expected list
    assert parquet_files == expected_files
