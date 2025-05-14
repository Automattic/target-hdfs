from unittest.mock import patch, Mock

import pytest
import pyarrow as pa

from target_hdfs.utils.hdfs import (
    read_most_recent_file,
    SchemaChangedError,
    get_most_recent_file,
)


@pytest.fixture(autouse=True)
def mock_pa_parquet_read_table(monkeypatch):
    def mock_read_table(file):
        return pa.Table.from_pydict(
            {"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [True, False, True]}
        )

    monkeypatch.setattr("pyarrow.parquet.read_table", mock_read_table)


SCHEMA = pa.schema([("col1", pa.int64()), ("col2", pa.string()), ("col3", pa.bool_())])


# Test cases
def test_read_most_recent_file_successful():
    hdfs_file_path = "/some/hdfs/path"
    hdfs_block_size_limit = "1m"

    result = read_most_recent_file(hdfs_file_path, SCHEMA, hdfs_block_size_limit)
    expected_content = pa.Table.from_pydict(
        {"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [True, False, True]}
    )
    assert result["content"] == expected_content
    assert result["path"] == "/mock/path"


def test_read_most_recent_file_successful_no_hdfs_size():
    hdfs_file_path = "/some/hdfs/path"

    result = read_most_recent_file(hdfs_file_path, SCHEMA, None)
    expected_content = pa.Table.from_pydict(
        {"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [True, False, True]}
    )
    assert result["content"] == expected_content
    assert result["path"] == "/mock/path"


def test_read_most_recent_file_file_too_large():
    # Test when the most recent file is larger than the block size limit
    hdfs_file_path = "/some/hdfs/path"
    hdfs_relative_block_size_limit = "1k"

    result = read_most_recent_file(
        hdfs_file_path, SCHEMA, hdfs_relative_block_size_limit
    )
    assert result is None


def test_read_most_recent_file_schema_mismatch():
    # Test when the schema of the file doesn't match the expected schema
    hdfs_file_path = "/some/hdfs/path"
    pyarrow_schema = pa.schema([("col1", pa.int64()), ("col2", pa.string())])
    hdfs_block_size_limit = "1m"

    with pytest.raises(SchemaChangedError):
        read_most_recent_file(hdfs_file_path, pyarrow_schema, hdfs_block_size_limit)


@patch("target_hdfs.utils.hdfs.run")
def test_get_most_recent_file(mock_run):
    # Simulated output of `hdfs dfs -ls` with two parquet files
    mock_run.return_value = Mock(
        stdout=(
            "Found 3 items\n"
            "-rw-r--r--   3 user group 1234 2025-05-12 15:00 /path/to/first.parquet\n"
            "-rw-r--r--   3 user group 4567 2025-05-13 16:00 /path/to/second.parquet\n"
            "-rw-r--r--   3 user group 5567 2025-05-13 17:00 /path/to/third 123.parquet\n"
        )
    )

    result = get_most_recent_file("/some/hdfs/path")
    assert result == {'path': '/path/to/third 123.parquet', 'size': '5567'}

    mock_run.return_value = Mock(
        stdout=(
            "Found 3 items\n"
            "-rw-r--r--   3 user group 1234 2025-05-12 15:00 /path/to/first.parquet\n"
            "drw-r--r--   3 user group 4567 2025-05-13 18:00 /path/to/second.parquet\n"
            "-rw-r--r--   3 user group 5567 2025-05-13 17:00 /path/to/third 123.parquet\n"
        )
    )

    result = get_most_recent_file("/some/hdfs/path")
    assert result == {'path': '/path/to/third 123.parquet', 'size': '5567'}

    mock_run.return_value = Mock(
        stdout=(
            "Found 1 items\n"
            "drw-r--r--   3 user group 4567 2025-05-13 18:00 /path/to/second.parquet\n"
        )
    )

    result = get_most_recent_file("/some/hdfs/path")
    assert result is None
