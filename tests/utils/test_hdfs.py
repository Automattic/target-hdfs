import pytest
from pyarrow._fs import FileInfo
import pyarrow as pa

from target_hdfs.utils.hdfs import read_most_recent_file, SchemaChangedError


@pytest.fixture(autouse=True)
def mock_get_most_recent_file(monkeypatch):
    def mock_get_most_recent_file(hdfs_file_path):
        # Mock implementation of get_most_recent_file
        return FileInfo(path="/mock/path", size=100)  # Example mock file

    monkeypatch.setattr(
        "target_hdfs.utils.hdfs.get_most_recent_file", mock_get_most_recent_file
    )


@pytest.fixture(autouse=True)
def mock_download_from_hdfs(monkeypatch):
    monkeypatch.setattr("target_hdfs.utils.hdfs.download_from_hdfs", lambda *args: None)


@pytest.fixture(autouse=True)
def mock_get_hdfs_block_size(monkeypatch):
    def mock_get_hdfs_block_size():
        return 1024

    monkeypatch.setattr(
        "target_hdfs.utils.hdfs.get_hdfs_block_size", mock_get_hdfs_block_size
    )


@pytest.fixture(autouse=True)
def mock_pa_parquet_read_table(monkeypatch):
    def mock_read_table(file):
        return pa.Table.from_pydict(
            {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c'], 'col3': [True, False, True]}
        )

    monkeypatch.setattr("pyarrow.parquet.read_table", mock_read_table)


SCHEMA = pa.schema([("col1", pa.int64()), ("col2", pa.string()), ("col3", pa.bool_())])


# Test cases
def test_read_most_recent_file_successful():
    hdfs_file_path = "/some/hdfs/path"
    hdfs_relative_block_size_limit = 0.85  # Example block size limit

    result = read_most_recent_file(
        hdfs_file_path, SCHEMA, hdfs_relative_block_size_limit
    )
    assert result is not None
    assert "content" in result
    assert "path" in result


def test_read_most_recent_file_file_too_large():
    # Test when the most recent file is larger than the block size limit
    hdfs_file_path = "/some/hdfs/path"
    hdfs_relative_block_size_limit = 0.01  # Example block size limit

    result = read_most_recent_file(
        hdfs_file_path, SCHEMA, hdfs_relative_block_size_limit
    )
    assert result is None


def test_read_most_recent_file_schema_correct():
    # Test when the schema of the file doesn't match the expected schema
    hdfs_file_path = "/some/hdfs/path"
    hdfs_relative_block_size_limit = 0.85  # Example block size limit
    read_most_recent_file(hdfs_file_path, SCHEMA, hdfs_relative_block_size_limit)


def test_read_most_recent_file_schema_mismatch():
    # Test when the schema of the file doesn't match the expected schema
    hdfs_file_path = "/some/hdfs/path"
    pyarrow_schema = pa.schema([("col1", pa.int64()), ("col2", pa.string())])
    hdfs_relative_block_size_limit = 0.85  # Example block size limit

    with pytest.raises(SchemaChangedError):
        read_most_recent_file(
            hdfs_file_path, pyarrow_schema, hdfs_relative_block_size_limit
        )
