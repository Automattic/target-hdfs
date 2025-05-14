"""Test Configuration."""

import pytest

from target_hdfs.utils.hdfs import FileSize


@pytest.fixture(autouse=True)
def mock_get_most_recent_file(monkeypatch):
    def mock_get_most_recent_file(hdfs_file_path):
        return FileSize(path="/mock/path", size=2048)  # Example mock file

    monkeypatch.setattr(
        "target_hdfs.utils.hdfs.get_most_recent_file", mock_get_most_recent_file
    )


@pytest.fixture(autouse=True)
def mock_download_from_hdfs(monkeypatch):
    monkeypatch.setattr("target_hdfs.utils.hdfs.download_from_hdfs", lambda *args: None)


@pytest.fixture(autouse=True)
def mock_get_hdfs_block_size(monkeypatch):
    def mock_get_hdfs_block_size():
        return 10240

    monkeypatch.setattr(
        "target_hdfs.utils.hdfs.get_hdfs_block_size", mock_get_hdfs_block_size
    )


@pytest.fixture(autouse=True)
def mock_hdfs_requests(monkeypatch):
    monkeypatch.setattr("target_hdfs.sinks.read_most_recent_file", lambda *args: None)


@pytest.fixture(autouse=True)
def mock_time(monkeypatch):
    monkeypatch.setattr("time.time", lambda: 1700000000)


@pytest.fixture(autouse=True)
def mock_create_hdfs_directory(monkeypatch):
    def mock_create_hdfs_directory(hdfs_destination_path):
        pass

    monkeypatch.setattr(
        "target_hdfs.utils.hdfs.create_hdfs_directory", mock_create_hdfs_directory
    )
