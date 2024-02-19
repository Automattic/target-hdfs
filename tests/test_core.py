"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from singer_sdk.testing import get_target_test_class

from target_hdfs.target import TargetHDFS

SAMPLE_CONFIG: dict[str, t.Any] = {
    "hdfs_destination_path": "/tmp/meltano_test",
    "skip_existing_files": False,
    "hdfs_block_size_limit": 800,
}


@pytest.fixture(autouse=True)
def mock_hdfs_requests(monkeypatch):
    monkeypatch.setattr("target_hdfs.sinks.read_most_recent_file", lambda *args: None)
    monkeypatch.setattr("target_hdfs.sinks.upload_to_hdfs", lambda *args: None)


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetHDFS,
    config=SAMPLE_CONFIG,
)


class TestTargetHDFS(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        return "resource"


# TODO: Create additional tests as appropriate for your target.
