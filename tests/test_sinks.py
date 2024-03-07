import unittest
from unittest.mock import patch, MagicMock

from target_hdfs.sinks import HDFSSink
from target_hdfs.target import TargetHDFS


@patch('target_hdfs.sinks.len', return_value=1)
@patch(
    'target_hdfs.sinks.get_parquet_files',
    return_value=[
        'output/test_stream/file1.parquet',
        'output/test_stream/file2.parquet',
    ],
)
@patch('target_hdfs.sinks.read_most_recent_file', return_value=None)
@patch('target_hdfs.sinks.Path.unlink', return_value=None)
@patch('target_hdfs.sinks.upload_to_hdfs', return_value=None)
def test_upload_files(
    mock_upload_to_hdfs: MagicMock,
    mock_unlink,
    mock_read_most_recent_file,
    mock_get_parquet_files,
    mock_len,
):
    sink = HDFSSink(
        target=TargetHDFS(
            config={'hdfs_destination_path': '/hdfs/path', 'skip_existing_files': False}
        ),
        stream_name="test_stream",
        schema={'properties': {}},
        key_properties=[],
    )
    sink.hdfs_file_path = '/hdfs/path/test_stream/file.parquet'
    sink.upload_files()
    mock_upload_to_hdfs.assert_any_call(
        'output/test_stream/file1.parquet', '/hdfs/path/test_stream/file.parquet'
    )
    mock_upload_to_hdfs.assert_any_call(
        'output/test_stream/file2.parquet', '/hdfs/path/test_stream/file2.parquet'
    )
