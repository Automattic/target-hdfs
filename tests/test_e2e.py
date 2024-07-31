import json
from io import StringIO
from unittest.mock import patch, call

import pytest
from singer_sdk import typing as th
from singer_sdk.testing import target_sync_test

from target_hdfs.target import TargetHDFS


@pytest.fixture(scope="session")
def sample_config():
    return {
        "hdfs_destination_path": "/tmp/meltano_test",
    }


@patch("target_hdfs.sinks.upload_to_hdfs", return_value=None)
def test_upload(mock_upload_to_hdfs, sample_config):
    """Test if the target uploads the expected file to HDFS."""
    stream_name = f"test_schema"
    schema_message = {
        "type": "SCHEMA",
        "stream": stream_name,
        "schema": {
            "type": "object",
            "properties": {
                "col_a": th.StringType().to_dict(),
                "col_b": th.StringType().to_dict(),
                "col_c": th.StringType().to_dict(),
                "col_d": th.StringType().to_dict(),
                "col_e": th.StringType().to_dict(),
                "col_f": th.StringType().to_dict(),
                "col_g": th.StringType().to_dict(),
                "col_h": th.StringType().to_dict(),
            },
        },
    }
    tap_output = "\n".join(
        json.dumps(msg)
        for msg in [schema_message]
        + [
            {
                "type": "RECORD",
                "stream": stream_name,
                "record": {
                    "col_a": "samplerow1",
                    "col_b": "samplerow1",
                    "col_c": "samplerow1",
                    "col_d": "samplerow1",
                    "col_e": "samplerow1",
                    "col_f": "samplerow1",
                    "col_g": "samplerow1",
                    "col_h": "samplerow1",
                },
            }
        ]
        * 1000000
    )

    target_sync_test(
        TargetHDFS(config=sample_config | {"max_pyarrow_table_size": 10}),
        input=StringIO(tap_output),
        finalize=True,
    )

    # 10 files are created but we should have 100 uploads
    expected_calls = [
        call(
            f"output/test_schema/test_schema-20231114_221320-{i}-0.gz.parquet",
            f"/tmp/meltano_test/test_schema/test_schema-20231114_221320-{i}-0.gz.parquet",
        )
        for i in range(10)
        for _ in range(10)
    ]

    mock_upload_to_hdfs.assert_has_calls(expected_calls, any_order=True)
