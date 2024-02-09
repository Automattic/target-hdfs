"""hdfs target class."""

from __future__ import annotations

from singer_sdk import typing as th
from target_parquet.target import TargetParquet

from target_hdfs.sinks import (
    HDFSSink,
)


class TargetHDFS(TargetParquet):
    """Sample target for hdfs."""

    name = "target-hdfs"

    @property
    def config_json_schema(self) -> th.Schema:
        """Returns target config json schema."""
        config = super().config_jsonschema
        del config["destination_path"]
        return (
            config
            | th.PropertiesList(
                th.Property(
                    "hdfs_destination_path",
                    th.StringType,
                    description="HDFS Destination Path",
                    required=True,
                ),
                th.Property(
                    "hdfs_block_size_limit",
                    th.StringType,
                    description="HDFS Block Size Limit (e.g. 200M) "
                    "(default: it will use 85% of the current block size). "
                    "If the size is lower than this limit, the data will be appended to the existing file",
                ),
                th.Property(
                    "skip_existing_files",
                    th.BooleanType,
                    description="If set to true, the data will not be appended to the existing file",
                    default=False,
                ),
            ).to_dict()
        )

    default_sink_class = HDFSSink


if __name__ == "__main__":
    TargetHDFS.cli()
