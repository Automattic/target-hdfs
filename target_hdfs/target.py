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
                )
            ).to_dict()
        )

    default_sink_class = HDFSSink


if __name__ == "__main__":
    TargetHDFS.cli()
