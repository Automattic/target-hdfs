"""hdfs target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_hdfs.sinks import (
    hdfsSink,
)


class Targethdfs(Target):
    """Sample target for hdfs."""

    name = "target-hdfs"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "hdfs_destination_path",
            th.StringType,
            description="HDFS Destination Path",
            required=True,
        ),
        th.Property(
            "compression_method",
            th.StringType,
            description="(Default - gzip) Compression methods have to be supported by Pyarrow, and currently the compression modes available are - snappy, zstd, brotli and gzip.",
            default="gzip",
        ),
        th.Property(
            "max_pyarrow_table_size",
            th.IntegerType,
            description="Max size of pyarrow table in MB (before writing to parquet file). It can control the memory usage of the target.",
            default=800,
        ),
        th.Property(
            "max_batch_size",
            th.IntegerType,
            description="Max records to write in one batch. It can control the memory usage of the target.",
            default=10000,
        ),
        th.Property(
            "extra_fields",
            th.StringType,
            description="Extra fields to add to the flattened record. (e.g. extra_col1=value1,extra_col2=value2)",
        ),
        th.Property(
            "extra_fields_types",
            th.StringType,
            description="Extra fields types. (e.g. extra_col1=string,extra_col2=integer)",
        ),
        th.Property(
            "partition_cols",
            th.StringType,
            description="Extra fields to add to the flattened record. (e.g. extra_col1,extra_col2)",
        ),
    ).to_dict()

    default_sink_class = hdfsSink


if __name__ == "__main__":
    Targethdfs.cli()
