"""hdfs target sink class, which handles writing streams."""

from __future__ import annotations

import os.path
from datetime import datetime
from tempfile import TemporaryDirectory

from singer_sdk.helpers._flattening import flatten_record, flatten_schema
from singer_sdk.sinks import BatchSink

from target_hdfs.utils.parquet import (
    concat_tables,
    flatten_schema_to_pyarrow_schema,
    get_pyarrow_table_size,
    get_single_parquet_file,
    write_parquet_file,
)
from target_hdfs.utils.hdfs import upload_to_hdfs


class hdfsSink(BatchSink):
    """hdfs target sink class."""

    flatten_max_level = 20  # Max level of nesting to flatten

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pyarrow_df = None
        self.temp_output_dir = TemporaryDirectory(self.stream_name)
        self.temp_output_path = (
            f"output/{self.stream_name}"  # self.temp_output_dir.name
        )
        timestamp = datetime.utcfromtimestamp(self.sync_started_at / 1000).strftime(
            "%Y%m%d_%H%M%S"
        )
        self.basename_template = f"{self.stream_name}-{timestamp}-{{i}}"
        self.hdfs_path = os.path.join(
            self.config["hdfs_destination_path"], self.basename_template
        )

        # Extra fields
        self.extra_values = (
            dict([kv.split("=") for kv in self.config["extra_fields"].split(",")])
            if self.config.get("extra_fields")
            else {}
        )
        self.extra_values_types = {}
        if self.config.get("extra_fields_types"):
            for field_type in self.config["extra_fields_types"].split(","):
                field_name, field_type = field_type.split("=")
                self.extra_values_types[field_name] = {"type": [field_type]}

        # Create pyarrow schema
        self.flatten_schema = flatten_schema(
            self.schema, max_level=self.flatten_max_level
        )
        self.flatten_schema.get("properties", {}).update(self.extra_values_types)
        self.pyarrow_schema = flatten_schema_to_pyarrow_schema(self.flatten_schema)

        self.partition_cols = (
            self.config["partition_cols"].split(",")
            if self.config.get("partition_cols")
            else None
        )

        self.validation()

    def validation(self):
        # Extra fields validation
        assert bool(self.extra_values) == bool(
            self.extra_values_types
        ), "extra_fields and extra_fields_types must be both set or both unset"
        if self.extra_values:
            assert (
                self.extra_values.keys() == self.extra_values_types.keys()
            ), "extra_fields and extra_fields_types must have the same keys"
        if self.partition_cols:
            self.logger.info(self.partition_cols)
            self.logger.info(self.flatten_schema.keys())
            assert set(self.partition_cols).issubset(
                set(self.pyarrow_schema.names)
            ), "partition_cols must be in the schema"

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("max_batch_size", 10000)

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        record_flatten = (
            flatten_record(
                record,
                flattened_schema=self.flatten_schema,
                max_level=self.flatten_max_level,
            )
            | self.extra_values
        )
        super().process_record(record_flatten, context)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.logger.info(
            f'Processing batch for {self.stream_name} with {len(context["records"])} records.'
        )
        self.pyarrow_df = concat_tables(
            context.get("records", []), self.pyarrow_df, self.pyarrow_schema
        )
        del context["records"]
        if (
            get_pyarrow_table_size(self.pyarrow_df)
            > self.config["max_pyarrow_table_size"]
        ):
            self._write_and_upload_file()

    def _upload_file(self):
        """Upload a local file to HDFS"""
        parquet_file = get_single_parquet_file(self.temp_output_path)
        upload_to_hdfs(
            parquet_file,
            os.path.join(
                self.hdfs_path, os.path.relpath(parquet_file, self.temp_output_path)
            ),
        )
        os.remove(parquet_file)

    def _write_and_upload_file(self):
        """Write a local file"""
        if self.pyarrow_df is not None:
            write_parquet_file(
                self.pyarrow_df,
                self.temp_output_path,
                compression_method=self.config.get("compression", "gzip"),
                basename_template=self.basename_template,
                partition_cols=self.partition_cols,
            )
            self.pyarrow_df = None
            # self._upload_file()

    def clean_up(self) -> None:
        """Perform any clean up actions required at end of a stream"""
        self._write_and_upload_file()
        super().clean_up()
