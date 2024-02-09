"""hdfs target sink class, which handles writing streams."""

from __future__ import annotations

import os.path
from pathlib import Path

from target_parquet.sinks import ParquetSink

from target_hdfs.utils.hdfs import (
    read_most_recent_file,
    upload_to_hdfs,
)
from target_hdfs.utils.parquet import get_parquet_files


class CanNotUploadFileError(Exception):
    """Can not upload file error."""


class HDFSSink(ParquetSink):
    """hdfs target sink class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hdfs_destination_path = os.path.join(
            self.config["hdfs_destination_path"], self.stream_name
        )
        self.skip_existing_files = self.config["skip_existing_files"]
        # Don't read the most recent file if partition_cols is set or if skip_existing_files is set
        hdfs_file = (
            read_most_recent_file(
                self.hdfs_destination_path,
                self.pyarrow_schema,
                self.config["hdfs_relative_block_size_limit"],
            )
            if not self.config.get("partition_cols") and not self.skip_existing_files
            else {}
        )
        self.hdfs_file_path = hdfs_file.get("path")
        self.pyarrow_df = hdfs_file.get("content")

    def upload_files(self) -> None:
        """Upload a local file to HDFS."""
        local_parquet_files = get_parquet_files(self.destination_path)

        if len(local_parquet_files) > 1 and self.hdfs_file_path:
            raise CanNotUploadFileError(
                "Multiple files were found in the local path, "
                "but only one HDFS file was loaded."
            )

        self.logger.debug(f"Uploading {local_parquet_files} to HDFS")
        for file in local_parquet_files:
            new_hdfs_file_path = (
                self.hdfs_file_path
                or os.path.join(
                    self.hdfs_destination_path,
                    os.path.relpath(file, self.destination_path),
                )
            ) + "_new"
            upload_to_hdfs(file, new_hdfs_file_path)
            Path(file).unlink()

        # Reset hdfs_file_path to None after uploading (no file to append)
        self.hdfs_file_path = None

    def write_file(self) -> None:
        """Write a local file and upload to hdfs."""
        super().write_file()
        self.upload_files()
