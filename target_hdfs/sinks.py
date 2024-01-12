"""hdfs target sink class, which handles writing streams."""

from __future__ import annotations

import os.path
from pathlib import Path

from target_parquet.sinks import ParquetSink

from target_hdfs.utils.hdfs import (
    delete_old_files,
    read_most_recent_file,
    upload_to_hdfs,
)
from target_hdfs.utils.parquet import get_parquet_files


class HDFSSink(ParquetSink):
    """hdfs target sink class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hdfs_destination_path = os.path.join(
            self.config["hdfs_destination_path"], self.stream_name
        )
        self.pyarrow_df = read_most_recent_file(
            self.hdfs_destination_path, self.pyarrow_schema
        )
        self.append_data_to_existing_file = self.pyarrow_df is not None

    def upload_files(self) -> None:
        """Upload a local file to HDFS."""
        local_parquet_files = get_parquet_files(self.destination_path)
        self.logger.debug(f"Uploading {local_parquet_files} to HDFS")
        for file in local_parquet_files:
            new_hdfs_file_path = os.path.join(
                self.hdfs_destination_path,
                os.path.relpath(file, self.destination_path) + "_new",
            )
            upload_to_hdfs(file, new_hdfs_file_path)
            Path(file).unlink()

    def write_file(self) -> None:
        """Write a local file and upload to hdfs."""
        super().write_file()
        self.upload_files()

    def clean_up(self) -> None:
        """Cleanup."""
        super().clean_up()
        if self.append_data_to_existing_file:
            delete_old_files(self.hdfs_destination_path)
