"""hdfs target sink class, which handles writing streams."""

from __future__ import annotations

import os.path
from target_parquet.sinks import ParquetSink

from target_hdfs.utils.hdfs import upload_to_hdfs, read_most_recent_file
from utils.parquet import get_parquet_files


class HDFSSink(ParquetSink):
    """hdfs target sink class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hdfs_destination_path = self.config["hdfs_destination_path"]
        self.pyarrow_df = read_most_recent_file(self.hdfs_destination_path)

    def upload_files(self):
        """Upload a local file to HDFS"""
        local_parquet_files = get_parquet_files(self.destination_path)
        for file in local_parquet_files:
            new_hdfs_file_path = os.path.join(
                self.hdfs_destination_path,
                os.path.relpath(file, self.destination_path),
                '_new',
            )
            upload_to_hdfs(file, new_hdfs_file_path)
            os.remove(file)

    def write_file(self):
        """Write a local file and upload to hdfs"""
        super().write_file()
        # if self.pyarrow_df is not None:
        #     self.upload_files()

    def cleanup(self):
        """Cleanup"""
        super().cleanup()
        # self.delete_old_files(self.hdfs_destination_path)
